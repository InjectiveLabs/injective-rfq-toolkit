"""Probe whether RFQ market makers are quoting a market.

This sends a TakerStream RFQ request, waits for the indexer ACK, then collects
quotes for the assigned RFQ ID. It does not require wallet funds unless you use
--accept, and even then settlement failure is reported after quote detection.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import dotenv
import httpx

from rfq_test.clients.contract import ContractClient
from rfq_test.clients.websocket import TakerStreamClient
from rfq_test.config import get_environment_config, get_settings
from rfq_test.crypto.wallet import Wallet
from rfq_test.models.config import EnvironmentConfig, MarketConfig
from rfq_test.models.types import Direction
from rfq_test.utils.price import (
    PriceFetcher,
    quantize_for_fpdecimal,
    quantize_quantity,
    quantize_to_tick,
)

logger = logging.getLogger("rfq_quote_probe")


def now_ms() -> int:
    return int(time.time() * 1000)


def elapsed_ms(start: float) -> int:
    return int((time.monotonic() - start) * 1000)


@dataclass(frozen=True)
class TakerIdentity:
    address: str
    private_key: str | None
    generated: bool


@dataclass(frozen=True)
class ProbeParams:
    quantity: str
    margin: str
    worst_price: str
    mark_price: Decimal | None
    price_tick: Decimal | None
    quantity_tick: Decimal | None


def parse_args() -> argparse.Namespace:
    dotenv.load_dotenv()
    parser = argparse.ArgumentParser(
        description="Submit an RFQ request and report whether maker quotes are returned.",
    )
    parser.add_argument(
        "--env",
        default=os.getenv("RFQ_ENV", "testnet"),
        help="RFQ environment config to use: testnet, mainnet, or local.",
    )
    market = parser.add_mutually_exclusive_group()
    market.add_argument("--market-symbol", help="Configured market symbol, e.g. 'INJ/USDC PERP'.")
    market.add_argument("--market-id", help="Derivative market ID to probe, including new markets.")
    parser.add_argument(
        "--symbol",
        help="Display symbol when --market-id is not in configs and chain metadata has no ticker.",
    )
    parser.add_argument("--taker-address", help="Injective taker address for the request stream.")
    parser.add_argument(
        "--taker-private-key",
        default=os.getenv("RFQ_TAKER_PRIVATE_KEY"),
        help="Taker private key. Defaults to RFQ_TAKER_PRIVATE_KEY or the active env retail key.",
    )
    parser.add_argument("--direction", choices=["long", "short"], default="long")
    parser.add_argument("--quantity", help="Base quantity. Defaults to the market minimum.")
    parser.add_argument("--margin", help="USDC margin. Defaults to mark * quantity / leverage.")
    parser.add_argument("--leverage", default="5", help="Used only when --margin is omitted.")
    parser.add_argument(
        "--worst-price",
        help="Worst acceptable price. Defaults from mark +/- slippage.",
    )
    parser.add_argument("--mark-price", help="Override mark price used for defaults.")
    parser.add_argument(
        "--slippage-bps",
        default="500",
        help="Worst-price distance from mark when --worst-price is omitted.",
    )
    parser.add_argument("--quote-timeout", type=float, default=8.0)
    parser.add_argument(
        "--quiet-period",
        type=float,
        default=1.0,
        help="After the first quote, stop once no more quotes arrive for this many seconds.",
    )
    parser.add_argument(
        "--wait-full-window",
        action="store_true",
        help="Wait for the full quote timeout even after quotes arrive.",
    )
    parser.add_argument("--min-quotes", type=int, default=1)
    parser.add_argument("--request-timeout", type=float, default=10.0)
    parser.add_argument("--rfq-expiry-seconds", type=float, default=300.0)
    parser.add_argument(
        "--accept",
        action="store_true",
        help=(
            "After receiving quotes, submit AcceptQuote. "
            "Failure does not fail the probe by default."
        ),
    )
    parser.add_argument(
        "--strict-settlement",
        action="store_true",
        help="With --accept, return non-zero if AcceptQuote fails.",
    )
    parser.add_argument(
        "--unfilled-action",
        choices=["none", "market"],
        default="none",
        help="AcceptQuote fallback for unfilled quantity.",
    )
    parser.add_argument("--json", action="store_true", help="Print only a JSON summary.")
    parser.add_argument("--log-level", default="WARNING")
    return parser.parse_args()


def emit(args: argparse.Namespace, message: str = "") -> None:
    if not args.json:
        print(message)


def decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return Decimal(str(value))


def find_configured_market(
    config: EnvironmentConfig,
    args: argparse.Namespace,
) -> MarketConfig | None:
    if args.market_symbol:
        return config.get_market(args.market_symbol)
    if args.market_id:
        try:
            return config.get_market_by_id(args.market_id)
        except ValueError:
            return None
    return config.default_market


def split_ticker(ticker: str | None) -> tuple[str, str]:
    if not ticker:
        return ("UNKNOWN", "UNKNOWN")
    first = ticker.split()[0]
    if "/" in first:
        base, quote = first.split("/", 1)
        return (base or "UNKNOWN", quote or "UNKNOWN")
    if "-" in first:
        base, quote = first.split("-", 1)
        return (base or "UNKNOWN", quote or "UNKNOWN")
    return (first, "UNKNOWN")


def pick_market_payload(data: dict[str, Any]) -> dict[str, Any]:
    market = data.get("market") or {}
    if isinstance(market, dict):
        nested = market.get("market")
        if isinstance(nested, dict):
            merged = dict(market)
            merged.update(nested)
            return merged
        return market
    return {}


async def fetch_market_by_id(
    config: EnvironmentConfig,
    market_id: str,
    symbol_override: str | None,
) -> MarketConfig:
    url = (
        f"{config.chain.lcd_endpoint.rstrip('/')}"
        f"/injective/exchange/v2/derivative/markets/{market_id}"
    )
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.get(url)
    if response.status_code != 200:
        raise RuntimeError(
            f"Could not load market {market_id} from chain LCD "
            f"({response.status_code}): {response.text[:300]}"
        )

    data = response.json()
    payload = pick_market_payload(data)
    ticker = (
        symbol_override
        or payload.get("ticker")
        or payload.get("market_ticker")
        or payload.get("marketTicker")
        or f"{market_id[:12]}..."
    )
    base, quote = split_ticker(ticker)
    mark_price = (
        data.get("mark_price")
        or data.get("markPrice")
        or payload.get("mark_price")
        or payload.get("markPrice")
        or payload.get("oracle_price")
        or payload.get("oraclePrice")
    )

    return MarketConfig(
        id=market_id,
        symbol=ticker,
        base=payload.get("oracle_base") or payload.get("oracleBase") or base,
        quote=payload.get("oracle_quote") or payload.get("oracleQuote") or quote,
        quote_denom=payload.get("quote_denom") or payload.get("quoteDenom"),
        price=decimal_or_none(mark_price),
        price_source="static" if mark_price not in (None, "") else "oracle",
        min_quantity=decimal_or_none(payload.get("min_quantity_tick_size")) or Decimal("1"),
        min_price_tick=decimal_or_none(payload.get("min_price_tick_size")),
        min_quantity_tick=decimal_or_none(payload.get("min_quantity_tick_size")),
        min_notional=decimal_or_none(payload.get("min_notional")),
    )


def manual_market_from_args(args: argparse.Namespace) -> MarketConfig:
    mark_price = decimal_or_none(args.mark_price)
    min_quantity = Decimal(str(args.quantity)) if args.quantity else Decimal("1")
    return MarketConfig(
        id=args.market_id,
        symbol=args.symbol or f"{args.market_id[:12]}...",
        base="UNKNOWN",
        quote="UNKNOWN",
        price=mark_price,
        price_source="static" if mark_price is not None else "oracle",
        min_quantity=min_quantity,
    )


async def resolve_market(config: EnvironmentConfig, args: argparse.Namespace) -> MarketConfig:
    configured = find_configured_market(config, args)
    if configured:
        return configured
    if not args.market_id:
        raise SystemExit("No market configured. Pass --market-symbol or --market-id.")
    try:
        return await fetch_market_by_id(config, args.market_id, args.symbol)
    except Exception as exc:
        if args.quantity and args.margin and args.worst_price:
            logger.warning(
                "Could not fetch market metadata; using manual request parameters: %s",
                exc,
            )
            return manual_market_from_args(args)
        raise SystemExit(
            "Could not fetch market metadata. Pass --quantity, --margin, and "
            f"--worst-price to probe manually. Error: {exc}"
        ) from exc


def resolve_taker(args: argparse.Namespace, settings) -> TakerIdentity:
    private_key = args.taker_private_key or settings.retail_private_key
    generated = False

    if args.taker_address:
        if private_key:
            derived = Wallet.from_private_key(private_key).inj_address
            if derived != args.taker_address:
                if args.accept:
                    raise SystemExit(
                        "--taker-address must match --taker-private-key when --accept is used."
                    )
                logger.warning(
                    "Taker address %s does not match supplied private key %s; using address only.",
                    args.taker_address,
                    derived,
                )
                private_key = None
        return TakerIdentity(args.taker_address, private_key, generated)

    if private_key:
        wallet = Wallet.from_private_key(private_key)
        return TakerIdentity(wallet.inj_address, wallet.private_key, generated)

    wallet = Wallet.generate()
    generated = True
    return TakerIdentity(wallet.inj_address, wallet.private_key, generated)


async def build_probe_params(
    config: EnvironmentConfig,
    market: MarketConfig,
    args: argparse.Namespace,
) -> ProbeParams:
    price_fetcher = PriceFetcher(config)
    mark_price: Decimal | None = decimal_or_none(args.mark_price)
    needs_mark_price = not args.worst_price or not args.margin
    if mark_price is None and needs_mark_price:
        try:
            mark_price = await price_fetcher.get_price(market)
        except Exception as exc:
            if not args.worst_price or not args.margin:
                raise SystemExit(
                    "Could not fetch mark price. Pass --mark-price, or pass both "
                    f"--worst-price and --margin. Error: {exc}"
                ) from exc

    price_tick = price_fetcher.get_price_tick(market)
    quantity_tick = price_fetcher.get_qty_tick(market)

    raw_quantity = Decimal(str(args.quantity)) if args.quantity else market.min_quantity
    quantity = quantize_quantity(raw_quantity, quantity_tick)
    if Decimal(quantity) <= 0:
        raise SystemExit("Quantity rounded to zero. Pass a larger --quantity.")

    if args.margin:
        margin = quantize_for_fpdecimal(args.margin)
    elif mark_price is not None:
        leverage = Decimal(str(args.leverage))
        if leverage <= 0:
            raise SystemExit("--leverage must be greater than zero.")
        margin = quantize_for_fpdecimal((mark_price * Decimal(quantity)) / leverage)
    else:
        margin = "10"

    if args.worst_price:
        worst_price = quantize_to_tick(args.worst_price, price_tick)
    else:
        if mark_price is None:
            raise SystemExit("Pass --worst-price when mark price is unavailable.")
        slippage = Decimal(str(args.slippage_bps)) / Decimal("10000")
        if args.direction == "long":
            raw_worst = mark_price * (Decimal("1") + slippage)
            rounding = ROUND_CEILING
        else:
            raw_worst = mark_price * (Decimal("1") - slippage)
            rounding = ROUND_FLOOR
        worst_price = quantize_to_tick(raw_worst, price_tick, rounding=rounding)

    return ProbeParams(
        quantity=quantity,
        margin=margin,
        worst_price=worst_price,
        mark_price=mark_price,
        price_tick=price_tick,
        quantity_tick=quantity_tick,
    )


async def collect_quotes_for_window(
    client: TakerStreamClient,
    rfq_id: int,
    timeout: float,
    quiet_period: float,
    wait_full_window: bool,
) -> list[dict[str, Any]]:
    quotes: list[dict[str, Any]] = []
    start = time.monotonic()
    last_quote_at: float | None = None

    while (time.monotonic() - start) < timeout:
        remaining = timeout - (time.monotonic() - start)
        event = await client.get_next_event(timeout=min(max(remaining, 0.0), 0.5))
        now = time.monotonic()

        if event is None:
            if quotes and not wait_full_window and last_quote_at is not None:
                if now - last_quote_at >= quiet_period:
                    break
            continue

        event_type, data = event
        if event_type == "quote" and int(data.rfq_id) == rfq_id:
            quotes.append(client._quote_to_dict(data))
            last_quote_at = now
            continue

        if event_type == "error":
            message = getattr(data, "message_", None) or getattr(data, "message", "")
            code = getattr(data, "code", "")
            raise RuntimeError(f"TakerStream error while collecting quotes: {code}: {message}")

    return quotes


def sorted_quotes(quotes: list[dict[str, Any]], direction: str) -> list[dict[str, Any]]:
    reverse = direction == "short"
    return sorted(quotes, key=lambda quote: Decimal(str(quote["price"])), reverse=reverse)


def contract_quote(quote: dict[str, Any], fallback_evm_chain_id: int) -> dict[str, Any]:
    normalized = {
        "maker": quote["maker"],
        "margin": quote["margin"],
        "quantity": quote["quantity"],
        "price": quote["price"],
        "expiry": int(quote["expiry"]),
        "signature": quote["signature"],
        "sign_mode": quote.get("sign_mode") or "v2",
        "evm_chain_id": int(quote.get("evm_chain_id") or fallback_evm_chain_id),
        "maker_subaccount_nonce": int(quote.get("maker_subaccount_nonce") or 0),
    }
    if quote.get("min_fill_quantity") not in (None, ""):
        normalized["min_fill_quantity"] = str(quote["min_fill_quantity"])
    return normalized


def parse_rfc3339_ms(value: str) -> int | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


async def fetch_tx_block_timing(config: EnvironmentConfig, tx_hash: str) -> dict[str, Any]:
    lcd = config.chain.lcd_endpoint.rstrip("/")
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            tx_response = await client.get(f"{lcd}/cosmos/tx/v1beta1/txs/{tx_hash}")
            tx_response.raise_for_status()
            tx_data = tx_response.json().get("tx_response", {})
            height = str(tx_data.get("height") or "")
            if not height:
                return {}

            block_response = await client.get(
                f"{lcd}/cosmos/base/tendermint/v1beta1/blocks/{height}"
            )
            block_response.raise_for_status()
            block_time = (
                block_response.json()
                .get("block", {})
                .get("header", {})
                .get("time", "")
            )
    except Exception as exc:
        logger.warning("Could not fetch tx block timing for %s: %s", tx_hash, exc)
        return {}

    block_time_ms = parse_rfc3339_ms(block_time)
    return {
        "height": height,
        "block_time": block_time,
        "block_time_ms": block_time_ms,
    }


async def maybe_accept_quotes(
    args: argparse.Namespace,
    config: EnvironmentConfig,
    taker: TakerIdentity,
    market: MarketConfig,
    params: ProbeParams,
    rfq_id: int,
    quotes: list[dict[str, Any]],
) -> dict[str, Any]:
    if not args.accept:
        return {"attempted": False}
    if not taker.private_key:
        raise SystemExit("--accept requires a taker private key.")

    evm_chain_id, _ = config.signing_context_v2
    contract_quotes = [
        contract_quote(quote, evm_chain_id)
        for quote in sorted_quotes(quotes, args.direction)
    ]
    direction = Direction.LONG if args.direction == "long" else Direction.SHORT
    unfilled_action = {"market": {}} if args.unfilled_action == "market" else None
    cid = f"rfq-probe-{uuid.uuid4()}"

    try:
        accept_started = time.monotonic()
        tx_hash = await ContractClient(config.contract, config.chain).accept_quote(
            private_key=taker.private_key,
            quotes=contract_quotes,
            rfq_id=str(rfq_id),
            market_id=market.id,
            direction=direction,
            margin=Decimal(params.margin),
            quantity=Decimal(params.quantity),
            worst_price=Decimal(params.worst_price),
            unfilled_action=unfilled_action,
            cid=cid,
        )
        timing = {"accept_confirm_ms": elapsed_ms(accept_started)}
        timing.update(await fetch_tx_block_timing(config, tx_hash))
        block_time_ms = timing.get("block_time_ms")
        if block_time_ms:
            timing["quote_expiry_vs_block_ms"] = [
                {
                    "maker": quote.get("maker"),
                    "price": quote.get("price"),
                    "delta_ms": int(quote.get("expiry") or 0) - int(block_time_ms),
                }
                for quote in contract_quotes
            ]
        return {
            "attempted": True,
            "ok": True,
            "tx_hash": tx_hash,
            "cid": cid,
            "timing": timing,
        }
    except Exception as exc:
        if args.strict_settlement:
            raise
        return {"attempted": True, "ok": False, "error": str(exc), "cid": cid}


def quote_summary(quote: dict[str, Any], reference_ms: int | None = None) -> dict[str, Any]:
    expiry = quote.get("expiry")
    expiry_ms = int(expiry) if expiry not in (None, "") else 0
    ttl_ms = expiry_ms - reference_ms if reference_ms and expiry_ms else None
    return {
        "maker": quote.get("maker"),
        "price": quote.get("price"),
        "quantity": quote.get("quantity"),
        "margin": quote.get("margin"),
        "expiry": expiry,
        "ttl_ms_at_collection_end": ttl_ms,
        "sign_mode": quote.get("sign_mode"),
        "maker_subaccount_nonce": quote.get("maker_subaccount_nonce"),
    }


async def run_probe(args: argparse.Namespace) -> int:
    os.environ["RFQ_ENV"] = args.env
    dotenv.load_dotenv()
    settings = get_settings()
    config = get_environment_config()
    market = await resolve_market(config, args)
    taker = resolve_taker(args, settings)
    params = await build_probe_params(config, market, args)

    chain_id, contract_address = config.signing_context
    emit(args, "RFQ quote probe")
    emit(args, f"  env:       {config.environment}")
    emit(args, f"  chain:     {chain_id}")
    emit(args, f"  contract:  {contract_address}")
    emit(args, f"  taker:     {taker.address}" + (" (generated)" if taker.generated else ""))
    emit(args, f"  market:    {market.symbol}")
    emit(args, f"  market_id: {market.id}")
    emit(args, f"  direction: {args.direction}")
    emit(args, f"  quantity:  {params.quantity}")
    emit(args, f"  margin:    {params.margin}")
    emit(args, f"  worst:     {params.worst_price}")
    if params.mark_price is not None:
        emit(args, f"  mark:      {params.mark_price}")
    if params.price_tick is not None or params.quantity_tick is not None:
        emit(args, f"  ticks:     price={params.price_tick} quantity={params.quantity_tick}")
    emit(args)

    client = TakerStreamClient(
        config.indexer.ws_endpoint,
        request_address=taker.address,
        timeout=args.request_timeout,
        auth_private_key=taker.private_key,
        auth_contract_address=contract_address if taker.private_key else None,
    )

    summary: dict[str, Any] = {
        "ok": False,
        "env": config.environment,
        "taker": taker.address,
        "market": market.symbol,
        "market_id": market.id,
        "direction": args.direction,
        "quantity": params.quantity,
        "margin": params.margin,
        "worst_price": params.worst_price,
        "mark_price": str(params.mark_price) if params.mark_price is not None else None,
        "quotes": [],
        "settlement": {"attempted": False},
        "timing": {},
    }

    try:
        connect_started = time.monotonic()
        await client.connect()
        summary["timing"]["connect_ms"] = elapsed_ms(connect_started)
        if taker.private_key:
            auth_result = await client.wait_for_auth_result(timeout=args.request_timeout)
            summary["authentication"] = auth_result
            if not auth_result["authenticated"]:
                raise RuntimeError(f"Taker authentication failed: {auth_result}")
            emit(args, "TakerStream authentication succeeded")
        else:
            summary["authentication"] = {"authenticated": False, "code": "not_requested"}
            emit(args, "TakerStream authentication not requested (address-only probe)")
        client_id = str(uuid.uuid4())
        request_data = {
            "request_address": taker.address,
            "client_id": client_id,
            "market_id": market.id,
            "direction": args.direction,
            "margin": params.margin,
            "quantity": params.quantity,
            "worst_price": params.worst_price,
            "expiry": int(time.time() * 1000) + int(args.rfq_expiry_seconds * 1000),
        }
        emit(args, f"Sending RFQ request client_id={client_id}")
        request_started = time.monotonic()
        ack = await client.send_request(
            request_data,
            wait_for_response=True,
            response_timeout=args.request_timeout,
        )
        summary["timing"]["request_ack_ms"] = elapsed_ms(request_started)
        summary["request_ack"] = ack
        if not ack or ack.get("type") != "ack" or not ack.get("rfq_id"):
            emit(args, f"No RFQ ACK received: {ack}")
            if args.json:
                print(json.dumps(summary, sort_keys=True))
            return 1

        rfq_id = int(ack["rfq_id"])
        summary["rfq_id"] = rfq_id
        emit(
            args,
            f"Request ACK: RFQ#{rfq_id} status={ack.get('status')} "
            f"ack_ms={summary['timing']['request_ack_ms']}",
        )
        emit(args, f"Collecting quotes for up to {args.quote_timeout:g}s...")

        collect_started = time.monotonic()
        quotes = await collect_quotes_for_window(
            client,
            rfq_id=rfq_id,
            timeout=args.quote_timeout,
            quiet_period=args.quiet_period,
            wait_full_window=args.wait_full_window,
        )
        collection_end_ms = now_ms()
        summary["timing"]["quote_collect_ms"] = elapsed_ms(collect_started)
        quotes = sorted_quotes(quotes, args.direction)
        summary["quotes"] = [quote_summary(quote, collection_end_ms) for quote in quotes]
        summary["ok"] = len(quotes) >= args.min_quotes

        if quotes:
            emit(
                args,
                f"Received {len(quotes)} quote(s) "
                f"collect_ms={summary['timing']['quote_collect_ms']}.",
            )
            for index, quote in enumerate(quotes, start=1):
                quote_expiry = int(quote.get("expiry") or 0)
                ttl_ms = quote_expiry - collection_end_ms if quote_expiry else 0
                emit(
                    args,
                    "  "
                    f"{index}. maker={quote['maker']} "
                    f"price={quote['price']} "
                    f"qty={quote['quantity']} "
                    f"margin={quote['margin']} "
                    f"ttl_ms={ttl_ms} "
                    f"sign_mode={quote.get('sign_mode') or 'unknown'}",
                )
        else:
            emit(args, "Received 0 quotes.")

        if summary["ok"]:
            settlement = await maybe_accept_quotes(
                args,
                config,
                taker,
                market,
                params,
                rfq_id,
                quotes,
            )
            summary["settlement"] = settlement
            if settlement.get("attempted"):
                if settlement.get("ok"):
                    accept_ms = settlement.get("timing", {}).get("accept_confirm_ms")
                    emit(args, f"AcceptQuote succeeded: {settlement['tx_hash']} ({accept_ms}ms)")
                    for delta in settlement.get("timing", {}).get(
                        "quote_expiry_vs_block_ms",
                        [],
                    ):
                        emit(
                            args,
                            "  "
                            f"maker={delta['maker']} "
                            f"price={delta['price']} "
                            f"expiry_vs_block_ms={delta['delta_ms']}",
                        )
                else:
                    emit(args, f"AcceptQuote failed after quotes arrived: {settlement['error']}")
            emit(args, "RESULT: quoting")
            if args.json:
                print(json.dumps(summary, sort_keys=True))
            return 0

        emit(args, f"RESULT: not quoting (needed {args.min_quotes}, got {len(quotes)})")
        if args.json:
            print(json.dumps(summary, sort_keys=True))
        return 2
    finally:
        await client.close()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.WARNING),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    return asyncio.run(run_probe(args))


if __name__ == "__main__":
    raise SystemExit(main())
