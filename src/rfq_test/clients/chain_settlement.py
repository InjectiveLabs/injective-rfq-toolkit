"""CometBFT settlement event stream helpers."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from typing import Any, Optional
from urllib.parse import urlparse, urlunparse

import websockets

from rfq_test.proto.injective_rfq_rpc_pb2 import (
    RFQExpiryType,
    RFQSettlementLimitActionType,
    RFQSettlementMakerUpdate,
    RFQSettlementMarketActionType,
    RFQSettlementQuote,
    RFQSettlementUnfilledActionType,
)

logger = logging.getLogger(__name__)

EVENT_TYPE = "wasm-rfq-accept-quote"


def comet_ws_url(endpoint: str) -> str:
    """Return the CometBFT websocket URL for an HTTP or host endpoint."""
    parsed = urlparse(endpoint)
    if parsed.scheme in {"http", "https"}:
        scheme = "wss" if parsed.scheme == "https" else "ws"
        path = parsed.path.rstrip("/")
        if not path.endswith("/websocket"):
            path = f"{path}/websocket"
        return urlunparse(parsed._replace(scheme=scheme, path=path))
    if parsed.scheme in {"ws", "wss"}:
        path = parsed.path.rstrip("/")
        if not path.endswith("/websocket"):
            path = f"{path}/websocket"
        return urlunparse(parsed._replace(path=path))
    return f"ws://{endpoint.rstrip('/')}/websocket"


async def stream_maker_settlements(
    endpoint: str,
    contract_address: str,
    maker_address: str,
    out_queue: asyncio.Queue[RFQSettlementMakerUpdate],
    stop: Optional[asyncio.Event] = None,
) -> None:
    """Subscribe to chain settlement events for a maker."""
    query = f"tm.event='Tx' AND {EVENT_TYPE}._contract_address='{contract_address}'"
    ws_url = comet_ws_url(endpoint)
    logger.info("Subscribing to CometBFT settlements: endpoint=%s query=%s", ws_url, query)

    async with websockets.connect(ws_url) as ws:
        await ws.send(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "method": "subscribe",
                    "id": 1,
                    "params": {"query": query},
                }
            )
        )

        while stop is None or not stop.is_set():
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
            except TimeoutError:
                continue

            msg = json.loads(raw)
            events = msg.get("result", {}).get("events")
            if not isinstance(events, dict):
                continue

            settlement = settlement_from_events(events)
            if settlement is None or not maker_has_traded(settlement, maker_address):
                continue

            await out_queue.put(settlement_to_maker_update(settlement, events))


def first_event_value(events: Mapping[str, list[str]], key: str) -> str:
    values = events.get(key) or []
    return values[0] if values else ""


def prefixed_attrs(events: Mapping[str, list[str]]) -> dict[str, str]:
    prefix = f"{EVENT_TYPE}."
    return {
        key.removeprefix(prefix): values[0]
        for key, values in events.items()
        if key.startswith(prefix) and values
    }


def load_json_attr(attrs: Mapping[str, str], key: str, default: Any) -> Any:
    raw = attrs.get(key)
    if not raw:
        return default
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.warning("invalid JSON %s on settlement event: %s", key, exc)
        return default


def int_attr(attrs: Mapping[str, str], key: str) -> int:
    raw = attrs.get(key)
    if not raw:
        return 0
    try:
        return int(raw)
    except ValueError:
        logger.warning("invalid %s on settlement event: %r", key, raw)
        return 0


def settlement_from_events(events: Mapping[str, list[str]]) -> Optional[dict[str, Any]]:
    """Decode a wasm RFQ settlement event into a dict."""
    for key in (
        f"{EVENT_TYPE}.settlement",
        f"{EVENT_TYPE}.data",
        f"{EVENT_TYPE}.payload",
    ):
        raw = first_event_value(events, key)
        if raw:
            return json.loads(raw)

    attrs = prefixed_attrs(events)
    if not attrs:
        return None

    return {
        "_contract_address": attrs.get("_contract_address", ""),
        "rfq_id": int_attr(attrs, "rfq_id"),
        "market_id": attrs.get("market_id", ""),
        "taker": attrs.get("taker", ""),
        "execution_mode": attrs.get("execution_mode", ""),
        "direction": attrs.get("direction", ""),
        "margin": attrs.get("margin", ""),
        "quantity": attrs.get("quantity", ""),
        "worst_price": attrs.get("worst_price", ""),
        "quotes": load_json_attr(attrs, "quotes", []),
        "results": load_json_attr(attrs, "results", []),
        "unfilled_action": load_json_attr(attrs, "unfilled_action", None),
        "fallback_quantity": attrs.get("fallback_quantity", ""),
        "fallback_margin": attrs.get("fallback_margin", ""),
        "cid": attrs.get("cid", ""),
    }


def maker_has_traded(settlement: Mapping[str, Any], maker_address: str) -> bool:
    """Return true when the maker has non-zero settlement result quantity or margin."""
    for result in settlement.get("results", []):
        if result.get("maker") != maker_address:
            continue
        quantity = _decimal_or_zero(result.get("q") or result.get("quantity"))
        margin = _decimal_or_zero(result.get("m") or result.get("margin"))
        return not quantity.is_zero() or not margin.is_zero()
    return False


def _decimal_or_zero(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal(0)
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        logger.warning("invalid settlement decimal value: %r", value)
        return Decimal(0)


def result_status(result: Mapping[str, Any]) -> str:
    if result.get("e") or result.get("error"):
        return "rejected"
    if result.get("q") is not None or result.get("m") is not None:
        return "accepted"
    return ""


def settlement_to_maker_update(
    settlement: Mapping[str, Any],
    events: Mapping[str, list[str]],
) -> RFQSettlementMakerUpdate:
    """Convert decoded settlement event data to the MakerStream update shape."""
    results_by_maker = {
        result.get("maker"): result for result in settlement.get("results", [])
    }

    quotes: list[RFQSettlementQuote] = []
    for quote in settlement.get("quotes", []):
        result = results_by_maker.get(quote.get("maker"), {})
        expiry = quote.get("expiry") or {}
        quotes.append(
            RFQSettlementQuote(
                maker=quote.get("maker", ""),
                price=quote.get("price", ""),
                quoted_margin=quote.get("margin", ""),
                quoted_quantity=quote.get("quantity", ""),
                executed_margin=result.get("m", ""),
                executed_quantity=result.get("q", ""),
                expiry=RFQExpiryType(
                    timestamp=int(expiry.get("ts") or 0),
                    height=int(expiry.get("h") or 0),
                ),
                signature=quote.get("signature", ""),
                nonce=int(quote.get("nonce") or 0),
                status=result_status(result),
            )
        )

    unfilled_action = None
    action = settlement.get("unfilled_action")
    if isinstance(action, dict):
        unfilled_action = RFQSettlementUnfilledActionType()
        if action.get("limit") is not None:
            unfilled_action.limit.CopyFrom(
                RFQSettlementLimitActionType(price=action["limit"].get("price", ""))
            )
        if action.get("market") is not None:
            unfilled_action.market.CopyFrom(RFQSettlementMarketActionType())

    update = RFQSettlementMakerUpdate(
        quotes=quotes,
        rfq_id=int(settlement.get("rfq_id") or 0),
        market_id=settlement.get("market_id", ""),
        taker=settlement.get("taker", ""),
        direction=settlement.get("direction", ""),
        margin=settlement.get("margin", ""),
        quantity=settlement.get("quantity", ""),
        worst_price=settlement.get("worst_price", ""),
        fallback_quantity=settlement.get("fallback_quantity", ""),
        fallback_margin=settlement.get("fallback_margin", ""),
        height=int(first_event_value(events, "tx.height") or 0),
        cid=settlement.get("cid", ""),
        tx_hash=first_event_value(events, "tx.hash"),
    )
    if unfilled_action is not None:
        update.unfilled_action.CopyFrom(unfilled_action)
    return update
