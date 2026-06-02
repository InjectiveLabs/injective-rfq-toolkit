"""
RFQ Market Maker main flow (WebSocket/gRPC-web, v2 EIP-712 signing).

This is the Python WebSocket market-maker reference for the current
MakerStream protocol:

1. Connect to MakerStream with `maker_address` metadata.
2. Answer the first `MakerChallenge` with `MakerAuth`.
3. Receive RFQ requests.
4. Sign quotes with EIP-712 v2.
5. Send quotes back on the same stream.

The same auth handshake is covered end-to-end by `examples/test_settlement.py`.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path

import dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

dotenv.load_dotenv(Path(__file__).with_name(".env"))
dotenv.load_dotenv(ROOT / ".env")

from rfq_test.clients.websocket import MakerStreamClient
from rfq_test.config import get_environment_config, get_settings
from rfq_test.crypto.eip712 import sign_quote_v2
from rfq_test.crypto.wallet import Wallet
from rfq_test.exceptions import IndexerTimeoutError, IndexerValidationError


def stream_error_fields(error) -> tuple[str, str, str, int]:
    return (
        getattr(error, "code", ""),
        getattr(error, "message_", None) or getattr(error, "message", ""),
        getattr(error, "taker", ""),
        getattr(error, "rfq_id", 0),
    )


class FIFOSet:
    def __init__(self, limit: int):
        self.seen: set[str] = set()
        self.order: list[str] = []
        self.limit = limit

    def add(self, key: str) -> bool:
        if key in self.seen:
            return False

        self.seen.add(key)
        self.order.append(key)

        if len(self.order) > self.limit:
            oldest = self.order.pop(0)
            self.seen.discard(oldest)

        return True


def _env_private_key() -> str:
    settings = get_settings()
    private_key = os.getenv("MM_PRIVATE_KEY") or settings.mm_private_key
    if not private_key:
        active = settings.rfq_env.upper()
        raise RuntimeError(
            f"Set MM_PRIVATE_KEY or {active}_MM_PRIVATE_KEY before running this example"
        )
    return private_key


async def quote_request(
    *,
    client: MakerStreamClient,
    request: dict,
    wallet: Wallet,
    chain_id: str,
    contract_address: str,
    evm_chain_id: int,
) -> None:
    """Sign and send one full-size quote for an incoming RFQ request."""
    expiry_ms = int(time.time() * 1000) + 20_000

    # Demo pricing: quote exactly at the taker's worst price. Real makers
    # should replace this with their pricing engine while preserving the exact
    # decimal strings used for signing and wire payloads.
    price = request["worst_price"]
    margin = request["margin"]
    quantity = request["quantity"]

    signature = sign_quote_v2(
        private_key=wallet.private_key,
        evm_chain_id=evm_chain_id,
        verifying_contract_bech32=contract_address,
        market_id=request["market_id"],
        rfq_id=int(request["rfq_id"]),
        taker=request["request_address"],
        direction=request["direction"],
        taker_margin=margin,
        taker_quantity=quantity,
        maker=wallet.inj_address,
        maker_subaccount_nonce=0,
        maker_quantity=quantity,
        maker_margin=margin,
        price=price,
        expiry_ms=expiry_ms,
    )

    ack = await client.send_quote(
        {
            "chain_id": chain_id,  # Cosmos chain ID; mainnet="injective-1" (not 1776)
            "contract_address": contract_address,
            "market_id": request["market_id"],
            "rfq_id": int(request["rfq_id"]),
            "taker_direction": request["direction"],
            "margin": margin,
            "quantity": quantity,
            "price": price,
            "expiry": expiry_ms,
            "maker": wallet.inj_address,
            "taker": request["request_address"],
            "signature": signature,
            "maker_subaccount_nonce": 0,
            "sign_mode": "v2",
            "evm_chain_id": evm_chain_id,  # EIP-712 chain ID; mainnet=1776
        },
        wait_for_response=True,
        response_timeout=5.0,
    )

    print(
        "Quote sent:",
        f"rfq_id={request['rfq_id']}",
        f"taker={ack.get('taker') if ack else request['request_address']}",
        f"price={price}",
        f"status={ack.get('status') if ack else 'unknown'}",
    )


async def main() -> None:
    config = get_environment_config()
    mm_private_key = _env_private_key()
    wallet = Wallet.from_private_key(mm_private_key)

    default_chain_id, default_contract_address = config.signing_context
    default_evm_chain_id, _ = config.signing_context_v2

    chain_id = os.getenv("CHAIN_ID") or default_chain_id
    contract_address = os.getenv("CONTRACT_ADDRESS") or default_contract_address
    evm_chain_id = int(
        os.getenv("EVM_CHAIN_ID")
        or os.getenv("RFQ_EVM_CHAIN_ID")
        or str(default_evm_chain_id)
    )
    ws_endpoint = os.getenv("RFQ_WS_URL") or config.indexer.ws_endpoint
    comet_bft_endpoint = os.getenv("CHAIN_COMETBFT_ENDPOINT")

    print("Connecting to MakerStream")
    print(f"  endpoint: {ws_endpoint}/MakerStream")
    print(f"  maker:    {wallet.inj_address}")
    print(f"  chain:    {chain_id}")
    print(f"  contract: {contract_address}")
    if comet_bft_endpoint:
        print("Subscribing to cometBFT chain")
        print(f"  endpoint: {comet_bft_endpoint}")

    settlements_seen = FIFOSet(5000)

    async with MakerStreamClient(
        ws_endpoint,
        maker_address=wallet.inj_address,
        subscribe_to_quotes_updates=True,
        subscribe_to_settlement_updates=True,
        auth_private_key=wallet.private_key,
        auth_evm_chain_id=evm_chain_id,
        auth_contract_address=contract_address,
        comet_bft_endpoint=comet_bft_endpoint,
        settlement_contract_address=contract_address,
        timeout=30.0,
    ) as client:
        print("Connected. Waiting for RFQ requests.")

        while True:
            try:
                event = await client.get_next_event(timeout=60.0)
            except IndexerTimeoutError:
                print("No RFQs in the last 60s; still listening.")
                continue
            if event is None:
                print("No RFQs in the last 60s; still listening.")
                continue

            msg_type, data = event

            if msg_type == "settlement_update":
                if not settlements_seen.add(f"{data.taker}:{data.rfq_id}"):
                    continue
                print(
                    "Settlement update:",
                    f"rfq_id={data.rfq_id}",
                    f"taker={data.taker}",
                    f"direction={data.direction}",
                    f"quantity={data.quantity}",
                    f"margin={data.margin}",
                    f"cid={data.cid}",
                    f"source={client.settlement_source(data)}",
                    f"quotes={[q.maker for q in data.quotes]}",
                )
                continue
            if msg_type == "quote_update":
                update = client._processed_quote_to_dict(data)
                if update.get("maker") == wallet.inj_address:
                    print(
                        "Quote update:",
                        f"rfq_id={update['rfq_id']}",
                        f"status={update.get('status')}",
                        f"executed_qty={update.get('executed_quantity')}",
                        f"executed_margin={update.get('executed_margin')}",
                        f"error={update.get('error')}",
                    )
                continue
            if msg_type == "error":
                code, message, taker, rfq_id = stream_error_fields(data)
                print(
                    "Stream error:",
                    f"code={code}",
                    f"message={message}",
                    f"taker={taker}",
                    f"rfq_id={rfq_id}",
                )
                continue
            if msg_type != "request":
                continue

            request = client._request_to_dict(data)
            print(
                "RFQ request:",
                f"rfq_id={request['rfq_id']}",
                f"market={request['market_id']}",
                f"direction={request['direction']}",
                f"margin={request['margin']}",
                f"quantity={request['quantity']}",
                f"worst_price={request['worst_price']}",
            )

            try:
                await quote_request(
                    client=client,
                    request=request,
                    wallet=wallet,
                    chain_id=chain_id,
                    contract_address=contract_address,
                    evm_chain_id=evm_chain_id,
                )
            except IndexerValidationError as exc:
                print(f"Quote rejected by indexer: {exc}")


if __name__ == "__main__":
    asyncio.run(main())
