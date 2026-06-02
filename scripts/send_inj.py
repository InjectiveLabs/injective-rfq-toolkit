#!/usr/bin/env python3
"""Send INJ from the MM wallet to a target Injective address."""

import argparse
import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from pyinjective.composer_v2 import Composer
from pyinjective.core.broadcaster import MsgBroadcasterWithPk

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from rfq_test.clients.chain import ChainClient
from rfq_test.config import load_environment_config
from rfq_test.crypto.wallet import Wallet


def _extract_tx_hash_and_code(result) -> tuple[str | None, int | None, str]:
    """Normalize pyinjective broadcast results."""
    tx_response = result.txResponse if hasattr(result, "txResponse") else result
    tx_hash = getattr(tx_response, "txhash", None) or getattr(tx_response, "txHash", None)
    code = getattr(tx_response, "code", None)
    raw_log = getattr(tx_response, "rawLog", "") or getattr(tx_response, "raw_log", "") or ""
    return tx_hash, code, raw_log


async def main() -> None:
    parser = argparse.ArgumentParser(description="Send INJ from MM wallet to another address")
    parser.add_argument("--env", default="testnet", help="Environment name (default: testnet)")
    parser.add_argument(
        "--to-address",
        default="inj15xjkvekq32xhppnhx76x09tes9wq4jjvztsjyj",
        help="Recipient Injective address",
    )
    parser.add_argument(
        "--amount",
        type=float,
        default=150.0,
        help="Amount of INJ to send (default: 150)",
    )
    args = parser.parse_args()

    load_dotenv()

    env_prefix = args.env.upper()
    private_key = os.getenv(f"{env_prefix}_MM_PRIVATE_KEY")
    if not private_key:
        raise SystemExit(f"Missing {env_prefix}_MM_PRIVATE_KEY in .env")

    sender_wallet = Wallet.from_private_key(private_key)
    amount_wei = int(args.amount * 10**18)
    if amount_wei <= 0:
        raise SystemExit("Amount must be greater than zero")

    config = load_environment_config(args.env)
    chain_client = ChainClient(config.chain)
    await chain_client.connect()

    try:
        composer = Composer(network=chain_client._network.string())
        msg = composer.msg_send(
            from_address=sender_wallet.inj_address,
            to_address=args.to_address,
            amount=amount_wei,
            denom="inj",
        )

        broadcaster = MsgBroadcasterWithPk.new_using_gas_heuristics(
            network=chain_client._network,
            private_key=sender_wallet.private_key,
        )

        print("=" * 60)
        print(f"Sending {args.amount} INJ")
        print(f"From: {sender_wallet.inj_address}")
        print(f"To:   {args.to_address}")
        print("=" * 60)

        result = await broadcaster.broadcast([msg])
        tx_hash, code, raw_log = _extract_tx_hash_and_code(result)

        if not tx_hash:
            raise SystemExit(f"Broadcast succeeded but no tx hash was returned: {result}")
        if code and code != 0:
            raise SystemExit(f"Broadcast failed with code {code}: {raw_log}")

        print(f"Broadcast accepted: {tx_hash}")
        tx_result = await chain_client.wait_for_tx(tx_hash, timeout=30.0)
        confirmed_code = tx_result.get("code", 0) if isinstance(tx_result, dict) else 0
        confirmed_log = tx_result.get("rawLog", "") if isinstance(tx_result, dict) else ""
        if confirmed_code and confirmed_code != 0:
            raise SystemExit(f"Transaction failed on-chain with code {confirmed_code}: {confirmed_log}")

        print("Transfer confirmed.")
        print(f"Explorer: https://testnet.explorer.injective.network/transaction/{tx_hash}")
    finally:
        await chain_client.close()


if __name__ == "__main__":
    asyncio.run(main())
