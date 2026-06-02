#!/usr/bin/env python3
"""Set up RFQ authz grants for a single retail wallet from .env."""

import argparse
import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from rfq_test.clients.chain import ChainClient
from rfq_test.config import load_environment_config
from rfq_test.crypto.wallet import Wallet
from rfq_test.utils.setup import RETAIL_AUTHZ_GRANTS


async def verify_grants(
    chain_client: ChainClient,
    granter: str,
    grantee: str,
    msg_types: list[str],
) -> None:
    """Query and print grant status for each required message type."""
    print("\nVerifying grants on-chain:")
    for msg_type in msg_types:
        response = await chain_client._client.fetch_grants(
            granter=granter,
            grantee=grantee,
            msg_type_url=msg_type,
        )
        grants = response.get("grants", []) if isinstance(response, dict) else []
        status = "OK" if grants else "MISSING"
        print(f"  [{status}] {msg_type}")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Setup authz grants for a single retail wallet")
    parser.add_argument("--env", default="testnet", help="Environment name (default: testnet)")
    parser.add_argument(
        "--expected-address",
        default=None,
        help="Optional Injective address to verify against the private key before broadcasting",
    )
    args = parser.parse_args()

    load_dotenv()

    env_prefix = args.env.upper()
    private_key = os.getenv(f"{env_prefix}_RETAIL_PRIVATE_KEY")
    if not private_key:
        raise SystemExit(f"Missing {env_prefix}_RETAIL_PRIVATE_KEY in .env")

    wallet = Wallet.from_private_key(private_key)
    if args.expected_address and wallet.inj_address != args.expected_address:
        raise SystemExit(
            f"Retail key mismatch: got {wallet.inj_address}, expected {args.expected_address}"
        )

    config = load_environment_config(args.env)
    chain_client = ChainClient(config.chain)

    print("=" * 60)
    print(f"Setting up retail authz for {wallet.inj_address}")
    print(f"Environment: {args.env}")
    print(f"Contract:    {config.contract.address}")
    print("Grants:")
    for msg_type in RETAIL_AUTHZ_GRANTS:
        print(f"  - {msg_type}")
    print("=" * 60)

    await chain_client.connect()
    try:
        for msg_type in RETAIL_AUTHZ_GRANTS:
            print(f"\nGranting {msg_type} ...")
            tx_hash = await chain_client.grant_authz(
                private_key=wallet.private_key,
                grantee=config.contract.address,
                msg_type=msg_type,
            )
            print(f"  TX: {tx_hash}")

        await verify_grants(
            chain_client=chain_client,
            granter=wallet.inj_address,
            grantee=config.contract.address,
            msg_types=RETAIL_AUTHZ_GRANTS,
        )
    finally:
        await chain_client.close()

    print("Retail authz grants complete.")


if __name__ == "__main__":
    asyncio.run(main())
