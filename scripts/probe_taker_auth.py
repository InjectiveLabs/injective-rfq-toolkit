"""Authenticate a TakerStream without creating an RFQ request."""

import asyncio
import sys

from rfq_test.clients.websocket import TakerStreamClient
from rfq_test.config import get_environment_config, get_settings
from rfq_test.crypto.wallet import Wallet


async def main() -> int:
    config = get_environment_config()
    settings = get_settings()
    if settings.retail_private_key:
        wallet = Wallet.from_private_key(settings.retail_private_key)
    else:
        wallet = Wallet.generate()
        print("No retail key configured; using an ephemeral taker for this auth-only probe")
    client = TakerStreamClient(
        config.indexer.ws_endpoint,
        request_address=wallet.inj_address,
        auth_private_key=wallet.private_key,
        auth_contract_address=config.contract.address,
        timeout=10.0,
    )

    try:
        await client.connect()
        result = await client.wait_for_auth_result(timeout=10.0)
    finally:
        await client.close()

    print(
        "Taker authentication: "
        f"authenticated={result['authenticated']} code={result['code']} "
        f"message={result['message']}"
    )
    return 0 if result["authenticated"] else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
