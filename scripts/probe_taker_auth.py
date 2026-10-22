"""Authenticate a TakerStream without creating an RFQ request."""

import argparse
import asyncio
import base64
import json
import sys
from urllib.parse import quote

import httpx

from rfq_test.clients.websocket import TakerStreamClient
from rfq_test.config import get_environment_config, get_settings
from rfq_test.crypto.wallet import Wallet


async def main(principal: str | None = None) -> int:
    config = get_environment_config()
    settings = get_settings()
    if settings.retail_private_key:
        wallet = Wallet.from_private_key(settings.retail_private_key)
    elif principal:
        print("Delegated probe requires the delegate's key in the environment's RETAIL_PRIVATE_KEY")
        return 1
    else:
        wallet = Wallet.generate()
        print("No retail key configured; using an ephemeral taker for this auth-only probe")
    if principal:
        if principal == wallet.inj_address:
            print("Delegated probe requires a signer different from the principal")
            return 1
        registry = config.contract.sigauth_registry_address
        if not registry:
            print("No SigAuth registry configured for this environment")
            return 1
        query = {
            "get_authorization_state": {
                "principal": principal,
                "scope": {"consumer": config.contract.address, "action_id": 3},
                "delegate": wallet.inj_address,
            }
        }
        encoded = quote(base64.b64encode(json.dumps(query).encode()).decode(), safe="")
        url = (
            f"{config.chain.lcd_endpoint.rstrip('/')}/cosmwasm/wasm/v1/contract/"
            f"{registry}/smart/{encoded}"
        )
        async with httpx.AsyncClient(timeout=10.0) as http:
            response = await http.get(url)
            response.raise_for_status()
            active = response.json()["data"]["active"]
        print(
            f"SigAuth: registry={registry} principal={principal} delegate={wallet.inj_address} "
            f"consumer={config.contract.address} action_id=3 active={active}"
        )
        if not active:
            print("The principal must authorize this delegate for the takerStream scope first")
            return 1
    client = TakerStreamClient(
        config.indexer.ws_endpoint,
        request_address=principal or wallet.inj_address,
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
        f"nonce={result['nonce'] or 'not returned'} message={result['message']}"
    )
    return 0 if result["authenticated"] else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--principal",
        help="Taker principal; RETAIL_PRIVATE_KEY must belong to its SigAuth delegate",
    )
    args = parser.parse_args()
    sys.exit(asyncio.run(main(args.principal)))
