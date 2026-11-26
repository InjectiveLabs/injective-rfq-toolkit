"""RFQ pricer: read indicative RFQ prices without sending an RFQ.

Opens the pricing stream with the current snapshot of each market in the first
message, then follows the updates.
Public and read-only: no keys, no auth.

Usage:
    RFQ_ENV=mainnet python examples/pricer.py [market_id ...]

Without arguments it uses the markets of the active environment config.
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import dotenv
dotenv.load_dotenv()

from rfq_test.clients.pricer import PricerClient
from rfq_test.config import get_environment_config


def show(snapshot) -> None:
    if not snapshot.tiers:
        print(f"{snapshot.market_id} ts={snapshot.ts} withdrawn (no current pricing)")
        return
    print(f"{snapshot.market_id} ts={snapshot.ts} mark={snapshot.mark_price}")
    for tier in snapshot.tiers:
        print(
            f"  ${tier.notional_usd}: bid={tier.bid or '-'} ask={tier.ask or '-'} "
            f"age={tier.age_ms}ms"
        )


async def main() -> None:
    config = get_environment_config()
    if config.pricer is None:
        sys.exit(f"no pricer configured for environment {config.environment}")
    market_ids = sys.argv[1:] or [market.id for market in config.markets]

    client = PricerClient(config.pricer.grpc_endpoint)
    try:
        stream = client.pricing_stream(market_ids, with_snapshot=True)
        first = await anext(aiter(stream))
        priced = {snapshot.market_id.lower() for snapshot in first.snapshots}
        for snapshot in first.snapshots:
            show(snapshot)
        for market_id in market_ids:
            if market_id.lower() not in priced:
                print(f"{market_id} no snapshot")

        print("streaming, Ctrl+C to stop")
        async for snapshot in stream:
            show(snapshot)
    finally:
        await client.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
