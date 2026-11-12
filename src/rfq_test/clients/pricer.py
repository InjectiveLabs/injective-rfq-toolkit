"""Client for the RFQ pricer: indicative RFQ prices per market and notional tier.

The pricer is public and read-only: no auth, no signing, no chain access.
"""

from typing import AsyncIterator, Optional

import grpc
import grpc.aio

from rfq_test.proto.injective_rfq_pricer_rpc_pb2 import (
    GetPricingRequest,
    GetPricingResponse,
    PricingStreamRequest,
    PricingStreamResponse,
)

_SERVICE = "/injective_rfq_pricer_rpc.InjectiveRfqPricerRPC"


class PricerClient:
    """Native gRPC client for InjectiveRfqPricerRPC."""

    def __init__(self, grpc_endpoint: str):
        if grpc_endpoint.startswith(("localhost", "127.0.0.1")):
            self._channel = grpc.aio.insecure_channel(grpc_endpoint)
        else:
            self._channel = grpc.aio.secure_channel(
                grpc_endpoint, grpc.ssl_channel_credentials()
            )
        self._get_pricing = self._channel.unary_unary(
            f"{_SERVICE}/GetPricing",
            request_serializer=GetPricingRequest.SerializeToString,
            response_deserializer=GetPricingResponse.FromString,
        )
        self._pricing_stream = self._channel.unary_stream(
            f"{_SERVICE}/PricingStream",
            request_serializer=PricingStreamRequest.SerializeToString,
            response_deserializer=PricingStreamResponse.FromString,
        )

    async def get_pricing(self, market_id: str) -> Optional[GetPricingResponse]:
        """Latest snapshot of a market, or None when it has no unexpired snapshot."""
        try:
            return await self._get_pricing(GetPricingRequest(market_id=market_id))
        except grpc.aio.AioRpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                return None
            raise

    def pricing_stream(self, market_ids: list[str]) -> AsyncIterator[PricingStreamResponse]:
        """Snapshots for the given markets, one message per market per tick.

        A message with empty `tiers` is a withdrawal: the market's last snapshot
        expired and it has no current pricing until a newer snapshot arrives.
        """
        return self._pricing_stream(PricingStreamRequest(market_ids=market_ids))

    async def close(self) -> None:
        await self._channel.close()
