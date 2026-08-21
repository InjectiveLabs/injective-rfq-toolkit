"""Protobuf message types for RFQ API (generated from injective_rfq_rpc.proto)."""

from rfq_test.proto.injective_rfq_rpc_pb2 import (
    CreateRFQRequestType,
    MakerStreamStreamingRequest,
    MakerStreamResponse,
    QuoteStreamAck,
    RFQExpiryType,
    RFQProcessedQuoteType,
    RFQQuoteType,
    RFQRequestType,
    RFQSettlementLimitActionType,
    RFQSettlementMakerUpdate,
    RFQSettlementMarketActionType,
    RFQSettlementType,
    RFQSettlementUnfilledActionType,
    RequestStreamAck,
    StreamError,
    TakerAuth,
    TakerAuthResult,
    TakerChallenge,
    TakerStreamStreamingRequest,
    TakerStreamResponse,
)

# Backward-compat aliases
MakerStreamRequest = MakerStreamStreamingRequest
TakerStreamRequest = TakerStreamStreamingRequest

__all__ = [
    "CreateRFQRequestType",
    "MakerStreamRequest",
    "MakerStreamStreamingRequest",
    "MakerStreamResponse",
    "QuoteStreamAck",
    "RFQExpiryType",
    "RFQProcessedQuoteType",
    "RFQQuoteType",
    "RFQRequestType",
    "RFQSettlementLimitActionType",
    "RFQSettlementMakerUpdate",
    "RFQSettlementMarketActionType",
    "RFQSettlementType",
    "RFQSettlementUnfilledActionType",
    "RequestStreamAck",
    "StreamError",
    "TakerAuth",
    "TakerAuthResult",
    "TakerChallenge",
    "TakerStreamRequest",
    "TakerStreamStreamingRequest",
    "TakerStreamResponse",
]
