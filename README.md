# injective-rfq-toolkit

**The Injective RFQ developer toolkit.** A Python package, generated protobuf stubs, EIP-712 v2 signing primitives, end-to-end test harness, and reference market-maker / retail implementations in Python, TypeScript, and Go — all in one repo, with testnet defaults and configurable mainnet/private deployments.

> **Positioning.** This is a *toolkit*: importable client library + signing helpers + generated proto + reference scripts + integration test suite, packaged together. Partners use it three ways:
> 1. **`pip install -e .`** and import `rfq_test` to build a Python bot on top of `MakerStreamClient`, `sign_quote_v2`, etc.
> 2. **Clone the gRPC examples** in `examples/{python,go,ts}-mm/main-grpc.*` as a starting point in their language of choice.
> 3. **Run the test harness** against testnet to verify their own integrations end-to-end.

**Companion guides:**
- [PYTHON_BUILDING_GUIDE.md](PYTHON_BUILDING_GUIDE.md) — full protocol walkthrough for teams that want to build standalone (no `rfq_test` dependency)
- Live HTML docs: [rfq.inj.so/onboarding.html](https://rfq.inj.so/onboarding.html) and [rfq.inj.so/runbook.html](https://rfq.inj.so/runbook.html)

---

## What's in the box

```
src/rfq_test/                # Python package (importable as `rfq_test`)
  ├── clients/               # Network clients
  │   ├── websocket.py       #   TakerStreamClient, MakerStreamClient (auth-handshake aware)
  │   ├── chain.py           #   ChainClient — authz grants, balances, txs
  │   └── contract.py        #   ContractClient — AcceptQuote, CancelIntentLane, CancelAllIntents
  ├── crypto/                # Signing & wallets
  │   ├── eip712.py          #   sign_quote_v2, sign_conditional_order_v2,
  │   │                      #   sign_maker_challenge_v2, sign_taker_challenge_v1,
  │   │                      #   domain_separator, bech32_to_evm
  │   └── wallet.py          #   Wallet, mnemonic + address conversion helpers
  ├── proto/                 # gRPC-generated stubs + hand-written gRPC-web framing
  ├── actors/                # High-level orchestration (MarketMaker, RetailUser, Admin)
  ├── models/                # Pydantic types — Request, Quote, Settlement, EnvironmentConfig, …
  ├── factories/             # Builders — RequestFactory, QuoteFactory, WalletFactory
  ├── utils/                 # Decimal canonicalization, retry, logging, price/tick helpers
  ├── config.py              # Env-aware config loader (RFQ_ENV=testnet|mainnet|local)
  └── exceptions.py          # IndexerValidationError, IndexerTimeoutError, …

configs/                     # Per-environment YAML (testnet, local, …)
scripts/                     # Operational scripts — authz grants, maker registration,
                             #   funding, conditional-order demo, signing self-test
examples/                    # End-to-end reference implementations
  ├── test_roundtrip.py      #   Python: retail request → MM quote → retail receives
  ├── test_settlement.py     #     "      + on-chain AcceptQuote (full E2E)
  ├── test_settlement_grpc.py#   Same flow over native gRPC
  ├── taker_multi_quote.py   #   Multiple MMs quoting the same RFQ
  ├── python-mm/main-grpc.py #   Standalone MM bot (no rfq_test dep) — gRPC, auth-handshake
  ├── python-mm/mark_quote_loop.py # Configurable mark-based MM quote loop
  ├── go-mm/main-grpc/       #   Same bot in Go
  └── ts-mm/main-grpc.ts     #   Same bot in TypeScript
tests/                       # pytest suite — smoke / functional / contract / load / validation
```

### Capabilities at a glance

| Capability | Where it lives | Notes |
|---|---|---|
| **MakerStream WS subscribe + auth handshake** | `clients.websocket.MakerStreamClient` | Auto-signs `MakerChallenge` when given `auth_private_key` + `auth_evm_chain_id` + `auth_contract_address` |
| **TakerStream WS request + ACK + quote collection** | `clients.websocket.TakerStreamClient` | `send_request`, `wait_for_ack`, `collect_quotes`, `send_conditional_order` |
| **Quote signing (EIP-712 v2)** | `crypto.eip712.sign_quote_v2` | 16-field digest including `evmChainId` first; byte-compatible with the Rust contract |
| **Conditional-order signing (TP/SL)** | `crypto.eip712.sign_conditional_order_v2` | 19-field `SignedTakerIntent` digest; supports both blind and taker-bound paths |
| **Auth-handshake signing** | `crypto.eip712.{sign_maker_challenge_v2, sign_taker_challenge_v1}` | Maker v2 and chain-independent taker v1 challenge digests |
| **Decimal canonicalization** | `utils.price.quantize_for_fpdecimal` | Quantize-to-tick + strip-trailing-zeros — what the indexer requires |
| **bech32 ↔ EVM address conversion** | `crypto.eip712.bech32_to_evm` + `crypto.wallet.{eth_to_inj,inj_to_eth}_address` | Used in domain separator and address-typed digest fields |
| **Wallet generation** | `crypto.wallet.Wallet`, `WalletFactory` | From private key, mnemonic, or generated |
| **On-chain settlement** | `clients.contract.ContractClient.accept_quote` | Builds `MsgPrivilegedExecuteContract` with the right wrapping |
| **Conditional-order cancellation** | `clients.contract.ContractClient.{cancel_intent_lane, cancel_all_intents}` | Lane-level vs global epoch bumps |
| **Authz grant orchestration** | `clients.chain.ChainClient.grant_authz` | `GenericAuthorization`, no expiration, gas-heuristic broadcast |
| **Generated proto bindings** | `proto/injective_rfq_rpc_pb2.py` + hand-written `rfq_messages.py` | Includes `MakerChallenge`, `MakerAuth`, conditional-order frames |
| **Test harness** | `tests/`, `factories/`, `utils.scenario`, `actors/` | pytest with smoke/functional/contract/load/validation marks |

### Public API

The package's importable surface today:

```python
# Top-level
from rfq_test import Settings, get_settings, Direction, Quote, Request, Settlement

# Clients
from rfq_test.clients.websocket import MakerStreamClient, TakerStreamClient
from rfq_test.clients.chain    import ChainClient
from rfq_test.clients.contract import ContractClient

# Signing
from rfq_test.crypto.eip712 import (
    sign_quote_v2,
    sign_conditional_order_v2,
    sign_maker_challenge_v2,
    domain_separator,
    bech32_to_evm,
)
from rfq_test.crypto.wallet import Wallet, eth_to_inj_address, inj_to_eth_address

# Config + actors
from rfq_test.config        import get_environment_config
from rfq_test.models.config import EnvironmentConfig
from rfq_test.actors.market_maker import MarketMaker
from rfq_test.actors.retail       import RetailUser
from rfq_test.actors.admin        import Admin

# Decimal hygiene
from rfq_test.utils.price import quantize_for_fpdecimal, quantize_to_tick
```

> **API stability:** not committed to semver yet. Pin to a commit SHA if you're vendoring. The signing helpers (`sign_quote_v2`, `sign_conditional_order_v2`, `sign_maker_challenge_v2`) are the most stable surface — their digest layouts are locked to the on-chain contract.

---

## Quick start

### 1. Install

```bash
pip install -U pip
pip install -e ".[dev]"
```

Python 3.11+ required.

### 2. Configure

```bash
cp .env.example .env       # edit with your private keys
export RFQ_ENV=testnet     # testnet | mainnet | local
```

The harness reads `TESTNET_MM_PRIVATE_KEY`, `TESTNET_RETAIL_PRIVATE_KEY`, and (for TP/SL) `TESTNET_RELAYER_PRIVATE_KEY` from your env. Raw 64-char hex, no `0x` prefix. The bech32 `inj1…` is derived at runtime — you don't need to write it down.

### 3. Setup (one-time)

```bash
python scripts/setup_authz_grants.py   # both MM and retail wallets need this
python scripts/register_makers.py      # admin-only
python scripts/fund_subaccounts.py     # USDC margin into the maker/retail subaccounts
```

### 4. Run a flow

```bash
python examples/test_roundtrip.py      # WS round-trip: request → ACK → quote
python examples/test_settlement.py     # full E2E with on-chain AcceptQuote
python examples/python-mm/main-grpc.py # standalone MM bot (no rfq_test dep)
python examples/python-mm/mark_quote_loop.py --edge-bps 25 --max-quantity 20
```

To check whether external MMs are quoting a market without needing taker funds:

```bash
python scripts/probe_quotes.py --market-symbol "INJ/USDC PERP"
python scripts/probe_quotes.py --market-id 0x... --quantity 1 --margin 10 --worst-price 0.08
```

The probe submits a TakerStream RFQ request and reports returned quotes. Add
`--accept` if you also want to submit `AcceptQuote`; a settlement failure after
quotes arrive is reported but does not fail the probe unless `--strict-settlement`
is set.

Use `--json` when diagnosing latency. The summary includes request ACK time,
quote collection time, per-quote TTL at collection end, `AcceptQuote`
confirmation time, and, after successful settlement, each quote expiry compared
with the execution block time.

For TypeScript and Go reference makers:

```bash
cd examples/ts-mm && npm install && npm run start
cd examples/go-mm/main-grpc && go run .
```

### 5. Run the test suite

```bash
pytest -m smoke          # ~30s, fast health check
pytest -m functional     # E2E flows
pytest                   # everything except `load`
```

---

## Protocol cheat-sheet

The RFQ Indexer uses **gRPC-web over WebSocket** with protobuf framing. Two streams — `TakerStream` and `MakerStream` — and a settlement path that goes directly to the CosmWasm contract on Injective.

- **Subprotocol:** `grpc-ws`
- **Framing:** `[1 byte flags][4 bytes length BE][protobuf payload]`
- **Keep-alive:** send `ping` every ~1s; the indexer drops idle streams.
- **Signing:** **EIP-712 v2** typed-data digest → secp256k1 raw → `0x` + `r ‖ s ‖ v` (v=0/1, **not** 27/28). Custom layout, *not* `eth_signTypedData_v4`. Spec in [`crypto/eip712.py`](src/rfq_test/crypto/eip712.py); recipe in [PYTHON_BUILDING_GUIDE.md § Quote Signing (v2)](PYTHON_BUILDING_GUIDE.md#quote-signing-v2).
- **Wire-required fields:** every quote and conditional-order create carries `sign_mode="v2"` and `evm_chain_id` (`1439` testnet, `1776` mainnet). Missing or empty values are rejected. Keep `chain_id` as the Cosmos string (`injective-888` / `injective-1`); do not put `1439` or `1776` in `chain_id`.
- **MakerStream auth handshake:** the first server message after a maker connects is a `MakerChallenge`. Sign the `StreamAuthChallenge` typed-data and reply with `MakerAuth{evm_chain_id, signature}`. `MakerStreamClient` does this for you when you pass `auth_private_key` + `auth_evm_chain_id` + `auth_contract_address`. Standalone implementations in `examples/{python,go,ts}-mm/main-grpc.*`. Full protocol: [PYTHON_BUILDING_GUIDE.md § MakerStream Auth Handshake](PYTHON_BUILDING_GUIDE.md#makerstream-auth-handshake).
- **TakerStream auth handshake:** pass `request_address`, `auth_private_key`, and `auth_contract_address` to `TakerStreamClient`. It requests auth v1, signs the chain-independent `TakerStreamAuthChallenge(address taker,bytes32 nonce,uint64 expiresAt)`, replies with `TakerAuth{signature}`, and exposes `authenticated`, `code`, `message`, and the correlating `nonce` through `wait_for_auth_result()`. The private key may belong to the taker or to an authorized Authz grantee. Authentication failure is informational and does not close the stream. Native gRPC/TypeScript reference: `examples/ts-retail/main-grpc.ts`.

Test only the taker handshake against testnet (this does not submit an RFQ):

```bash
RFQ_ENV=testnet PYTHONPATH=src python scripts/probe_taker_auth.py
```

### Production timing lessons

Maker quote expiries may be short. Some production makers require roughly 1.5s
of total quote lifetime, including the taker's quote collection window. Any
frontend or client that does fresh market, oracle, account, or grant checks
after the user clicks submit will feel slow and may settle against a worse
surviving quote.

For browser gateway flows, instrument prepare duration, local signing duration,
broadcast/accept duration, confirmation polling, and cleanup. For toolkit
TakerStream flows, instrument request ACK time, quote collection time, quote TTL
at collection end, and `AcceptQuote` confirmation time. When a quote expires
on-chain, compare the quote expiry timestamp with the execution block time
before guessing whether time was lost in collection, signing, broadcast, or
block inclusion.

For market makers, log quote send-to-ACK duration and quote expiry. A
`quote_ack` only means the indexer accepted and routed the quote; if there is no
later quote or settlement update before the maker-set expiry, treat that quote
as not filled.

### Maker subscriptions

`MakerStreamClient` accepts these options on construction:

```python
from rfq_test.clients.websocket import MakerStreamClient

mm_client = MakerStreamClient(
    ws_url,
    maker_address=maker_inj_address,
    market_ids=[inj_usdc_market_id, btc_usdc_market_id],  # only these markets
    subscribe_to_quotes_updates=True,           # quote_update events
    subscribe_to_settlement_updates=True,       # settlement_update events
    auth_private_key=maker_private_key,         # auto-signs MakerChallenge
    auth_evm_chain_id=1439,
    auth_contract_address=contract_address,
)
```

Omit `market_ids` (or pass an empty list) to receive requests from every market.

Update event semantics:
- `quote_ack status="success"` only means the indexer accepted and routed the quote. There is intentionally no later "not accepted" event; if no update arrives before the maker-set `expiry`, treat the quote as not accepted by the taker.
- `quote_update` arrives for any quote whose `maker` matches `maker_address`. `status="accepted"` means used in settlement; `status="rejected"` means evaluated but not used. `executed_quantity` / `executed_margin` are the actual fill.
- `settlement_update` arrives when the taker accepts and settlement is attempted, with the trade result or failure. It also arrives whenever a settlement included at least one quote from this maker, even quotes that were not the winning one.

### Conditional orders (TP/SL)

Takers pre-sign trigger-based orders that fire when mark price crosses a threshold. Two paths:
- **TakerStream** with `message_type: "conditional_order"` and `conditional_order_sign_mode="v2"` + `conditional_order_evm_chain_id` (proto field 6). `TakerStreamClient.send_conditional_order(...)` sets both for you.
- **REST API** `POST /conditionalOrder` with `sign_mode` + `evm_chain_id` (proto field 4 on direct creates).

Cancellation is on-chain via `ContractClient.cancel_intent_lane(market_id, subaccount_nonce)` (lane-scoped) or `cancel_all_intents()` (taker-wide epoch bump).

Reference: [PYTHON_BUILDING_GUIDE.md § Conditional Orders](PYTHON_BUILDING_GUIDE.md#conditional-orders-tpsl), `scripts/conditional_order_example.py`.

## Regenerating proto code

After editing `src/rfq_test/proto/injective_rfq_rpc.proto`:

```bash
.venv/bin/python -m grpc_tools.protoc \
  -I src/rfq_test/proto \
  --python_out=src/rfq_test/proto \
  --grpc_python_out=src/rfq_test/proto \
  src/rfq_test/proto/injective_rfq_rpc.proto
```

Overwrites `injective_rfq_rpc_pb2.py` and `injective_rfq_rpc_pb2_grpc.py`. After regen, review field/type changes (especially `evm_chain_id` field numbers, nested `Expiry`, and the `MakerChallenge` / `MakerAuth` shapes) and update `clients/websocket.py` if needed. `grpcio-tools` is a dev extra (`pip install -e ".[dev]"`).

---

## License

Apache License 2.0. See [LICENSE](LICENSE).
