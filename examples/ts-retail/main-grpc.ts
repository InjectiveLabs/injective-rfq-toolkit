/**
 * RFQ – Retail User Main Flow (gRPC)
 *
 * Retail doesn't sign quotes — it forwards the MM's signature to AcceptQuote.
 * The wire RFQQuoteType carries `sign_mode` and `evm_chain_id`; this script
 * forwards the v2 fields into AcceptQuote.
 *
 * Uses native gRPC APIs instead of WebSocket.
 *
 * Flow:
 * 0. Retail user has already granted permissions to RFQ contract (see setup.ts)
 * 1. Retail opens TakerStream (bidirectional gRPC) with request_address metadata
 * 2. Retail sends an RFQ request over the stream (message_type "request")
 * 3. Indexer answers with a request_ack carrying the rfq_id
 * 4. Makers' quotes arrive on the same stream (message_type "quote")
 * 5. Retail picks best quote within slippage tolerance and accepts on-chain
 *
 * Note: the proto also declares a unary Request RPC, but the indexer does
 * not implement it (returns [unimplemented]). TakerStream is the supported
 * path for submitting requests and receiving quotes.
 */

import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";
import { v4 as uuidv4 } from "uuid";
import {
  MsgBroadcasterWithPk,
  MsgExecuteContractCompat,
} from "@injectivelabs/sdk-ts";
import { Network, getNetworkEndpoints } from "@injectivelabs/networks";
import { ChainId } from "@injectivelabs/ts-types";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/* -------------------------------------------------------------------------- */
/*                                PROTO LOADING                               */
/* -------------------------------------------------------------------------- */

const PROTO_PATH = path.resolve(
  __dirname,
  "../../src/rfq_test/proto/injective_rfq_rpc.proto"
);

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const proto = grpc.loadPackageDefinition(packageDefinition);
const InjectiveRfqRPC = (proto.injective_rfq_rpc as any).InjectiveRfqRPC;

function formatStreamError(e: any): string {
  const parts = [`${e.code}: ${e.message_}`];
  if (e.taker) parts.push(`taker=${e.taker}`);
  if (e.rfq_id) parts.push(`rfq_id=${e.rfq_id}`);
  if (e.id) parts.push(`id=${e.id}`);
  return parts.join(" ");
}

/* -------------------------------------------------------------------------- */
/*                                   CONFIG                                   */
/* -------------------------------------------------------------------------- */

const GRPC_ENDPOINT = process.env.GRPC_ENDPOINT!;
const CONTRACT_ADDRESS = process.env.CONTRACT_ADDRESS!;
const RETAIL_PRIVATE_KEY = process.env.RETAIL_PRIVATE_KEY!;
const CHAIN_ID = process.env.CHAIN_ID!;

// Market
const INJUSDT_MARKET_ID =
  "0x7cc8b10d7deb61e744ef83bdec2bbcf4a056867e89b062c6a453020ca82bd4e4";

// Taker address (derive from private key in production)
const TAKER_ADDRESS = "inj1cml96vmptgw99syqrrz8az79xer2pcgp0a885r";

// Network
const NETWORK = Network.Local;
const ENDPOINTS = getNetworkEndpoints(NETWORK);

/* -------------------------------------------------------------------------- */
/*                              ENV VALIDATION                                */
/* -------------------------------------------------------------------------- */

if (!GRPC_ENDPOINT) throw new Error("GRPC_ENDPOINT is not set");
if (!CONTRACT_ADDRESS) throw new Error("CONTRACT_ADDRESS is not set");
if (!RETAIL_PRIVATE_KEY) throw new Error("RETAIL_PRIVATE_KEY is not set");

/* -------------------------------------------------------------------------- */
/*                              INPUT PARAMETERS                              */
/* -------------------------------------------------------------------------- */

const args = process.argv.slice(2);
const margin = args[0] || "100";
const quantity = args[1] || "140";
const maxSlippageBps = args[2] || "100"; // 100 = 1%
const leverage = 2;

const expectedPrice = (Number(margin) * leverage) / Number(quantity);
const maxAcceptablePrice =
  expectedPrice * (1 + Number(maxSlippageBps) / 10000);

console.log("📥 RFQ input params");
console.log("margin:", margin);
console.log("quantity:", quantity);
console.log("max slippage:", maxSlippageBps, "bps");
console.log("expected price:", expectedPrice.toFixed(4));
console.log("max acceptable price:", maxAcceptablePrice.toFixed(4));

/* -------------------------------------------------------------------------- */
/*                                  TYPES                                     */
/* -------------------------------------------------------------------------- */

interface CollectedQuote {
  maker: string;
  margin: string;
  price: string;
  quantity: string;
  expiry: { ts?: number; h?: number };
  signature: string;
  sign_mode?: "v2"; // v2 only
  evm_chain_id?: number;
  maker_subaccount_nonce?: number;
  min_fill_quantity?: string;
}

/* -------------------------------------------------------------------------- */
/*                              QUOTE SELECTION                               */
/* -------------------------------------------------------------------------- */

function chooseBestQuotes(
  maxQuantity: number,
  quotes: CollectedQuote[],
  maxPrice: number
): CollectedQuote[] {
  if (quotes.length === 0 || maxQuantity <= 0) return [];

  const eligible = quotes
    .filter((q) => Number(q.price) <= maxPrice)
    .sort((a, b) => Number(a.price) - Number(b.price));

  if (eligible.length === 0) {
    console.log("⚠️  No quotes within slippage tolerance");
    return [];
  }

  let accQty = 0;
  const selected: CollectedQuote[] = [];
  for (const q of eligible) {
    selected.push(q);
    accQty += Number(q.quantity);
    if (accQty >= maxQuantity) break;
  }
  return selected;
}

/* -------------------------------------------------------------------------- */
/*                            ON-CHAIN SETTLEMENT                             */
/* -------------------------------------------------------------------------- */

async function acceptQuote(
  worstPrice: number,
  rfqId: number,
  marketId: string,
  quotes: CollectedQuote[]
) {
  console.log("\n📌 Accepting quotes on-chain...");

  // Convert signatures from hex to base64 for the contract.
  // Contract quote payloads carry v2 signing metadata.
  const contractQuotes = quotes.map((q) => ({
    maker: q.maker,
    margin: q.margin,
    quantity: q.quantity,
    price: q.price,
      expiry: q.expiry.ts ? { ts: q.expiry.ts } : { h: q.expiry.h || 0 },
    signature: Buffer.from(
      q.signature.replace("0x", ""),
      "hex"
    ).toString("base64"),
    sign_mode: q.sign_mode ?? "v2",
    evm_chain_id: q.evm_chain_id,
    maker_subaccount_nonce: q.maker_subaccount_nonce ?? 0,
    ...(q.min_fill_quantity ? { min_fill_quantity: q.min_fill_quantity } : {}),
  }));

  const action = {
    accept_quote: {
      rfq_id: rfqId,
      market_id: marketId,
      margin,
      direction: "long",
      quantity,
      worst_price: worstPrice.toFixed(3),
      quotes: contractQuotes,
      unfilled_action: {
        limit: { price: worstPrice.toFixed(3) },
      },
      cid: `fe-rfq-grpc-${uuidv4()}`,
    },
  };

  const msg = MsgExecuteContractCompat.fromJSON({
    sender: TAKER_ADDRESS,
    contractAddress: CONTRACT_ADDRESS,
    msg: action,
    funds: [],
  });

  const broadcaster = new MsgBroadcasterWithPk({
    privateKey: RETAIL_PRIVATE_KEY,
    network: NETWORK,
    chainId: CHAIN_ID as ChainId,
    endpoints: ENDPOINTS,
    simulateTx: true,
    gasBufferCoefficient: 1.2,
  });

  const tx = await broadcaster.broadcast({ msgs: [msg] });
  console.log("✅ Quote accepted");
  console.log("TxHash:", tx.txHash);
}

/* -------------------------------------------------------------------------- */
/*                                   MAIN                                     */
/* -------------------------------------------------------------------------- */

async function main() {
  const useSsl =
    !GRPC_ENDPOINT.startsWith("localhost") &&
    !GRPC_ENDPOINT.startsWith("127.0.0.1");

  const credentials = useSsl
    ? grpc.credentials.createSsl()
    : grpc.credentials.createInsecure();

  const client = new InjectiveRfqRPC(GRPC_ENDPOINT, credentials);

  console.log("\n🔌 Connecting to gRPC TakerStream...");
  console.log(`   Endpoint: ${GRPC_ENDPOINT}`);
  console.log(`   Taker:    ${TAKER_ADDRESS}`);

  // ── Step 1: Open bidirectional TakerStream ─────────────────────────────
  // The indexer identifies the taker via the request_address metadata header.
  const metadata = new grpc.Metadata();
  metadata.add("request_address", TAKER_ADDRESS);

  const takerStream = client.TakerStream(metadata);
  const receivedQuotes: CollectedQuote[] = [];
  let ackResolve: ((id: number) => void) | null = null;
  let ackReject: ((err: Error) => void) | null = null;

  takerStream.on("data", (response: any) => {
    switch (response.message_type) {
      case "pong":
        return;
      case "request_ack": {
        const ack = response.request_ack;
        const id = Number(ack.rfq_id);
        console.log(`📬 Request ACK: RFQ#${id} status=${ack.status}`);
        ackResolve?.(id);
        ackResolve = null;
        ackReject = null;
        return;
      }
      case "quote": {
        const q = response.quote;
        console.log(
          `📩 Quote received | price=${q.price} maker=${q.maker} rfq_id=${q.rfq_id}`
        );
        const expiryTs = q.expiry?.timestamp
          ? Number(q.expiry.timestamp)
          : undefined;
        const expiryH = q.expiry?.height ? Number(q.expiry.height) : undefined;
        receivedQuotes.push({
          maker: q.maker,
          margin: q.margin,
          price: q.price,
          quantity: q.quantity,
          expiry: { ts: expiryTs, h: expiryH },
          signature: q.signature,
          sign_mode: q.sign_mode,
          evm_chain_id: q.evm_chain_id ? Number(q.evm_chain_id) : undefined,
          maker_subaccount_nonce: q.maker_subaccount_nonce
            ? Number(q.maker_subaccount_nonce)
            : 0,
          min_fill_quantity: q.min_fill_quantity || undefined,
        });
        return;
      }
      case "error": {
        const e = response.error;
        const msg = formatStreamError(e);
        console.error("❌ Stream error:", msg);
        ackReject?.(new Error(msg));
        ackResolve = null;
        ackReject = null;
        return;
      }
      default:
        console.log("ℹ️  Unhandled message_type:", response.message_type);
    }
  });

  takerStream.on("error", (err: any) => {
    console.error("❌ TakerStream error:", err.message);
    ackReject?.(err);
    ackResolve = null;
    ackReject = null;
  });

  console.log("📡 TakerStream open");

  // ── Step 2: Send RFQ request over the stream ───────────────────────────
  const clientId = uuidv4();
  const expiryMs = Date.now() + 5 * 60 * 1000; // 5 minutes

  const ackPromise = new Promise<number>((resolve, reject) => {
    ackResolve = resolve;
    ackReject = reject;
    setTimeout(() => reject(new Error("Timed out waiting for request_ack")), 5000);
  });

  console.log(`\n📤 Sending RFQ request (client_id=${clientId})...`);
  takerStream.write({
    message_type: "request",
    request: {
      client_id: clientId,
      market_id: INJUSDT_MARKET_ID,
      direction: "long",
      margin,
      quantity,
      worst_price: parseFloat(maxAcceptablePrice.toFixed(3)).toString(),
      expiry: expiryMs,
    },
  });

  const rfqId = await ackPromise;

  // ── Step 3-4: Wait for quotes, then pick the best ─────────────────────
  const QUOTE_WAIT_MS = 2000;
  console.log(`\n⏳ Waiting ${QUOTE_WAIT_MS}ms for quotes...`);

  await new Promise((r) => setTimeout(r, QUOTE_WAIT_MS));

  const best = chooseBestQuotes(
    Number(quantity),
    receivedQuotes,
    maxAcceptablePrice
  );

  if (best.length === 0) {
    console.log("❌ No acceptable quotes received");
    takerStream.end();
    return;
  }

  console.log(`✅ ${receivedQuotes.length} quote(s) received, using ${best.length} quote(s)`);
  for (const q of best) {
    console.log(`   price=${q.price} maker=${q.maker}`);
  }

  // ── Step 5: Accept quote on-chain ──────────────────────────────────────
  await acceptQuote(maxAcceptablePrice, rfqId, INJUSDT_MARKET_ID, best);

  takerStream.end();
  console.log("\n🔌 gRPC stream closed");
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
