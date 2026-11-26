/**
 * RFQ pricer (gRPC): read indicative RFQ prices without sending an RFQ.
 *
 * Opens the pricing stream with the current snapshot of each market in the
 * first message, then follows the updates.
 * Public and read-only: no keys, no auth.
 *
 * Env:
 *   PRICER_GRPC_ENDPOINT  default rfq-pricer.grpc.injective.network:443 (mainnet)
 *   PRICER_MARKET_IDS     comma-separated market IDs, default mainnet BTC/USDC PERP
 */

import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";

dotenv.config();

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const packageDefinition = protoLoader.loadSync(
  path.resolve(__dirname, "../../src/rfq_test/proto/injective_rfq_pricer_rpc.proto"),
  { keepCase: true, longs: String, defaults: true }
);
const proto = grpc.loadPackageDefinition(packageDefinition);
const InjectiveRfqPricerRPC = (proto.injective_rfq_pricer_rpc as any)
  .InjectiveRfqPricerRPC;

const endpoint =
  process.env.PRICER_GRPC_ENDPOINT ?? "rfq-pricer.grpc.injective.network:443";
const marketIds = (
  process.env.PRICER_MARKET_IDS ??
  "0x0ee7ca44147bab6ec81ac293b5fe7915488e612af59964b2d663d6008d861dee"
)
  .split(",")
  .map((id) => id.trim())
  .filter((id) => id !== "");

type Tier = { notional_usd: string; bid: string; ask: string; age_ms: string };
type Snapshot = { market_id: string; ts: string; mark_price: string; tiers: Tier[] };
type StreamMessage = Snapshot & { snapshots: Snapshot[] };

function show(s: Snapshot): void {
  if (s.tiers.length === 0) {
    console.log(`${s.market_id} ts=${s.ts} withdrawn (no current pricing)`);
    return;
  }
  console.log(`${s.market_id} ts=${s.ts} mark=${s.mark_price}`);
  for (const t of s.tiers) {
    console.log(
      `  $${t.notional_usd}: bid=${t.bid || "-"} ask=${t.ask || "-"} age=${t.age_ms}ms`
    );
  }
}

async function main(): Promise<void> {
  const isLocal = endpoint.startsWith("localhost") || endpoint.startsWith("127.0.0.1");
  const client = new InjectiveRfqPricerRPC(
    endpoint,
    isLocal ? grpc.credentials.createInsecure() : grpc.credentials.createSsl()
  );

  const stream = client.PricingStream({ market_ids: marketIds, with_snapshot: true });
  let first = true;
  stream.on("data", (message: StreamMessage) => {
    if (!first) {
      show(message);
      return;
    }
    first = false;
    const priced = new Set(message.snapshots.map((s) => s.market_id.toLowerCase()));
    message.snapshots.forEach(show);
    for (const marketId of marketIds) {
      if (!priced.has(marketId.toLowerCase())) console.log(`${marketId} no snapshot`);
    }
    console.log("streaming, Ctrl+C to stop");
  });
  stream.on("error", (err: grpc.ServiceError) => {
    console.error(`PricingStream: ${err.message}`);
    process.exit(1);
  });
  stream.on("end", () => process.exit(0));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
