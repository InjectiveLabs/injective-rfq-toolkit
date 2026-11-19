/**
 * RFQ pricer (gRPC): read indicative RFQ prices without sending an RFQ.
 *
 * Fetches the latest snapshot of each market, then follows the pricing stream.
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
).split(",");

type Tier = { notional_usd: string; bid: string; ask: string; age_ms: string };
type Snapshot = { market_id: string; ts: string; mark_price: string; tiers: Tier[] };

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

function getPricing(client: any, marketId: string): Promise<Snapshot | null> {
  return new Promise((resolve, reject) => {
    client.GetPricing({ market_id: marketId }, (err: grpc.ServiceError | null, res: Snapshot) => {
      if (err?.code === grpc.status.NOT_FOUND) return resolve(null);
      if (err) return reject(err);
      resolve(res);
    });
  });
}

async function main(): Promise<void> {
  const isLocal = endpoint.startsWith("localhost") || endpoint.startsWith("127.0.0.1");
  const client = new InjectiveRfqPricerRPC(
    endpoint,
    isLocal ? grpc.credentials.createInsecure() : grpc.credentials.createSsl()
  );

  for (const marketId of marketIds) {
    const snapshot = await getPricing(client, marketId);
    if (snapshot === null) {
      console.log(`${marketId} no snapshot`);
      continue;
    }
    show(snapshot);
  }

  console.log("streaming, Ctrl+C to stop");
  const stream = client.PricingStream({ market_ids: marketIds });
  stream.on("data", show);
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
