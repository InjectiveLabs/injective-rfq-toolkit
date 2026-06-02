import WebSocket from "ws";

const EVENT_TYPE = "wasm-rfq-accept-quote";

export interface ChainSettlementQuote {
  maker: string;
  margin: string;
  price: string;
  quantity: string;
  expiry?: { ts?: number; h?: number };
  signature: string;
  nonce?: number;
}

export interface ChainSettlementResult {
  maker: string;
  e?: string;
  error?: string;
  q?: string;
  m?: string;
}

export interface ChainSettlement {
  _contract_address?: string;
  rfq_id: number;
  market_id: string;
  taker: string;
  execution_mode?: string;
  direction: string;
  margin: string;
  quantity: string;
  worst_price: string;
  quotes: ChainSettlementQuote[];
  results: ChainSettlementResult[];
  unfilled_action?: unknown;
  fallback_quantity: string;
  fallback_margin: string;
  cid: string;
  height?: number;
  tx_hash?: string;
}

interface StreamOptions {
  endpoint: string;
  contractAddress: string;
  makerAddress?: string;
  onSettlement: (settlement: ChainSettlement) => void;
  onError?: (err: Error) => void;
}

export function cometWsUrl(endpoint: string): string {
  const withScheme = /^[a-z]+:\/\//i.test(endpoint) ? endpoint : `http://${endpoint}`;
  const url = new URL(withScheme);
  if (url.protocol === "http:") url.protocol = "ws:";
  if (url.protocol === "https:") url.protocol = "wss:";
  url.pathname = url.pathname.replace(/\/$/, "");
  if (!url.pathname.endsWith("/websocket")) {
    url.pathname = `${url.pathname}/websocket`;
  }
  return url.toString();
}

export function streamChainSettlements(options: StreamOptions): WebSocket {
  const ws = new WebSocket(cometWsUrl(options.endpoint));
  const query = `tm.event='Tx' AND ${EVENT_TYPE}._contract_address='${options.contractAddress}'`;

  ws.on("open", () => {
    console.log(`   Query:    ${query}`);
    ws.send(JSON.stringify({
      jsonrpc: "2.0",
      method: "subscribe",
      id: 1,
      params: { query },
    }));
  });

  ws.on("message", (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      const events = msg?.result?.events;
      if (!events) return;

      const settlement = settlementFromEvents(events);
      if (!settlement) return;
      if (
        options.makerAddress &&
        !settlement.quotes.some((q) => q.maker === options.makerAddress)
      ) {
        return;
      }
      options.onSettlement(settlement);
    } catch (err) {
      options.onError?.(err instanceof Error ? err : new Error(String(err)));
    }
  });

  ws.on("error", (err) => {
    options.onError?.(err instanceof Error ? err : new Error(String(err)));
  });

  return ws;
}

function firstEventValue(events: Record<string, string[]>, key: string): string {
  const values = events[key] || [];
  return values[0] || "";
}

function prefixedAttrs(events: Record<string, string[]>): Record<string, string> {
  const prefix = `${EVENT_TYPE}.`;
  const attrs: Record<string, string> = {};
  for (const [key, values] of Object.entries(events)) {
    if (key.startsWith(prefix) && values.length > 0) {
      attrs[key.slice(prefix.length)] = values[0];
    }
  }
  return attrs;
}

function parseJsonAttr<T>(attrs: Record<string, string>, key: string, fallback: T): T {
  const raw = attrs[key];
  if (!raw) return fallback;
  try {
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
}

function settlementFromEvents(events: Record<string, string[]>): ChainSettlement | null {
  for (const key of [
    `${EVENT_TYPE}.settlement`,
    `${EVENT_TYPE}.data`,
    `${EVENT_TYPE}.payload`,
  ]) {
    const raw = firstEventValue(events, key);
    if (raw) {
      const settlement = JSON.parse(raw) as ChainSettlement;
      settlement.height = Number(firstEventValue(events, "tx.height") || 0);
      settlement.tx_hash = firstEventValue(events, "tx.hash");
      return settlement;
    }
  }

  const attrs = prefixedAttrs(events);
  if (Object.keys(attrs).length === 0) return null;

  return {
    _contract_address: attrs._contract_address,
    rfq_id: Number(attrs.rfq_id || 0),
    market_id: attrs.market_id || "",
    taker: attrs.taker || "",
    execution_mode: attrs.execution_mode || "",
    direction: attrs.direction || "",
    margin: attrs.margin || "",
    quantity: attrs.quantity || "",
    worst_price: attrs.worst_price || "",
    quotes: parseJsonAttr(attrs, "quotes", []),
    results: parseJsonAttr(attrs, "results", []),
    unfilled_action: parseJsonAttr(attrs, "unfilled_action", undefined),
    fallback_quantity: attrs.fallback_quantity || "",
    fallback_margin: attrs.fallback_margin || "",
    cid: attrs.cid || "",
    height: Number(firstEventValue(events, "tx.height") || 0),
    tx_hash: firstEventValue(events, "tx.hash"),
  };
}
