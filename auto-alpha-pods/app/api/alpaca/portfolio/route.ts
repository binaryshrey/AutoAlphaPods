import { NextResponse } from "next/server";
import { getAlpacaServerConfig, hasAlpacaCredentials } from "@/lib/alpaca";

export const runtime = "nodejs";

interface AnyObj {
  [key: string]: unknown;
}

function toNum(value: unknown, fallback = 0): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function parseJsonSafe(raw: string): AnyObj | AnyObj[] {
  if (!raw) return {};
  try {
    return JSON.parse(raw) as AnyObj | AnyObj[];
  } catch {
    return {};
  }
}

function getApiBase(orderUrl: string) {
  const u = new URL(orderUrl);
  u.pathname = "/v2";
  u.search = "";
  return u.toString().replace(/\/$/, "");
}

function toOrder(order: AnyObj) {
  return {
    id: String(order.id || ""),
    symbol: String(order.symbol || ""),
    side: String(order.side || ""),
    type: String(order.type || ""),
    status: String(order.status || ""),
    time_in_force: String(order.time_in_force || ""),
    qty: String(order.qty || ""),
    filled_qty: String(order.filled_qty || ""),
    limit_price:
      order.limit_price === null || order.limit_price === undefined
        ? null
        : String(order.limit_price),
    submitted_at: String(order.submitted_at || ""),
    filled_at: String(order.filled_at || ""),
  };
}

export async function GET() {
  if (!hasAlpacaCredentials()) {
    return NextResponse.json(
      { error: "Trading credentials are not configured." },
      { status: 400 }
    );
  }

  const { keyId, secretKey, orderUrl } = getAlpacaServerConfig();
  const base = getApiBase(orderUrl);
  const headers = {
    Accept: "application/json",
    "APCA-API-KEY-ID": keyId as string,
    "APCA-API-SECRET-KEY": secretKey as string,
  };

  const accountUrl = `${base}/account`;
  const positionsUrl = `${base}/positions`;
  const openOrdersUrl = `${base}/orders?status=open&limit=100&direction=desc&nested=true`;
  const closedOrdersUrl = `${base}/orders?status=closed&limit=100&direction=desc&nested=true`;

  try {
    const [accountRes, positionsRes, openOrdersRes, closedOrdersRes] =
      await Promise.all([
        fetch(accountUrl, { headers, cache: "no-store" }),
        fetch(positionsUrl, { headers, cache: "no-store" }),
        fetch(openOrdersUrl, { headers, cache: "no-store" }),
        fetch(closedOrdersUrl, { headers, cache: "no-store" }),
      ]);

    const accountRaw = await accountRes.text();
    const positionsRaw = await positionsRes.text();
    const openRaw = await openOrdersRes.text();
    const closedRaw = await closedOrdersRes.text();

    const accountData = parseJsonSafe(accountRaw) as AnyObj;
    const positionsData = parseJsonSafe(positionsRaw);
    const openOrdersData = parseJsonSafe(openRaw);
    const closedOrdersData = parseJsonSafe(closedRaw);

    if (!accountRes.ok) {
      const message =
        typeof accountData.message === "string"
          ? accountData.message
          : `Failed to fetch portfolio (${accountRes.status}).`;
      return NextResponse.json({ error: message }, { status: accountRes.status });
    }

    if (!Array.isArray(positionsData)) {
      return NextResponse.json(
        { error: "Failed to fetch positions." },
        { status: 500 }
      );
    }
    if (!Array.isArray(openOrdersData) || !Array.isArray(closedOrdersData)) {
      return NextResponse.json(
        { error: "Failed to fetch orders." },
        { status: 500 }
      );
    }

    const positions = positionsData.map((p) => {
      const costBasis = toNum(p.cost_basis);
      const unrealizedPl = toNum(p.unrealized_pl);
      return {
        symbol: String(p.symbol || ""),
        side: String(p.side || ""),
        qty: String(p.qty || ""),
        avg_entry_price: String(p.avg_entry_price || ""),
        market_value: String(p.market_value || ""),
        cost_basis: String(p.cost_basis || ""),
        unrealized_pl: String(p.unrealized_pl || ""),
        unrealized_plpc: toNum(p.unrealized_plpc, 0) * 100,
        unrealized_intraday_pl: String(p.unrealized_intraday_pl || ""),
        unrealized_intraday_plpc: toNum(p.unrealized_intraday_plpc, 0) * 100,
        allocation_pct: costBasis > 0 ? (costBasis / Math.max(1, toNum(accountData.equity))) * 100 : 0,
        _cost_basis_num: costBasis,
        _unrealized_pl_num: unrealizedPl,
      };
    });

    const unrealizedTotal = positions.reduce(
      (sum, p) => sum + (p._unrealized_pl_num || 0),
      0
    );
    const costBasisTotal = positions.reduce(
      (sum, p) => sum + (p._cost_basis_num || 0),
      0
    );

    const cleanedPositions = positions.map(
      ({ _cost_basis_num, _unrealized_pl_num, ...rest }) => rest
    );

    const pendingStatuses = new Set([
      "new",
      "accepted",
      "pending_new",
      "accepted_for_bidding",
      "pending_cancel",
      "pending_replace",
      "stopped",
      "calculated",
      "held",
    ]);

    const openOrders = openOrdersData.map(toOrder);
    const closedOrders = closedOrdersData.map(toOrder);
    const pending = openOrders.filter((o) => pendingStatuses.has(o.status));
    const partiallyFilled = [...openOrders, ...closedOrders].filter(
      (o) => o.status === "partially_filled"
    );
    const filled = closedOrders.filter((o) => o.status === "filled").slice(0, 25);

    const equity = toNum(accountData.equity);
    const lastEquity = toNum(accountData.last_equity);
    const dayPnl = equity - lastEquity;
    const dayPnlPct = lastEquity > 0 ? (dayPnl / lastEquity) * 100 : 0;

    return NextResponse.json({
      summary: {
        equity,
        last_equity: lastEquity,
        cash: toNum(accountData.cash),
        buying_power: toNum(accountData.buying_power),
        day_pnl: dayPnl,
        day_pnl_pct: dayPnlPct,
        unrealized_pnl_total: unrealizedTotal,
        unrealized_pnl_pct:
          costBasisTotal > 0 ? (unrealizedTotal / costBasisTotal) * 100 : 0,
        positions_count: cleanedPositions.length,
        pending_orders_count: pending.length,
        partially_filled_orders_count: partiallyFilled.length,
        filled_orders_count: filled.length,
      },
      account: {
        account_number: accountData.account_number,
        status: accountData.status,
        currency: accountData.currency,
      },
      positions: cleanedPositions,
      orders: {
        pending,
        partially_filled: partiallyFilled,
        filled,
      },
      fetched_at: new Date().toISOString(),
    });
  } catch (err) {
    const message =
      err instanceof Error ? err.message : "Failed to load portfolio.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

