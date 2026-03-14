import { NextRequest, NextResponse } from "next/server";
import { getAlpacaServerConfig, hasAlpacaCredentials } from "@/lib/alpaca";

export const runtime = "nodejs";

type Side = "buy" | "sell";
type OrderType = "market" | "limit";
type TimeInForce = "gtc" | "ioc";

interface OrderRequestBody {
  symbol?: string;
  side?: Side;
  type?: OrderType;
  timeInForce?: TimeInForce;
  qty?: number;
  limitPrice?: number;
}

function isValidSide(value: string): value is Side {
  return value === "buy" || value === "sell";
}

function isValidType(value: string): value is OrderType {
  return value === "market" || value === "limit";
}

function isValidTif(value: string): value is TimeInForce {
  return value === "gtc" || value === "ioc";
}

async function parseJsonSafe(res: Response) {
  const raw = await res.text();
  if (!raw) return {};
  try {
    return JSON.parse(raw) as Record<string, unknown>;
  } catch {
    return {};
  }
}

export async function POST(req: NextRequest) {
  if (!hasAlpacaCredentials()) {
    return NextResponse.json(
      {
        error:
          "Alpaca API credentials are not configured. Add APCA_API_KEY_ID and APCA_API_SECRET_KEY.",
      },
      { status: 400 }
    );
  }

  let body: OrderRequestBody;
  try {
    body = (await req.json()) as OrderRequestBody;
  } catch {
    return NextResponse.json({ error: "Invalid JSON request body." }, { status: 400 });
  }

  const symbol = (body.symbol || "").trim().toUpperCase();
  const side = body.side;
  const type = body.type;
  const timeInForce = body.timeInForce;
  const qty = Number(body.qty);
  const limitPrice =
    body.limitPrice !== undefined ? Number(body.limitPrice) : undefined;

  if (!symbol) {
    return NextResponse.json({ error: "symbol is required." }, { status: 400 });
  }
  if (!side || !isValidSide(side)) {
    return NextResponse.json({ error: "side must be buy or sell." }, { status: 400 });
  }
  if (!type || !isValidType(type)) {
    return NextResponse.json(
      { error: "type must be market or limit." },
      { status: 400 }
    );
  }
  if (!timeInForce || !isValidTif(timeInForce)) {
    return NextResponse.json(
      { error: "timeInForce must be gtc or ioc." },
      { status: 400 }
    );
  }
  if (!Number.isFinite(qty) || qty <= 0) {
    return NextResponse.json({ error: "qty must be greater than 0." }, { status: 400 });
  }
  if (type === "limit" && (limitPrice == null || !Number.isFinite(limitPrice) || limitPrice <= 0)) {
    return NextResponse.json(
      { error: "limitPrice must be greater than 0 for limit orders." },
      { status: 400 }
    );
  }

  const { keyId, secretKey, orderUrl } = getAlpacaServerConfig();
  const endpoint = orderUrl;
  const headers = {
    Accept: "application/json",
    "Content-Type": "application/json",
    "APCA-API-KEY-ID": keyId as string,
    "APCA-API-SECRET-KEY": secretKey as string,
  };
  const payload: Record<string, string> = {
    symbol,
    side,
    type,
    time_in_force: timeInForce,
    qty: qty.toString(),
  };

  if (type === "limit" && limitPrice !== undefined) {
    payload.limit_price = limitPrice.toString();
  }

  try {
    const res = await fetch(endpoint, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
      cache: "no-store",
    });

    const data = await parseJsonSafe(res);

    if (!res.ok) {
      const errorMessage =
        typeof data.message === "string"
          ? data.message
          : `Alpaca order failed (${res.status}).`;
      return NextResponse.json({ error: errorMessage }, { status: res.status });
    }

    return NextResponse.json({
      order: {
        id: data.id,
        status: data.status,
        symbol: data.symbol,
        side: data.side,
        type: data.type,
        time_in_force: data.time_in_force,
        qty: data.qty,
        limit_price: data.limit_price,
        submitted_at: data.submitted_at,
      },
    });
  } catch (err) {
    const message =
      err instanceof Error ? err.message : "Failed to place Alpaca order.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
