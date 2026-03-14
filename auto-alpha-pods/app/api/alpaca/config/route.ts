import { NextResponse } from "next/server";
import { getAlpacaServerConfig, hasAlpacaCredentials } from "@/lib/alpaca";

export const runtime = "nodejs";

export async function GET() {
  const { orderUrl } = getAlpacaServerConfig();
  return NextResponse.json({
    configured: hasAlpacaCredentials(),
    orderUrl,
  });
}
