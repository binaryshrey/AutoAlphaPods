import { NextResponse } from "next/server";

const YFINANCE_BACKEND_URL =
  process.env.YFINANCE_BACKEND_URL ||
  (process.env.NODE_ENV === "production"
    ? "https://autoalphapods-1.onrender.com"
    : "http://localhost:8000");

export const runtime = "nodejs";

export async function GET() {
  try {
    const res = await fetch(
      `${YFINANCE_BACKEND_URL.replace(/\/$/, "")}/yfinance/commodities`,
      {
        headers: { Accept: "application/json" },
        cache: "no-store",
      },
    );

    if (!res.ok) {
      throw new Error(`yfinance commodities ${res.status}`);
    }

    const payload = await res.json();
    return NextResponse.json(payload);
  } catch (err) {
    console.error("commodities route error:", err);
    return NextResponse.json({ items: [], strip: [] }, { status: 500 });
  }
}
