import { NextResponse } from "next/server";

const BACKEND_URL =
  process.env.YFINANCE_BACKEND_URL ||
  (process.env.NODE_ENV === "production"
    ? "https://autoalphapods-1.onrender.com"
    : "http://localhost:8000");

export const runtime = "nodejs";

export async function GET() {
  try {
    const res = await fetch(
      `${BACKEND_URL.replace(/\/$/, "")}/equity/dashboard`,
      {
        headers: { Accept: "application/json" },
        cache: "no-store",
      },
    );

    if (!res.ok) {
      throw new Error(`equity dashboard ${res.status}`);
    }

    const payload = await res.json();
    return NextResponse.json(payload);
  } catch (err) {
    console.error("equity dashboard route error:", err);
    return NextResponse.json(
      {
        cards: [],
        sectors: [],
        indicators: [],
        strip: [],
        earnings: [],
        news: [],
      },
      { status: 500 },
    );
  }
}
