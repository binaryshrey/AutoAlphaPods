import { NextRequest, NextResponse } from "next/server";

const COINGECKO_BASE = "https://api.coingecko.com/api/v3";

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const endpoint = searchParams.get("endpoint") || "coins/markets";

  const cgParams = new URLSearchParams();
  searchParams.forEach((value, key) => {
    if (key !== "endpoint") cgParams.set(key, value);
  });

  try {
    const url = `${COINGECKO_BASE}/${endpoint}?${cgParams.toString()}`;
    const response = await fetch(url, {
      headers: {
        Accept: "application/json",
        "User-Agent": "FalseMarkets/1.0",
      },
      next: { revalidate: 60 },
    });

    if (!response.ok) {
      throw new Error(`CoinGecko ${response.status}`);
    }

    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error("Crypto API error:", error);
    return NextResponse.json(
      { error: "Failed to fetch crypto data" },
      { status: 500 }
    );
  }
}
