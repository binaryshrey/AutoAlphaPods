import { NextRequest, NextResponse } from "next/server";

const BASE = "https://api.coingecko.com/api/v3";

// CoinGecko free tier only supports interval=daily (or auto via no param).
// Auto-granularity: days=1 → ~5-min, days≤90 → hourly, days>90 → daily.
const RANGE: Record<string, { days: string }> = {
  "1D": { days: "1" },
  "1W": { days: "7" },
  "1M": { days: "30" },
  "3M": { days: "90" },
  "1Y": { days: "365" },
};

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const coinId = searchParams.get("coinId");
  const range = searchParams.get("range") || "1D";

  if (!coinId) {
    return NextResponse.json({ error: "coinId required" }, { status: 400 });
  }

  const { days } = RANGE[range] ?? RANGE["1D"];

  try {
    const headers = {
      Accept: "application/json",
      "User-Agent": "FalseMarkets/1.0",
    };

    // Fetch coin stats and chart data; chart may 429 on free tier — that's OK
    const [coinRes, chartRes] = await Promise.all([
      fetch(
        `${BASE}/coins/${coinId}?localization=false&tickers=false&community_data=false&developer_data=false`,
        { headers, next: { revalidate: 60 } }
      ),
      fetch(
        `${BASE}/coins/${coinId}/market_chart?vs_currency=usd&days=${days}`,
        { headers, next: { revalidate: 60 } }
      ),
    ]);

    if (!coinRes.ok) {
      throw new Error(`CoinGecko coin ${coinRes.status}`);
    }

    const coin = await coinRes.json();
    const chart = chartRes.ok ? await chartRes.json() : { prices: [] };

    const md = coin.market_data ?? {};

    return NextResponse.json({
      id: coin.id,
      name: coin.name,
      symbol: (coin.symbol ?? "").toUpperCase(),
      image: coin.image?.large ?? coin.image?.small ?? null,
      current_price: md.current_price?.usd ?? 0,
      price_change_24h: md.price_change_24h ?? 0,
      price_change_percentage_24h: md.price_change_percentage_24h ?? 0,
      market_cap: md.market_cap?.usd ?? 0,
      total_volume: md.total_volume?.usd ?? 0,
      high_24h: md.high_24h?.usd ?? 0,
      low_24h: md.low_24h?.usd ?? 0,
      ath: md.ath?.usd ?? 0,
      atl: md.atl?.usd ?? 0,
      last_updated: md.last_updated ?? new Date().toISOString(),
      prices: (chart.prices ?? []) as [number, number][],
    });
  } catch (err) {
    console.error("coin-detail error:", err);
    return NextResponse.json(
      { error: "Failed to fetch coin data" },
      { status: 500 }
    );
  }
}
