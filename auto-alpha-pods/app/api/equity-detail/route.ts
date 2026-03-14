import { NextRequest, NextResponse } from "next/server";

const YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote";
const YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart";
const YFINANCE_BACKEND_URL =
  process.env.YFINANCE_BACKEND_URL ||
  (process.env.NODE_ENV === "production"
    ? "https://falsemarkets.onrender.com"
    : "http://localhost:8000");

const RANGE: Record<string, { range: string; interval: string }> = {
  "1D": { range: "1d", interval: "5m" },
  "1W": { range: "5d", interval: "30m" },
  "1M": { range: "1mo", interval: "1d" },
  "3M": { range: "3mo", interval: "1d" },
  "1Y": { range: "1y", interval: "1wk" },
};

function num(value: unknown, fallback = 0): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const symbol = (searchParams.get("symbol") || "").trim().toUpperCase();
  const range = searchParams.get("range") || "1D";

  if (!symbol) {
    return NextResponse.json({ error: "symbol required" }, { status: 400 });
  }

  const { range: yahooRange, interval } = RANGE[range] ?? RANGE["1D"];

  try {
    // Preferred path: local backend yfinance endpoint.
    try {
      const yfRes = await fetch(
        `${YFINANCE_BACKEND_URL.replace(
          /\/$/,
          ""
        )}/yfinance/detail?symbol=${encodeURIComponent(
          symbol
        )}&range=${encodeURIComponent(range)}`,
        {
          headers: { Accept: "application/json" },
          cache: "no-store",
        }
      );

      if (yfRes.ok) {
        const yfData = await yfRes.json();
        if (yfData && !yfData.error) {
          return NextResponse.json(yfData);
        }
      }
    } catch {
      // fall through to Yahoo Finance direct API path
    }

    const headers = {
      Accept: "application/json",
      "User-Agent": "FalseMarkets/1.0",
    };

    const [quoteRes, chartRes] = await Promise.all([
      fetch(`${YAHOO_QUOTE_URL}?symbols=${encodeURIComponent(symbol)}`, {
        headers,
        next: { revalidate: 60 },
      }),
      fetch(
        `${YAHOO_CHART_URL}/${encodeURIComponent(
          symbol
        )}?range=${yahooRange}&interval=${interval}&includePrePost=false`,
        {
          headers,
          next: { revalidate: 60 },
        }
      ),
    ]);

    if (!quoteRes.ok) {
      throw new Error(`Yahoo quote ${quoteRes.status}`);
    }
    if (!chartRes.ok) {
      throw new Error(`Yahoo chart ${chartRes.status}`);
    }

    const quoteJson = await quoteRes.json();
    const chartJson = await chartRes.json();

    const quote = quoteJson?.quoteResponse?.result?.[0];
    const chartResult = chartJson?.chart?.result?.[0];

    if (!quote || !chartResult) {
      return NextResponse.json(
        { error: "No equity data found for symbol" },
        { status: 404 }
      );
    }

    const timestamps: number[] = Array.isArray(chartResult.timestamp)
      ? chartResult.timestamp
      : [];
    const closes: unknown[] =
      chartResult.indicators?.quote?.[0]?.close &&
      Array.isArray(chartResult.indicators.quote[0].close)
        ? chartResult.indicators.quote[0].close
        : [];

    const prices: [number, number][] = [];
    const points = Math.min(timestamps.length, closes.length);
    for (let i = 0; i < points; i++) {
      const ts = num(timestamps[i], 0);
      const close = num(closes[i], Number.NaN);
      if (ts > 0 && Number.isFinite(close)) {
        prices.push([ts * 1000, close]);
      }
    }

    const currentPrice =
      num(quote.regularMarketPrice, Number.NaN) ||
      (prices.length ? prices[prices.length - 1][1] : 0);

    return NextResponse.json({
      id: symbol.toLowerCase(),
      name: quote.longName || quote.shortName || symbol,
      symbol,
      image: null,
      current_price: num(currentPrice, 0),
      price_change_24h: num(quote.regularMarketChange, 0),
      price_change_percentage_24h: num(quote.regularMarketChangePercent, 0),
      market_cap: num(quote.marketCap, 0),
      total_volume: num(quote.regularMarketVolume, 0),
      high_24h: num(quote.regularMarketDayHigh, 0),
      low_24h: num(quote.regularMarketDayLow, 0),
      ath: num(quote.fiftyTwoWeekHigh, 0),
      atl: num(quote.fiftyTwoWeekLow, 0),
      last_updated: new Date(
        num(quote.regularMarketTime, Date.now() / 1000) * 1000
      ).toISOString(),
      prices,
      asset_type: "equity",
    });
  } catch (err) {
    console.error("equity-detail error:", err);
    return NextResponse.json(
      { error: "Failed to fetch equity data" },
      { status: 500 }
    );
  }
}
