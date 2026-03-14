import { NextRequest, NextResponse } from "next/server";

const YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote";
const YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart";

const ASSET_SYMBOLS: Record<string, string[]> = {
  crypto: [
    "BTC-USD",
    "ETH-USD",
    "USDT-USD",
    "BNB-USD",
    "XRP-USD",
    "USDC-USD",
    "SOL-USD",
    "TRX-USD",
    "ADA-USD",
    "DOGE-USD",
  ],
  "fixed-income": [
    "TLT",
    "IEF",
    "SHY",
    "LQD",
    "HYG",
    "BND",
    "TIP",
    "AGG",
    "BIL",
    "VGSH",
  ],
  equity: [
    "AAPL",
    "MSFT",
    "NVDA",
    "AMZN",
    "GOOGL",
    "META",
    "TSLA",
    "BRK-B",
    "JPM",
    "V",
  ],
  commodity: [
    "GLD",
    "SLV",
    "USO",
    "UNG",
    "DBC",
    "DBA",
    "PPLT",
    "CPER",
    "WEAT",
    "CORN",
  ],
};

function num(value: unknown, fallback = 0): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function extractSparkline(chartResult: any): number[] {
  const closes: unknown[] =
    chartResult?.indicators?.quote?.[0]?.close &&
    Array.isArray(chartResult.indicators.quote[0].close)
      ? chartResult.indicators.quote[0].close
      : [];

  return closes
    .map((v) => num(v, Number.NaN))
    .filter((v) => Number.isFinite(v))
    .slice(-30);
}

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const asset = (searchParams.get("asset") || "crypto").toLowerCase();
  const symbols = ASSET_SYMBOLS[asset] ?? ASSET_SYMBOLS.crypto;

  try {
    const headers = {
      Accept: "application/json",
      "User-Agent": "FalseMarkets/1.0",
    };

    const quoteRes = await fetch(
      `${YAHOO_QUOTE_URL}?symbols=${encodeURIComponent(symbols.join(","))}`,
      {
        headers,
        next: { revalidate: 60 },
      },
    );

    if (!quoteRes.ok) {
      throw new Error(`Yahoo quote ${quoteRes.status}`);
    }

    const quoteJson = await quoteRes.json();
    const quotes: any[] = Array.isArray(quoteJson?.quoteResponse?.result)
      ? quoteJson.quoteResponse.result
      : [];

    const quoteBySymbol = new Map(
      quotes
        .filter((q) => q?.symbol)
        .map((q) => [String(q.symbol).toUpperCase(), q]),
    );

    const chartResults = await Promise.all(
      symbols.map(async (symbol) => {
        try {
          const chartRes = await fetch(
            `${YAHOO_CHART_URL}/${encodeURIComponent(
              symbol,
            )}?range=5d&interval=30m&includePrePost=false`,
            {
              headers,
              next: { revalidate: 60 },
            },
          );

          if (!chartRes.ok) {
            return { symbol, sparkline: [] as number[] };
          }

          const chartJson = await chartRes.json();
          const chartResult = chartJson?.chart?.result?.[0];
          return {
            symbol,
            sparkline: extractSparkline(chartResult),
          };
        } catch {
          return { symbol, sparkline: [] as number[] };
        }
      }),
    );

    const sparklineBySymbol = new Map(
      chartResults.map((entry) => [
        entry.symbol.toUpperCase(),
        entry.sparkline,
      ]),
    );

    const items = symbols
      .map((symbol) => {
        const quote = quoteBySymbol.get(symbol.toUpperCase());
        if (!quote) return null;

        return {
          symbol: String(quote.symbol || symbol).toUpperCase(),
          name:
            quote.longName || quote.shortName || quote.displayName || symbol,
          price: num(quote.regularMarketPrice, 0),
          changePercent: num(quote.regularMarketChangePercent, 0),
          changeAbs: num(quote.regularMarketChange, 0),
          marketCap: num(quote.marketCap, 0),
          volume: num(quote.regularMarketVolume, 0),
          sparkline: sparklineBySymbol.get(symbol.toUpperCase()) ?? [],
        };
      })
      .filter(Boolean);

    return NextResponse.json({ items });
  } catch (err) {
    console.error("market-overview error:", err);
    return NextResponse.json(
      { error: "Failed to fetch market overview data" },
      { status: 500 },
    );
  }
}
