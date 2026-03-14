import { NextResponse } from "next/server";

interface RawMarket {
  id: string;
  question?: string;
  outcomes?: string | string[];
  outcomePrices?: string | string[];
  volume?: string | number;
  endDate?: string;
  image?: string;
  active?: boolean;
  closed?: boolean;
}

interface RawEvent {
  id: string;
  slug?: string;
  title?: string;
  active?: boolean;
  closed?: boolean;
  endDate?: string;
  image?: string;
  volume?: string | number;
  markets?: RawMarket[];
}

function parseField<T>(value: T | string | undefined): T | undefined {
  if (typeof value === "string") {
    try {
      return JSON.parse(value) as T;
    } catch {
      return undefined;
    }
  }
  return value as T;
}

export async function GET() {
  try {
    const response = await fetch(
      "https://gamma-api.polymarket.com/events?active=true&closed=false&limit=12",
      {
        headers: { Accept: "application/json" },
        next: { revalidate: 120 },
      }
    );

    if (!response.ok) {
      throw new Error(`Polymarket ${response.status}`);
    }

    const raw: RawEvent[] = await response.json();

    const markets = raw
      .filter((e) => e.active && !e.closed && (e.title || e.markets?.[0]?.question))
      .slice(0, 6)
      .map((e) => {
        const firstMarket = e.markets?.[0];
        const outcomes =
          parseField<string[]>(firstMarket?.outcomes) ?? ["Yes", "No"];
        const outcomePrices =
          parseField<string[]>(firstMarket?.outcomePrices) ?? ["0.5", "0.5"];
        return {
          id: e.id,
          slug: e.slug ?? null,
          question: e.title ?? firstMarket?.question ?? "",
          outcomes,
          outcomePrices,
          volume: String(e.volume ?? firstMarket?.volume ?? "0"),
          endDate: e.endDate ?? null,
          image: e.image ?? null,
        };
      });

    return NextResponse.json(markets);
  } catch (error) {
    console.error("Prediction market API error:", error);
    return NextResponse.json(
      { error: "Failed to fetch prediction markets" },
      { status: 500 }
    );
  }
}
