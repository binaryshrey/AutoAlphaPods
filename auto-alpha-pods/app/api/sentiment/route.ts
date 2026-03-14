import { NextResponse } from "next/server";

interface FngEntry {
  value: string;
  value_classification: string;
  timestamp: string;
}

interface FngResponse {
  data: FngEntry[];
}

function toSentiment(value: number): {
  label: string;
  tone: "bearish" | "neutral" | "bullish";
} {
  if (value <= 25) return { label: "Bearish Sentiment", tone: "bearish" };
  if (value <= 44) return { label: "Bearish Sentiment", tone: "bearish" };
  if (value <= 55) return { label: "Neutral Sentiment", tone: "neutral" };
  if (value <= 75) return { label: "Bullish Sentiment", tone: "bullish" };
  return { label: "Bullish Sentiment", tone: "bullish" };
}

export async function GET() {
  try {
    const res = await fetch(
      "https://api.alternative.me/fng/?limit=1&format=json",
      {
        headers: { Accept: "application/json" },
        next: { revalidate: 3600 },
      }
    );

    if (!res.ok) throw new Error(`FNG ${res.status}`);

    const json: FngResponse = await res.json();
    const entry = json.data?.[0];
    if (!entry) throw new Error("No data");

    const value = parseInt(entry.value, 10);
    const { label, tone } = toSentiment(value);

    return NextResponse.json({
      value,
      classification: entry.value_classification,
      label,
      tone,
      timestamp: parseInt(entry.timestamp, 10) * 1000,
    });
  } catch (err) {
    console.error("Sentiment API error:", err);
    return NextResponse.json({ error: "Unavailable" }, { status: 500 });
  }
}
