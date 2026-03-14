import { NextRequest, NextResponse } from "next/server";

export const runtime = "nodejs";

interface NewsItem {
  title: string;
  link: string;
  source?: string;
  publishedAt?: string;
}

function decodeXmlEntities(input: string) {
  return input
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'");
}

function stripCdata(value: string) {
  return value
    .replace(/^<!\[CDATA\[/, "")
    .replace(/\]\]>$/, "")
    .trim();
}

function getTagValue(block: string, tag: string): string {
  const re = new RegExp(`<${tag}>([\\s\\S]*?)<\\/${tag}>`, "i");
  const match = block.match(re);
  if (!match?.[1]) return "";
  return decodeXmlEntities(stripCdata(match[1]));
}

function getSource(block: string): string {
  const match = block.match(/<source[^>]*>([\s\S]*?)<\/source>/i);
  if (!match?.[1]) return "";
  return decodeXmlEntities(stripCdata(match[1]));
}

function formatPublishedAt(dateRaw: string): string {
  if (!dateRaw) return "";
  const dt = new Date(dateRaw);
  if (Number.isNaN(dt.getTime())) return "";
  return dt.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
  });
}

function parseRss(xml: string): NewsItem[] {
  const itemMatches = [...xml.matchAll(/<item>([\s\S]*?)<\/item>/gi)];
  return itemMatches
    .slice(0, 5)
    .map((m) => m[1])
    .map((block) => {
      const title = getTagValue(block, "title");
      const link = getTagValue(block, "link");
      const pubDate = getTagValue(block, "pubDate");
      const source = getSource(block);

      return {
        title,
        link,
        source: source || undefined,
        publishedAt: formatPublishedAt(pubDate) || undefined,
      };
    })
    .filter((item) => item.title && item.link);
}

function buildFollowUps(query: string, items: NewsItem[]) {
  const subject = query.trim();
  if (/\bportfolio|positions|pnl|orders\b/i.test(subject)) {
    return [
      "Which position is contributing the most to my unrealized P/L right now?",
      "Show a risk reduction plan for my current open positions.",
      "Given my pending and filled orders, what should I monitor next?",
    ];
  }

  const headlineHint = items[0]?.title;
  const first = headlineHint
    ? `How could this headline impact ${subject}: "${headlineHint}"?`
    : `How could today's news affect ${subject} in the next 24 hours?`;

  return [
    first,
    `Give me bullish vs bearish takeaways for ${subject} from these headlines.`,
    `Based on this news, what is a cautious trade setup for ${subject}?`,
  ];
}

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const q = (searchParams.get("q") || "").trim();

  if (!q) {
    return NextResponse.json({ items: [], followUps: [] });
  }

  const lower = q.toLowerCase();
  const looksLikeTicker = /^[A-Z.\-]{1,6}$/.test(q);
  const cryptoHint =
    /\b(crypto|bitcoin|ethereum|btc|eth|sol|doge|xrp|cardano|altcoin)\b/.test(
      lower
    );

  const query = cryptoHint
    ? `${q} crypto`
    : looksLikeTicker
      ? `${q} stock`
      : q;
  const rssUrl = new URL("https://news.google.com/rss/search");
  rssUrl.searchParams.set("q", query);
  rssUrl.searchParams.set("hl", "en-US");
  rssUrl.searchParams.set("gl", "US");
  rssUrl.searchParams.set("ceid", "US:en");

  try {
    const res = await fetch(rssUrl.toString(), {
      headers: {
        Accept: "application/xml,text/xml",
        "User-Agent": "FalseMarkets/1.0",
      },
      next: { revalidate: 300 },
    });

    if (!res.ok) {
      throw new Error(`News ${res.status}`);
    }

    const xml = await res.text();
    const items = parseRss(xml);
    const followUps = buildFollowUps(q, items);

    return NextResponse.json({ items, followUps });
  } catch {
    return NextResponse.json({
      items: [],
      followUps: buildFollowUps(q, []),
    });
  }
}
