import { NextResponse } from "next/server";

export const runtime = "nodejs";

interface CommodityNewsItem {
  title: string;
  link: string;
  tag: string;
  publisher: string;
  timeAgo: string;
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

function formatTimeAgo(dateRaw: string) {
  const dt = new Date(dateRaw);
  if (Number.isNaN(dt.getTime())) return "";
  const diffMs = Date.now() - dt.getTime();
  const minutes = Math.floor(diffMs / 60000);
  if (minutes < 60) return `${Math.max(minutes, 1)}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function detectTag(title: string) {
  const lower = title.toLowerCase();
  if (/gold|xau/.test(lower)) return "GOLD";
  if (/silver|xag/.test(lower)) return "SILVER";
  if (/crude|oil|wti/.test(lower)) return "OIL";
  if (/natural gas|nat gas|ng=/.test(lower)) return "NAT GAS";
  if (/copper|hg=/.test(lower)) return "COPPER";
  if (/platinum|xpt/.test(lower)) return "PLAT";
  if (/wheat|zw=/.test(lower)) return "WHEAT";
  if (/corn|zc=/.test(lower)) return "CORN";
  if (/soybean|soybeans|zs=/.test(lower)) return "SOY";
  if (/coffee|kc=/.test(lower)) return "COFFEE";
  return "COMMODITY";
}

function parseRss(xml: string): CommodityNewsItem[] {
  const itemMatches = [...xml.matchAll(/<item>([\s\S]*?)<\/item>/gi)];
  return itemMatches
    .slice(0, 10)
    .map((m) => m[1])
    .map((block) => {
      const title = getTagValue(block, "title");
      const link = getTagValue(block, "link");
      const pubDate = getTagValue(block, "pubDate");
      const source = getSource(block);
      return {
        title,
        link,
        tag: detectTag(title),
        publisher: source || "Google News",
        timeAgo: formatTimeAgo(pubDate) || "",
      };
    })
    .filter((item) => item.title && item.link);
}

export async function GET() {
  const rssUrl = new URL("https://news.google.com/rss/search");
  rssUrl.searchParams.set("q", "commodity markets");
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
    return NextResponse.json({ items });
  } catch (err) {
    console.error("commodity news error:", err);
    return NextResponse.json({ items: [] }, { status: 500 });
  }
}
