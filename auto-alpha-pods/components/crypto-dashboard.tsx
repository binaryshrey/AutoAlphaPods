"use client";

import { useState, useEffect, useRef, useCallback } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import dynamic from "next/dynamic";
import TradingDashboardModal from "@/components/trading-dashboard-modal";
import {
  Search,
  Send,
  X,
  RefreshCw,
  BarChart2,
  Zap,
  Sparkles,
  Globe,
  ArrowUpRight,
  ArrowDownRight,
  ExternalLink,
  TrendingUp,
  ChevronUp,
  ChevronDown,
} from "lucide-react";

// ApexCharts — no SSR
const ReactApexChart = dynamic(() => import("react-apexcharts"), {
  ssr: false,
  loading: () => <div className="w-20 h-9" />,
});

// ─── Types ────────────────────────────────────────────────────────────────────

interface CoinData {
  id: string;
  symbol: string;
  name: string;
  image: string;
  current_price: number;
  price_change_percentage_24h: number;
  price_change_24h: number;
  market_cap: number;
  total_volume: number;
  high_24h: number;
  low_24h: number;
  sparkline_in_7d?: { price: number[] };
}

interface GlobalData {
  total_market_cap: { usd: number };
  total_volume: { usd: number };
  market_cap_change_percentage_24h_usd: number;
  market_cap_percentage: { btc: number; eth: number };
}

interface MarketItem {
  symbol: string;
  name: string;
  price: number;
  changePercent: number;
  changeAbs: number;
  marketCap: number;
  volume: number;
  sparkline: number[];
}

interface SentimentData {
  value: number;
  classification: string;
  label: string;
  tone: "bearish" | "neutral" | "bullish";
  timestamp: number;
}

interface PredictionMarket {
  id: string;
  slug?: string | null;
  question?: string;
  outcomes: string[];
  outcomePrices: string[];
  volume: string;
  endDate: string | null;
  image: string | null;
}

interface CoinDetail {
  id: string;
  name: string;
  symbol: string;
  image: string | null;
  current_price: number;
  price_change_24h: number;
  price_change_percentage_24h: number;
  market_cap: number;
  total_volume: number;
  high_24h: number;
  low_24h: number;
  ath: number;
  atl: number;
  last_updated: string;
  prices: [number, number][];
  asset_type?: "crypto" | "equity";
}

interface CoinCardMeta {
  assetType: "crypto" | "equity";
  coinId?: string;
  symbol: string;
  name: string;
  range: string;
  loading: boolean;
  data?: CoinDetail;
  buyIntent?: boolean;
}

interface NewsItem {
  title: string;
  link: string;
  source?: string;
  publishedAt?: string;
}

interface CommodityItem {
  ticker: string;
  symbol: string;
  name: string;
  price: number;
  changePercent: number;
  changeAbs: number;
  sparkline: number[];
}

interface CommodityStripItem {
  id: string;
  label: string;
  price: number;
  changePercent: number;
}

interface CommodityNewsItem {
  title: string;
  link: string;
  tag: string;
  publisher: string;
  timeAgo: string;
}

interface PortfolioPosition {
  symbol: string;
  side: string;
  qty: string;
  avg_entry_price: string;
  market_value: string;
  cost_basis: string;
  unrealized_pl: string;
  unrealized_plpc: number;
  unrealized_intraday_pl: string;
  unrealized_intraday_plpc: number;
  allocation_pct: number;
}

interface PortfolioOrder {
  id: string;
  symbol: string;
  side: string;
  type: string;
  status: string;
  time_in_force: string;
  qty: string;
  filled_qty: string;
  limit_price: string | null;
  submitted_at: string;
  filled_at: string;
}

interface PortfolioData {
  summary: {
    equity: number;
    last_equity: number;
    cash: number;
    buying_power: number;
    day_pnl: number;
    day_pnl_pct: number;
    unrealized_pnl_total: number;
    unrealized_pnl_pct: number;
    positions_count: number;
    pending_orders_count: number;
    partially_filled_orders_count: number;
    filled_orders_count: number;
  };
  account: {
    account_number?: string;
    status?: string;
    currency?: string;
  };
  positions: PortfolioPosition[];
  orders: {
    pending: PortfolioOrder[];
    partially_filled: PortfolioOrder[];
    filled: PortfolioOrder[];
  };
  fetched_at: string;
}

interface PortfolioContextPayload {
  fetched_at: string;
  summary: PortfolioData["summary"];
  top_positions: Array<{
    symbol: string;
    qty: string;
    avg_entry_price: string;
    market_value: string;
    unrealized_pl: string;
    unrealized_plpc: number;
  }>;
  pending_orders: Array<{
    symbol: string;
    side: string;
    type: string;
    status: string;
    qty: string;
    filled_qty: string;
    limit_price: string | null;
  }>;
  partially_filled_orders: Array<{
    symbol: string;
    side: string;
    type: string;
    status: string;
    qty: string;
    filled_qty: string;
    limit_price: string | null;
  }>;
  recent_filled_orders: Array<{
    symbol: string;
    side: string;
    type: string;
    status: string;
    qty: string;
    filled_qty: string;
    limit_price: string | null;
  }>;
}

interface NewsContextPayload {
  fetched_from_message_id: string;
  headlines: Array<{
    title: string;
    source?: string;
    publishedAt?: string;
    link: string;
  }>;
}

interface ChatMessage {
  id: string;
  role: "user" | "assistant";
  content: string;
  coinCard?: CoinCardMeta;
  portfolio?: PortfolioData;
  portfolioLoading?: boolean;
  news?: NewsItem[];
  followUps?: string[];
  newsLoading?: boolean;
}

type AlpacaOrderSide = "buy" | "sell";
type AlpacaOrderType = "market" | "limit";
type AlpacaTimeInForce = "gtc" | "ioc";

interface AlpacaConfigResponse {
  configured: boolean;
  orderUrl?: string;
}

interface AlpacaPlacedOrder {
  id: string;
  status: string;
  symbol: string;
  side: string;
  type: string;
  time_in_force: string;
  qty: string;
  limit_price?: string | null;
  submitted_at?: string;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function fmt(price: number): string {
  if (price >= 1000)
    return `$${price.toLocaleString("en-US", { maximumFractionDigits: 0 })}`;
  if (price >= 1) return `$${price.toFixed(2)}`;
  if (price >= 0.01) return `$${price.toFixed(4)}`;
  return `$${price.toFixed(6)}`;
}

function fmtBig(value: number): string {
  if (value >= 1e12) return `$${(value / 1e12).toFixed(2)}T`;
  if (value >= 1e9) return `$${(value / 1e9).toFixed(2)}B`;
  if (value >= 1e6) return `$${(value / 1e6).toFixed(2)}M`;
  return `$${value.toFixed(0)}`;
}

function pct(n: number): string {
  return `${n >= 0 ? "+" : ""}${n.toFixed(2)}%`;
}

const DEFAULT_POSITION_NOTIONAL = 100;

const ASSET_TABS = [
  { label: "Crypto", value: "crypto" },
  { label: "Fixed Income", value: "fixed-income" },
  { label: "Equity", value: "equity" },
  { label: "Commodity", value: "commodity" },
] as const;

const COMMODITY_META: Record<
  string,
  { label: string; abbr: string; color: string; tint: string }
> = {
  XAU: {
    label: "Gold",
    abbr: "AU",
    color: "#f5c14b",
    tint: "rgba(245, 193, 75, 0.12)",
  },
  XAG: {
    label: "Silver",
    abbr: "AG",
    color: "#9ca3af",
    tint: "rgba(156, 163, 175, 0.12)",
  },
  WTI: {
    label: "Crude Oil",
    abbr: "WT",
    color: "#f97316",
    tint: "rgba(249, 115, 22, 0.12)",
  },
  NG: {
    label: "Nat Gas",
    abbr: "NG",
    color: "#22d3ee",
    tint: "rgba(34, 211, 238, 0.12)",
  },
  HG: {
    label: "Copper",
    abbr: "CU",
    color: "#fb923c",
    tint: "rgba(251, 146, 60, 0.12)",
  },
  XPT: {
    label: "Platinum",
    abbr: "PT",
    color: "#cbd5e1",
    tint: "rgba(203, 213, 225, 0.12)",
  },
  ZW: {
    label: "Wheat",
    abbr: "WH",
    color: "#fbbf24",
    tint: "rgba(251, 191, 36, 0.12)",
  },
  ZC: {
    label: "Corn",
    abbr: "CN",
    color: "#f59e0b",
    tint: "rgba(245, 158, 11, 0.12)",
  },
  ZS: {
    label: "Soybeans",
    abbr: "SB",
    color: "#84cc16",
    tint: "rgba(132, 204, 22, 0.12)",
  },
  KC: {
    label: "Coffee",
    abbr: "CF",
    color: "#a16207",
    tint: "rgba(161, 98, 7, 0.12)",
  },
};

type AssetTab = (typeof ASSET_TABS)[number]["value"];

function normalizeAssetTab(value: string | null): AssetTab {
  if (!value) return "crypto";
  const normalized = value.toLowerCase();
  const match = ASSET_TABS.find((tab) => tab.value === normalized);
  return match?.value ?? "crypto";
}

function formatQtyFromNotional(
  price: number,
  notional = DEFAULT_POSITION_NOTIONAL,
) {
  if (!Number.isFinite(price) || price <= 0) return "1";
  const qty = notional / price;
  if (!Number.isFinite(qty) || qty <= 0) return "1";
  if (qty >= 1) return qty.toFixed(4).replace(/\.?0+$/, "");
  return qty.toFixed(6).replace(/\.?0+$/, "");
}

function getCommodityMeta(symbol: string) {
  return (
    COMMODITY_META[symbol.toUpperCase()] ?? {
      label: symbol.toUpperCase(),
      abbr: symbol.slice(0, 2).toUpperCase(),
      color: "#94a3b8",
      tint: "rgba(148, 163, 184, 0.12)",
    }
  );
}

// ─── Sparkline ────────────────────────────────────────────────────────────────

function Sparkline({
  data,
  positive,
  width = 80,
  height = 36,
}: {
  data: number[];
  positive: boolean;
  width?: number;
  height?: number;
}) {
  const color = positive ? "#22c55e" : "#ef4444";
  const series = [{ data: data.filter(Boolean).slice(-30) }];
  const options = {
    chart: {
      type: "line" as const,
      sparkline: { enabled: true },
      animations: { enabled: false },
      background: "transparent",
    },
    stroke: { curve: "smooth" as const, width: 1.5 },
    tooltip: { enabled: false },
    colors: [color],
    grid: { show: false },
  };

  if (series[0].data.length < 2) return <div style={{ width, height }} />;

  return (
    <ReactApexChart
      type="line"
      series={series}
      options={options}
      width={width}
      height={height}
    />
  );
}

function buildSparklinePath(values: number[], width: number, height: number) {
  const points = values.filter((v) => Number.isFinite(v));
  if (points.length < 2) return "";
  const min = Math.min(...points);
  const max = Math.max(...points);
  const range = max - min || 1;
  return points
    .map((value, index) => {
      const x = (index / (points.length - 1)) * width;
      const y = height - ((value - min) / range) * height;
      return `${index === 0 ? "M" : "L"} ${x.toFixed(2)} ${y.toFixed(2)}`;
    })
    .join(" ");
}

function MiniSparkline({
  data,
  positive,
  width = 96,
  height = 40,
}: {
  data: number[];
  positive: boolean;
  width?: number;
  height?: number;
}) {
  const color = positive ? "#22c55e" : "#ef4444";
  const path = buildSparklinePath(data, width, height);

  if (!path) return <div style={{ width, height }} />;

  return (
    <svg
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      aria-hidden="true"
    >
      <path
        d={`${path} L ${width} ${height} L 0 ${height} Z`}
        fill={color}
        opacity={0.12}
      />
      <path d={path} fill="none" stroke={color} strokeWidth={1.6} />
    </svg>
  );
}

// ─── Card Sparkline (full-width area chart) ───────────────────────────────────

function CardSparkline({
  data,
  positive,
  height = 52,
}: {
  data: number[];
  positive: boolean;
  height?: number;
}) {
  const color = positive ? "#22c55e" : "#ef4444";
  const filtered = data.filter(Boolean).slice(-40);

  const series = [{ data: filtered }];
  const options = {
    chart: {
      type: "area" as const,
      sparkline: { enabled: true },
      animations: { enabled: false },
      background: "transparent",
    },
    stroke: { curve: "smooth" as const, width: 1.5 },
    fill: {
      type: "gradient",
      gradient: {
        shadeIntensity: 1,
        opacityFrom: 0.25,
        opacityTo: 0.02,
        stops: [0, 100],
        colorStops: [
          { offset: 0, color, opacity: 0.25 },
          { offset: 100, color, opacity: 0.02 },
        ],
      },
    },
    tooltip: { enabled: false },
    colors: [color],
    grid: { show: false },
  };

  if (filtered.length < 2) return <div style={{ height }} className="w-full" />;

  return (
    <ReactApexChart
      type="area"
      series={series}
      options={options}
      width="100%"
      height={height}
    />
  );
}

// ─── Markdown renderer ────────────────────────────────────────────────────────

function renderInline(text: string): React.ReactNode {
  // Split on **bold**, *italic*, `code` markers
  const tokens = text.split(/(\*\*[^*]+\*\*|\*[^*]+\*|`[^`]+`)/g);
  return (
    <>
      {tokens.map((token, i) => {
        if (token.startsWith("**") && token.endsWith("**"))
          return (
            <strong key={i} className="font-semibold text-white">
              {token.slice(2, -2)}
            </strong>
          );
        if (token.startsWith("*") && token.endsWith("*"))
          return (
            <em key={i} className="italic text-zinc-300">
              {token.slice(1, -1)}
            </em>
          );
        if (token.startsWith("`") && token.endsWith("`"))
          return (
            <code
              key={i}
              className="bg-white/10 rounded px-1 font-mono text-sm text-zinc-300"
            >
              {token.slice(1, -1)}
            </code>
          );
        return token;
      })}
    </>
  );
}

const SOURCE_URLS: Record<string, string> = {
  CoinGecko: "https://www.coingecko.com",
  "Yahoo Finance": "https://finance.yahoo.com",
  Alpaca: "https://alpaca.markets",
  Polymarket: "https://polymarket.com",
  "Alternative.me": "https://alternative.me/crypto/fear-and-greed-index/",
  OpenRouter: "https://openrouter.ai",
};

function makeMessageId(): string {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function buildPortfolioContext(
  portfolio?: PortfolioData,
): PortfolioContextPayload | undefined {
  if (!portfolio) return undefined;

  return {
    fetched_at: portfolio.fetched_at,
    summary: portfolio.summary,
    top_positions: portfolio.positions.slice(0, 12).map((p) => ({
      symbol: p.symbol,
      qty: p.qty,
      avg_entry_price: p.avg_entry_price,
      market_value: p.market_value,
      unrealized_pl: p.unrealized_pl,
      unrealized_plpc: p.unrealized_plpc,
    })),
    pending_orders: portfolio.orders.pending.slice(0, 20).map((o) => ({
      symbol: o.symbol,
      side: o.side,
      type: o.type,
      status: o.status,
      qty: o.qty,
      filled_qty: o.filled_qty,
      limit_price: o.limit_price,
    })),
    partially_filled_orders: portfolio.orders.partially_filled
      .slice(0, 20)
      .map((o) => ({
        symbol: o.symbol,
        side: o.side,
        type: o.type,
        status: o.status,
        qty: o.qty,
        filled_qty: o.filled_qty,
        limit_price: o.limit_price,
      })),
    recent_filled_orders: portfolio.orders.filled.slice(0, 20).map((o) => ({
      symbol: o.symbol,
      side: o.side,
      type: o.type,
      status: o.status,
      qty: o.qty,
      filled_qty: o.filled_qty,
      limit_price: o.limit_price,
    })),
  };
}

function buildNewsContext(
  messages: ChatMessage[],
): NewsContextPayload | undefined {
  const latestNewsMessage = [...messages]
    .reverse()
    .find(
      (m) =>
        m.role === "assistant" && Array.isArray(m.news) && m.news.length > 0,
    );

  if (!latestNewsMessage?.news?.length) return undefined;

  return {
    fetched_from_message_id: latestNewsMessage.id,
    headlines: latestNewsMessage.news.slice(0, 6).map((n) => ({
      title: n.title,
      source: n.source,
      publishedAt: n.publishedAt,
      link: n.link,
    })),
  };
}

function SourcesFooter({ raw }: { raw: string }) {
  // raw = "CoinGecko · Polymarket · OpenRouter"
  const sources = raw
    .split(/\s*·\s*/)
    .map((s) => s.trim())
    .filter(Boolean);
  return (
    <div className="flex flex-wrap items-center gap-1.5 pt-2 mt-1 border-t border-white/6">
      <span className="text-sm text-zinc-600">Sources:</span>
      {sources.map((src) => {
        const url = SOURCE_URLS[src];
        return url ? (
          <a
            key={src}
            href={url}
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-white/5 border border-white/8 text-sm text-zinc-400 hover:text-zinc-200 hover:border-white/16 transition-colors"
          >
            {src}
            <ExternalLink className="w-2 h-2 opacity-60" />
          </a>
        ) : (
          <span
            key={src}
            className="px-1.5 py-0.5 rounded-md bg-white/5 border border-white/8 text-sm text-zinc-400"
          >
            {src}
          </span>
        );
      })}
    </div>
  );
}

function MarkdownMessage({
  text,
  streaming,
}: {
  text: string;
  streaming: boolean;
}) {
  const lines = text.split("\n");
  const blocks: React.ReactNode[] = [];
  let listItems: string[] = [];
  let orderedItems: string[] = [];
  let sourcesRaw: string | null = null;

  const flushList = () => {
    if (listItems.length) {
      blocks.push(
        <ul
          key={blocks.length}
          className="list-disc list-inside space-y-0.5 text-zinc-300 text-sm"
        >
          {listItems.map((item, i) => (
            <li key={i}>{renderInline(item)}</li>
          ))}
        </ul>,
      );
      listItems = [];
    }
    if (orderedItems.length) {
      blocks.push(
        <ol
          key={blocks.length}
          className="list-decimal list-inside space-y-0.5 text-zinc-300 text-sm"
        >
          {orderedItems.map((item, i) => (
            <li key={i}>{renderInline(item)}</li>
          ))}
        </ol>,
      );
      orderedItems = [];
    }
  };

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    // Detect **Sources:** line (assistant appends this at the end)
    const srcMatch = line.match(/^\*\*Sources:\*\*\s*(.+)/);
    if (srcMatch) {
      flushList();
      sourcesRaw = srcMatch[1].trim();
      continue;
    }

    if (/^### /.test(line)) {
      flushList();
      blocks.push(
        <p
          key={blocks.length}
          className="text-sm font-semibold text-zinc-200 mt-2"
        >
          {renderInline(line.slice(4))}
        </p>,
      );
    } else if (/^## /.test(line)) {
      flushList();
      blocks.push(
        <p key={blocks.length} className="text-sm font-bold text-white mt-2">
          {renderInline(line.slice(3))}
        </p>,
      );
    } else if (/^# /.test(line)) {
      flushList();
      blocks.push(
        <p key={blocks.length} className="text-sm font-bold text-white mt-2">
          {renderInline(line.slice(2))}
        </p>,
      );
    } else if (/^> /.test(line)) {
      flushList();
      blocks.push(
        <blockquote
          key={blocks.length}
          className="border-l-2 border-zinc-600 pl-3 text-zinc-400 italic text-sm"
        >
          {renderInline(line.slice(2))}
        </blockquote>,
      );
    } else if (/^[-*] /.test(line)) {
      if (orderedItems.length) flushList();
      listItems.push(line.slice(2));
    } else if (/^\d+\. /.test(line)) {
      if (listItems.length) flushList();
      orderedItems.push(line.replace(/^\d+\. /, ""));
    } else if (line.trim() === "") {
      flushList();
    } else {
      flushList();
      blocks.push(
        <p
          key={blocks.length}
          className="text-sm text-zinc-300 leading-relaxed"
        >
          {renderInline(line)}
        </p>,
      );
    }
  }
  flushList();

  return (
    <div className="space-y-1.5">
      {blocks}
      {streaming && (
        <span className="inline-block w-0.5 h-3.5 bg-zinc-400 animate-pulse ml-0.5 align-middle" />
      )}
      {!streaming && sourcesRaw && <SourcesFooter raw={sourcesRaw} />}
    </div>
  );
}

function MessageContext({
  message,
  disabled,
  onFollowUp,
}: {
  message: ChatMessage;
  disabled: boolean;
  onFollowUp: (prompt: string) => void;
}) {
  const hasNews = Boolean(message.news?.length);
  const hasFollowUps = Boolean(message.followUps?.length);

  if (!message.newsLoading && !hasNews && !hasFollowUps) return null;

  return (
    <div className="mt-2.5 space-y-2">
      {message.newsLoading && (
        <div className="flex items-center gap-1.5 text-sm text-zinc-500">
          <RefreshCw className="w-2.5 h-2.5 animate-spin" />
          Fetching related news…
        </div>
      )}

      {hasNews && (
        <div className="rounded-lg border border-white/8 bg-white/[0.02] p-2.5">
          <p className="text-sm text-zinc-500 uppercase tracking-wide mb-1.5">
            Related News
          </p>
          <div className="space-y-1.5">
            {(message.news ?? []).slice(0, 3).map((item) => (
              <a
                key={`${item.link}-${item.title}`}
                href={item.link}
                target="_blank"
                rel="noopener noreferrer"
                className="block rounded-md border border-white/8 bg-white/[0.02] px-2 py-1.5 hover:border-white/16 hover:bg-white/[0.04] transition-colors"
              >
                <p className="text-sm text-zinc-200 leading-snug">
                  {item.title}
                </p>
                <p className="text-sm text-zinc-500 mt-0.5">
                  {[item.source, item.publishedAt].filter(Boolean).join(" · ")}
                </p>
              </a>
            ))}
          </div>
        </div>
      )}

      {hasFollowUps && (
        <div className="rounded-lg border border-white/8 bg-white/[0.02] p-2.5">
          <p className="text-sm text-zinc-500 uppercase tracking-wide mb-1.5">
            Suggested Follow-Ups
          </p>
          <div className="flex flex-wrap gap-1.5">
            {(message.followUps ?? []).slice(0, 3).map((q) => (
              <button
                key={q}
                type="button"
                disabled={disabled}
                onClick={() => onFollowUp(q)}
                className="text-left px-2 py-1 rounded-md border border-white/10 bg-white/[0.03] text-sm text-zinc-300 hover:text-white hover:border-white/20 hover:bg-white/[0.06] transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {q}
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

// ─── Coin Detail Card ─────────────────────────────────────────────────────────

const RANGES = ["1D", "1W", "1M", "3M", "1Y"] as const;

function CoinDetailCard({ meta }: { meta: CoinCardMeta }) {
  const assetType = meta.assetType;
  const [range, setRange] = useState(meta.range);
  // localDetail is only set after a range-tab fetch; fall back to meta.data
  // so when meta.data arrives via prop update the card re-renders correctly.
  const [localDetail, setLocalDetail] = useState<CoinDetail | undefined>(
    undefined,
  );
  const detail = localDetail ?? meta.data;
  const [loadingRange, setLoadingRange] = useState(false);
  const [showTradeWidget, setShowTradeWidget] = useState(false);
  const [checkingConfig, setCheckingConfig] = useState(false);
  const [alpacaConfig, setAlpacaConfig] = useState<AlpacaConfigResponse | null>(
    null,
  );
  const [orderSide, setOrderSide] = useState<AlpacaOrderSide>("buy");
  const [orderType, setOrderType] = useState<AlpacaOrderType>("market");
  const [timeInForce, setTimeInForce] = useState<AlpacaTimeInForce>("gtc");
  const [tradeSymbol, setTradeSymbol] = useState(() => {
    const base = (meta.symbol || "BTC").toUpperCase();
    return meta.assetType === "crypto" ? `${base}USD` : base;
  });
  const [quantity, setQuantity] = useState("1");
  const [limitPrice, setLimitPrice] = useState(() => {
    const p = meta.data?.current_price ?? 0;
    if (p <= 0) return "";
    return p >= 1 ? p.toFixed(2) : p.toFixed(6);
  });
  const [submittingOrder, setSubmittingOrder] = useState(false);
  const [tradeError, setTradeError] = useState<string | null>(null);
  const [placedOrder, setPlacedOrder] = useState<AlpacaPlacedOrder | null>(
    null,
  );
  const detailSymbol = detail?.symbol;
  const detailPrice = detail?.current_price;

  useEffect(() => {
    if (showTradeWidget || !detailSymbol || detailPrice === undefined) return;
    setTradeSymbol(
      assetType === "crypto"
        ? `${detailSymbol.toUpperCase()}USD`
        : detailSymbol.toUpperCase(),
    );
    setQuantity(formatQtyFromNotional(detailPrice));
    setLimitPrice(
      detailPrice >= 1 ? detailPrice.toFixed(2) : detailPrice.toFixed(6),
    );
    setOrderSide("buy");
  }, [assetType, showTradeWidget, detailPrice, detailSymbol]);

  const changeRange = async (r: string) => {
    if (r === range || loadingRange) return;
    setRange(r);

    // For crypto, 1D and 1W are derived from cached 7D sparkline.
    const base = meta.data;
    if (assetType === "crypto" && base) {
      if (r === "1D") {
        // Last 24 hourly points from the 168-point 7D sparkline
        setLocalDetail({ ...base, prices: base.prices.slice(-24) });
        return;
      }
      if (r === "1W") {
        // All 168 points — fall back to original meta.data
        setLocalDetail(undefined);
        return;
      }
    }

    setLoadingRange(true);
    try {
      if (assetType === "crypto" && !meta.coinId) {
        throw new Error("coinId missing");
      }
      const res =
        assetType === "crypto"
          ? await fetch(`/api/coin-detail?coinId=${meta.coinId}&range=${r}`)
          : await fetch(
              `/api/equity-detail?symbol=${encodeURIComponent(
                detail?.symbol || meta.symbol,
              )}&range=${r}`,
            );
      if (!res.ok) throw new Error(`${res.status}`);
      const d: CoinDetail = await res.json();
      if (d.prices?.length) setLocalDetail(d);
    } catch {
      // Keep existing data on rate-limit / error — only range label updates
    } finally {
      setLoadingRange(false);
    }
  };

  const checkAlpacaConfig = useCallback(async () => {
    setCheckingConfig(true);
    setTradeError(null);
    try {
      const res = await fetch("/api/alpaca/config");
      const payload = (await res.json()) as AlpacaConfigResponse;
      setAlpacaConfig(payload);
      if (!res.ok) setAlpacaConfig({ configured: false });
    } catch {
      setAlpacaConfig({ configured: false });
    } finally {
      setCheckingConfig(false);
    }
  }, []);

  const toggleTradeWidget = async () => {
    if (showTradeWidget) {
      setShowTradeWidget(false);
      return;
    }
    setShowTradeWidget(true);
    setPlacedOrder(null);
    await checkAlpacaConfig();
  };

  const submitOrder = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (!alpacaConfig?.configured || submittingOrder) return;

    const qty = parseFloat(quantity);
    if (!Number.isFinite(qty) || qty <= 0) {
      setTradeError("Quantity must be a number greater than 0.");
      return;
    }

    const normalizedSymbol = tradeSymbol.trim().toUpperCase();
    if (!normalizedSymbol) {
      setTradeError("Symbol is required.");
      return;
    }

    let parsedLimitPrice: number | undefined;
    if (orderType === "limit") {
      parsedLimitPrice = parseFloat(limitPrice);
      if (!Number.isFinite(parsedLimitPrice) || parsedLimitPrice <= 0) {
        setTradeError("Limit price must be a number greater than 0.");
        return;
      }
    }

    setSubmittingOrder(true);
    setTradeError(null);
    setPlacedOrder(null);

    try {
      const res = await fetch("/api/alpaca/order", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          symbol: normalizedSymbol,
          side: orderSide,
          type: orderType,
          timeInForce,
          qty,
          limitPrice: parsedLimitPrice,
        }),
      });

      const payload = (await res.json()) as
        | { order?: AlpacaPlacedOrder; error?: string }
        | undefined;

      if (!res.ok) {
        throw new Error(payload?.error || `Order failed (${res.status}).`);
      }

      if (!payload?.order) {
        throw new Error("Order response was empty.");
      }

      setPlacedOrder(payload.order);
    } catch (err) {
      setTradeError("Order could not be placed. Please try again.");
    } finally {
      setSubmittingOrder(false);
    }
  };

  if (meta.loading || !detail) {
    return (
      <div className="bg-[#0a0a0a] border border-white/[0.07] rounded-xl p-6 flex items-center gap-3">
        <RefreshCw className="w-4 h-4 animate-spin text-zinc-500" />
        <span className="text-sm text-zinc-500">
          Loading {meta.name || meta.symbol} data…
        </span>
      </div>
    );
  }

  const pos = detail.price_change_percentage_24h >= 0;
  const chartData = detail.prices.map(([, price]) => price);
  const changeAbs = Math.abs(detail.price_change_24h);

  const stats = [
    { label: "Market Cap", value: fmtBig(detail.market_cap) },
    { label: "24H Volume", value: fmtBig(detail.total_volume) },
    { label: "24H High", value: fmt(detail.high_24h) },
    { label: "24H Low", value: fmt(detail.low_24h) },
    { label: "All-Time High", value: fmt(detail.ath) },
    { label: "All-Time Low", value: fmt(detail.atl) },
  ];

  return (
    <div className="bg-[#0a0a0a] border border-white/[0.07] rounded-xl overflow-hidden w-full">
      {/* Header */}
      <div className="px-4 pt-4 pb-3">
        <div className="flex items-start justify-between gap-2 mb-3">
          <div className="flex items-center gap-2">
            {detail.image && (
              /* eslint-disable-next-line @next/next/no-img-element */
              <img
                src={detail.image}
                alt={detail.name}
                className="w-7 h-7 rounded-full"
              />
            )}
            <div>
              <p className="text-base font-semibold text-white leading-tight">
                {detail.name} USD
              </p>
              <p className="text-sm text-zinc-500 uppercase tracking-wide">
                {detail.symbol} ·{" "}
                {assetType === "crypto" ? "CRYPTO" : "EQUITY / ETF"}
              </p>
            </div>
          </div>
          <a
            href="https://app.alpaca.markets"
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-1 px-2.5 py-1 rounded-lg bg-emerald-500/15 border border-emerald-500/30 text-sm font-semibold text-emerald-400 hover:bg-emerald-500/25 hover:border-emerald-500/50 transition-colors whitespace-nowrap shrink-0"
          >
            Open Alpaca
            <ExternalLink className="w-2.5 h-2.5" />
          </a>
        </div>

        <div className="flex items-baseline gap-2 flex-wrap">
          <span className="text-2xl font-bold tabular-nums">
            {fmt(detail.current_price)}
          </span>
          <span
            className={`text-sm font-medium tabular-nums ${pos ? "text-emerald-400" : "text-red-400"}`}
          >
            {pos ? "+" : "-"}
            {fmt(changeAbs)}
          </span>
          <span
            className={`text-sm font-medium flex items-center gap-0.5 ${pos ? "text-emerald-400" : "text-red-400"}`}
          >
            {pos ? (
              <ArrowUpRight className="w-3.5 h-3.5" />
            ) : (
              <ArrowDownRight className="w-3.5 h-3.5" />
            )}
            {Math.abs(detail.price_change_percentage_24h).toFixed(2)}%
          </span>
        </div>

        <p className="text-sm text-zinc-600 mt-1.5">
          {new Date(detail.last_updated).toLocaleString("en-US", {
            month: "short",
            day: "numeric",
            hour: "2-digit",
            minute: "2-digit",
            second: "2-digit",
            timeZoneName: "short",
          })}
        </p>
      </div>

      {/* Range tabs */}
      <div className="flex border-b border-white/[0.05] px-3">
        {RANGES.map((r) => (
          <button
            key={r}
            onClick={() => changeRange(r)}
            className={`px-2.5 py-2 text-sm font-medium border-b-2 transition-colors ${
              range === r
                ? "border-white text-white"
                : "border-transparent text-zinc-500 hover:text-zinc-300"
            }`}
          >
            {r}
          </button>
        ))}
      </div>

      {/* Chart */}
      <div
        className={
          loadingRange
            ? "opacity-40 pointer-events-none transition-opacity"
            : "transition-opacity"
        }
      >
        <CardSparkline data={chartData} positive={pos} height={120} />
      </div>

      {/* Stats grid */}
      <div className="grid grid-cols-3 gap-px bg-white/[0.04] border-t border-white/[0.05]">
        {stats.map(({ label, value }) => (
          <div key={label} className="bg-[#0a0a0a] px-3 py-2.5">
            <p className="text-sm text-zinc-600 mb-0.5">{label}</p>
            <p className="text-sm font-semibold text-zinc-200 tabular-nums">
              {value}
            </p>
          </div>
        ))}
      </div>

      {/* Source */}
      <div className="px-3 py-2 border-t border-white/5 flex items-center justify-between">
        <span className="text-sm text-zinc-700">
          Chart:{" "}
          {assetType === "crypto"
            ? range === "1D" || range === "1W"
              ? "7D sparkline (CoinGecko)"
              : "CoinGecko market chart"
            : "Yahoo Finance chart"}
        </span>
        <a
          href={
            assetType === "crypto"
              ? "https://www.coingecko.com"
              : `https://finance.yahoo.com/quote/${encodeURIComponent(
                  detail.symbol,
                )}`
          }
          target="_blank"
          rel="noopener noreferrer"
          className="flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-white/5 border border-white/8 text-sm text-zinc-400 hover:text-zinc-200 hover:border-white/16 transition-colors"
        >
          {assetType === "crypto" ? "CoinGecko" : "Yahoo Finance"}
          <ExternalLink className="w-2 h-2 opacity-60" />
        </a>
      </div>

      {/* Trade links — always visible, more prominent on buyIntent */}
      <div
        className={`${meta.buyIntent ? "border-emerald-500/20 bg-emerald-500/5" : "border-white/5"} border-t`}
      >
        <div className="px-3 py-2 flex items-center gap-2">
          <span className="text-sm text-zinc-500 mr-1">Trade on</span>
          <button
            type="button"
            onClick={() => {
              void toggleTradeWidget();
            }}
            className="flex items-center gap-1 px-2 py-0.5 rounded-md bg-emerald-500/10 border border-emerald-500/20 text-sm text-emerald-400 hover:bg-emerald-500/20 hover:border-emerald-500/40 transition-colors"
          >
            {showTradeWidget ? "Close Widget" : "In-App Widget"}
            {showTradeWidget ? (
              <ChevronUp className="w-2 h-2 opacity-70" />
            ) : (
              <ChevronDown className="w-2 h-2 opacity-70" />
            )}
          </button>
          <a
            href="https://app.alpaca.markets"
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-0.5 px-2 py-0.5 rounded-md bg-yellow-500/10 border border-yellow-500/20 text-sm text-yellow-400 hover:bg-yellow-500/20 hover:border-yellow-500/40 transition-colors"
          >
            Alpaca
            <ExternalLink className="w-2 h-2 opacity-60" />
          </a>
          <span className="text-[9px] text-zinc-700 ml-auto">
            DYOR · Not financial advice
          </span>
        </div>

        {showTradeWidget && (
          <div className="border-t border-white/5 px-3 py-3 space-y-2.5">
            {checkingConfig && (
              <div className="flex items-center gap-2 text-sm text-zinc-500">
                <RefreshCw className="w-3 h-3 animate-spin" />
                Checking Alpaca credentials…
              </div>
            )}

            {!checkingConfig && !alpacaConfig?.configured && (
              <div className="rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2">
                <p className="text-sm text-amber-300 font-medium">
                  Alpaca keys are not configured on the server.
                </p>
                <p className="text-sm text-zinc-300 mt-1">
                  Add `APCA_API_KEY_ID` and `APCA_API_SECRET_KEY` in
                  `false-markets/.env.local`, then restart `npm run dev`.
                </p>
              </div>
            )}

            {!checkingConfig && alpacaConfig?.configured && (
              <form onSubmit={submitOrder} className="space-y-2.5">
                <div className="grid grid-cols-2 gap-2">
                  <label className="text-sm text-zinc-500 space-y-1">
                    Symbol
                    <input
                      value={tradeSymbol}
                      onChange={(e) =>
                        setTradeSymbol(e.target.value.toUpperCase())
                      }
                      className="w-full bg-[#000] border border-white/10 rounded-md px-2 py-1.5 text-sm text-zinc-200 outline-none focus:border-white/25"
                      placeholder="BTCUSD"
                    />
                  </label>
                  <label className="text-sm text-zinc-500 space-y-1">
                    Side
                    <select
                      value={orderSide}
                      onChange={(e) =>
                        setOrderSide(e.target.value as AlpacaOrderSide)
                      }
                      className="w-full bg-[#000] border border-white/10 rounded-md px-2 py-1.5 text-sm text-zinc-200 outline-none focus:border-white/25"
                    >
                      <option value="buy">Buy</option>
                      <option value="sell">Sell</option>
                    </select>
                  </label>
                  <label className="text-sm text-zinc-500 space-y-1">
                    Order Type
                    <select
                      value={orderType}
                      onChange={(e) =>
                        setOrderType(e.target.value as AlpacaOrderType)
                      }
                      className="w-full bg-[#000] border border-white/10 rounded-md px-2 py-1.5 text-sm text-zinc-200 outline-none focus:border-white/25"
                    >
                      <option value="market">Market</option>
                      <option value="limit">Limit</option>
                    </select>
                  </label>
                  <label className="text-sm text-zinc-500 space-y-1">
                    Time In Force
                    <select
                      value={timeInForce}
                      onChange={(e) =>
                        setTimeInForce(e.target.value as AlpacaTimeInForce)
                      }
                      className="w-full bg-[#000] border border-white/10 rounded-md px-2 py-1.5 text-sm text-zinc-200 outline-none focus:border-white/25"
                    >
                      <option value="gtc">GTC</option>
                      <option value="ioc">IOC</option>
                    </select>
                  </label>
                  <label className="text-sm text-zinc-500 space-y-1">
                    Quantity
                    <input
                      value={quantity}
                      onChange={(e) => setQuantity(e.target.value)}
                      type="number"
                      inputMode="decimal"
                      min="0"
                      step="any"
                      className="w-full bg-[#000] border border-white/10 rounded-md px-2 py-1.5 text-sm text-zinc-200 outline-none focus:border-white/25"
                      placeholder="0.01"
                    />
                  </label>
                  <label className="text-sm text-zinc-500 space-y-1">
                    Price (USD)
                    <input
                      value={limitPrice}
                      onChange={(e) => setLimitPrice(e.target.value)}
                      type="number"
                      inputMode="decimal"
                      min="0"
                      step="any"
                      disabled={orderType !== "limit"}
                      className="w-full bg-[#000] border border-white/10 rounded-md px-2 py-1.5 text-sm text-zinc-200 outline-none focus:border-white/25 disabled:opacity-50 disabled:cursor-not-allowed"
                      placeholder={detail.current_price.toString()}
                    />
                  </label>
                </div>
                <p className="text-sm text-zinc-600">
                  Quantity auto-sizes to about ${DEFAULT_POSITION_NOTIONAL}{" "}
                  notional.
                </p>

                {tradeError && (
                  <div className="rounded-md border border-red-500/30 bg-red-500/10 px-2 py-1.5 text-sm text-red-300">
                    {tradeError}
                  </div>
                )}

                {placedOrder && (
                  <div className="rounded-md border border-emerald-500/30 bg-emerald-500/10 px-2 py-1.5 text-sm text-emerald-300 space-y-0.5">
                    <p>
                      Order submitted:{" "}
                      <span className="font-semibold">
                        {placedOrder.symbol}
                      </span>
                    </p>
                    <p>
                      {placedOrder.side.toUpperCase()} {placedOrder.qty} ·{" "}
                      {placedOrder.type.toUpperCase()} ·{" "}
                      {placedOrder.time_in_force.toUpperCase()} · Status{" "}
                      {placedOrder.status.toUpperCase()}
                    </p>
                  </div>
                )}

                <button
                  type="submit"
                  disabled={submittingOrder}
                  className="w-full rounded-md bg-emerald-500/20 border border-emerald-500/30 text-emerald-300 text-sm font-semibold py-2 hover:bg-emerald-500/30 hover:border-emerald-500/45 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {submittingOrder
                    ? "Submitting Order..."
                    : `Place ${orderSide.toUpperCase()} Order`}
                </button>
              </form>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

function PortfolioCard({ data }: { data: PortfolioData }) {
  const s = data.summary;
  const dayPos = s.day_pnl >= 0;
  const unrlPos = s.unrealized_pnl_total >= 0;

  const chip = (v: number) => `${v >= 0 ? "+" : ""}${v.toFixed(2)}%`;

  return (
    <div className="bg-[#0a0a0a] border border-white/[0.07] rounded-xl overflow-hidden w-full">
      <div className="px-4 py-3 border-b border-white/[0.05]">
        <div className="flex items-center justify-between gap-2">
          <div>
            <p className="text-base font-semibold text-white leading-tight">
              Portfolio Snapshot
            </p>
            <p className="text-sm text-zinc-500">
              {new Date(data.fetched_at).toLocaleString("en-US")}
            </p>
          </div>
          <span className="text-sm text-zinc-500">
            {data.account.status || "active"} · {data.account.currency || "USD"}
          </span>
        </div>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-px bg-white/[0.04]">
        <div className="bg-[#0a0a0a] px-3 py-2.5">
          <p className="text-sm text-zinc-600">Equity</p>
          <p className="text-[12px] font-semibold text-zinc-200 tabular-nums">
            {fmtBig(s.equity)}
          </p>
        </div>
        <div className="bg-[#0a0a0a] px-3 py-2.5">
          <p className="text-sm text-zinc-600">Cash</p>
          <p className="text-[12px] font-semibold text-zinc-200 tabular-nums">
            {fmtBig(s.cash)}
          </p>
        </div>
        <div className="bg-[#0a0a0a] px-3 py-2.5">
          <p className="text-sm text-zinc-600">Day P/L</p>
          <p
            className={`text-[12px] font-semibold tabular-nums ${
              dayPos ? "text-emerald-400" : "text-red-400"
            }`}
          >
            {dayPos ? "+" : ""}
            {fmtBig(s.day_pnl)} ({chip(s.day_pnl_pct)})
          </p>
        </div>
        <div className="bg-[#0a0a0a] px-3 py-2.5">
          <p className="text-sm text-zinc-600">Unrealized P/L</p>
          <p
            className={`text-[12px] font-semibold tabular-nums ${
              unrlPos ? "text-emerald-400" : "text-red-400"
            }`}
          >
            {unrlPos ? "+" : ""}
            {fmtBig(s.unrealized_pnl_total)} ({chip(s.unrealized_pnl_pct)})
          </p>
        </div>
      </div>

      <div className="px-3 py-2 border-t border-white/5">
        <p className="text-sm text-zinc-600 mb-1.5">
          Positions ({s.positions_count})
        </p>
        {data.positions.length === 0 ? (
          <p className="text-sm text-zinc-500">No open positions.</p>
        ) : (
          <div className="space-y-1.5">
            {data.positions.slice(0, 8).map((p) => {
              const pl = Number(p.unrealized_pl);
              const pos = pl >= 0;
              return (
                <div
                  key={`${p.symbol}-${p.side}`}
                  className="flex items-center justify-between gap-2 rounded-md border border-white/8 bg-white/[0.02] px-2 py-1.5"
                >
                  <div>
                    <p className="text-sm text-zinc-200 font-semibold">
                      {p.symbol}
                    </p>
                    <p className="text-sm text-zinc-500">
                      Qty {p.qty} · Avg $
                      {Number(p.avg_entry_price || 0).toFixed(2)}
                    </p>
                  </div>
                  <div className="text-right">
                    <p className="text-sm text-zinc-500">
                      MV {fmtBig(Number(p.market_value || 0))}
                    </p>
                    <p
                      className={`text-sm font-semibold ${
                        pos ? "text-emerald-400" : "text-red-400"
                      }`}
                    >
                      {pos ? "+" : ""}
                      {fmtBig(pl)} ({chip(p.unrealized_plpc)})
                    </p>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-px bg-white/[0.04] border-t border-white/5">
        <div className="bg-[#0a0a0a] px-3 py-2.5">
          <p className="text-sm text-zinc-600 mb-1">Pending Orders</p>
          <p className="text-[12px] font-semibold text-zinc-200">
            {s.pending_orders_count}
          </p>
        </div>
        <div className="bg-[#0a0a0a] px-3 py-2.5">
          <p className="text-sm text-zinc-600 mb-1">Partially Filled</p>
          <p className="text-[12px] font-semibold text-zinc-200">
            {s.partially_filled_orders_count}
          </p>
        </div>
        <div className="bg-[#0a0a0a] px-3 py-2.5">
          <p className="text-sm text-zinc-600 mb-1">Filled (Recent)</p>
          <p className="text-[12px] font-semibold text-zinc-200">
            {s.filled_orders_count}
          </p>
        </div>
      </div>
    </div>
  );
}

// ─── Donut for dominance ──────────────────────────────────────────────────────

function DominanceDonut({ btc, eth }: { btc: number; eth: number }) {
  const other = Math.max(0, 100 - btc - eth);
  const options = {
    chart: {
      type: "donut" as const,
      background: "transparent",
      animations: { enabled: false },
    },
    colors: ["#f97316", "#818cf8", "#52525b"],
    labels: ["BTC", "ETH", "Others"],
    legend: { show: false },
    dataLabels: { enabled: false },
    plotOptions: {
      pie: {
        donut: {
          size: "68%",
          labels: {
            show: true,
            total: {
              show: true,
              label: "BTC",
              color: "#f97316",
              fontSize: "11px",
              formatter: () => `${btc.toFixed(1)}%`,
            },
          },
        },
      },
    },
    tooltip: { enabled: false },
    stroke: { width: 0 },
  };

  return (
    <ReactApexChart
      type="donut"
      series={[btc, eth, other]}
      options={options}
      width={100}
      height={100}
    />
  );
}

// ─── Volume Bar chart ─────────────────────────────────────────────────────────

function VolumeBar({ coins }: { coins: CoinData[] }) {
  const top5 = coins.slice(0, 5);
  const options = {
    chart: {
      type: "bar" as const,
      background: "transparent",
      toolbar: { show: false },
      animations: { enabled: false },
      sparkline: { enabled: true },
    },
    colors: ["#3b82f6"],
    plotOptions: {
      bar: { borderRadius: 3, columnWidth: "55%" },
    },
    dataLabels: { enabled: false },
    tooltip: { enabled: false },
    xaxis: {
      categories: top5.map((c) => c.symbol.toUpperCase()),
      labels: { show: false },
      axisBorder: { show: false },
      axisTicks: { show: false },
    },
    yaxis: { show: false },
    grid: { show: false },
  };

  return (
    <ReactApexChart
      type="bar"
      series={[{ data: top5.map((c) => c.total_volume) }]}
      options={options}
      width={120}
      height={50}
    />
  );
}

// ─── Main Component ───────────────────────────────────────────────────────────

export default function CryptoDashboard() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const activeAsset = normalizeAssetTab(searchParams.get("asset"));
  const isCrypto = activeAsset === "crypto";
  const isCommodity = activeAsset === "commodity";
  const [coins, setCoins] = useState<CoinData[]>([]);
  const [globalData, setGlobalData] = useState<GlobalData | null>(null);
  const [assetItems, setAssetItems] = useState<MarketItem[]>([]);
  const [assetLoading, setAssetLoading] = useState(false);
  const [assetRefreshing, setAssetRefreshing] = useState(false);
  const [commodityItems, setCommodityItems] = useState<CommodityItem[]>([]);
  const [commodityStrip, setCommodityStrip] = useState<CommodityStripItem[]>(
    [],
  );
  const [commodityNews, setCommodityNews] = useState<CommodityNewsItem[]>([]);
  const [commodityNewsLoading, setCommodityNewsLoading] = useState(false);
  const [predictions, setPredictions] = useState<PredictionMarket[]>([]);
  const [sentiment, setSentiment] = useState<SentimentData | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [query, setQuery] = useState("");
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [streaming, setStreaming] = useState(false);
  const [showChat, setShowChat] = useState(false);
  const [activeTab, setActiveTab] = useState<"gainers" | "losers">("gainers");
  const [showTradingModal, setShowTradingModal] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const chatEndRef = useRef<HTMLDivElement>(null);

  const updateAssetTab = useCallback(
    (next: AssetTab) => {
      const params = new URLSearchParams(searchParams.toString());
      if (next === "crypto") {
        params.delete("asset");
      } else {
        params.set("asset", next);
      }
      const queryString = params.toString();
      router.replace(queryString ? `?${queryString}` : "/dashboard", {
        scroll: false,
      });
    },
    [router, searchParams],
  );

  const fetchData = useCallback(async (silent = false) => {
    if (!silent) setLoading(true);
    else setRefreshing(true);

    try {
      const [marketsRes, globalRes, predRes, sentRes] = await Promise.all([
        fetch(
          "/api/crypto?endpoint=coins/markets&vs_currency=usd&order=market_cap_desc&per_page=20&page=1&sparkline=true&price_change_percentage=24h",
        ),
        fetch("/api/crypto?endpoint=global"),
        fetch("/api/prediction"),
        fetch("/api/sentiment"),
      ]);

      const [markets, global, pred, sent] = await Promise.all([
        marketsRes.json(),
        globalRes.json(),
        predRes.json(),
        sentRes.json(),
      ]);

      if (Array.isArray(markets)) setCoins(markets);
      if (global?.data) setGlobalData(global.data);
      if (Array.isArray(pred)) setPredictions(pred);
      if (sent?.tone) setSentiment(sent);
    } catch (e) {
      console.error("Data fetch error:", e);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  const fetchAssetData = useCallback(
    async (silent = false) => {
      if (!silent) setAssetLoading(true);
      else setAssetRefreshing(true);

      try {
        if (activeAsset === "commodity") {
          const res = await fetch("/api/commodities");
          if (!res.ok) throw new Error(`${res.status}`);
          const payload = (await res.json()) as {
            items?: CommodityItem[];
            strip?: CommodityStripItem[];
          };
          if (Array.isArray(payload.items)) setCommodityItems(payload.items);
          else setCommodityItems([]);
          if (Array.isArray(payload.strip)) setCommodityStrip(payload.strip);
          else setCommodityStrip([]);
          setAssetItems([]);
        } else {
          const res = await fetch(`/api/market-overview?asset=${activeAsset}`);
          if (!res.ok) throw new Error(`${res.status}`);
          const payload = (await res.json()) as { items?: MarketItem[] };
          if (Array.isArray(payload.items)) setAssetItems(payload.items);
          else setAssetItems([]);
        }
      } catch (e) {
        console.error("Market overview error:", e);
        setAssetItems([]);
        setCommodityItems([]);
        setCommodityStrip([]);
      } finally {
        setAssetLoading(false);
        setAssetRefreshing(false);
      }
    },
    [activeAsset],
  );

  const fetchCommodityNews = useCallback(async () => {
    if (!isCommodity) return;
    setCommodityNewsLoading(true);
    try {
      const res = await fetch("/api/commodity-news");
      if (!res.ok) throw new Error(`${res.status}`);
      const payload = (await res.json()) as { items?: CommodityNewsItem[] };
      if (Array.isArray(payload.items)) setCommodityNews(payload.items);
      else setCommodityNews([]);
    } catch (e) {
      console.error("Commodity news error:", e);
      setCommodityNews([]);
    } finally {
      setCommodityNewsLoading(false);
    }
  }, [isCommodity]);

  const refreshMarketData = useCallback(() => {
    void fetchAssetData(true);
    if (isCrypto) {
      void fetchData(true);
    }
    if (isCommodity) {
      void fetchCommodityNews();
    }
  }, [fetchAssetData, fetchData, fetchCommodityNews, isCommodity, isCrypto]);

  useEffect(() => {
    if (!isCrypto) {
      setLoading(false);
      return;
    }
    fetchData();
    const timer = setInterval(() => fetchData(true), 60_000);
    return () => clearInterval(timer);
  }, [fetchData, isCrypto]);

  useEffect(() => {
    fetchAssetData();
    const timer = setInterval(() => fetchAssetData(true), 60_000);
    return () => clearInterval(timer);
  }, [fetchAssetData]);

  useEffect(() => {
    if (!isCommodity) return;
    fetchCommodityNews();
    const timer = setInterval(() => fetchCommodityNews(), 300_000);
    return () => clearInterval(timer);
  }, [fetchCommodityNews, isCommodity]);

  useEffect(() => {
    if (showChat) chatEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, showChat]);

  const updateMessage = useCallback(
    (messageId: string, updater: (prev: ChatMessage) => ChatMessage) => {
      setMessages((prev) =>
        prev.map((msg) => (msg.id === messageId ? updater(msg) : msg)),
      );
    },
    [],
  );

  const enrichAssistantMessage = useCallback(
    async ({
      messageId,
      userText,
      coinName,
      coinSymbol,
    }: {
      messageId: string;
      userText: string;
      coinName?: string;
      coinSymbol?: string;
    }) => {
      const newsQuery = coinName || coinSymbol || userText;

      updateMessage(messageId, (prev) => ({ ...prev, newsLoading: true }));

      try {
        const res = await fetch(`/api/news?q=${encodeURIComponent(newsQuery)}`);
        const payload = (await res.json()) as {
          items?: NewsItem[];
          followUps?: string[];
        };

        updateMessage(messageId, (prev) => ({
          ...prev,
          newsLoading: false,
          news: payload.items ?? [],
          followUps: payload.followUps ?? [],
        }));
      } catch {
        updateMessage(messageId, (prev) => ({ ...prev, newsLoading: false }));
      }
    },
    [updateMessage],
  );

  const sendPrompt = useCallback(
    async (rawPrompt: string) => {
      const text = rawPrompt.trim();
      if (!text || streaming) return;
      const latestPortfolio = [...messages]
        .reverse()
        .find((m) => m.portfolio)?.portfolio;
      const portfolioContext = buildPortfolioContext(latestPortfolio);
      const newsContext = buildNewsContext(messages);

      setQuery("");
      setShowChat(true);
      setStreaming(true);

      const userId = makeMessageId();
      const assistantId = makeMessageId();

      setMessages((prev) => [
        ...prev,
        { id: userId, role: "user", content: text },
        { id: assistantId, role: "assistant", content: "" },
      ]);

      try {
        const res = await fetch("/api/chat", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            message: text,
            portfolioContext,
            newsContext,
            backtestEnabled: false,
          }),
        });
        if (!res.body) throw new Error("No body");

        const reader = res.body.getReader();
        const dec = new TextDecoder();
        let accumulated = "";
        let cardMode: "asset" | "portfolio" | null = null;

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          const chunk = dec.decode(value);
          accumulated += chunk;

          if (
            accumulated.startsWith("__ASSET_CARD__") ||
            accumulated.startsWith("__COIN_CARD__")
          ) {
            if (!cardMode) {
              cardMode = "asset";
              updateMessage(assistantId, (prev) => ({
                ...prev,
                role: "assistant",
                content: "__ASSET_CARD__",
                coinCard: {
                  assetType: "crypto",
                  coinId: undefined,
                  symbol: "",
                  name: "",
                  range: "1D",
                  loading: true,
                },
              }));
            }
          } else if (accumulated.startsWith("__PORTFOLIO_CARD__")) {
            if (!cardMode) {
              cardMode = "portfolio";
              updateMessage(assistantId, (prev) => ({
                ...prev,
                role: "assistant",
                content: "__PORTFOLIO_CARD__",
                portfolio: undefined,
                portfolioLoading: true,
              }));
            }
          } else if (!accumulated.startsWith("_")) {
            updateMessage(assistantId, (prev) => ({
              ...prev,
              role: "assistant",
              content: accumulated,
            }));
          }
        }

        if (cardMode === "asset") {
          try {
            const match = accumulated.match(
              /__(?:ASSET|COIN)_CARD__\s*(\{[^}]+\})\s*([\s\S]*)/,
            );
            if (!match) throw new Error("No JSON found in asset card response");

            const parsed = JSON.parse(match[1]) as {
              assetType?: "crypto" | "equity";
              coinId?: string;
              symbol: string;
              name: string;
              buyIntent?: boolean;
            };
            const assetType =
              parsed.assetType === "equity" ? "equity" : "crypto";
            const commentary = match[2].trim();
            const symbol = (parsed.symbol || "").trim().toUpperCase();
            const name = (parsed.name || "").trim() || symbol;
            const buyIntent = parsed.buyIntent;

            let detail: CoinDetail;
            let coinId = parsed.coinId;

            if (assetType === "crypto") {
              const cached = coins.find(
                (c) =>
                  c.id === coinId ||
                  c.symbol.toUpperCase() === symbol ||
                  c.name.toLowerCase() === name.toLowerCase(),
              );

              if (cached) {
                const sparkline = cached.sparkline_in_7d?.price ?? [];
                const now = Date.now();
                const step =
                  sparkline.length > 1
                    ? (7 * 24 * 3600 * 1000) / (sparkline.length - 1)
                    : 3600 * 1000;
                detail = {
                  id: cached.id,
                  name: cached.name,
                  symbol: cached.symbol.toUpperCase(),
                  image: cached.image,
                  current_price: cached.current_price,
                  price_change_24h: cached.price_change_24h,
                  price_change_percentage_24h:
                    cached.price_change_percentage_24h,
                  market_cap: cached.market_cap,
                  total_volume: cached.total_volume,
                  high_24h: cached.high_24h,
                  low_24h: cached.low_24h,
                  ath: 0,
                  atl: 0,
                  last_updated: new Date().toISOString(),
                  prices: sparkline.map(
                    (p, i) =>
                      [now - (sparkline.length - 1 - i) * step, p] as [
                        number,
                        number,
                      ],
                  ),
                  asset_type: "crypto",
                };
                coinId = cached.id;
              } else {
                const resolvedCoinId = coinId || symbol.toLowerCase();
                const detailRes = await fetch(
                  `/api/coin-detail?coinId=${encodeURIComponent(resolvedCoinId)}`,
                );
                if (!detailRes.ok)
                  throw new Error("Failed to load crypto detail");
                detail = (await detailRes.json()) as CoinDetail;
                coinId = detail.id || resolvedCoinId;
              }
            } else {
              const detailRes = await fetch(
                `/api/equity-detail?symbol=${encodeURIComponent(symbol)}`,
              );
              detail = (await detailRes.json()) as CoinDetail;
              if (!detailRes.ok)
                throw new Error("Failed to load equity detail");
            }

            updateMessage(assistantId, (prev) => ({
              ...prev,
              role: "assistant",
              content: commentary || "__ASSET_CARD__",
              coinCard: {
                assetType,
                coinId,
                symbol,
                name,
                range: "1W",
                loading: false,
                data: detail,
                buyIntent,
              },
            }));

            void enrichAssistantMessage({
              messageId: assistantId,
              userText: text,
              coinName: name,
              coinSymbol: symbol,
            });
          } catch {
            updateMessage(assistantId, (prev) => ({
              ...prev,
              role: "assistant",
              content: "Could not load coin data. Please try again.",
            }));
          }
        } else if (cardMode === "portfolio") {
          try {
            const match = accumulated.match(
              /__PORTFOLIO_CARD__\s*(\{[^}]*\})?\s*([\s\S]*)/,
            );
            const commentary = match?.[2]?.trim() || "";

            const portfolioRes = await fetch("/api/alpaca/portfolio");
            if (!portfolioRes.ok) {
              throw new Error("Failed to load portfolio.");
            }
            const portfolioData = (await portfolioRes.json()) as PortfolioData;

            updateMessage(assistantId, (prev) => ({
              ...prev,
              role: "assistant",
              content: commentary || "__PORTFOLIO_CARD__",
              portfolio: portfolioData,
              portfolioLoading: false,
            }));

            const firstSymbol = portfolioData.positions?.[0]?.symbol;
            void enrichAssistantMessage({
              messageId: assistantId,
              userText: text,
              coinSymbol: firstSymbol,
            });
          } catch {
            updateMessage(assistantId, (prev) => ({
              ...prev,
              role: "assistant",
              content:
                "I couldn't load your portfolio right now. Please try again.",
              portfolioLoading: false,
            }));
          }
        } else {
          void enrichAssistantMessage({
            messageId: assistantId,
            userText: text,
          });
        }
      } catch {
        updateMessage(assistantId, (prev) => ({
          ...prev,
          role: "assistant",
          content: "Sorry, I couldn't process your request. Please try again.",
        }));
      } finally {
        setStreaming(false);
      }
    },
    [coins, enrichAssistantMessage, messages, streaming, updateMessage],
  );

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    if (query.trim()) {
      router.push(`/chat?q=${encodeURIComponent(query.trim())}`);
    }
  };

  const cryptoItems = coins.map((coin) => ({
    id: coin.id,
    symbol: coin.symbol.toUpperCase(),
    name: coin.name,
    price: coin.current_price,
    changePercent: coin.price_change_percentage_24h,
    changeAbs: coin.price_change_24h,
    marketCap: coin.market_cap,
    volume: coin.total_volume,
    sparkline: coin.sparkline_in_7d?.price ?? [],
    image: coin.image,
  }));

  const yahooItems = assetItems.map((item) => ({
    id: item.symbol,
    symbol: item.symbol.toUpperCase(),
    name: item.name,
    price: item.price,
    changePercent: item.changePercent,
    changeAbs: item.changeAbs,
    marketCap: item.marketCap,
    volume: item.volume,
    sparkline: item.sparkline,
    image: undefined,
  }));

  const commodityOverviewItems = commodityItems.map((item) => ({
    id: item.symbol,
    symbol: item.symbol.toUpperCase(),
    name: item.name,
    price: item.price,
    changePercent: item.changePercent,
    changeAbs: item.changeAbs,
    marketCap: 0,
    volume: 0,
    sparkline: item.sparkline,
    image: undefined,
  }));

  const activeItems = isCrypto
    ? yahooItems.length > 0
      ? yahooItems
      : cryptoItems
    : isCommodity
      ? commodityOverviewItems
      : yahooItems;
  const marketOverviewItems = isCommodity
    ? commodityOverviewItems
    : activeItems.slice(0, 10);

  const summaryMarketCap = yahooItems.reduce(
    (sum, item) => sum + item.marketCap,
    0,
  );
  const summaryVolume = yahooItems.reduce((sum, item) => sum + item.volume, 0);
  const summaryGainer = [...yahooItems].sort(
    (a, b) => b.changePercent - a.changePercent,
  )[0];
  const summaryLoser = [...yahooItems].sort(
    (a, b) => a.changePercent - b.changePercent,
  )[0];

  const moversBase = isCommodity ? commodityOverviewItems : activeItems;

  const gainers = moversBase
    .filter((item) => item.changePercent > 0)
    .sort((a, b) => b.changePercent - a.changePercent)
    .slice(0, 6);

  const losers = moversBase
    .filter((item) => item.changePercent < 0)
    .sort((a, b) => a.changePercent - b.changePercent)
    .slice(0, 6);

  const movers = activeTab === "gainers" ? gainers : losers;
  const isPageLoading = isCrypto ? loading || assetLoading : assetLoading;
  const isRefreshing = isCrypto
    ? refreshing || assetRefreshing
    : assetRefreshing;

  // ── Skeleton ──
  if (isPageLoading) {
    return (
      <div className="min-h-screen bg-[#000000] flex items-center justify-center">
        <div className="flex items-center gap-3 text-zinc-500">
          <RefreshCw className="w-4 h-4 animate-spin" />
          <span className="text-sm">Loading market data…</span>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-[#000000] text-white">
      {/* ── Header ── */}
      <header className="sticky top-0 z-50 border-b border-white/[0.06] bg-[#000000]/95 backdrop-blur-sm">
        <div className="max-w-7xl mx-auto px-5 h-14 flex items-center justify-between gap-4">
          <span className="text-xl font-semibold tracking-tight">
            AutoAlpha<span className="text-zinc-400">Pods</span>
          </span>

          <div className="flex items-center gap-4">
            <button
              onClick={() => router.push("/orchestration")}
              className="group relative overflow-hidden flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg bg-linear-to-r from-emerald-400/30 via-cyan-400/20 to-violet-400/30 border border-emerald-300/40 text-sm font-semibold text-emerald-100 hover:text-white hover:border-emerald-300/75 transition-all whitespace-nowrap shadow-[0_0_22px_rgba(16,185,129,0.28)]"
            >
              <span className="absolute inset-0 bg-linear-to-r from-transparent via-white/20 to-transparent -translate-x-full group-hover:translate-x-full transition-transform duration-700" />
              <Sparkles className="w-3.5 h-3.5 relative" />
              <span className="relative">AGENT ORCHESTRATION</span>
            </button>

            {/* Trading Dashboard button */}
            <button
              onClick={() => setShowTradingModal(true)}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-white/[0.06] border border-white/[0.12] text-sm font-semibold text-zinc-200 hover:bg-white/[0.10] hover:border-white/[0.22] hover:text-white transition-all whitespace-nowrap"
            >
              <BarChart2 className="w-3.5 h-3.5" />
              TRADING DASHBOARD
            </button>

            {/* Sentiment widget */}
            {isCrypto && sentiment && (
              <a
                href="https://alternative.me/crypto/fear-and-greed-index/"
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center gap-2.5 hover:opacity-80 transition-opacity cursor-pointer"
              >
                {/* Equalizer bars */}
                <div className="flex items-end gap-[3px] h-7">
                  {[7, 12, 10, 19, 15, 22, 14, 18, 11, 16, 8, 14].map(
                    (h, i) => (
                      <span
                        key={i}
                        className="w-[3px] rounded-full"
                        style={{
                          height: `${h}px`,
                          background:
                            sentiment.tone === "bearish"
                              ? "#fb7185"
                              : sentiment.tone === "bullish"
                                ? "#34d399"
                                : "#fbbf24",
                          opacity: 0.55 + (i % 3) * 0.15,
                          animation: `eqBar ${0.8 + (i % 4) * 0.15}s ease-in-out ${i * 0.07}s infinite alternate`,
                        }}
                      />
                    ),
                  )}
                </div>

                {/* Text */}
                <div className="flex flex-col leading-none gap-1">
                  <span
                    className="text-sm font-semibold"
                    style={{
                      color:
                        sentiment.tone === "bearish"
                          ? "#fb7185"
                          : sentiment.tone === "bullish"
                            ? "#34d399"
                            : "#fbbf24",
                    }}
                  >
                    {sentiment.label}
                  </span>
                  <span className="text-sm text-zinc-500">
                    Crypto
                    <span className="mx-1 text-zinc-700">·</span>
                    {new Date(sentiment.timestamp).toLocaleDateString("en-US", {
                      month: "short",
                      day: "numeric",
                      year: "numeric",
                    })}
                    {", "}
                    EST
                  </span>
                </div>
              </a>
            )}

            <button
              onClick={refreshMarketData}
              className="text-zinc-500 hover:text-zinc-200 transition-colors"
              title="Refresh"
            >
              <RefreshCw
                className={`w-3.5 h-3.5 ${isRefreshing ? "animate-spin" : ""}`}
              />
            </button>
          </div>
        </div>
      </header>

      <style>{`
        @keyframes eqBar {
          from { transform: scaleY(0.4); }
          to   { transform: scaleY(1); }
        }
      `}</style>

      <main className="max-w-7xl mx-auto px-5 py-5 pb-28">
        <div className="flex flex-col lg:flex-row lg:items-start gap-5">
          <div className="flex-1 min-w-0 space-y-5">
            <div className="flex flex-wrap items-center gap-2 border-b border-white/[0.07] pb-3">
              {ASSET_TABS.map((tab) => {
                const isActive = tab.value === activeAsset;
                return (
                  <button
                    key={tab.value}
                    type="button"
                    onClick={() => updateAssetTab(tab.value)}
                    className={`rounded-full px-3 py-1.5 text-sm font-medium transition-colors border ${
                      isActive
                        ? "bg-white/10 text-white border-white/20"
                        : "text-zinc-500 border-transparent hover:text-zinc-200 hover:border-white/10"
                    }`}
                  >
                    {tab.label}
                  </button>
                );
              })}
            </div>

            {/* ── Global stats bar ── */}
            {isCrypto && globalData && (
              <div className="flex flex-wrap items-center gap-x-5 gap-y-2 text-sm text-zinc-500 border-b border-white/[0.05] pb-4">
                <div className="flex items-center gap-1.5">
                  <Globe className="w-3 h-3" />
                  <span>Market Cap</span>
                  <span className="text-zinc-200 font-medium">
                    {fmtBig(globalData.total_market_cap.usd)}
                  </span>
                  <span
                    className={
                      globalData.market_cap_change_percentage_24h_usd >= 0
                        ? "text-emerald-400"
                        : "text-red-400"
                    }
                  >
                    {pct(globalData.market_cap_change_percentage_24h_usd)}
                  </span>
                </div>
                <div className="flex items-center gap-1.5">
                  <BarChart2 className="w-3 h-3" />
                  <span>24h Volume</span>
                  <span className="text-zinc-200 font-medium">
                    {fmtBig(globalData.total_volume.usd)}
                  </span>
                </div>
                <div className="flex items-center gap-1.5">
                  <span>BTC Dom.</span>
                  <span className="text-zinc-200 font-medium">
                    {globalData.market_cap_percentage.btc.toFixed(1)}%
                  </span>
                </div>
                <div className="flex items-center gap-1.5">
                  <span>ETH Dom.</span>
                  <span className="text-zinc-200 font-medium">
                    {globalData.market_cap_percentage.eth.toFixed(1)}%
                  </span>
                </div>
              </div>
            )}
            {isCommodity && (
              <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-2.5 border-b border-white/[0.05] pb-4">
                {commodityStrip.length === 0 ? (
                  <div className="col-span-full bg-[#0a0a0a] border border-white/[0.07] rounded-xl p-4 text-center text-sm text-zinc-600">
                    Market strip unavailable
                  </div>
                ) : (
                  commodityStrip.map((item) => {
                    const pos = item.changePercent >= 0;
                    return (
                      <div
                        key={item.id}
                        className="bg-[#0a0a0a] border border-white/[0.07] rounded-xl px-3.5 py-3"
                      >
                        <p className="text-xs uppercase tracking-wide text-zinc-500">
                          {item.label}
                        </p>
                        <div className="mt-2 flex items-baseline justify-between">
                          <span className="text-sm font-semibold text-zinc-200 tabular-nums">
                            {fmt(item.price)}
                          </span>
                          <span
                            className={`text-xs font-semibold tabular-nums ${
                              pos ? "text-emerald-400" : "text-red-400"
                            }`}
                          >
                            {pos ? "+" : "-"}
                            {Math.abs(item.changePercent).toFixed(2)}%
                          </span>
                        </div>
                      </div>
                    );
                  })
                )}
              </div>
            )}
            {!isCrypto && !isCommodity && activeItems.length > 0 && (
              <div className="flex flex-wrap items-center gap-x-5 gap-y-2 text-sm text-zinc-500 border-b border-white/[0.05] pb-4">
                <div className="flex items-center gap-1.5">
                  <Globe className="w-3 h-3" />
                  <span>Market Cap</span>
                  <span className="text-zinc-200 font-medium">
                    {fmtBig(summaryMarketCap)}
                  </span>
                </div>
                <div className="flex items-center gap-1.5">
                  <BarChart2 className="w-3 h-3" />
                  <span>24h Volume</span>
                  <span className="text-zinc-200 font-medium">
                    {fmtBig(summaryVolume)}
                  </span>
                </div>
                {summaryGainer && (
                  <div className="flex items-center gap-1.5">
                    <span>Top Gainer</span>
                    <span className="text-zinc-200 font-medium">
                      {summaryGainer.symbol}
                    </span>
                    <span className="text-emerald-400">
                      {pct(summaryGainer.changePercent)}
                    </span>
                  </div>
                )}
                {summaryLoser && (
                  <div className="flex items-center gap-1.5">
                    <span>Top Loser</span>
                    <span className="text-zinc-200 font-medium">
                      {summaryLoser.symbol}
                    </span>
                    <span className="text-red-400">
                      {pct(summaryLoser.changePercent)}
                    </span>
                  </div>
                )}
              </div>
            )}

            {/* ── Market overview cards ── */}
            <section>
              <h2 className="text-sm font-medium text-zinc-500 uppercase tracking-wider mb-3">
                Market Overview
              </h2>
              <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-2.5">
                {marketOverviewItems.length === 0 ? (
                  <div className="col-span-full bg-[#0a0a0a] border border-white/[0.07] rounded-xl p-6 text-center text-sm text-zinc-600">
                    No market data available
                  </div>
                ) : isCommodity ? (
                  marketOverviewItems.map((item) => {
                    const pos = item.changePercent >= 0;
                    const changeAbs = Math.abs(item.changeAbs ?? 0);
                    return (
                      <div
                        key={item.id}
                        className="group bg-[#0a0a0a] border border-white/[0.07] rounded-xl hover:border-white/[0.14] hover:bg-[#111] transition-all cursor-pointer overflow-hidden"
                      >
                        <div className="px-3.5 pt-3.5 pb-2">
                          <div className="flex items-start justify-between gap-2">
                            <div className="min-w-0">
                              <p className="text-sm font-semibold text-white truncate leading-tight">
                                {item.name}
                              </p>
                              <p className="text-sm text-zinc-500 mt-1 tabular-nums">
                                {fmt(item.price)}
                              </p>
                            </div>
                            <div className="text-right shrink-0">
                              <span
                                className={`text-sm font-semibold flex items-center justify-end gap-0.5 ${
                                  pos ? "text-emerald-400" : "text-red-400"
                                }`}
                              >
                                {pos ? (
                                  <ArrowUpRight className="w-3 h-3" />
                                ) : (
                                  <ArrowDownRight className="w-3 h-3" />
                                )}
                                {Math.abs(item.changePercent).toFixed(2)}%
                              </span>
                              <p
                                className={`text-sm font-medium mt-0.5 tabular-nums ${
                                  pos ? "text-emerald-600" : "text-red-600"
                                }`}
                              >
                                {pos ? "+" : "-"}
                                {fmt(changeAbs)}
                              </p>
                            </div>
                          </div>
                        </div>
                        {item.sparkline.length > 0 && (
                          <CardSparkline data={item.sparkline} positive={pos} />
                        )}
                      </div>
                    );
                  })
                ) : (
                  marketOverviewItems.map((item) => {
                    const pos = item.changePercent >= 0;
                    const changeAbs = Math.abs(item.changeAbs ?? 0);
                    const detailUrl = `https://finance.yahoo.com/quote/${item.symbol}`;
                    return (
                      <div
                        key={item.id}
                        onClick={() =>
                          window.open(
                            detailUrl,
                            "_blank",
                            "noopener,noreferrer",
                          )
                        }
                        className="group bg-[#0a0a0a] border border-white/[0.07] rounded-xl hover:border-white/[0.14] hover:bg-[#111] transition-all cursor-pointer overflow-hidden"
                      >
                        <div className="px-3.5 pt-3.5 pb-2">
                          <div className="flex items-start justify-between gap-2">
                            <div className="min-w-0">
                              <p className="text-sm font-semibold text-white truncate leading-tight">
                                {item.name}
                              </p>
                              <p className="text-sm text-zinc-500 mt-1 tabular-nums">
                                {fmt(item.price)}
                              </p>
                            </div>
                            <div className="text-right shrink-0">
                              <span
                                className={`text-sm font-semibold flex items-center justify-end gap-0.5 ${
                                  pos ? "text-emerald-400" : "text-red-400"
                                }`}
                              >
                                {pos ? (
                                  <ArrowUpRight className="w-3 h-3" />
                                ) : (
                                  <ArrowDownRight className="w-3 h-3" />
                                )}
                                {Math.abs(item.changePercent).toFixed(2)}%
                              </span>
                              <p
                                className={`text-sm font-medium mt-0.5 tabular-nums ${
                                  pos ? "text-emerald-600" : "text-red-600"
                                }`}
                              >
                                {pos ? "+" : "-"}
                                {fmt(changeAbs)}
                              </p>
                            </div>
                          </div>
                        </div>
                        {item.sparkline.length > 0 && (
                          <CardSparkline data={item.sparkline} positive={pos} />
                        )}
                      </div>
                    );
                  })
                )}
              </div>
            </section>

            {/* ── Top Movers ── */}
            <section>
              <div className="flex items-center justify-between mb-3">
                <h2 className="text-sm font-medium text-zinc-500 uppercase tracking-wider">
                  Top Movers
                </h2>
                <div className="flex items-center gap-0.5 bg-white/[0.04] rounded-lg p-0.5">
                  {(["gainers", "losers"] as const).map((tab) => (
                    <button
                      key={tab}
                      onClick={() => setActiveTab(tab)}
                      className={`px-2.5 py-1 rounded-md text-sm font-medium capitalize transition-colors ${
                        activeTab === tab
                          ? "bg-white/10 text-zinc-200"
                          : "text-zinc-500 hover:text-zinc-300"
                      }`}
                    >
                      {tab}
                    </button>
                  ))}
                </div>
              </div>
              <div className="bg-[#0a0a0a] border border-white/[0.07] rounded-xl overflow-hidden">
                {movers.length === 0 ? (
                  <div className="p-6 text-center text-sm text-zinc-600">
                    No data
                  </div>
                ) : isCommodity ? (
                  <div className="divide-y divide-white/[0.04]">
                    {movers.map((item, i) => {
                      const pos = item.changePercent >= 0;
                      const meta = getCommodityMeta(item.symbol);
                      return (
                        <div
                          key={item.id}
                          className="flex items-center gap-3 px-4 py-3 hover:bg-white/[0.02] transition-colors"
                        >
                          <span className="text-sm text-zinc-700 w-4 shrink-0 tabular-nums">
                            {i + 1}
                          </span>
                          <div
                            className="w-7 h-7 rounded-full shrink-0 text-xs font-semibold text-zinc-100 flex items-center justify-center"
                            style={{ background: meta.tint, color: meta.color }}
                          >
                            {meta.abbr}
                          </div>
                          <div className="min-w-0 flex-1">
                            <p className="text-sm font-semibold text-zinc-200">
                              {item.symbol}
                              <span className="text-zinc-500 font-medium">
                                {" "}
                                {item.name}
                              </span>
                            </p>
                            <p className="text-xs text-zinc-600 mt-0.5">
                              {fmt(item.price)}
                            </p>
                          </div>
                          <div className="shrink-0">
                            <MiniSparkline
                              data={item.sparkline}
                              positive={pos}
                              width={60}
                              height={24}
                            />
                          </div>
                          <div
                            className={`text-sm font-semibold tabular-nums flex items-center gap-1 ${
                              pos ? "text-emerald-400" : "text-red-400"
                            }`}
                          >
                            {pos ? (
                              <ChevronUp className="w-3 h-3" />
                            ) : (
                              <ChevronDown className="w-3 h-3" />
                            )}
                            {Math.abs(item.changePercent).toFixed(2)}%
                          </div>
                        </div>
                      );
                    })}
                  </div>
                ) : (
                  <div className="divide-y divide-white/[0.04]">
                    {movers.map((item, i) => {
                      const pos = item.changePercent >= 0;
                      return (
                        <div
                          key={item.id}
                          className="flex items-center gap-2.5 px-4 py-2.5 hover:bg-white/[0.02] transition-colors cursor-pointer"
                        >
                          <span className="text-sm text-zinc-700 w-3 shrink-0 tabular-nums">
                            {i + 1}
                          </span>
                          {item.image ? (
                            // eslint-disable-next-line @next/next/no-img-element
                            <img
                              src={item.image}
                              alt={item.name}
                              className="w-6 h-6 rounded-full shrink-0"
                            />
                          ) : (
                            <div className="w-6 h-6 rounded-full shrink-0 bg-white/10 text-xs font-semibold text-zinc-200 flex items-center justify-center">
                              {item.symbol.slice(0, 1)}
                            </div>
                          )}
                          <div className="flex-1 min-w-0">
                            <div className="flex items-center justify-between">
                              <span className="text-sm font-semibold text-zinc-200 uppercase">
                                {item.symbol}
                              </span>
                              <span className="text-sm font-semibold tabular-nums">
                                {fmt(item.price)}
                              </span>
                            </div>
                            <div className="flex items-center justify-between mt-0.5">
                              <span className="text-sm text-zinc-600 truncate">
                                {item.name}
                              </span>
                              <span
                                className={`text-sm font-medium tabular-nums flex items-center gap-0.5 ${
                                  pos ? "text-emerald-400" : "text-red-400"
                                }`}
                              >
                                {pos ? (
                                  <ArrowUpRight className="w-2.5 h-2.5" />
                                ) : (
                                  <ArrowDownRight className="w-2.5 h-2.5" />
                                )}
                                {Math.abs(item.changePercent).toFixed(2)}%
                              </span>
                            </div>
                          </div>
                          {item.sparkline.length > 0 && (
                            <div className="shrink-0">
                              <Sparkline
                                data={item.sparkline}
                                positive={pos}
                                width={56}
                                height={28}
                              />
                            </div>
                          )}
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            </section>

            {/* ── AI Chat panel ── */}
            {isCrypto && showChat && messages.length > 0 && (
              <section className="bg-[#0a0a0a] border border-white/[0.07] rounded-xl overflow-hidden">
                <div className="flex items-center justify-between px-4 py-2.5 border-b border-white/[0.05]">
                  <div className="flex items-center gap-2">
                    <Zap className="w-3.5 h-3.5 text-blue-400" />
                    <span className="text-sm font-medium text-zinc-300">
                      AI Analysis
                    </span>
                    <span className="text-sm text-zinc-600">
                      · claude-sonnet-4-6
                    </span>
                  </div>
                  <button
                    onClick={() => {
                      setShowChat(false);
                      setMessages([]);
                    }}
                    className="text-zinc-600 hover:text-zinc-300 transition-colors"
                  >
                    <X className="w-3.5 h-3.5" />
                  </button>
                </div>
                <div className="p-4 max-h-80 overflow-y-auto space-y-4">
                  {messages.map((msg, idx) => (
                    <div
                      key={msg.id || idx}
                      className={`flex gap-2.5 ${
                        msg.role === "user" ? "justify-end" : "justify-start"
                      }`}
                    >
                      {msg.role === "assistant" && (
                        <div className="w-5 h-5 rounded-full bg-blue-500/20 border border-blue-500/30 flex items-center justify-center shrink-0 mt-0.5">
                          <Zap className="w-2.5 h-2.5 text-blue-400" />
                        </div>
                      )}
                      {msg.role === "assistant" && msg.coinCard ? (
                        <div className="flex-1 min-w-0 space-y-3">
                          <CoinDetailCard meta={msg.coinCard} />
                          {!msg.coinCard.loading &&
                            msg.content &&
                            msg.content !== "__ASSET_CARD__" && (
                              <MarkdownMessage
                                text={msg.content}
                                streaming={false}
                              />
                            )}
                          <MessageContext
                            message={msg}
                            disabled={streaming}
                            onFollowUp={(prompt) => {
                              void sendPrompt(prompt);
                            }}
                          />
                        </div>
                      ) : msg.role === "assistant" &&
                        (msg.portfolio || msg.portfolioLoading) ? (
                        <div className="flex-1 min-w-0 space-y-3">
                          {msg.portfolio ? (
                            <PortfolioCard data={msg.portfolio} />
                          ) : (
                            <div className="bg-[#0a0a0a] border border-white/[0.07] rounded-xl p-6 flex items-center gap-3">
                              <RefreshCw className="w-4 h-4 animate-spin text-zinc-500" />
                              <span className="text-sm text-zinc-500">
                                Loading portfolio data…
                              </span>
                            </div>
                          )}
                          {msg.content &&
                            msg.content !== "__PORTFOLIO_CARD__" && (
                              <MarkdownMessage
                                text={msg.content}
                                streaming={false}
                              />
                            )}
                          <MessageContext
                            message={msg}
                            disabled={streaming}
                            onFollowUp={(prompt) => {
                              void sendPrompt(prompt);
                            }}
                          />
                        </div>
                      ) : msg.role === "assistant" ? (
                        <div className="flex-1 min-w-0 max-w-[88%]">
                          <MarkdownMessage
                            text={msg.content}
                            streaming={streaming && idx === messages.length - 1}
                          />
                          <MessageContext
                            message={msg}
                            disabled={streaming}
                            onFollowUp={(prompt) => {
                              void sendPrompt(prompt);
                            }}
                          />
                        </div>
                      ) : (
                        <div className="text-sm leading-relaxed max-w-[88%] bg-white/8 border border-white/8 rounded-2xl rounded-tr-sm px-3 py-2 text-zinc-200">
                          {msg.content}
                        </div>
                      )}
                    </div>
                  ))}
                  <div ref={chatEndRef} />
                </div>
              </section>
            )}
          </div>
          {/* end left column */}

          {/* ── Right sidebar: Prediction Markets ── */}
          {isCrypto && (
            <aside className="w-full lg:w-[300px] lg:shrink-0 lg:sticky lg:top-[57px] lg:self-start">
              <section>
                <div className="flex items-center justify-between mb-3">
                  <h2 className="text-sm font-medium text-zinc-500 uppercase tracking-wider">
                    Prediction Markets
                  </h2>
                  <a
                    href="https://polymarket.com"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-sm text-zinc-600 hover:text-zinc-400 flex items-center gap-1 transition-colors"
                  >
                    Polymarket
                    <ExternalLink className="w-2.5 h-2.5" />
                  </a>
                </div>

                {predictions.length === 0 ? (
                  <div className="bg-[#0a0a0a] border border-white/[0.07] rounded-xl p-8 text-center">
                    <p className="text-sm text-zinc-600">
                      Prediction markets unavailable
                    </p>
                  </div>
                ) : (
                  <div className="space-y-2.5">
                    {predictions.map((market) => {
                      const yesPct = Math.round(
                        parseFloat(market.outcomePrices?.[0] ?? "0.5") * 100,
                      );
                      const isHigh = yesPct >= 50;
                      const vol = parseFloat(market.volume ?? "0");

                      return (
                        <a
                          key={market.id}
                          href={
                            market.slug
                              ? `https://polymarket.com/event/${market.slug}`
                              : "https://polymarket.com"
                          }
                          target="_blank"
                          rel="noopener noreferrer"
                          className="block bg-[#0a0a0a] border border-white/[0.14] rounded-xl p-4 hover:border-white/[0.25] hover:bg-[#111] transition-all cursor-pointer"
                        >
                          <p className="text-sm font-medium text-zinc-200 leading-snug mb-3 line-clamp-2">
                            {market.question}
                          </p>
                          <div className="space-y-1.5">
                            <div className="flex items-center justify-between text-sm">
                              <div className="flex gap-2">
                                <span
                                  className={
                                    isHigh
                                      ? "text-emerald-400"
                                      : "text-zinc-400"
                                  }
                                >
                                  Yes{" "}
                                  <span className="font-semibold tabular-nums">
                                    {yesPct}%
                                  </span>
                                </span>
                                <span
                                  className={
                                    !isHigh ? "text-red-400" : "text-zinc-500"
                                  }
                                >
                                  No{" "}
                                  <span className="font-semibold tabular-nums">
                                    {100 - yesPct}%
                                  </span>
                                </span>
                              </div>
                            </div>
                            <div className="h-1.5 bg-white/[0.05] rounded-full overflow-hidden">
                              <div
                                className="h-full rounded-full"
                                style={{
                                  width: `${yesPct}%`,
                                  background: isHigh
                                    ? "linear-gradient(90deg,#22c55e,#16a34a)"
                                    : "linear-gradient(90deg,#ef4444,#dc2626)",
                                }}
                              />
                            </div>
                            <div className="flex items-center justify-between text-sm text-zinc-600">
                              <span>Vol {fmtBig(vol)}</span>
                              {market.endDate && (
                                <span>
                                  {new Date(market.endDate).toLocaleDateString(
                                    "en-US",
                                    {
                                      month: "short",
                                      day: "numeric",
                                      year: "2-digit",
                                    },
                                  )}
                                </span>
                              )}
                            </div>
                          </div>
                        </a>
                      );
                    })}
                  </div>
                )}
              </section>
            </aside>
          )}
          {isCommodity && (
            <aside className="w-full lg:w-[300px] lg:shrink-0 lg:sticky lg:top-[57px] lg:self-start">
              <section>
                <div className="flex items-center justify-between mb-3">
                  <h2 className="text-sm font-medium text-zinc-500 uppercase tracking-wider">
                    Commodity News
                  </h2>
                  <span className="text-sm text-zinc-600">Google News</span>
                </div>
                <div className="bg-[#0a0a0a] border border-white/[0.07] rounded-xl overflow-hidden">
                  {commodityNewsLoading ? (
                    <div className="p-6 text-center text-sm text-zinc-600">
                      Loading news...
                    </div>
                  ) : commodityNews.length === 0 ? (
                    <div className="p-6 text-center text-sm text-zinc-600">
                      No news available
                    </div>
                  ) : (
                    <div className="divide-y divide-white/[0.04]">
                      {commodityNews.map((item, idx) => (
                        <a
                          key={`${item.link}-${idx}`}
                          href={item.link}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="block px-4 py-3 hover:bg-white/[0.02] transition-colors"
                        >
                          <p className="text-sm text-zinc-200 leading-snug mb-2">
                            {item.title}
                          </p>
                          <div className="flex items-center gap-2 text-xs text-zinc-600">
                            <span className="uppercase tracking-wide text-[10px] px-2 py-0.5 rounded-full bg-white/10 text-zinc-300">
                              {item.tag}
                            </span>
                            <span>{item.publisher}</span>
                            <span className="text-zinc-700">
                              {item.timeAgo}
                            </span>
                          </div>
                        </a>
                      ))}
                    </div>
                  )}
                </div>
              </section>
            </aside>
          )}
        </div>
        {/* end outer flex */}
      </main>

      {/* ── Trading Dashboard Modal ── */}
      <TradingDashboardModal
        open={showTradingModal}
        onClose={() => setShowTradingModal(false)}
      />

      {/* ── Fixed search bar ── */}
      <div className="fixed bottom-0 left-0 right-0 z-50 bg-linear-to-t from-[#000000] via-[#000000]/95 to-transparent pt-6 pb-4 px-5">
        <div className="max-w-7xl mx-auto">
          <form onSubmit={handleSearch}>
            <div className="flex items-center gap-3 bg-[#0a0a0a] border border-white/12 rounded-2xl px-4 py-3 focus-within:border-white/26 hover:border-white/18 transition-colors shadow-[0_0_40px_rgba(0,0,0,0.8)]">
              <Search className="w-4 h-4 text-zinc-500 shrink-0" />
              <input
                ref={inputRef}
                type="text"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Ask about crypto, stocks, ETFs, prices, trends…"
                className="flex-1 bg-transparent text-sm text-zinc-100 placeholder:text-zinc-500 outline-none"
                disabled={streaming}
              />
              {query && !streaming && (
                <button
                  type="button"
                  onClick={() => setQuery("")}
                  className="text-zinc-600 hover:text-zinc-300 transition-colors"
                >
                  <X className="w-3.5 h-3.5" />
                </button>
              )}
              <button
                type="submit"
                disabled={!query.trim() || streaming}
                className="flex items-center gap-1.5 bg-white text-black rounded-xl px-3 py-1.5 text-sm font-semibold disabled:opacity-30 disabled:cursor-not-allowed hover:bg-zinc-200 transition-colors shrink-0"
              >
                {streaming ? (
                  <>
                    <RefreshCw className="w-3 h-3 animate-spin" />
                    <span>Thinking</span>
                  </>
                ) : (
                  <>
                    <Send className="w-3 h-3" />
                    <span>Ask</span>
                  </>
                )}
              </button>
            </div>
            <p className="text-center text-sm text-zinc-700 mt-2">
              <TrendingUp className="w-3 h-3 inline -mt-0.5 mr-1" />
              Powered by OpenRouter · Market data from CoinGecko, Yahoo Finance
              & Polymarket
            </p>
          </form>
        </div>
      </div>
    </div>
  );
}
