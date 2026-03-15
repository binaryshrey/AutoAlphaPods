"use client";

import { useState, useEffect, useRef, useCallback } from "react";
import { useSearchParams } from "next/navigation";
import dynamic from "next/dynamic";
import Link from "next/link";
import {
  Search,
  Send,
  X,
  RefreshCw,
  ArrowUpRight,
  ArrowDownRight,
  ExternalLink,
  TrendingUp,
  Zap,
  ChevronUp,
  ChevronDown,
  ArrowLeft,
} from "lucide-react";

const ReactApexChart = dynamic(() => import("react-apexcharts"), {
  ssr: false,
  loading: () => <div className="w-20 h-9" />,
});

// ─── Types ─────────────────────────────────────────────────────────────────────

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

interface EquityPoint {
  date: string;
  equity: number;
}

interface DrawdownPoint {
  date: string;
  drawdown_pct: number;
}

interface ExposurePoint {
  date: string;
  gross: number;
  net: number;
}

interface RollingSharpePoint {
  date: string;
  sharpe: number;
}

interface PositionPoint {
  date: string;
  price: number;
  position: number;
}

interface TradeEventPoint extends PositionPoint {
  delta: number;
}

interface TickerPositionSeries {
  ticker: string;
  points: PositionPoint[];
  trade_events: TradeEventPoint[];
}

interface BacktestResult {
  prompt: string;
  tickers: string;
  generated_code: string;
  sharpe: number;
  sortino: number;
  calmar: number;
  max_dd: string;
  cagr: string;
  volatility?: string;
  win_rate: string;
  max_streak: number;
  total_return: string;
  end_equity: string;
  best_day?: string;
  worst_day?: string;
  rebalance_days?: number;
  avg_daily_turnover?: string;
  avg_gross_exposure?: string;
  avg_net_exposure?: string;
  equity_curve: EquityPoint[];
  drawdown_curve?: DrawdownPoint[];
  exposure_curve?: ExposurePoint[];
  rolling_sharpe_63d?: RollingSharpePoint[];
  position_series?: TickerPositionSeries[];
}

interface BacktestStreamLog {
  id: string;
  message: string;
  stage: string;
  level: "info" | "warning" | "error";
  timestamp: string;
}

interface ChatMessage {
  id: string;
  role: "user" | "assistant";
  content: string;
  coinCard?: CoinCardMeta;
  portfolio?: PortfolioData;
  portfolioLoading?: boolean;
  backtest?: BacktestResult;
  backtestLoading?: boolean;
  backtestLogs?: BacktestStreamLog[];
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

function makeMessageId(): string {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

const BACKTEST_API_BASE =
  process.env.NEXT_PUBLIC_BACKTEST_API_BASE_URL?.replace(/\/$/, "") ||
  "https://autoalphapods-1.onrender.com";
const DEFAULT_BACKTEST_START = "2015-01-01";
const DEFAULT_BACKTEST_INITIAL_CASH = 100000;

function getDefaultBacktestEndDate(): string {
  return new Date().toISOString().slice(0, 10);
}

const BACKTEST_STRONG_HINT_RE =
  /\b(backtest|back-test|equity curve|max drawdown|cagr|sharpe|sortino|calmar|win rate)\b/i;
const BACKTEST_STRATEGY_HINT_RE =
  /\b(strategy|signal|signals|weights?|allocation|rebalance|long|short|entry|exit)\b/i;
const BACKTEST_EVAL_HINT_RE =
  /\b(test|evaluate|simulate|simulation|optimi[sz]e|historical|history|performance|returns?)\b/i;
const YEAR_HINT_RE = /\b(?:19|20)\d{2}\b/;

function shouldRunBacktest(prompt: string): boolean {
  const text = prompt.trim();
  if (!text) return false;
  if (BACKTEST_STRONG_HINT_RE.test(text)) return true;
  const hasStrategyHint = BACKTEST_STRATEGY_HINT_RE.test(text);
  if (!hasStrategyHint) return false;
  return BACKTEST_EVAL_HINT_RE.test(text) || YEAR_HINT_RE.test(text);
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

function appendBacktestLog(
  existing: BacktestStreamLog[] | undefined,
  next: BacktestStreamLog,
): BacktestStreamLog[] {
  return [...(existing ?? []), next].slice(-120);
}

function parseSseEvent(
  rawChunk: string,
): { event: string; data: unknown } | null {
  const lines = rawChunk
    .replace(/\r\n/g, "\n")
    .split("\n")
    .map((line) => line.trimEnd());
  let event = "message";
  const dataLines: string[] = [];

  for (const line of lines) {
    if (!line) continue;
    if (line.startsWith("event:")) {
      event = line.slice(6).trim();
      continue;
    }
    if (line.startsWith("data:")) {
      dataLines.push(line.slice(5).trim());
    }
  }

  if (!dataLines.length) return null;
  const payloadText = dataLines.join("\n");

  try {
    return { event, data: JSON.parse(payloadText) };
  } catch {
    return { event, data: payloadText };
  }
}

// ─── Inline markdown renderer ─────────────────────────────────────────────────

function renderInline(text: string): React.ReactNode {
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
              className="bg-white/10 rounded px-1 font-mono text-[10px] text-zinc-300"
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

function SourcesFooter({ raw }: { raw: string }) {
  const sources = raw
    .split(/\s*·\s*/)
    .map((s) => s.trim())
    .filter(Boolean);
  return (
    <div className="flex flex-wrap items-center gap-1.5 pt-2 mt-1 border-t border-white/6">
      <span className="text-[10px] text-zinc-600">Sources:</span>
      {sources.map((src) => {
        const url = SOURCE_URLS[src];
        return url ? (
          <a
            key={src}
            href={url}
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-white/5 border border-white/8 text-[10px] text-zinc-400 hover:text-zinc-200 hover:border-white/16 transition-colors"
          >
            {src}
            <ExternalLink className="w-2 h-2 opacity-60" />
          </a>
        ) : (
          <span
            key={src}
            className="px-1.5 py-0.5 rounded-md bg-white/5 border border-white/8 text-[10px] text-zinc-400"
          >
            {src}
          </span>
        );
      })}
    </div>
  );
}

function MetricPill({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center gap-2 rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-zinc-300">
      <span className="text-zinc-500 uppercase tracking-wide">{label}</span>
      <span className="text-zinc-100 font-semibold">{value}</span>
    </div>
  );
}

function BacktestCard({ data }: { data: BacktestResult }) {
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [showCode, setShowCode] = useState(false);

  const tickerPositionSeries = data.position_series ?? [];
  const [selectedTicker, setSelectedTicker] = useState(
    tickerPositionSeries[0]?.ticker ?? "",
  );
  const activeTicker =
    selectedTicker &&
    tickerPositionSeries.some((t) => t.ticker === selectedTicker)
      ? selectedTicker
      : (tickerPositionSeries[0]?.ticker ?? "");

  const equitySeries = [
    {
      name: "Equity",
      data: data.equity_curve.map((point) => [
        new Date(point.date).getTime(),
        point.equity,
      ]),
    },
  ];
  const rollingSharpeSeries = [
    {
      name: "Rolling Sharpe (63D)",
      data: (data.rolling_sharpe_63d ?? []).map((point) => [
        new Date(point.date).getTime(),
        point.sharpe,
      ]),
    },
  ];

  const selectedTickerSeries =
    tickerPositionSeries.find((item) => item.ticker === activeTicker) ??
    tickerPositionSeries[0];
  const tickerOverlaySeries = selectedTickerSeries
    ? [
        {
          name: `${selectedTickerSeries.ticker} Price`,
          type: "line",
          data: selectedTickerSeries.points.map((point) => [
            new Date(point.date).getTime(),
            point.price,
          ]),
        },
        {
          name: "Position %",
          type: "area",
          data: selectedTickerSeries.points.map((point) => [
            new Date(point.date).getTime(),
            point.position * 100,
          ]),
        },
      ]
    : [];

  const baseChartOptions = {
    chart: {
      toolbar: { show: false },
      animations: { enabled: false },
      background: "transparent",
    },
    grid: {
      borderColor: "rgba(255,255,255,0.08)",
      strokeDashArray: 3,
      padding: { left: 8, right: 8, top: 4, bottom: 4 },
    },
    xaxis: {
      type: "datetime" as const,
      labels: { style: { colors: "#6b7280", fontSize: "10px" } },
      axisBorder: { show: false },
      axisTicks: { show: false },
    },
    tooltip: {
      theme: "dark" as const,
      x: { format: "dd MMM yyyy" },
    },
  };

  const primaryOptions = {
    ...baseChartOptions,
    dataLabels: {
      enabled: false,
    },
    markers: {
      size: 2,
    },
    stroke: {
      curve: "smooth" as const,
      width: 2,
    },
    fill: {
      type: "gradient" as const,
      gradient: {
        shadeIntensity: 1,
        opacityFrom: 0.3,
        opacityTo: 0.03,
        stops: [0, 100],
      },
    },
    colors: ["#10b981"],
    yaxis: {
      labels: {
        style: { colors: "#6b7280", fontSize: "10px" },
        formatter: (val: number) =>
          `$${val.toLocaleString("en-US", { maximumFractionDigits: 0 })}`,
      },
    },
    legend: {
      show: false,
      labels: { colors: "#a1a1aa" },
    },
  };

  const tickerOverlayOptions = {
    ...baseChartOptions,
    colors: ["#f8fafc", "#22c55e"],
    stroke: {
      curve: "smooth" as const,
      width: [2, 1.8],
    },
    fill: {
      type: ["solid", "gradient"],
      gradient: {
        shadeIntensity: 1,
        opacityFrom: 0.35,
        opacityTo: 0.04,
        stops: [0, 100],
      },
    },
    yaxis: [
      {
        labels: {
          style: { colors: "#6b7280", fontSize: "10px" },
          formatter: (val: number) =>
            `$${val.toLocaleString("en-US", { maximumFractionDigits: 2 })}`,
        },
        title: {
          text: "Price",
          style: { color: "#71717a", fontSize: "10px" },
        },
      },
      {
        opposite: true,
        min: -100,
        max: 100,
        labels: {
          style: { colors: "#6b7280", fontSize: "10px" },
          formatter: (val: number) => `${val.toFixed(0)}%`,
        },
        title: {
          text: "Position",
          style: { color: "#71717a", fontSize: "10px" },
        },
      },
    ],
    legend: {
      show: true,
      labels: { colors: "#a1a1aa" },
    },
  };

  const rollingSharpeOptions = {
    ...baseChartOptions,
    stroke: { curve: "smooth" as const, width: 2 },
    colors: ["#facc15"],
    yaxis: {
      labels: {
        style: { colors: "#6b7280", fontSize: "10px" },
        formatter: (val: number) => val.toFixed(2),
      },
    },
  };

  return (
    <div className="rounded-2xl border border-white/10 bg-linear-to-br from-[#0a0b14] via-[#0a0a0a] to-[#10131f] p-5 space-y-4 shadow-[0_25px_80px_rgba(0,0,0,0.45)]">
      <div className="rounded-xl border border-white/10 bg-black/35 p-4 space-y-3">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="space-y-1.5">
            <p className="text-[10px] uppercase tracking-[0.2em] text-zinc-500">
              Backtest Strategy
            </p>
            <p className="text-sm text-zinc-100 leading-relaxed">
              {data.prompt}
            </p>
          </div>
          <div className="flex flex-wrap gap-2">
            <MetricPill label="Tickers" value={data.tickers} />
            <MetricPill label="End Equity" value={data.end_equity} />
            <MetricPill label="Total Return" value={data.total_return} />
          </div>
        </div>
        <div className="flex flex-wrap gap-2">
          <MetricPill label="Sharpe" value={data.sharpe.toFixed(2)} />
          <MetricPill label="Sortino" value={data.sortino.toFixed(2)} />
          <MetricPill label="Calmar" value={data.calmar.toFixed(2)} />
          <MetricPill label="Volatility" value={data.volatility || "—"} />
          <MetricPill label="Max DD" value={data.max_dd} />
          <MetricPill label="CAGR" value={data.cagr} />
          <MetricPill label="Win Rate" value={data.win_rate} />
          <MetricPill label="Best Day" value={data.best_day || "—"} />
          <MetricPill label="Worst Day" value={data.worst_day || "—"} />
          <MetricPill label="Turnover" value={data.avg_daily_turnover || "—"} />
        </div>
      </div>

      <div className="rounded-xl border border-white/10 bg-black/35 p-4">
        <div className="flex items-center justify-between gap-3 mb-3">
          <span className="px-2.5 py-1 rounded-md text-[11px] border bg-white text-black border-white">
            Equity
          </span>
          <span className="text-[10px] text-zinc-500">Daily</span>
        </div>
        <ReactApexChart
          type="area"
          series={equitySeries}
          options={primaryOptions}
          height={250}
        />
      </div>

      <div className="rounded-xl border border-white/10 bg-black/35 p-3">
        <button
          type="button"
          onClick={() => setShowAdvanced((prev) => !prev)}
          className="w-full flex items-center justify-between text-xs text-zinc-300 hover:text-white transition-colors"
        >
          <span>
            Advanced analytics (positions, rolling Sharpe, model code)
          </span>
          {showAdvanced ? (
            <ChevronUp className="w-3.5 h-3.5" />
          ) : (
            <ChevronDown className="w-3.5 h-3.5" />
          )}
        </button>

        {showAdvanced && (
          <div className="mt-3 space-y-4">
            <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
              <div className="rounded-lg border border-white/8 bg-white/[0.02] px-2.5 py-2">
                <p className="text-[10px] text-zinc-500">Rebalance Days</p>
                <p className="text-[12px] font-semibold text-zinc-100">
                  {data.rebalance_days ?? "—"}
                </p>
              </div>
              <div className="rounded-lg border border-white/8 bg-white/[0.02] px-2.5 py-2">
                <p className="text-[10px] text-zinc-500">Avg Gross Exposure</p>
                <p className="text-[12px] font-semibold text-zinc-100">
                  {data.avg_gross_exposure || "—"}
                </p>
              </div>
              <div className="rounded-lg border border-white/8 bg-white/[0.02] px-2.5 py-2">
                <p className="text-[10px] text-zinc-500">Avg Net Exposure</p>
                <p className="text-[12px] font-semibold text-zinc-100">
                  {data.avg_net_exposure || "—"}
                </p>
              </div>
              <div className="rounded-lg border border-white/8 bg-white/[0.02] px-2.5 py-2">
                <p className="text-[10px] text-zinc-500">Max Losing Streak</p>
                <p className="text-[12px] font-semibold text-zinc-100">
                  {data.max_streak}
                </p>
              </div>
            </div>

            {tickerPositionSeries.length > 0 && (
              <div className="rounded-xl border border-white/10 bg-black/30 p-3">
                <div className="flex items-center justify-between gap-3 mb-3">
                  <p className="text-[11px] uppercase tracking-wider text-zinc-500">
                    Positions On Price
                  </p>
                  <div className="flex flex-wrap gap-1.5">
                    {tickerPositionSeries.map((series) => (
                      <button
                        key={series.ticker}
                        type="button"
                        onClick={() => setSelectedTicker(series.ticker)}
                        className={`px-2 py-0.5 rounded-md text-[10px] border transition-colors ${
                          activeTicker === series.ticker
                            ? "bg-emerald-400/20 border-emerald-400/50 text-emerald-300"
                            : "bg-white/[0.02] border-white/10 text-zinc-400 hover:text-zinc-200 hover:border-white/25"
                        }`}
                      >
                        {series.ticker}
                      </button>
                    ))}
                  </div>
                </div>
                <ReactApexChart
                  type="line"
                  series={tickerOverlaySeries}
                  options={tickerOverlayOptions}
                  height={300}
                />
              </div>
            )}

            {(data.rolling_sharpe_63d ?? []).length > 0 && (
              <div className="rounded-xl border border-white/10 bg-black/30 p-3">
                <p className="text-[11px] uppercase tracking-wider text-zinc-500 mb-2">
                  Rolling Sharpe (63 Trading Days)
                </p>
                <ReactApexChart
                  type="line"
                  series={rollingSharpeSeries}
                  options={rollingSharpeOptions}
                  height={200}
                />
              </div>
            )}

            <div className="rounded-xl border border-white/10 bg-black/30 p-3">
              <button
                type="button"
                onClick={() => setShowCode((prev) => !prev)}
                className="w-full flex items-center justify-between text-[11px] text-zinc-400 hover:text-zinc-200 transition-colors"
              >
                <span>Generated strategy code</span>
                {showCode ? (
                  <ChevronUp className="w-3.5 h-3.5" />
                ) : (
                  <ChevronDown className="w-3.5 h-3.5" />
                )}
              </button>
              {showCode && (
                <pre className="mt-2 text-xs text-zinc-200 overflow-x-auto rounded-lg border border-white/10 bg-black/50 p-3">
                  <code>{data.generated_code}</code>
                </pre>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function BacktestLogPanel({
  logs,
  running,
}: {
  logs: BacktestStreamLog[];
  running: boolean;
}) {
  const [open, setOpen] = useState(true);
  const logEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (open) {
      logEndRef.current?.scrollIntoView({ behavior: "smooth", block: "end" });
    }
  }, [logs, open]);

  const latest = logs[logs.length - 1];

  return (
    <div className="rounded-2xl border border-white/10 bg-[#0a0a0a] overflow-hidden">
      <button
        type="button"
        onClick={() => setOpen((prev) => !prev)}
        className="w-full flex items-center justify-between gap-3 px-4 py-3 bg-white/[0.02] hover:bg-white/[0.04] transition-colors"
      >
        <div className="min-w-0 flex items-center gap-3 text-left">
          <div className="w-8 h-8 rounded-xl border border-emerald-500/20 bg-emerald-500/10 flex items-center justify-center shrink-0">
            <RefreshCw
              className={`w-4 h-4 text-emerald-400 ${running ? "animate-spin" : ""}`}
            />
          </div>
          <div className="min-w-0">
            <p className="text-xs font-semibold uppercase tracking-[0.18em] text-zinc-500">
              Backtest Log
            </p>
            <p className="text-sm text-zinc-200 truncate">
              {latest?.message || "Connecting to stream…"}
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2 shrink-0">
          <span className="rounded-full border border-white/10 bg-white/5 px-2.5 py-1 text-[10px] uppercase tracking-wide text-zinc-400">
            {running ? `${logs.length || 1} events` : "complete"}
          </span>
          {open ? (
            <ChevronUp className="w-4 h-4 text-zinc-500" />
          ) : (
            <ChevronDown className="w-4 h-4 text-zinc-500" />
          )}
        </div>
      </button>

      {open && (
        <div className="border-t border-white/8 bg-black/50">
          <div className="max-h-80 overflow-y-auto px-4 py-3 font-mono text-[11px] leading-5">
            {logs.length ? (
              <div className="space-y-2">
                {logs.map((log) => {
                  const timestamp = new Date(log.timestamp);
                  const timeLabel = Number.isNaN(timestamp.getTime())
                    ? "--:--:--"
                    : timestamp.toLocaleTimeString("en-US", {
                        hour12: false,
                        hour: "2-digit",
                        minute: "2-digit",
                        second: "2-digit",
                      });
                  const levelClass =
                    log.level === "error"
                      ? "text-red-300"
                      : log.level === "warning"
                        ? "text-amber-300"
                        : "text-zinc-200";

                  return (
                    <div
                      key={log.id}
                      className="grid grid-cols-[68px_92px_minmax(0,1fr)] gap-3"
                    >
                      <span className="text-zinc-600">{timeLabel}</span>
                      <span className="text-zinc-500 uppercase truncate">
                        {log.stage.replace(/_/g, " ")}
                      </span>
                      <span className={levelClass}>{log.message}</span>
                    </div>
                  );
                })}
                <div ref={logEndRef} />
              </div>
            ) : (
              <div className="text-zinc-500">
                Waiting for the first log line…
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

// ─── Markdown message ─────────────────────────────────────────────────────────

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
          className="text-[13px] font-semibold text-zinc-200 mt-2"
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
    <div className="space-y-2">
      {blocks}
      {streaming && (
        <span className="inline-block w-0.5 h-3.5 bg-zinc-400 animate-pulse ml-0.5 align-middle" />
      )}
      {!streaming && sourcesRaw && <SourcesFooter raw={sourcesRaw} />}
    </div>
  );
}

// ─── Message context (news + follow-ups) ─────────────────────────────────────

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
    <div className="mt-3 space-y-2.5">
      {message.newsLoading && (
        <div className="flex items-center gap-1.5 text-[11px] text-zinc-500">
          <RefreshCw className="w-2.5 h-2.5 animate-spin" />
          Fetching related news…
        </div>
      )}
      {hasNews && (
        <div className="rounded-xl border border-white/8 bg-white/[0.02] p-3">
          <p className="text-[10px] text-zinc-500 uppercase tracking-wide mb-2">
            Related News
          </p>
          <div className="space-y-2">
            {(message.news ?? []).slice(0, 3).map((item) => (
              <a
                key={`${item.link}-${item.title}`}
                href={item.link}
                target="_blank"
                rel="noopener noreferrer"
                className="block rounded-lg border border-white/8 bg-white/[0.02] px-2.5 py-2 hover:border-white/16 hover:bg-white/[0.04] transition-colors"
              >
                <p className="text-[12px] text-zinc-200 leading-snug">
                  {item.title}
                </p>
                <p className="text-[10px] text-zinc-500 mt-0.5">
                  {[item.source, item.publishedAt].filter(Boolean).join(" · ")}
                </p>
              </a>
            ))}
          </div>
        </div>
      )}
      {hasFollowUps && (
        <div className="rounded-xl border border-white/8 bg-white/[0.02] p-3">
          <p className="text-[10px] text-zinc-500 uppercase tracking-wide mb-2">
            Suggested Follow-Ups
          </p>
          <div className="flex flex-wrap gap-1.5">
            {(message.followUps ?? []).slice(0, 3).map((q) => (
              <button
                key={q}
                type="button"
                disabled={disabled}
                onClick={() => onFollowUp(q)}
                className="text-left px-2.5 py-1.5 rounded-lg border border-white/10 bg-white/[0.03] text-[11px] text-zinc-300 hover:text-white hover:border-white/20 hover:bg-white/[0.06] transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
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

// ─── Card Sparkline (full-width area) ────────────────────────────────────────

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

// ─── Coin Detail Card ─────────────────────────────────────────────────────────

const RANGES = ["1D", "1W", "1M", "3M", "1Y"] as const;

function CoinDetailCard({ meta }: { meta: CoinCardMeta }) {
  const assetType = meta.assetType;
  const [range, setRange] = useState(meta.range);
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
    const base = meta.data;
    if (assetType === "crypto" && base) {
      if (r === "1D") {
        setLocalDetail({ ...base, prices: base.prices.slice(-24) });
        return;
      }
      if (r === "1W") {
        setLocalDetail(undefined);
        return;
      }
    }
    setLoadingRange(true);
    try {
      if (assetType === "crypto" && !meta.coinId)
        throw new Error("coinId missing");
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
      // keep existing
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
      if (!payload?.order) throw new Error("Order response was empty.");
      setPlacedOrder(payload.order);
    } catch {
      setTradeError("Order could not be placed. Please try again.");
    } finally {
      setSubmittingOrder(false);
    }
  };

  if (meta.loading || !detail) {
    return (
      <div className="bg-[#0a0a0a] border border-white/[0.07] rounded-xl p-6 flex items-center gap-3">
        <RefreshCw className="w-4 h-4 animate-spin text-zinc-500" />
        <span className="text-xs text-zinc-500">
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
      <div className="px-4 pt-4 pb-3">
        <div className="flex items-start justify-between gap-2 mb-3">
          <div className="flex items-center gap-2">
            {detail.image && (
              // eslint-disable-next-line @next/next/no-img-element
              <img
                src={detail.image}
                alt={detail.name}
                className="w-7 h-7 rounded-full"
              />
            )}
            <div>
              <p className="text-sm font-semibold text-white leading-tight">
                {detail.name} USD
              </p>
              <p className="text-[10px] text-zinc-500 uppercase tracking-wide">
                {detail.symbol} ·{" "}
                {assetType === "crypto" ? "CRYPTO" : "EQUITY / ETF"}
              </p>
            </div>
          </div>
          <a
            href="https://app.alpaca.markets"
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-1 px-2.5 py-1 rounded-lg bg-emerald-500/15 border border-emerald-500/30 text-[11px] font-semibold text-emerald-400 hover:bg-emerald-500/25 hover:border-emerald-500/50 transition-colors whitespace-nowrap shrink-0"
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
            className={`text-sm font-medium tabular-nums ${
              pos ? "text-emerald-400" : "text-red-400"
            }`}
          >
            {pos ? "+" : "-"}
            {fmt(changeAbs)}
          </span>
          <span
            className={`text-sm font-medium flex items-center gap-0.5 ${
              pos ? "text-emerald-400" : "text-red-400"
            }`}
          >
            {pos ? (
              <ArrowUpRight className="w-3.5 h-3.5" />
            ) : (
              <ArrowDownRight className="w-3.5 h-3.5" />
            )}
            {Math.abs(detail.price_change_percentage_24h).toFixed(2)}%
          </span>
        </div>
        <p className="text-[10px] text-zinc-600 mt-1.5">
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

      <div className="flex border-b border-white/[0.05] px-3">
        {RANGES.map((r) => (
          <button
            key={r}
            onClick={() => changeRange(r)}
            className={`px-2.5 py-2 text-[11px] font-medium border-b-2 transition-colors ${
              range === r
                ? "border-white text-white"
                : "border-transparent text-zinc-500 hover:text-zinc-300"
            }`}
          >
            {r}
          </button>
        ))}
      </div>

      <div
        className={
          loadingRange
            ? "opacity-40 pointer-events-none transition-opacity"
            : "transition-opacity"
        }
      >
        <CardSparkline data={chartData} positive={pos} height={120} />
      </div>

      <div className="grid grid-cols-3 gap-px bg-white/[0.04] border-t border-white/[0.05]">
        {stats.map(({ label, value }) => (
          <div key={label} className="bg-[#0a0a0a] px-3 py-2.5">
            <p className="text-[10px] text-zinc-600 mb-0.5">{label}</p>
            <p className="text-[11px] font-semibold text-zinc-200 tabular-nums">
              {value}
            </p>
          </div>
        ))}
      </div>

      <div className="px-3 py-2 border-t border-white/5 flex items-center justify-between">
        <span className="text-[10px] text-zinc-700">
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
          className="flex items-center gap-1 px-1.5 py-0.5 rounded-md bg-white/5 border border-white/8 text-[10px] text-zinc-400 hover:text-zinc-200 hover:border-white/16 transition-colors"
        >
          {assetType === "crypto" ? "CoinGecko" : "Yahoo Finance"}
          <ExternalLink className="w-2 h-2 opacity-60" />
        </a>
      </div>

      <div
        className={`${
          meta.buyIntent
            ? "border-emerald-500/20 bg-emerald-500/5"
            : "border-white/5"
        } border-t`}
      >
        <div className="px-3 py-2 flex items-center gap-2">
          <span className="text-[10px] text-zinc-500 mr-1">Trade on</span>
          <button
            type="button"
            onClick={() => void toggleTradeWidget()}
            className="flex items-center gap-1 px-2 py-0.5 rounded-md bg-emerald-500/10 border border-emerald-500/20 text-[10px] text-emerald-400 hover:bg-emerald-500/20 hover:border-emerald-500/40 transition-colors"
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
            className="flex items-center gap-0.5 px-2 py-0.5 rounded-md bg-yellow-500/10 border border-yellow-500/20 text-[10px] text-yellow-400 hover:bg-yellow-500/20 hover:border-yellow-500/40 transition-colors"
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
              <div className="flex items-center gap-2 text-[11px] text-zinc-500">
                <RefreshCw className="w-3 h-3 animate-spin" />
                Checking Alpaca credentials…
              </div>
            )}
            {!checkingConfig && !alpacaConfig?.configured && (
              <div className="rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2">
                <p className="text-[11px] text-amber-300 font-medium">
                  Alpaca keys are not configured on the server.
                </p>
                <p className="text-[10px] text-zinc-300 mt-1">
                  Add `APCA_API_KEY_ID` and `APCA_API_SECRET_KEY` in
                  `false-markets/.env.local`, then restart `npm run dev`.
                </p>
              </div>
            )}
            {!checkingConfig && alpacaConfig?.configured && (
              <form onSubmit={submitOrder} className="space-y-2.5">
                <div className="grid grid-cols-2 gap-2">
                  <label className="text-[10px] text-zinc-500 space-y-1">
                    Symbol
                    <input
                      value={tradeSymbol}
                      onChange={(e) =>
                        setTradeSymbol(e.target.value.toUpperCase())
                      }
                      className="w-full bg-[#000] border border-white/10 rounded-md px-2 py-1.5 text-[11px] text-zinc-200 outline-none focus:border-white/25"
                      placeholder="BTCUSD"
                    />
                  </label>
                  <label className="text-[10px] text-zinc-500 space-y-1">
                    Side
                    <select
                      value={orderSide}
                      onChange={(e) =>
                        setOrderSide(e.target.value as AlpacaOrderSide)
                      }
                      className="w-full bg-[#000] border border-white/10 rounded-md px-2 py-1.5 text-[11px] text-zinc-200 outline-none focus:border-white/25"
                    >
                      <option value="buy">Buy</option>
                      <option value="sell">Sell</option>
                    </select>
                  </label>
                  <label className="text-[10px] text-zinc-500 space-y-1">
                    Order Type
                    <select
                      value={orderType}
                      onChange={(e) =>
                        setOrderType(e.target.value as AlpacaOrderType)
                      }
                      className="w-full bg-[#000] border border-white/10 rounded-md px-2 py-1.5 text-[11px] text-zinc-200 outline-none focus:border-white/25"
                    >
                      <option value="market">Market</option>
                      <option value="limit">Limit</option>
                    </select>
                  </label>
                  <label className="text-[10px] text-zinc-500 space-y-1">
                    Time In Force
                    <select
                      value={timeInForce}
                      onChange={(e) =>
                        setTimeInForce(e.target.value as AlpacaTimeInForce)
                      }
                      className="w-full bg-[#000] border border-white/10 rounded-md px-2 py-1.5 text-[11px] text-zinc-200 outline-none focus:border-white/25"
                    >
                      <option value="gtc">GTC</option>
                      <option value="ioc">IOC</option>
                    </select>
                  </label>
                  <label className="text-[10px] text-zinc-500 space-y-1">
                    Quantity
                    <input
                      value={quantity}
                      onChange={(e) => setQuantity(e.target.value)}
                      type="number"
                      inputMode="decimal"
                      min="0"
                      step="any"
                      className="w-full bg-[#000] border border-white/10 rounded-md px-2 py-1.5 text-[11px] text-zinc-200 outline-none focus:border-white/25"
                      placeholder="0.01"
                    />
                  </label>
                  <label className="text-[10px] text-zinc-500 space-y-1">
                    Price (USD)
                    <input
                      value={limitPrice}
                      onChange={(e) => setLimitPrice(e.target.value)}
                      type="number"
                      inputMode="decimal"
                      min="0"
                      step="any"
                      disabled={orderType !== "limit"}
                      className="w-full bg-[#000] border border-white/10 rounded-md px-2 py-1.5 text-[11px] text-zinc-200 outline-none focus:border-white/25 disabled:opacity-50 disabled:cursor-not-allowed"
                      placeholder={detail.current_price.toString()}
                    />
                  </label>
                </div>
                <p className="text-[10px] text-zinc-600">
                  Quantity auto-sizes to about ${DEFAULT_POSITION_NOTIONAL}{" "}
                  notional.
                </p>
                {tradeError && (
                  <div className="rounded-md border border-red-500/30 bg-red-500/10 px-2 py-1.5 text-[10px] text-red-300">
                    {tradeError}
                  </div>
                )}
                {placedOrder && (
                  <div className="rounded-md border border-emerald-500/30 bg-emerald-500/10 px-2 py-1.5 text-[10px] text-emerald-300 space-y-0.5">
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
                  className="w-full rounded-md bg-emerald-500/20 border border-emerald-500/30 text-emerald-300 text-[11px] font-semibold py-2 hover:bg-emerald-500/30 hover:border-emerald-500/45 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
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

// ─── Portfolio Card ───────────────────────────────────────────────────────────

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
            <p className="text-sm font-semibold text-white leading-tight">
              Portfolio Snapshot
            </p>
            <p className="text-[10px] text-zinc-500">
              {new Date(data.fetched_at).toLocaleString("en-US")}
            </p>
          </div>
          <span className="text-[10px] text-zinc-500">
            {data.account.status || "active"} · {data.account.currency || "USD"}
          </span>
        </div>
      </div>
      <div className="grid grid-cols-2 md:grid-cols-4 gap-px bg-white/[0.04]">
        <div className="bg-[#0a0a0a] px-3 py-2.5">
          <p className="text-[10px] text-zinc-600">Equity</p>
          <p className="text-[12px] font-semibold text-zinc-200 tabular-nums">
            {fmtBig(s.equity)}
          </p>
        </div>
        <div className="bg-[#0a0a0a] px-3 py-2.5">
          <p className="text-[10px] text-zinc-600">Cash</p>
          <p className="text-[12px] font-semibold text-zinc-200 tabular-nums">
            {fmtBig(s.cash)}
          </p>
        </div>
        <div className="bg-[#0a0a0a] px-3 py-2.5">
          <p className="text-[10px] text-zinc-600">Day P/L</p>
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
          <p className="text-[10px] text-zinc-600">Unrealized P/L</p>
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
        <p className="text-[10px] text-zinc-600 mb-1.5">
          Positions ({s.positions_count})
        </p>
        {data.positions.length === 0 ? (
          <p className="text-[11px] text-zinc-500">No open positions.</p>
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
                    <p className="text-[11px] text-zinc-200 font-semibold">
                      {p.symbol}
                    </p>
                    <p className="text-[10px] text-zinc-500">
                      Qty {p.qty} · Avg $
                      {Number(p.avg_entry_price || 0).toFixed(2)}
                    </p>
                  </div>
                  <div className="text-right">
                    <p className="text-[10px] text-zinc-500">
                      MV {fmtBig(Number(p.market_value || 0))}
                    </p>
                    <p
                      className={`text-[11px] font-semibold ${
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
      <div className="grid grid-cols-3 gap-px bg-white/[0.04] border-t border-white/5">
        <div className="bg-[#0a0a0a] px-3 py-2.5">
          <p className="text-[10px] text-zinc-600 mb-1">Pending Orders</p>
          <p className="text-[12px] font-semibold text-zinc-200">
            {s.pending_orders_count}
          </p>
        </div>
        <div className="bg-[#0a0a0a] px-3 py-2.5">
          <p className="text-[10px] text-zinc-600 mb-1">Partially Filled</p>
          <p className="text-[12px] font-semibold text-zinc-200">
            {s.partially_filled_orders_count}
          </p>
        </div>
        <div className="bg-[#0a0a0a] px-3 py-2.5">
          <p className="text-[10px] text-zinc-600 mb-1">Filled (Recent)</p>
          <p className="text-[12px] font-semibold text-zinc-200">
            {s.filled_orders_count}
          </p>
        </div>
      </div>
    </div>
  );
}

// ─── Main Chat Page ───────────────────────────────────────────────────────────

export default function ChatPageContent() {
  const searchParams = useSearchParams();
  const initialQuery = searchParams.get("q") || "";

  const [coins, setCoins] = useState<CoinData[]>([]);
  const [query, setQuery] = useState("");
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [streaming, setStreaming] = useState(false);
  const chatEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const sentInitialRef = useRef(false);
  const prevMessageCountRef = useRef(0);

  // Fetch market data for sidebar
  useEffect(() => {
    fetch(
      "/api/crypto?endpoint=coins/markets&vs_currency=usd&order=market_cap_desc&per_page=10&page=1&sparkline=true&price_change_percentage=24h",
    )
      .then((r) => r.json())
      .then((data) => {
        if (Array.isArray(data)) setCoins(data);
      })
      .catch(() => {});
  }, []);

  useEffect(() => {
    if (messages.length > prevMessageCountRef.current) {
      prevMessageCountRef.current = messages.length;
      chatEndRef.current?.scrollIntoView({ behavior: "smooth" });
    }
  }, [messages]);

  const updateMessage = useCallback(
    (messageId: string, updater: (prev: ChatMessage) => ChatMessage) => {
      setMessages((prev) =>
        prev.map((msg) => (msg.id === messageId ? updater(msg) : msg)),
      );
    },
    [],
  );

  const streamBacktestToMessage = useCallback(
    async ({
      messageId,
      prompt,
      start,
      end,
      initialCash,
    }: {
      messageId: string;
      prompt: string;
      start: string;
      end: string;
      initialCash: number;
    }) => {
      const res = await fetch(`${BACKTEST_API_BASE}/backtest/sphinx/stream`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          prompt,
          start,
          end,
          initial_cash: initialCash,
        }),
      });

      if (!res.ok || !res.body) {
        throw new Error(`Backtest ${res.status}`);
      }

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      let finalResult: BacktestResult | null = null;

      const handleRawEvent = (rawEvent: string) => {
        const parsed = parseSseEvent(rawEvent);
        if (!parsed) return;

        if (parsed.event === "log") {
          const payload = parsed.data as Partial<BacktestStreamLog> & {
            message?: unknown;
            stage?: unknown;
            level?: unknown;
            timestamp?: unknown;
          };
          const log: BacktestStreamLog = {
            id: makeMessageId(),
            message:
              typeof payload.message === "string"
                ? payload.message
                : "Backtest update",
            stage:
              typeof payload.stage === "string" ? payload.stage : "progress",
            level:
              payload.level === "warning" || payload.level === "error"
                ? payload.level
                : "info",
            timestamp:
              typeof payload.timestamp === "string"
                ? payload.timestamp
                : new Date().toISOString(),
          };

          updateMessage(messageId, (prev) => ({
            ...prev,
            backtestLogs: appendBacktestLog(prev.backtestLogs, log),
          }));
          return;
        }

        if (parsed.event === "result") {
          finalResult = parsed.data as BacktestResult;
          updateMessage(messageId, (prev) => ({
            ...prev,
            role: "assistant",
            content: "",
            backtest: finalResult ?? undefined,
            backtestLoading: false,
          }));
          return;
        }

        if (parsed.event === "error") {
          const payload = parsed.data as { message?: unknown } | string;
          const message =
            typeof payload === "string"
              ? payload
              : typeof payload?.message === "string"
                ? payload.message
                : "Backtest failed";
          throw new Error(message);
        }
      };

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });

        let separatorIndex = buffer.indexOf("\n\n");
        while (separatorIndex !== -1) {
          const rawEvent = buffer.slice(0, separatorIndex);
          buffer = buffer.slice(separatorIndex + 2);
          handleRawEvent(rawEvent);
          separatorIndex = buffer.indexOf("\n\n");
        }
      }

      const tail = buffer.trim();
      if (tail) {
        handleRawEvent(tail);
      }

      if (!finalResult) {
        throw new Error("Backtest stream ended without a result.");
      }
    },
    [updateMessage],
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
      const runBacktest = shouldRunBacktest(text);

      setQuery("");
      setStreaming(true);

      const userId = makeMessageId();
      const assistantId = makeMessageId();

      setMessages((prev) => [
        ...prev,
        { id: userId, role: "user", content: text },
        runBacktest
          ? {
              id: assistantId,
              role: "assistant",
              content: "",
              backtestLoading: true,
              backtestLogs: [],
            }
          : { id: assistantId, role: "assistant", content: "" },
      ]);

      try {
        if (runBacktest) {
          await streamBacktestToMessage({
            messageId: assistantId,
            prompt: text,
            start: DEFAULT_BACKTEST_START,
            end: getDefaultBacktestEndDate(),
            initialCash: DEFAULT_BACKTEST_INITIAL_CASH,
          });
          return;
        }

        const res = await fetch("/api/chat", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            message: text,
            portfolioContext,
            newsContext,
            backtestEnabled: true,
          }),
        });
        if (!res.ok || !res.body) throw new Error("No body");

        const reader = res.body.getReader();
        const dec = new TextDecoder();
        let accumulated = "";
        let cardMode: "asset" | "portfolio" | "backtest" | null = null;

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
          } else if (accumulated.startsWith("__BACKTEST_CARD__")) {
            if (!cardMode) {
              cardMode = "backtest";
              updateMessage(assistantId, (prev) => ({
                ...prev,
                role: "assistant",
                content: "__BACKTEST_CARD__",
                backtestLoading: true,
                backtestLogs: [],
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

        if (cardMode === "backtest") {
          try {
            const match = accumulated.match(
              /__BACKTEST_CARD__\s*(\{[^}]+\})\s*([\s\S]*)/,
            );
            const parsed = match?.[1]
              ? (JSON.parse(match[1]) as {
                  prompt?: string;
                  start?: string;
                  end?: string;
                  initial_cash?: number;
                })
              : undefined;

            await streamBacktestToMessage({
              messageId: assistantId,
              prompt: parsed?.prompt?.trim() || text,
              start: parsed?.start || DEFAULT_BACKTEST_START,
              end: parsed?.end || getDefaultBacktestEndDate(),
              initialCash:
                typeof parsed?.initial_cash === "number"
                  ? parsed.initial_cash
                  : DEFAULT_BACKTEST_INITIAL_CASH,
            });
          } catch {
            updateMessage(assistantId, (prev) => ({
              ...prev,
              role: "assistant",
              content: "Sorry, I couldn't run that backtest. Please try again.",
              backtestLoading: false,
            }));
          }
        } else if (cardMode === "asset") {
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
          content: runBacktest
            ? "Sorry, I couldn't run that backtest. Please try again."
            : "Sorry, I couldn't process your request. Please try again.",
          backtestLoading: runBacktest ? false : prev.backtestLoading,
        }));
      } finally {
        setStreaming(false);
      }
    },
    [
      coins,
      enrichAssistantMessage,
      messages,
      streamBacktestToMessage,
      streaming,
      updateMessage,
    ],
  );

  // Auto-send the initial query from URL params
  useEffect(() => {
    if (initialQuery && !sentInitialRef.current) {
      sentInitialRef.current = true;
      void sendPrompt(initialQuery);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [initialQuery]);

  const handleSearch = async (e: React.FormEvent) => {
    e.preventDefault();
    if (query.trim()) await sendPrompt(query);
  };

  return (
    <div className="min-h-screen bg-[#000000] text-white flex flex-col">
      {/* ── Header ── */}
      <header className="sticky top-0 z-50 border-b border-white/[0.06] bg-[#000000]/95 backdrop-blur-sm">
        <div className="max-w-7xl mx-auto px-5 h-14 flex items-center gap-4">
          <div className="flex items-center gap-3">
            <Link
              href="/dashboard"
              className="flex items-center gap-1.5 text-zinc-500 hover:text-zinc-200 transition-colors"
            >
              <ArrowLeft className="w-3.5 h-3.5" />
            </Link>
            <span className="text-xl font-semibold tracking-tight">
              AutoAlpha<span className="text-zinc-400">Pods</span>
            </span>
          </div>
        </div>
      </header>

      {/* ── Main layout ── */}
      <div className="flex flex-1 max-w-7xl mx-auto w-full px-5 py-6 gap-6 pb-32">
        {/* ── Chat area ── */}
        <div className="flex-1 min-w-0 space-y-8">
          {messages.length === 0 && (
            <div className="flex flex-col items-center justify-center py-24 text-center">
              <div className="w-10 h-10 rounded-full bg-blue-500/15 border border-blue-500/25 flex items-center justify-center mb-4">
                <Zap className="w-5 h-5 text-blue-400" />
              </div>
              <p className="text-zinc-500 text-sm">
                Ask anything about crypto, stocks, ETFs, or your portfolio
              </p>
            </div>
          )}

          {messages.map((msg, idx) => (
            <div key={msg.id || idx}>
              {msg.role === "user" ? (
                // User question — displayed like a Perplexity question heading
                <div className="mb-1">
                  <h1 className="text-2xl font-semibold text-white leading-snug">
                    {msg.content}
                  </h1>
                </div>
              ) : (
                // AI response
                <div className="flex gap-3">
                  <div className="w-6 h-6 rounded-full bg-blue-500/20 border border-blue-500/30 flex items-center justify-center shrink-0 mt-0.5">
                    <Zap className="w-3 h-3 text-blue-400" />
                  </div>
                  <div className="flex-1 min-w-0">
                    {msg.backtest || msg.backtestLoading ? (
                      <div className="space-y-3">
                        {msg.backtest ? (
                          <BacktestCard data={msg.backtest} />
                        ) : null}
                        {msg.backtestLoading ||
                        (msg.backtestLogs?.length ?? 0) > 0 ? (
                          <BacktestLogPanel
                            logs={msg.backtestLogs ?? []}
                            running={Boolean(msg.backtestLoading)}
                          />
                        ) : null}
                      </div>
                    ) : msg.coinCard ? (
                      <div className="space-y-3">
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
                          onFollowUp={(prompt) => void sendPrompt(prompt)}
                        />
                      </div>
                    ) : msg.portfolio || msg.portfolioLoading ? (
                      <div className="space-y-3">
                        {msg.portfolio ? (
                          <PortfolioCard data={msg.portfolio} />
                        ) : (
                          <div className="bg-[#0a0a0a] border border-white/[0.07] rounded-xl p-6 flex items-center gap-3">
                            <RefreshCw className="w-4 h-4 animate-spin text-zinc-500" />
                            <span className="text-xs text-zinc-500">
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
                          onFollowUp={(prompt) => void sendPrompt(prompt)}
                        />
                      </div>
                    ) : (
                      <div>
                        <MarkdownMessage
                          text={msg.content}
                          streaming={streaming && idx === messages.length - 1}
                        />
                        <MessageContext
                          message={msg}
                          disabled={streaming}
                          onFollowUp={(prompt) => void sendPrompt(prompt)}
                        />
                      </div>
                    )}
                  </div>
                </div>
              )}
            </div>
          ))}

          <div ref={chatEndRef} />
        </div>
      </div>

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
                placeholder="Ask a follow-up…"
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
                className="flex items-center gap-1.5 bg-white text-black rounded-xl px-3 py-1.5 text-[11px] font-semibold disabled:opacity-30 disabled:cursor-not-allowed hover:bg-zinc-200 transition-colors shrink-0"
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
            <p className="text-center text-[10px] text-zinc-700 mt-2">
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
