import warnings
import asyncio
import inspect
import json
import logging
import queue
import re
import shlex
import shutil
import subprocess
import tempfile
import threading
import uuid
import time
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Callable, Any

import numpy as np
import pandas as pd
import yfinance as yf
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from openai import OpenAI
from pydantic import BaseModel
from supabase import create_client

warnings.filterwarnings("ignore")
logger = logging.getLogger("uvicorn.error")

from dotenv import load_dotenv
import os

load_dotenv()

from routers.agent_orchestration import router as orchestration_router
from routers.fixed_income import router as fixed_income_router
from routers.equity import router as equity_router

SUPABASE_URL    = os.getenv("SUPABASE_URL")
SUPABASE_KEY    = os.getenv("SUPABASE_KEY")
MACRO_TABLE_NAME = os.getenv("MACRO_TABLE_NAME", os.getenv("TABLE_NAME", "fi_macro_data"))
COMMODITY_TABLE_NAME = os.getenv("COMMODITY_TABLE_NAME", "commodity_price_bars")
OPENROUTER_KEY  = os.getenv("OPENROUTER_KEY")
OPENROUTER_BASE = os.getenv("OPENROUTER_BASE", "https://openrouter.ai/api/v1")
MODEL           = os.getenv("MODEL", "anthropic/claude-sonnet-4-5")
START           = os.getenv("START", "2000-05-01")
END             = os.getenv("END", datetime.utcnow().date().isoformat())
SPHINX_API_KEY  = os.getenv("SPHINX_API_KEY")

# Fail fast on startup if required keys are missing
_required = {"SUPABASE_URL": SUPABASE_URL, "SUPABASE_KEY": SUPABASE_KEY, "OPENROUTER_KEY": OPENROUTER_KEY}
_missing  = [k for k, v in _required.items() if not v]
if _missing:
    raise RuntimeError(f"Missing required environment variables: {_missing}")


MACRO_COLUMNS = """
YIELD CURVE (%): yield_1m, yield_3m, yield_6m, yield_1y, yield_2y, yield_3y,
  yield_5y, yield_7y, yield_10y, yield_20y, yield_30y
CURVE SPREADS (%): spread_10y2y (10Y-2Y inversion), spread_10y3m, spread_5y10y
TIPS REAL YIELDS (%): real_5y, real_7y, real_10y, real_20y, real_30y
BREAKEVEN INFLATION (%): bei_5y, bei_10y, bei_20y, bei_30y
CREDIT SPREADS IG (bps): oas_ig_all, oas_aaa, oas_bbb, oas_baa_10y
CREDIT SPREADS HY (bps): oas_hy_all, oas_bb, oas_b, oas_ccc
FED POLICY: fed_funds, fed_upper, fed_lower, sofr, fed_balance, iorb
INFLATION (index): cpi, core_cpi, pce, core_pce
MORTGAGE (%): mortgage_30y, mortgage_15y
INTERNATIONAL (%): usd_broad, bund_10y, jgb_10y, gilt_10y
FISCAL: fed_debt, deficit
"""

COMMODITY_BAR_COLUMNS = """
MULTIINDEX: timestamp, symbol
IDENTITY: source, asset_class, universe, category, instrument_name
PRICE BARS: open, high, low, close, adj_close
FLOW / CORPORATE ACTIONS: volume, dividends, capital_gains, stock_splits
"""

ETF_UNIVERSE = """
FIXED INCOME: TLT IEF SHY TIP AGG LQD HYG JNK MBB MUB EMB TBT
COMMODITIES:  GLD SLV USO CPER DJP
EQUITIES:     SPY QQQ XLF XLU XLV XLP VNQ XLE
"""

ALL_ETFS = [
    "TLT","IEF","SHY","TIP","AGG","LQD","HYG","JNK",
    "MBB","MUB","EMB","TBT","GLD","SLV","USO","CPER",
    "DJP","SPY","QQQ","XLF","XLU","XLV","XLP","VNQ","XLE",
]

COMMODITY_NUMERIC_FIELDS = [
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume",
    "dividends",
    "capital_gains",
    "stock_splits",
]

COMMODITY_SELECT_FIELDS = [
    "timestamp",
    "symbol",
    "source",
    "asset_class",
    "universe",
    "category",
    "instrument_name",
    *COMMODITY_NUMERIC_FIELDS,
]

RANGE_MAP = {
    "1D": {"period": "1d", "interval": "5m"},
    "1W": {"period": "5d", "interval": "30m"},
    "1M": {"period": "1mo", "interval": "1d"},
    "3M": {"period": "3mo", "interval": "1d"},
    "1Y": {"period": "1y", "interval": "1wk"},
}

COMMODITY_CONFIG = [
    {"ticker": "GC=F", "symbol": "XAU", "name": "Gold"},
    {"ticker": "SI=F", "symbol": "XAG", "name": "Silver"},
    {"ticker": "CL=F", "symbol": "WTI", "name": "Crude Oil"},
    {"ticker": "NG=F", "symbol": "NG", "name": "Nat Gas"},
    {"ticker": "HG=F", "symbol": "HG", "name": "Copper"},
    {"ticker": "PL=F", "symbol": "XPT", "name": "Platinum"},
    {"ticker": "ZW=F", "symbol": "ZW", "name": "Wheat"},
    {"ticker": "ZC=F", "symbol": "ZC", "name": "Corn"},
    {"ticker": "ZS=F", "symbol": "ZS", "name": "Soybeans"},
    {"ticker": "KC=F", "symbol": "KC", "name": "Coffee"},
]

COMMODITY_STRIP_TICKERS = {
    "commodity-index": "^BCOM",
    "usd-index": "DX-Y.NYB",
}

_COMMODITY_CACHE: dict[str, Any] = {"timestamp": 0.0, "payload": None}
_COMMODITY_CACHE_TTL = 60.0

MACRO_FIELD_NAMES = [
    "yield_1m",
    "yield_3m",
    "yield_6m",
    "yield_1y",
    "yield_2y",
    "yield_3y",
    "yield_5y",
    "yield_7y",
    "yield_10y",
    "yield_20y",
    "yield_30y",
    "spread_10y2y",
    "spread_10y3m",
    "spread_5y10y",
    "real_5y",
    "real_7y",
    "real_10y",
    "real_20y",
    "real_30y",
    "bei_5y",
    "bei_10y",
    "bei_20y",
    "bei_30y",
    "oas_ig_all",
    "oas_aaa",
    "oas_bbb",
    "oas_baa_10y",
    "oas_hy_all",
    "oas_bb",
    "oas_b",
    "oas_ccc",
    "fed_funds",
    "fed_upper",
    "fed_lower",
    "sofr",
    "fed_balance",
    "iorb",
    "cpi",
    "core_cpi",
    "pce",
    "core_pce",
    "mortgage_30y",
    "mortgage_15y",
    "usd_broad",
    "bund_10y",
    "jgb_10y",
    "gilt_10y",
    "fed_debt",
    "deficit",
]

SPHINX_OUTPUT_CODE_BLOCK = re.compile(r"```(?:python)?\s*(.*?)```", re.DOTALL)
MONTH_END_RESAMPLE_RE = re.compile(r"(\.resample\(\s*)(['\"])M\2")
MONTH_END_FREQ_RE = re.compile(r"(freq\s*=\s*)(['\"])M\2")


# ─────────────────────────────────────────────────────────────
# IN-MEMORY JOB STORE
# ─────────────────────────────────────────────────────────────

jobs: dict[str, dict] = {}
# Structure: { job_id: { status, prompt, result, error, created_at } }


# ─────────────────────────────────────────────────────────────
# LIFESPAN — preload macro data on startup
# ─────────────────────────────────────────────────────────────

_cached_macro_all: Optional[pd.DataFrame] = None
_cached_macro_by_columns: dict[tuple[str, ...], pd.DataFrame] = {}
_cached_commodity_all: Optional[pd.DataFrame] = None
_cached_commodity_by_symbols: dict[tuple[str, ...], pd.DataFrame] = {}
_cached_commodity_prices_all: Optional[pd.DataFrame] = None


async def preload_macro():
    global _cached_macro_all
    if _cached_macro_all is not None:
        return
    logger.info("Preloading macro data from Supabase.")
    loop = asyncio.get_event_loop()
    _cached_macro_all = await loop.run_in_executor(None, _get_macro_sync)
    logger.info(
        "Macro data ready: %s rows x %s cols",
        f"{len(_cached_macro_all):,}",
        len(_cached_macro_all.columns),
    )


async def preload_commodity():
    global _cached_commodity_all
    if _cached_commodity_all is not None:
        return
    logger.info("Preloading commodity data from Supabase.")
    loop = asyncio.get_event_loop()
    _cached_commodity_all = await loop.run_in_executor(None, _load_commodity_sync)
    logger.info(
        "Commodity data ready: %s rows across %s symbols",
        f"{len(_cached_commodity_all):,}",
        len(_cached_commodity_all.index.get_level_values("symbol").unique()),
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    await preload_macro()
    await preload_commodity()
    yield


# ─────────────────────────────────────────────────────────────
# APP
# ─────────────────────────────────────────────────────────────

app = FastAPI(
    title="FI Backtest Engine",
    description="LLM-powered fixed income backtesting via OpenRouter + Supabase",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(orchestration_router)
app.include_router(fixed_income_router)
app.include_router(equity_router)


# ─────────────────────────────────────────────────────────────
# PYDANTIC MODELS
# ─────────────────────────────────────────────────────────────

class BacktestRequest(BaseModel):
    prompt: str
    model: Optional[str] = MODEL
    start: Optional[str] = START
    end: Optional[str] = END
    initial_cash: Optional[float] = 100_000

class BatchBacktestRequest(BaseModel):
    prompts: list[str]
    model: Optional[str] = MODEL
    start: Optional[str] = START
    end: Optional[str] = END
    initial_cash: Optional[float] = 100_000

class JobStatus(BaseModel):
    job_id: str
    status: str           # pending | running | done | failed
    prompt: str
    created_at: str
    result: Optional[dict] = None
    error: Optional[str] = None

class MetricsResult(BaseModel):
    prompt: str
    tickers: str
    generated_code: str
    sharpe: float
    sortino: float
    calmar: float
    max_dd: str
    cagr: str
    volatility: str
    win_rate: str
    max_streak: int
    total_return: str
    end_equity: str
    best_day: str
    worst_day: str
    rebalance_days: int
    avg_daily_turnover: str
    avg_gross_exposure: str
    avg_net_exposure: str
    equity_curve: list[dict]   # [{date, equity}]
    drawdown_curve: list[dict] # [{date, drawdown_pct}]
    exposure_curve: list[dict] # [{date, gross, net}]
    rolling_sharpe_63d: list[dict] # [{date, sharpe}]
    position_series: list[dict] # [{ticker, points:[{date, price, position}], trade_events:[...]}]

class BatchResult(BaseModel):
    job_id: str
    status: str
    results: list[MetricsResult]
    errors: list[dict]


StreamLogFn = Optional[Callable[[str, dict[str, Any]], None]]
_STREAM_SENTINEL = object()


# ─────────────────────────────────────────────────────────────
# CORE ENGINE (sync — run in executor)
# ─────────────────────────────────────────────────────────────

def _normalize_macro_columns(columns: Optional[list[str]]) -> tuple[str, ...]:
    if not columns:
        return tuple(MACRO_FIELD_NAMES)
    requested = sorted({column for column in columns if column})
    return tuple(requested)


def _normalize_commodity_symbols(symbols: Optional[list[str]]) -> tuple[str, ...]:
    if not symbols:
        return tuple()
    requested = sorted({symbol for symbol in symbols if symbol})
    return tuple(requested)


def _format_timestamp(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        if pd.isna(value):
            return "n/a"
    except Exception:
        pass
    try:
        return pd.Timestamp(value).date().isoformat()
    except Exception:
        return str(value)


def _format_index_date_range(index: pd.Index) -> str:
    if index is None or len(index) == 0:
        return "empty"
    return f"{_format_timestamp(index.min())} -> {_format_timestamp(index.max())}"


def _preview_columns(columns: list[str], limit: int = 8) -> list[str]:
    if len(columns) <= limit:
        return columns
    return [*columns[:limit], f"... (+{len(columns) - limit} more)"]


def _summarize_price_frame(prices: pd.DataFrame) -> dict[str, Any]:
    if prices.empty:
        return {
            "rows": 0,
            "columns": 0,
            "date_range": "empty",
            "columns_preview": [],
            "non_null_counts": {},
        }

    preview_columns = prices.columns.tolist()[:5]
    non_null_counts = {
        str(column): int(prices[column].notna().sum())
        for column in preview_columns
    }
    return {
        "rows": int(len(prices)),
        "columns": int(len(prices.columns)),
        "date_range": _format_index_date_range(prices.index),
        "columns_preview": _preview_columns(prices.columns.tolist()),
        "non_null_counts": non_null_counts,
    }


def _summarize_signal_frame(signals: pd.DataFrame) -> dict[str, Any]:
    if signals.empty:
        return {
            "rows": 0,
            "columns": 0,
            "date_range": "empty",
            "columns_preview": signals.columns.tolist(),
            "nonzero_rows": 0,
            "nonzero_cells": 0,
        }

    nonzero_mask = signals.fillna(0).ne(0)
    return {
        "rows": int(len(signals)),
        "columns": int(len(signals.columns)),
        "date_range": _format_index_date_range(signals.index),
        "columns_preview": _preview_columns(signals.columns.tolist()),
        "nonzero_rows": int(nonzero_mask.any(axis=1).sum()),
        "nonzero_cells": int(nonzero_mask.sum().sum()),
    }


def _infer_macro_columns(code: str) -> list[str]:
    referenced = []
    for column in MACRO_FIELD_NAMES:
        if f"'{column}'" in code or f'"{column}"' in code:
            referenced.append(column)
    return sorted(set(referenced)) or MACRO_FIELD_NAMES.copy()


def _build_system_prompt() -> str:
    commodity_summary = _get_commodity_universe_summary()
    commodity_symbols = ", ".join(commodity_summary["symbols"]) or "(none loaded)"
    category_summary = ", ".join(commodity_summary["categories"]) or "(none loaded)"

    return f"""You are a quantitative researcher at a hedge fund.
Translate the plain-English trading strategy into a Python function.

Available DataFrames:
- `macro`: daily macro data, DatetimeIndex. Columns:
{MACRO_COLUMNS}

- `prices`: daily close matrix for tradable assets referenced by your strategy, DatetimeIndex.
  ETFs available by default:
{ETF_UNIVERSE}
  Commodity symbols also available in `prices`:
  {commodity_symbols}

- `commodities`: daily commodity price-bar table with MultiIndex (`timestamp`, `symbol`). Columns:
{COMMODITY_BAR_COLUMNS}
  Available categories: {category_summary}
  Use exact `symbol` values from `commodities` when trading commodity instruments.

Write ONLY a function called `generate_signals`.
Preferred signature: `generate_signals(macro, prices, commodities)`
Backwards-compatible alternative: `generate_signals(macro, prices)`

Return:
- pd.DataFrame with DatetimeIndex
- Columns = tradable tickers/symbols present in `prices` (e.g. "TLT", "GLD", "GC=F", "CL=F")
- Values = float weights (1.0=long, -1.0=short, 0=flat)

Hard rules:
1. Raw Python only — no imports, no markdown fences, no explanation
2. pandas=pd and numpy=np are already available — never import them
3. Use .shift(1) on ALL signals to prevent look-ahead bias
4. Handle NaN with .fillna(0)
5. Return a DataFrame, never a Series
6. If you need month-end resampling, use `.resample('ME').last()` or `freq='ME'` for pandas 3 compatibility. Never use `'M'`.
7. Since the strategy uses daily time series, the positions are also expected on a daily granularity. If you build monthly signals, reindex/forward-fill them back to the daily prices index before returning.
"""


def _normalize_generated_code_for_pandas(
    code: str,
    stream_log: StreamLogFn = None,
) -> str:
    normalized = code
    changes: list[str] = []

    updated = MONTH_END_RESAMPLE_RE.sub(r"\1\2ME\2", normalized)
    if updated != normalized:
        normalized = updated
        changes.append("Normalized `.resample('M')` to `.resample('ME')` for pandas 3 compatibility.")

    updated = MONTH_END_FREQ_RE.sub(r"\1\2ME\2", normalized)
    if updated != normalized:
        normalized = updated
        changes.append("Normalized `freq='M'` to `freq='ME'` for pandas 3 compatibility.")

    for message in changes:
        logger.warning(message)
        _stream_log(stream_log, message, stage="execute_strategy", level="warning")

    return normalized


def _load_macro_sync(columns: Optional[list[str]] = None) -> pd.DataFrame:
    client = create_client(SUPABASE_URL, SUPABASE_KEY)
    rows, page, limit = [], 0, 1000
    requested = list(_normalize_macro_columns(columns))
    select_clause = ",".join(["date", *requested])
    while True:
        res = (
            client.table(MACRO_TABLE_NAME)
            .select(select_clause)
            .gte("date", START)
            .lte("date", END)
            .order("date")
            .range(page * limit, (page + 1) * limit - 1)
            .execute()
        )
        batch = res.data
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < limit:
            break
        page += 1

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _get_macro_sync(columns: Optional[list[str]] = None) -> pd.DataFrame:
    global _cached_macro_all

    cache_key = _normalize_macro_columns(columns)
    if cache_key in _cached_macro_by_columns:
        return _cached_macro_by_columns[cache_key]

    if _cached_macro_all is not None:
        subset = _cached_macro_all.loc[:, list(cache_key)].copy()
        _cached_macro_by_columns[cache_key] = subset
        return subset

    macro = _load_macro_sync(list(cache_key))
    _cached_macro_by_columns[cache_key] = macro

    all_columns_key = tuple(MACRO_FIELD_NAMES)
    if cache_key == all_columns_key:
        _cached_macro_all = macro

    return macro


def _load_commodity_sync(symbols: Optional[list[str]] = None) -> pd.DataFrame:
    client = create_client(SUPABASE_URL, SUPABASE_KEY)
    rows, page, limit = [], 0, 5000
    requested_symbols = list(_normalize_commodity_symbols(symbols))
    select_clause = ",".join(COMMODITY_SELECT_FIELDS)

    while True:
        query = (
            client.table(COMMODITY_TABLE_NAME)
            .select(select_clause)
            .gte("timestamp", START)
            .lte("timestamp", END)
            .order("timestamp")
            .order("symbol")
        )
        if requested_symbols:
            query = query.in_("symbol", requested_symbols)
        res = query.range(page * limit, (page + 1) * limit - 1).execute()
        batch = res.data
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < limit:
            break
        page += 1

    if not rows:
        empty = pd.DataFrame(columns=COMMODITY_SELECT_FIELDS)
        empty["timestamp"] = pd.to_datetime(pd.Series(dtype="datetime64[ns]"))
        return empty.set_index(["timestamp", "symbol"]).sort_index()

    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    for col in COMMODITY_NUMERIC_FIELDS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.set_index(["timestamp", "symbol"]).sort_index()


def _get_commodity_sync(symbols: Optional[list[str]] = None) -> pd.DataFrame:
    global _cached_commodity_all

    cache_key = _normalize_commodity_symbols(symbols)
    if cache_key in _cached_commodity_by_symbols:
        return _cached_commodity_by_symbols[cache_key]

    if _cached_commodity_all is not None:
        if not cache_key:
            return _cached_commodity_all
        mask = _cached_commodity_all.index.get_level_values("symbol").isin(cache_key)
        subset = _cached_commodity_all.loc[mask].copy()
        _cached_commodity_by_symbols[cache_key] = subset
        return subset

    commodity = _load_commodity_sync(list(cache_key) if cache_key else None)
    if not cache_key:
        _cached_commodity_all = commodity
    _cached_commodity_by_symbols[cache_key] = commodity
    return commodity


def _get_commodity_prices_sync(symbols: Optional[list[str]] = None) -> pd.DataFrame:
    global _cached_commodity_prices_all

    requested_symbols = list(_normalize_commodity_symbols(symbols))
    if requested_symbols:
        commodity = _get_commodity_sync(requested_symbols)
        if commodity.empty:
            return pd.DataFrame()
        return commodity["close"].unstack("symbol").sort_index()

    if _cached_commodity_prices_all is not None:
        return _cached_commodity_prices_all

    commodity = _get_commodity_sync()
    if commodity.empty:
        _cached_commodity_prices_all = pd.DataFrame()
    else:
        _cached_commodity_prices_all = commodity["close"].unstack("symbol").sort_index()
    return _cached_commodity_prices_all


def _get_commodity_universe_summary() -> dict[str, list[str]]:
    commodity = _get_commodity_sync()
    if commodity.empty:
        return {"symbols": [], "categories": [], "asset_classes": [], "universes": []}

    return {
        "symbols": sorted(commodity.index.get_level_values("symbol").unique().tolist()),
        "categories": sorted(commodity["category"].dropna().unique().tolist()),
        "asset_classes": sorted(commodity["asset_class"].dropna().unique().tolist()),
        "universes": sorted(commodity["universe"].dropna().unique().tolist()),
    }


def _load_yfinance_prices_sync(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    if not tickers:
        return pd.DataFrame()
    raw = yf.download(
        tickers, start=start, end=end,
        auto_adjust=True, progress=False,
        group_by="ticker"
    )
    if raw is None or raw.empty:
        return pd.DataFrame()
    if isinstance(raw.columns, pd.MultiIndex):
        closes = raw.xs("Close", axis=1, level=1)
    else:
        col = "Close" if "Close" in raw.columns else "close"
        closes = raw[[col]].rename(columns={col: tickers[0]})
    closes.columns = [str(c) for c in closes.columns]
    return closes.sort_index()


def _filter_price_history_window(
    prices: pd.DataFrame,
    start: Optional[str],
    end: Optional[str],
) -> pd.DataFrame:
    if prices.empty:
        return prices

    window = prices
    if start:
        window = window.loc[window.index >= pd.to_datetime(start)]
    if end:
        window = window.loc[window.index <= pd.to_datetime(end)]
    return window


def _tickers_missing_price_history(
    prices: pd.DataFrame,
    tickers: list[str],
    minimum_rows: int = 2,
) -> list[str]:
    if not tickers:
        return []
    if prices.empty:
        return tickers.copy()

    missing: list[str] = []
    for ticker in tickers:
        if ticker not in prices.columns:
            missing.append(ticker)
            continue
        if int(prices[ticker].dropna().shape[0]) < minimum_rows:
            missing.append(ticker)
    return missing


def _infer_referenced_assets(code: str) -> list[str]:
    commodity_symbols = _get_commodity_universe_summary()["symbols"]
    candidates = [*ALL_ETFS, *commodity_symbols]
    referenced = [ticker for ticker in candidates if f'"{ticker}"' in code or f"'{ticker}'" in code]
    if referenced:
        return list(dict.fromkeys(referenced))
    return ["TLT", "HYG", "GLD", "SPY", "TIP", "SHY"]


def _load_prices_sync(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    requested = list(dict.fromkeys([ticker for ticker in tickers if ticker]))
    if not requested:
        logger.warning("Price load skipped: no tickers requested. start=%s end=%s", start, end)
        return pd.DataFrame()

    commodity_symbols = set(_get_commodity_universe_summary()["symbols"])
    commodity_tickers = [ticker for ticker in requested if ticker in commodity_symbols]
    market_tickers = [ticker for ticker in requested if ticker not in commodity_symbols]

    logger.info(
        "Loading prices. start=%s end=%s requested=%s market=%s commodities=%s",
        start,
        end,
        requested,
        market_tickers,
        commodity_tickers,
    )

    frames: list[pd.DataFrame] = []
    if market_tickers:
        market_prices = _load_yfinance_prices_sync(market_tickers, start, end)
        logger.info(
            "Market price load complete. rows=%s cols=%s range=%s tickers=%s",
            len(market_prices),
            len(market_prices.columns),
            _format_index_date_range(market_prices.index),
            market_tickers,
        )
        if not market_prices.empty:
            frames.append(market_prices)

    if commodity_tickers:
        commodity_prices = _get_commodity_prices_sync(commodity_tickers)
        logger.info(
            "Commodity price cache load complete. rows=%s cols=%s range=%s tickers=%s",
            len(commodity_prices),
            len(commodity_prices.columns),
            _format_index_date_range(commodity_prices.index),
            commodity_tickers,
        )
        if not commodity_prices.empty:
            commodity_prices = _filter_price_history_window(commodity_prices, start, end)
            logger.info(
                "Commodity price range filtered. rows=%s cols=%s range=%s tickers=%s",
                len(commodity_prices),
                len(commodity_prices.columns),
                _format_index_date_range(commodity_prices.index),
                commodity_tickers,
            )

        missing_commodity_tickers = _tickers_missing_price_history(
            commodity_prices,
            commodity_tickers,
        )
        if missing_commodity_tickers:
            logger.warning(
                "Commodity price cache missing requested window; falling back to yfinance. "
                "requested_range=%s -> %s cache_range=%s fallback_tickers=%s",
                start,
                end,
                _format_index_date_range(commodity_prices.index),
                missing_commodity_tickers,
            )
            commodity_fallback_prices = _load_yfinance_prices_sync(
                missing_commodity_tickers,
                start,
                end,
            )
            logger.info(
                "Commodity fallback load complete. rows=%s cols=%s range=%s tickers=%s",
                len(commodity_fallback_prices),
                len(commodity_fallback_prices.columns),
                _format_index_date_range(commodity_fallback_prices.index),
                missing_commodity_tickers,
            )
            if not commodity_fallback_prices.empty:
                commodity_prices = commodity_prices.combine_first(commodity_fallback_prices)
                commodity_prices = commodity_prices.sort_index()

        frames.append(commodity_prices)

    if not frames:
        logger.warning(
            "Price load produced no frames. start=%s end=%s requested=%s",
            start,
            end,
            requested,
        )
        return pd.DataFrame()

    prices = pd.concat(frames, axis=1).sort_index()
    prices = prices.loc[:, ~prices.columns.duplicated()]
    available = [ticker for ticker in requested if ticker in prices.columns]
    result = prices[available]
    logger.info(
        "Combined price load complete. rows=%s cols=%s range=%s available=%s missing=%s",
        len(result),
        len(result.columns),
        _format_index_date_range(result.index),
        available,
        [ticker for ticker in requested if ticker not in available],
    )
    return result


def _emit_stream_event(stream_log: StreamLogFn, event: str, **payload: Any) -> None:
    if stream_log is None:
        return
    stream_log(event, payload)


def _stream_log(
    stream_log: StreamLogFn,
    message: str,
    *,
    stage: str,
    level: str = "info",
    **extra: Any,
) -> None:
    _emit_stream_event(
        stream_log,
        "log",
        message=message,
        stage=stage,
        level=level,
        timestamp=datetime.utcnow().isoformat() + "Z",
        **extra,
    )


def _ask_llm_sync(prompt: str, model: str, stream_log: StreamLogFn = None) -> str:
    system_prompt = _build_system_prompt()
    _stream_log(
        stream_log,
        f"Requesting strategy code from model {model}.",
        stage="generate_strategy",
    )
    client = OpenAI(
        api_key=OPENROUTER_KEY,
        base_url=OPENROUTER_BASE,
        default_headers={
            "HTTP-Referer": "https://github.com/backtester",
            "X-Title": "FI Backtest Engine",
        },
    )
    response = client.chat.completions.create(
        model=model,
        max_tokens=2000,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": prompt},
        ],
    )
    code = response.choices[0].message.content.strip()
    if "```" in code:
        code = "\n".join(
            line for line in code.split("\n")
            if not line.strip().startswith("```")
        )
    _stream_log(
        stream_log,
        "Strategy code received from model.",
        stage="generate_strategy",
        code_lines=len(code.splitlines()),
    )
    return code.strip()


def _resolve_sphinx_cli() -> str:
    configured = os.getenv("SPHINX_CLI_BIN")
    if configured:
        return configured

    for candidate in (
        "sphinx-cli",
        str(Path.home() / ".local" / "bin" / "sphinx-cli"),
    ):
        resolved = shutil.which(candidate)
        if resolved:
            return resolved

    raise RuntimeError(
        "sphinx-cli not found on PATH. Set SPHINX_CLI_BIN to the executable path."
    )


def _build_sphinx_command(prompt: str, notebook_filepath: str) -> list[str]:
    cmd = [
        _resolve_sphinx_cli(),
        "chat",
        "--prompt",
        prompt,
        "--notebook-filepath",
        notebook_filepath,
    ]

    optional_flags = {
        "SPHINX_URL": "--sphinx-url",
        "SPHINX_JUPYTER_SERVER_URL": "--jupyter-server-url",
        "SPHINX_JUPYTER_SERVER_TOKEN": "--jupyter-server-token",
        "SPHINX_RULES_PATH": "--sphinx-rules-path",
    }
    for env_var, flag in optional_flags.items():
        value = os.getenv(env_var)
        if value:
            cmd.extend([flag, value])

    for env_var, flag in (
        ("SPHINX_NO_MEMORY_READ", "--no-memory-read"),
        ("SPHINX_NO_MEMORY_WRITE", "--no-memory-write"),
        ("SPHINX_NO_PACKAGE_INSTALLATION", "--no-package-installation"),
        ("SPHINX_NO_COLLAPSE_EXPLORATORY_CELLS", "--no-collapse-exploratory-cells"),
        ("SPHINX_NO_FILE_SEARCH", "--no-file-search"),
        ("SPHINX_NO_RIPGREP_INSTALLATION", "--no-ripgrep-installation"),
        ("SPHINX_NO_WEB_SEARCH", "--no-web-search"),
        ("SPHINX_VERBOSE", "--verbose"),
    ):
        if os.getenv(env_var, "").lower() in {"1", "true", "yes"}:
            cmd.append(flag)

    log_level = os.getenv("SPHINX_LOG_LEVEL")
    if log_level:
        cmd.extend(["--log-level", log_level])

    extra_args = os.getenv("SPHINX_CLI_EXTRA_ARGS")
    if extra_args:
        cmd.extend(shlex.split(extra_args))

    return cmd


def _extract_code_from_sphinx_output(output: str) -> str:
    matches = SPHINX_OUTPUT_CODE_BLOCK.findall(output)
    if matches:
        return matches[-1].strip()

    start = output.find("def generate_signals(")
    if start >= 0:
        return output[start:].strip()

    raise RuntimeError(f"Could not find generate_signals() in Sphinx output:\n{output}")


def _extract_code_from_notebook(notebook_path: Path) -> str:
    if not notebook_path.exists():
        raise RuntimeError(f"Sphinx notebook was not created: {notebook_path}")

    notebook = json.loads(notebook_path.read_text())
    for cell in reversed(notebook.get("cells", [])):
        source = "".join(cell.get("source", []))
        if "def generate_signals(" in source:
            start = source.find("def generate_signals(")
            return source[start:].strip()

    raise RuntimeError(f"Could not find generate_signals() in notebook: {notebook_path}")


def _log_sphinx_logs(
    notebook_path: Path,
    stdout: str,
    stderr: str,
    stream_log: StreamLogFn = None,
) -> None:
    messages: list[tuple[str, str]] = [("info", f"Sphinx notebook: {notebook_path}")]
    if stdout.strip():
        messages.append(("info", f"Sphinx stdout:\n{stdout.strip()}"))
    if stderr.strip():
        messages.append(("warning", f"Sphinx stderr:\n{stderr.strip()}"))

    for level, message in messages:
        _stream_log(stream_log, message, stage="sphinx_cli", level=level)

    if logger.handlers:
        for level, message in messages:
            getattr(logger, level)(message)
        return

    for _, message in messages:
        print(message, flush=True)


def _build_sphinx_env() -> dict:
    if not SPHINX_API_KEY:
        raise RuntimeError(
            "SPHINX_API_KEY is not set. Export it to authenticate without browser login."
        )

    env = os.environ.copy()
    env["SPHINX_API_KEY"] = SPHINX_API_KEY
    return env


def _ensure_sphinx_ready(stream_log: StreamLogFn = None) -> None:
    # API-key auth is sufficient for non-interactive usage; avoid forcing local login
    # checks that can fail on machines without a pre-warmed nodeenv.
    force_status_check = os.getenv("SPHINX_FORCE_STATUS_CHECK", "").lower() in {
        "1",
        "true",
        "yes",
    }
    if SPHINX_API_KEY and not force_status_check:
        _stream_log(
            stream_log,
            "SPHINX_API_KEY detected; skipping sphinx-cli status/login preflight.",
            stage="sphinx_setup",
        )
        return

    _stream_log(stream_log, "Checking Sphinx CLI auth status.", stage="sphinx_setup")
    cmd = [_resolve_sphinx_cli(), "status"]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
        env=_build_sphinx_env(),
    )
    if result.returncode != 0:
        raise RuntimeError(
            "sphinx-cli status failed. Run `sphinx-cli login` first.\n"
            f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
        )
    _stream_log(stream_log, "Sphinx CLI is ready.", stage="sphinx_setup")


def _ask_sphinx_sync(user_prompt: str, stream_log: StreamLogFn = None) -> str:
    _ensure_sphinx_ready(stream_log)
    prompt = f"{_build_system_prompt()}\n\nTrading strategy:\n{user_prompt}"

    keep_notebooks = os.getenv("SPHINX_KEEP_NOTEBOOKS", "").lower() in {"1", "true", "yes"}
    explicit_notebook = os.getenv("SPHINX_NOTEBOOK_FILEPATH")

    temp_dir: Optional[str] = None
    if explicit_notebook:
        notebook_path = Path(explicit_notebook).expanduser().resolve()
        notebook_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        temp_dir = tempfile.mkdtemp(prefix="sphinx-run-", dir=str(Path.cwd()))
        notebook_path = Path(temp_dir) / "strategy.ipynb"

    cmd = _build_sphinx_command(prompt, str(notebook_path))
    _stream_log(
        stream_log,
        f"Running Sphinx CLI with notebook path {notebook_path}.",
        stage="generate_strategy",
    )
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=_build_sphinx_env(),
    )
    output_lines: list[str] = []
    if process.stdout is not None:
        for line in process.stdout:
            output_lines.append(line)
            cleaned = line.rstrip()
            if cleaned:
                _stream_log(stream_log, cleaned, stage="sphinx_cli")
    return_code = process.wait()
    stdout = "".join(output_lines)
    _log_sphinx_logs(notebook_path, "", "", stream_log)
    if return_code != 0:
        raise RuntimeError(
            "sphinx-cli chat failed.\n"
            f"Command: {' '.join(cmd)}\n"
            f"stdout:\n{stdout}"
        )

    stdout = stdout.strip()
    try:
        code = _extract_code_from_sphinx_output(stdout)
    except RuntimeError:
        code = _extract_code_from_notebook(notebook_path)

    if temp_dir and not keep_notebooks:
        shutil.rmtree(temp_dir, ignore_errors=True)

    _stream_log(
        stream_log,
        "Strategy code received from Sphinx CLI.",
        stage="generate_strategy",
        code_lines=len(code.splitlines()),
    )
    return code


def _execute_strategy(
    code: str,
    macro: pd.DataFrame,
    prices: pd.DataFrame,
    commodities: pd.DataFrame,
    stream_log: StreamLogFn = None,
) -> pd.DataFrame:
    code = _normalize_generated_code_for_pandas(code, stream_log=stream_log)
    logger.info(
        "Executing strategy. macro_rows=%s macro_cols=%s macro_range=%s price_rows=%s price_cols=%s price_range=%s commodity_rows=%s commodity_symbols=%s",
        len(macro),
        len(macro.columns),
        _format_index_date_range(macro.index),
        len(prices),
        len(prices.columns),
        _format_index_date_range(prices.index),
        len(commodities),
        len(commodities.index.get_level_values("symbol").unique()) if not commodities.empty else 0,
    )
    namespace = {"pd": pd, "np": np}
    try:
        exec(code, namespace)
    except Exception as e:
        raise RuntimeError(f"Compile error: {e}\n\nCode:\n{code}") from e
    if "generate_signals" not in namespace:
        raise RuntimeError(f"No generate_signals() function found in generated code.\n\nCode:\n{code}")
    generate_signals = namespace["generate_signals"]
    try:
        signature = inspect.signature(generate_signals)
        positional_params = [
            param for param in signature.parameters.values()
            if param.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
        ]
        has_varargs = any(
            param.kind == inspect.Parameter.VAR_POSITIONAL
            for param in signature.parameters.values()
        )
        if has_varargs or len(positional_params) >= 3:
            signals = generate_signals(macro.copy(), prices.copy(), commodities.copy())
        else:
            signals = generate_signals(macro.copy(), prices.copy())
    except Exception as e:
        raise RuntimeError(f"Runtime error in generate_signals(): {e}\n\nCode:\n{code}") from e
    if not isinstance(signals, pd.DataFrame):
        raise RuntimeError(f"generate_signals() must return DataFrame, got {type(signals)}")
    logger.info("Strategy execution complete. signal_summary=%s", _summarize_signal_frame(signals))
    return signals


def _simulate_portfolio(
    signals: pd.DataFrame,
    prices: pd.DataFrame,
    initial_cash: float,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> tuple[pd.Series, pd.Series, pd.DataFrame, pd.DataFrame]:
    requested_range = f"{start} -> {end}" if start and end else "unspecified"
    logger.info(
        "Simulating portfolio. requested_range=%s signal_summary=%s price_summary=%s",
        requested_range,
        _summarize_signal_frame(signals),
        _summarize_price_frame(prices),
    )
    if signals.empty or signals.index.empty:
        raise RuntimeError(
            "Strategy returned no signal rows. "
            f"requested_range={requested_range} signal_columns={signals.columns.tolist()}"
        )
    if prices.empty or prices.index.empty:
        raise RuntimeError(
            "No price history rows were loaded for the requested backtest window. "
            f"requested_range={requested_range} signal_columns={signals.columns.tolist()}"
        )
    tickers = [t for t in signals.columns if t in prices.columns]
    if not tickers:
        raise RuntimeError(
            f"No ticker overlap: signals={signals.columns.tolist()} "
            f"prices={prices.columns.tolist()}"
        )
    logger.info(
        "Portfolio overlap resolved. tickers=%s signal_only=%s price_only=%s",
        tickers,
        [column for column in signals.columns.tolist() if column not in prices.columns],
        [column for column in prices.columns.tolist() if column not in signals.columns],
    )
    px = prices[tickers].copy().sort_index()
    px = px.loc[~px.index.duplicated(keep="last")]
    px = px.dropna(how="all")
    if px.empty:
        raise RuntimeError(
            "Referenced assets have no usable price rows in the requested backtest window. "
            f"requested_range={requested_range} tickers={tickers} "
            f"loaded_price_range={_format_index_date_range(prices.index)}"
        )
    if len(px.index) < 2:
        raise RuntimeError(
            "Need at least 2 price rows to compute returns. "
            f"requested_range={requested_range} tickers={tickers} "
            f"loaded_price_range={_format_index_date_range(px.index)}"
        )
    returns = px.pct_change()
    weights = signals[tickers].reindex(returns.index, method="ffill").fillna(0)
    row_sum = weights.abs().sum(axis=1).replace(0, 1)
    weights = weights.div(row_sum, axis=0).shift(1).fillna(0)
    port_returns = (weights * returns).sum(axis=1).replace([np.inf, -np.inf], np.nan).fillna(0)
    equity       = (1 + port_returns).cumprod() * initial_cash
    logger.info(
        "Portfolio simulation intermediate stats. aligned_price_rows=%s aligned_price_range=%s return_rows=%s nonzero_return_days=%s weight_nonzero_rows=%s",
        len(px),
        _format_index_date_range(px.index),
        len(port_returns),
        int(port_returns.fillna(0).ne(0).sum()),
        int(weights.fillna(0).ne(0).any(axis=1).sum()),
    )
    if port_returns.empty or equity.empty:
        raise RuntimeError(
            "Portfolio simulation produced no returns after aligning signals and prices. "
            f"requested_range={requested_range} tickers={tickers} "
            f"signal_range={_format_index_date_range(signals.index)} "
            f"price_range={_format_index_date_range(px.index)}"
        )
    return port_returns, equity, weights, px


def _compute_metrics(
    port_returns: pd.Series,
    equity: pd.Series,
    weights: pd.DataFrame,
    prices: pd.DataFrame,
    prompt: str,
    tickers: list,
    code: str,
) -> dict:
    if port_returns.empty or equity.empty:
        raise RuntimeError("Backtest produced an empty return series.")

    ann    = 252
    mean_r = port_returns.mean()
    std_r  = port_returns.std()
    sharpe = (mean_r / std_r * np.sqrt(ann)) if std_r > 0 else 0.0

    rolling_max = equity.cummax().replace(0, np.nan)
    drawdown_series = ((equity - rolling_max) / rolling_max).fillna(0)
    max_dd      = drawdown_series.min()

    n_years = len(port_returns) / ann
    total_r = equity.iloc[-1] / equity.iloc[0] - 1
    cagr    = (1 + total_r) ** (1 / n_years) - 1 if n_years > 0 else 0.0
    calmar  = (cagr / abs(max_dd)) if max_dd != 0 else 0.0
    ann_vol = std_r * np.sqrt(ann) if std_r > 0 else 0.0

    downside = port_returns[port_returns < 0]
    sortino  = (mean_r / downside.std() * np.sqrt(ann)) if len(downside) > 1 else 0.0

    nonzero  = port_returns[port_returns != 0]
    win_rate = (nonzero > 0).sum() / len(nonzero) if len(nonzero) > 0 else 0.0

    losing_streak = max_streak = 0
    for r in port_returns:
        if r < 0:
            losing_streak += 1
            max_streak = max(max_streak, losing_streak)
        else:
            losing_streak = 0

    aligned_weights = weights.reindex(port_returns.index).fillna(0)
    aligned_prices = prices.reindex(port_returns.index).ffill()

    daily_turnover = aligned_weights.diff().abs().sum(axis=1).fillna(0)
    rebalance_days = int((daily_turnover > 1e-6).sum())
    gross_exposure = aligned_weights.abs().sum(axis=1)
    net_exposure = aligned_weights.sum(axis=1)

    rolling_sharpe_63d = (
        port_returns.rolling(63).mean()
        .div(port_returns.rolling(63).std().replace(0, np.nan))
        .mul(np.sqrt(ann))
        .replace([np.inf, -np.inf], np.nan)
        .dropna()
    )

    equity_curve = [
        {"date": str(d.date()), "equity": round(float(v), 2)}
        for d, v in equity.items()
    ]
    drawdown_curve = [
        {"date": str(d.date()), "drawdown_pct": round(float(v) * 100, 2)}
        for d, v in drawdown_series.items()
    ]
    exposure_curve = [
        {
            "date": str(d.date()),
            "gross": round(float(gross_exposure.loc[d]) * 100, 2),
            "net": round(float(net_exposure.loc[d]) * 100, 2),
        }
        for d in aligned_weights.index
    ]
    rolling_sharpe_curve = [
        {"date": str(d.date()), "sharpe": round(float(v), 3)}
        for d, v in rolling_sharpe_63d.items()
    ]

    position_series: list[dict] = []
    for ticker in aligned_prices.columns.tolist():
        price_series = aligned_prices[ticker].astype(float)
        position_series_ticker = aligned_weights[ticker].astype(float)
        valid_idx = price_series.index[price_series.notna()]

        points = [
            {
                "date": str(d.date()),
                "price": round(float(price_series.loc[d]), 4),
                "position": round(float(position_series_ticker.loc[d]), 4),
            }
            for d in valid_idx
        ]

        deltas = position_series_ticker.diff().fillna(position_series_ticker)
        trade_idx = deltas.index[(deltas.abs() > 1e-6) & price_series.notna()]
        trade_events = [
            {
                "date": str(d.date()),
                "price": round(float(price_series.loc[d]), 4),
                "position": round(float(position_series_ticker.loc[d]), 4),
                "delta": round(float(deltas.loc[d]), 4),
            }
            for d in trade_idx[:600]
        ]

        position_series.append(
            {"ticker": ticker, "points": points, "trade_events": trade_events}
        )

    return {
        "prompt"        : prompt,
        "tickers"       : ", ".join(tickers),
        "generated_code": code,
        "sharpe"        : round(sharpe, 2),
        "sortino"       : round(sortino, 2),
        "calmar"        : round(calmar, 2),
        "max_dd"        : f"{max_dd*100:.1f}%",
        "cagr"          : f"{cagr*100:.1f}%",
        "volatility"    : f"{ann_vol*100:.1f}%",
        "win_rate"      : f"{win_rate*100:.0f}%",
        "max_streak"    : max_streak,
        "total_return"  : f"{total_r*100:.1f}%",
        "end_equity"    : f"${equity.iloc[-1]:,.0f}",
        "best_day"      : f"{port_returns.max()*100:.2f}%",
        "worst_day"     : f"{port_returns.min()*100:.2f}%",
        "rebalance_days": rebalance_days,
        "avg_daily_turnover": f"{daily_turnover.mean()*100:.1f}%",
        "avg_gross_exposure": f"{gross_exposure.mean()*100:.1f}%",
        "avg_net_exposure": f"{net_exposure.mean()*100:.1f}%",
        "equity_curve"  : equity_curve,
        "drawdown_curve": drawdown_curve,
        "exposure_curve": exposure_curve,
        "rolling_sharpe_63d": rolling_sharpe_curve,
        "position_series": position_series,
    }


def _run_single_backtest(
    prompt: str,
    model: str,
    start: str,
    end: str,
    initial_cash: float,
    stream_log: StreamLogFn = None,
) -> dict:
    """Full pipeline for one prompt. Runs sync — call via executor."""
    _stream_log(stream_log, "Backtest started.", stage="start")
    code = _ask_llm_sync(prompt, model, stream_log)
    macro_columns = _infer_macro_columns(code)
    _stream_log(
        stream_log,
        f"Loading macro data for {len(macro_columns)} columns.",
        stage="load_macro",
        columns=macro_columns,
    )
    macro = _get_macro_sync(macro_columns)
    _stream_log(
        stream_log,
        "Macro data loaded.",
        stage="load_macro",
        rows=len(macro),
        date_range=_format_index_date_range(macro.index),
        columns_preview=_preview_columns(macro.columns.tolist()),
    )
    _stream_log(stream_log, "Loading commodity data.", stage="load_commodities")
    commodities = _get_commodity_sync()
    _stream_log(
        stream_log,
        "Commodity data loaded.",
        stage="load_commodities",
        rows=len(commodities),
        date_range=(
            _format_index_date_range(commodities.index.get_level_values("timestamp"))
            if not commodities.empty
            else "empty"
        ),
        symbols_preview=(
            _preview_columns(commodities.index.get_level_values("symbol").unique().tolist())
            if not commodities.empty
            else []
        ),
    )
    referenced = _infer_referenced_assets(code)

    _stream_log(
        stream_log,
        f"Loading price history for {len(referenced)} tickers.",
        stage="load_prices",
        tickers=referenced,
        start=start,
        end=end,
    )
    prices  = _load_prices_sync(referenced, start, end)
    _stream_log(
        stream_log,
        "Price history loaded.",
        stage="load_prices",
        **_summarize_price_frame(prices),
    )
    _stream_log(stream_log, "Executing generated strategy.", stage="execute_strategy")
    signals = _execute_strategy(code, macro, prices, commodities, stream_log=stream_log)
    _stream_log(
        stream_log,
        "Strategy execution produced signals.",
        stage="execute_strategy",
        **_summarize_signal_frame(signals),
    )

    _stream_log(stream_log, "Simulating portfolio.", stage="simulate_portfolio")
    port_returns, equity, weights, aligned_prices = _simulate_portfolio(
        signals, prices, initial_cash, start=start, end=end
    )
    _stream_log(
        stream_log,
        "Portfolio simulation complete.",
        stage="simulate_portfolio",
        tickers=_preview_columns(aligned_prices.columns.tolist()),
        aligned_price_rows=len(aligned_prices),
        aligned_price_range=_format_index_date_range(aligned_prices.index),
        return_rows=len(port_returns),
        nonzero_return_days=int(port_returns.fillna(0).ne(0).sum()),
    )
    _stream_log(stream_log, "Computing metrics.", stage="compute_metrics")
    metrics = _compute_metrics(
        port_returns, equity, weights, aligned_prices, prompt,
        tickers=signals.columns.tolist(),
        code=code,
    )
    _stream_log(
        stream_log,
        "Backtest completed.",
        stage="done",
        tickers=signals.columns.tolist(),
        sharpe=metrics["sharpe"],
        total_return=metrics["total_return"],
    )
    return metrics


def _run_single_backtest_sphinx(
    prompt: str,
    start: str,
    end: str,
    initial_cash: float,
    stream_log: StreamLogFn = None,
) -> dict:
    """Full pipeline for one prompt using Sphinx CLI. Runs sync — call via executor."""
    _stream_log(stream_log, "Backtest started.", stage="start")
    sphinx_fallback = os.getenv("SPHINX_FALLBACK_TO_OPENROUTER", "true").lower() in {
        "1",
        "true",
        "yes",
    }
    try:
        code = _ask_sphinx_sync(prompt, stream_log)
    except Exception as sphinx_err:
        if not sphinx_fallback:
            raise
        _stream_log(
            stream_log,
            f"Sphinx CLI failed; falling back to OpenRouter generation. Error: {sphinx_err}",
            stage="generate_strategy",
            level="warning",
        )
        logger.warning(
            "Sphinx strategy generation failed, falling back to OpenRouter model. Error: %s",
            sphinx_err,
        )
        code = _ask_llm_sync(prompt, MODEL, stream_log=stream_log)

    macro_columns = _infer_macro_columns(code)
    _stream_log(
        stream_log,
        f"Loading macro data for {len(macro_columns)} columns.",
        stage="load_macro",
        columns=macro_columns,
    )
    macro = _get_macro_sync(macro_columns)
    _stream_log(
        stream_log,
        "Macro data loaded.",
        stage="load_macro",
        rows=len(macro),
        date_range=_format_index_date_range(macro.index),
        columns_preview=_preview_columns(macro.columns.tolist()),
    )
    _stream_log(stream_log, "Loading commodity data.", stage="load_commodities")
    commodities = _get_commodity_sync()
    _stream_log(
        stream_log,
        "Commodity data loaded.",
        stage="load_commodities",
        rows=len(commodities),
        date_range=(
            _format_index_date_range(commodities.index.get_level_values("timestamp"))
            if not commodities.empty
            else "empty"
        ),
        symbols_preview=(
            _preview_columns(commodities.index.get_level_values("symbol").unique().tolist())
            if not commodities.empty
            else []
        ),
    )
    referenced = _infer_referenced_assets(code)

    _stream_log(
        stream_log,
        f"Loading price history for {len(referenced)} tickers.",
        stage="load_prices",
        tickers=referenced,
        start=start,
        end=end,
    )
    prices = _load_prices_sync(referenced, start, end)
    _stream_log(
        stream_log,
        "Price history loaded.",
        stage="load_prices",
        **_summarize_price_frame(prices),
    )
    _stream_log(stream_log, "Executing generated strategy.", stage="execute_strategy")
    signals = _execute_strategy(code, macro, prices, commodities, stream_log=stream_log)
    _stream_log(
        stream_log,
        "Strategy execution produced signals.",
        stage="execute_strategy",
        **_summarize_signal_frame(signals),
    )

    _stream_log(stream_log, "Simulating portfolio.", stage="simulate_portfolio")
    port_returns, equity, weights, px = _simulate_portfolio(
        signals,
        prices,
        initial_cash,
        start=start,
        end=end,
    )
    _stream_log(
        stream_log,
        "Portfolio simulation complete.",
        stage="simulate_portfolio",
        tickers=_preview_columns(px.columns.tolist()),
        aligned_price_rows=len(px),
        aligned_price_range=_format_index_date_range(px.index),
        return_rows=len(port_returns),
        nonzero_return_days=int(port_returns.fillna(0).ne(0).sum()),
    )
    _stream_log(stream_log, "Computing metrics.", stage="compute_metrics")
    metrics = _compute_metrics(
        port_returns,
        equity,
        weights,
        px,
        prompt,
        tickers=signals.columns.tolist(),
        code=code,
    )
    _stream_log(
        stream_log,
        "Backtest completed.",
        stage="done",
        tickers=signals.columns.tolist(),
        sharpe=metrics["sharpe"],
        total_return=metrics["total_return"],
    )
    return metrics


def _format_sse(event: str, payload: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(payload, default=str)}\n\n"


def _build_backtest_stream_response(
    worker: Callable[[Callable[[str, dict[str, Any]], None]], dict],
) -> StreamingResponse:
    event_queue: "queue.Queue[Any]" = queue.Queue()

    def push_event(event: str, payload: dict[str, Any]) -> None:
        event_queue.put((event, payload))

    def run_worker() -> None:
        try:
            result = worker(push_event)
            push_event("result", result)
            push_event("done", {"status": "completed"})
        except Exception as exc:
            logger.exception("Streaming backtest worker failed: %s", exc)
            push_event(
                "log",
                {
                    "message": f"Backtest failed: {exc}",
                    "stage": "error",
                    "level": "error",
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                },
            )
            push_event(
                "error",
                {
                    "message": str(exc),
                    "type": type(exc).__name__,
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                },
            )
        finally:
            event_queue.put(_STREAM_SENTINEL)

    def event_stream():
        yield _format_sse(
            "ready",
            {
                "status": "streaming",
                "timestamp": datetime.utcnow().isoformat() + "Z",
            },
        )
        worker_thread = threading.Thread(target=run_worker, daemon=True)
        worker_thread.start()

        while True:
            item = event_queue.get()
            if item is _STREAM_SENTINEL:
                break
            event, payload = item
            yield _format_sse(event, payload)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


def _run_batch_job(job_id: str, request: BatchBacktestRequest):
    """Background task — runs all prompts and stores results in jobs dict."""
    jobs[job_id]["status"] = "running"
    results = []
    errors  = []

    for prompt in request.prompts:
        try:
            metrics = _run_single_backtest(
                prompt, request.model,
                request.start, request.end,
                request.initial_cash,
            )
            results.append(metrics)
        except Exception as e:
            logger.exception("Batch backtest failed for job_id=%s prompt=%r", job_id, prompt)
            errors.append({"prompt": prompt, "error": str(e)})

    jobs[job_id]["status"]  = "done"
    jobs[job_id]["results"] = results
    jobs[job_id]["errors"]  = errors


def _run_batch_job_sphinx(job_id: str, request: BatchBacktestRequest):
    """Background task — runs all prompts with Sphinx CLI and stores results."""
    jobs[job_id]["status"] = "running"
    results = []
    errors = []

    for prompt in request.prompts:
        try:
            metrics = _run_single_backtest_sphinx(
                prompt,
                request.start,
                request.end,
                request.initial_cash,
            )
            results.append(metrics)
        except Exception as e:
            logger.exception("Sphinx batch backtest failed for job_id=%s prompt=%r", job_id, prompt)
            errors.append({"prompt": prompt, "error": str(e)})

    jobs[job_id]["status"] = "done"
    jobs[job_id]["results"] = results
    jobs[job_id]["errors"] = errors


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        num = float(value)
    except Exception:
        return default
    if np.isnan(num):
        return default
    return num


def _fetch_commodity_quote(ticker: str) -> dict[str, Any]:
    hist = None
    fast_info = None
    try:
        yf_ticker = yf.Ticker(ticker)
        hist = yf_ticker.history(period="1d", interval="30m")
        fast_info = yf_ticker.fast_info
    except Exception as exc:
        logger.warning("Commodity quote fetch failed for ticker=%s: %s", ticker, exc)

    closes: list[float] = []
    if hist is not None and not hist.empty and "Close" in hist.columns:
        closes = [
            _safe_float(value, default=None)
            for value in hist["Close"].tolist()
            if value is not None
        ]
        closes = [v for v in closes if isinstance(v, float)]

    history = closes[-12:]

    def fast_get(key: str, default: float = 0.0) -> float:
        try:
            if fast_info is None:
                return default
            if hasattr(fast_info, "get"):
                value = fast_info.get(key)
            else:
                value = fast_info[key]
            return _safe_float(value, default)
        except Exception:
            return default

    current_price = fast_get("last_price", history[-1] if history else 0.0)
    previous_close = fast_get(
        "previous_close",
        history[-2] if len(history) > 1 else current_price,
    )

    if not current_price and not history:
        try:
            import requests

            chart_url = (
                f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
                "?range=5d&interval=30m&includePrePost=false"
            )
            res = requests.get(
                chart_url,
                headers={"User-Agent": "FalseMarkets/1.0"},
                timeout=10,
            )
            if res.ok:
                payload = res.json()
                result = payload.get("chart", {}).get("result", [])
                if result:
                    closes = (
                        result[0]
                        .get("indicators", {})
                        .get("quote", [{}])[0]
                        .get("close", [])
                    )
                    closes = [
                        _safe_float(v, default=None)
                        for v in closes
                        if v is not None
                    ]
                    closes = [v for v in closes if isinstance(v, float)]
                    history = closes[-12:]
                    if history:
                        current_price = history[-1]
                        previous_close = history[-2] if len(history) > 1 else current_price
        except Exception as exc:
            logger.warning("Commodity chart fallback failed for ticker=%s: %s", ticker, exc)

    change_abs = current_price - previous_close
    change_pct = (change_abs / previous_close * 100) if previous_close else 0.0

    return {
        "price": _safe_float(current_price, 0.0),
        "change_abs": _safe_float(change_abs, 0.0),
        "change_pct": _safe_float(change_pct, 0.0),
        "history": history,
    }


# ─────────────────────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────────────────────

@app.get("/", tags=["Health"])
def root():
    return {
        "service" : "FI + Commodity Backtest Engine",
        "status"  : "running",
        "model"   : MODEL,
        "data_range": f"{START} → {END}",
    }


@app.get("/health", tags=["Health"])
def health():
    macro = _cached_macro_all
    commodity = _cached_commodity_all
    return {
        "status"          : "ok",
        "macro_loaded"    : macro is not None,
        "macro_rows"      : len(macro) if macro is not None else 0,
        "macro_date_range": _format_index_date_range(macro.index) if macro is not None else "empty",
        "commodity_loaded": commodity is not None,
        "commodity_rows"  : len(commodity) if commodity is not None else 0,
        "commodity_date_range": (
            _format_index_date_range(commodity.index.get_level_values("timestamp"))
            if commodity is not None and len(commodity.index)
            else "empty"
        ),
    }


@app.get("/yfinance/detail")
async def yfinance_detail(symbol: str = Query(...), range: str = Query("1D")):
    try:
        import yfinance as yf
    except Exception:
        logger.exception("yfinance import failed for /yfinance/detail")
        return {"error": "yfinance not installed"}

    symbol = symbol.strip().upper()
    if not symbol:
        return {"error": "symbol required"}

    cfg = RANGE_MAP.get(range, RANGE_MAP["1D"])
    ticker = yf.Ticker(symbol)

    try:
        hist = ticker.history(period=cfg["period"], interval=cfg["interval"])
        fast_info = ticker.fast_info
        full_info = ticker.info if hasattr(ticker, "info") else {}
    except Exception as e:
        logger.warning("yfinance detail fetch failed for symbol=%s range=%s: %s", symbol, range, e)
        return {"error": f"yfinance fetch failed: {e}"}

    def fast_get(key, default=0):
        try:
            if hasattr(fast_info, "get"):
                value = fast_info.get(key)
            else:
                value = fast_info[key]
            return default if value is None else value
        except Exception:
            return default

    prices: List[List[float]] = []
    if hist is not None and not hist.empty:
        for idx, row in hist.iterrows():
            ts = idx.to_pydatetime()
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=None)
            prices.append([int(ts.timestamp() * 1000), float(row["Close"])])

    current_price = float(fast_get("last_price", prices[-1][1] if prices else 0))
    day_high = float(fast_get("day_high", 0))
    day_low = float(fast_get("day_low", 0))
    market_cap = float(fast_get("market_cap", 0))
    volume = float(fast_get("last_volume", 0))
    previous_close = float(fast_get("previous_close", current_price or 0))

    change_abs = current_price - previous_close
    change_pct = (change_abs / previous_close * 100) if previous_close else 0

    if hist is not None and not hist.empty:
        ath = float(hist["High"].max())
        atl = float(hist["Low"].min())
    else:
        ath = 0
        atl = 0

    return {
        "id": symbol.lower(),
        "name": full_info.get("longName") or full_info.get("shortName") or symbol,
        "symbol": symbol,
        "image": None,
        "current_price": current_price,
        "price_change_24h": change_abs,
        "price_change_percentage_24h": change_pct,
        "market_cap": market_cap,
        "total_volume": volume,
        "high_24h": day_high,
        "low_24h": day_low,
        "ath": ath,
        "atl": atl,
        "last_updated": datetime.utcnow().isoformat() + "Z",
        "prices": prices,
        "asset_type": "equity",
    }


@app.get("/yfinance/commodities")
async def yfinance_commodities():
    now = time.time()
    cached = _COMMODITY_CACHE.get("payload")
    if cached and now - _COMMODITY_CACHE.get("timestamp", 0.0) < _COMMODITY_CACHE_TTL:
        return cached

    items = []
    for cfg in COMMODITY_CONFIG:
        quote = _fetch_commodity_quote(cfg["ticker"])
        items.append(
            {
                "ticker": cfg["ticker"],
                "symbol": cfg["symbol"],
                "name": cfg["name"],
                "price": quote["price"],
                "changePercent": quote["change_pct"],
                "changeAbs": quote["change_abs"],
                "sparkline": quote["history"],
            }
        )

    def item_by_symbol(symbol: str) -> Optional[dict[str, Any]]:
        for item in items:
            if item["symbol"] == symbol:
                return item
        return None

    grains_symbols = ["ZW", "ZC", "ZS"]
    grains_items = [item_by_symbol(sym) for sym in grains_symbols]
    grains_items = [item for item in grains_items if item is not None]
    grains_price = (
        sum(item["price"] for item in grains_items) / len(grains_items)
        if grains_items
        else 0.0
    )
    grains_change = (
        sum(item["changePercent"] for item in grains_items) / len(grains_items)
        if grains_items
        else 0.0
    )

    usd_quote = _fetch_commodity_quote(COMMODITY_STRIP_TICKERS["usd-index"])
    wti_item = item_by_symbol("WTI")
    gold_item = item_by_symbol("XAU")

    strip = [
        {
            "id": "energy",
            "label": "Energy (WTI)",
            "price": wti_item["price"] if wti_item else 0.0,
            "changePercent": wti_item["changePercent"] if wti_item else 0.0,
        },
        {
            "id": "metals",
            "label": "Metals (XAU)",
            "price": gold_item["price"] if gold_item else 0.0,
            "changePercent": gold_item["changePercent"] if gold_item else 0.0,
        },
        {
            "id": "grains",
            "label": "Grains",
            "price": grains_price,
            "changePercent": grains_change,
        },
        {
            "id": "usd-index",
            "label": "USD Index",
            "price": usd_quote["price"],
            "changePercent": usd_quote["change_pct"],
        },
    ]

    payload = {
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "items": items,
        "strip": strip,
    }

    _COMMODITY_CACHE["timestamp"] = now
    _COMMODITY_CACHE["payload"] = payload
    return payload


@app.get("/etfs", tags=["Config"])
def list_etfs():
    return {"etfs": ALL_ETFS}


@app.get("/commodities/symbols", tags=["Config"])
def list_commodity_symbols():
    summary = _get_commodity_universe_summary()
    commodity = _get_commodity_sync()
    symbols: list[dict[str, Any]] = []
    for symbol in summary["symbols"]:
        rows = commodity.xs(symbol, level="symbol", drop_level=False)
        latest = rows.iloc[-1]
        symbols.append(
            {
                "symbol": symbol,
                "instrument_name": latest.get("instrument_name"),
                "category": latest.get("category"),
                "asset_class": latest.get("asset_class"),
                "universe": latest.get("universe"),
            }
        )
    return {"symbols": symbols}


@app.get("/macro/columns", tags=["Config"])
def list_macro_columns():
    try:
        macro = _get_macro_sync()
    except Exception as e:
        logger.exception("Macro columns request failed.")
        raise HTTPException(503, str(e))
    return {"columns": sorted(macro.columns.tolist())}


@app.get("/macro/snapshot", tags=["Data"])
def macro_snapshot():
    """Latest available macro values — useful for prompt context."""
    try:
        macro = _get_macro_sync()
    except Exception as e:
        logger.exception("Macro snapshot request failed.")
        raise HTTPException(503, str(e))
    latest = macro.iloc[-1].dropna()
    return {
        "date"  : str(latest.name.date()),
        "values": {k: round(float(v), 4) for k, v in latest.items()},
    }


@app.get("/commodities/snapshot", tags=["Data"])
def commodity_snapshot():
    try:
        commodity = _get_commodity_sync()
    except Exception as e:
        logger.exception("Commodity snapshot request failed.")
        raise HTTPException(503, str(e))

    if commodity.empty:
        return {"date": None, "rows": []}

    latest_date = commodity.index.get_level_values("timestamp").max()
    latest_rows = commodity.xs(latest_date, level="timestamp", drop_level=False).reset_index()
    latest_rows = latest_rows.sort_values(["category", "instrument_name", "symbol"])
    return {
        "date": str(pd.Timestamp(latest_date).date()),
        "rows": latest_rows.to_dict(orient="records"),
    }


@app.post("/backtest", response_model=MetricsResult, tags=["Backtest"])
async def run_backtest(request: BacktestRequest):
    """
    Run a single backtest synchronously.
    The LLM writes the strategy code, it executes against real data,
    and metrics are returned directly.
    """
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(
            None,
            _run_single_backtest,
            request.prompt,
            request.model,
            request.start,
            request.end,
            request.initial_cash,
        )
        return result
    except Exception as e:
        logger.exception(
            "Backtest request failed. model=%s start=%s end=%s initial_cash=%s",
            request.model,
            request.start,
            request.end,
            request.initial_cash,
        )
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/backtest/stream", tags=["Backtest"])
def stream_backtest(request: BacktestRequest):
    """Run a single backtest and stream structured progress events via SSE."""

    def worker(push_event: Callable[[str, dict[str, Any]], None]) -> dict:
        push_event(
            "meta",
            {
                "mode": "openrouter",
                "model": request.model,
                "start": request.start,
                "end": request.end,
                "initial_cash": request.initial_cash,
                "timestamp": datetime.utcnow().isoformat() + "Z",
            },
        )
        return _run_single_backtest(
            request.prompt,
            request.model,
            request.start,
            request.end,
            request.initial_cash,
            stream_log=push_event,
        )

    return _build_backtest_stream_response(worker)


@app.post("/backtest/sphinx", response_model=MetricsResult, tags=["Backtest"])
async def run_backtest_sphinx(request: BacktestRequest):
    """
    Run a single backtest via Sphinx CLI for strategy generation.
    """
    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(
            None,
            _run_single_backtest_sphinx,
            request.prompt,
            request.start,
            request.end,
            request.initial_cash,
        )
        return result
    except Exception as e:
        logger.exception(
            "Sphinx backtest request failed. start=%s end=%s initial_cash=%s",
            request.start,
            request.end,
            request.initial_cash,
        )
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/backtest/sphinx/stream", tags=["Backtest"])
def stream_backtest_sphinx(request: BacktestRequest):
    """Run a single Sphinx backtest and stream structured progress events via SSE."""

    def worker(push_event: Callable[[str, dict[str, Any]], None]) -> dict:
        push_event(
            "meta",
            {
                "mode": "sphinx",
                "start": request.start,
                "end": request.end,
                "initial_cash": request.initial_cash,
                "timestamp": datetime.utcnow().isoformat() + "Z",
            },
        )
        return _run_single_backtest_sphinx(
            request.prompt,
            request.start,
            request.end,
            request.initial_cash,
            stream_log=push_event,
        )

    return _build_backtest_stream_response(worker)


@app.post("/backtest/batch", response_model=JobStatus, tags=["Backtest"])
async def run_batch_backtest(
    request: BatchBacktestRequest,
    background_tasks: BackgroundTasks,
):
    """
    Submit multiple prompts as a background job.
    Returns a job_id immediately — poll /backtest/batch/{job_id} for results.
    """
    if not request.prompts:
        raise HTTPException(400, "prompts list cannot be empty")
    if len(request.prompts) > 20:
        raise HTTPException(400, "max 20 prompts per batch")

    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "status"    : "pending",
        "prompts"   : request.prompts,
        "results"   : [],
        "errors"    : [],
        "created_at": datetime.utcnow().isoformat(),
    }

    background_tasks.add_task(_run_batch_job, job_id, request)

    return JobStatus(
        job_id     = job_id,
        status     = "pending",
        prompt     = f"{len(request.prompts)} prompts queued",
        created_at = jobs[job_id]["created_at"],
    )


@app.post("/backtest/sphinx/batch", response_model=JobStatus, tags=["Backtest"])
async def run_batch_backtest_sphinx(
    request: BatchBacktestRequest,
    background_tasks: BackgroundTasks,
):
    """
    Submit multiple prompts as a background job using Sphinx CLI.
    Returns a job_id immediately — poll /backtest/batch/{job_id} for results.
    """
    if not request.prompts:
        raise HTTPException(400, "prompts list cannot be empty")
    if len(request.prompts) > 20:
        raise HTTPException(400, "max 20 prompts per batch")

    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "status": "pending",
        "prompts": request.prompts,
        "results": [],
        "errors": [],
        "created_at": datetime.utcnow().isoformat(),
    }

    background_tasks.add_task(_run_batch_job_sphinx, job_id, request)

    return JobStatus(
        job_id=job_id,
        status="pending",
        prompt=f"{len(request.prompts)} prompts queued",
        created_at=jobs[job_id]["created_at"],
    )


@app.get("/backtest/batch/{job_id}", response_model=BatchResult, tags=["Backtest"])
def get_batch_result(job_id: str):
    """Poll this endpoint to check batch job status and retrieve results."""
    if job_id not in jobs:
        raise HTTPException(404, f"Job {job_id} not found")
    job = jobs[job_id]
    return BatchResult(
        job_id  = job_id,
        status  = job["status"],
        results = job.get("results", []),
        errors  = job.get("errors", []),
    )


@app.get("/backtest/batch/{job_id}/status", tags=["Backtest"])
def get_job_status(job_id: str):
    """Lightweight status check — just returns status without full results."""
    if job_id not in jobs:
        raise HTTPException(404, f"Job {job_id} not found")
    job = jobs[job_id]
    return {
        "job_id"    : job_id,
        "status"    : job["status"],
        "n_done"    : len(job.get("results", [])),
        "n_errors"  : len(job.get("errors", [])),
        "n_total"   : len(job.get("prompts", [])),
        "created_at": job["created_at"],
    }


@app.delete("/backtest/batch/{job_id}", tags=["Backtest"])
def delete_job(job_id: str):
    if job_id not in jobs:
        raise HTTPException(404, f"Job {job_id} not found")
    del jobs[job_id]
    return {"deleted": job_id}


@app.get("/jobs", tags=["Backtest"])
def list_jobs():
    """List all jobs and their current status."""
    return {
        "jobs": [
            {
                "job_id"    : jid,
                "status"    : j["status"],
                "n_prompts" : len(j.get("prompts", [])),
                "n_done"    : len(j.get("results", [])),
                "created_at": j["created_at"],
            }
            for jid, j in jobs.items()
        ]
    }
