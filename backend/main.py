import warnings
import asyncio
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
TABLE_NAME      = os.getenv("TABLE_NAME", "fi_macro_data")
OPENROUTER_KEY  = os.getenv("OPENROUTER_KEY")
OPENROUTER_BASE = os.getenv("OPENROUTER_BASE", "https://openrouter.ai/api/v1")
MODEL           = os.getenv("MODEL", "anthropic/claude-sonnet-4-5")
START           = os.getenv("START", "2000-05-01")
END             = os.getenv("END", "2025-01-01")
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

SYSTEM_PROMPT = f"""You are a quantitative researcher at a hedge fund.
Translate the plain-English trading strategy into a Python function.

Available DataFrames:
- `macro`: daily macro data, DatetimeIndex. Columns:
{MACRO_COLUMNS}

- `prices`: daily ETF closes, DatetimeIndex. Available ETFs:
{ETF_UNIVERSE}

Write ONLY a function called `generate_signals(macro, prices)` that returns:
- pd.DataFrame with DatetimeIndex
- Columns = tickers (e.g. "TLT", "GLD")
- Values = float weights (1.0=long, -1.0=short, 0=flat)

Hard rules:
1. Raw Python only — no imports, no markdown fences, no explanation
2. pandas=pd and numpy=np are already available — never import them
3. Use .shift(1) on ALL signals to prevent look-ahead bias
4. Handle NaN with .fillna(0)
5. Return a DataFrame, never a Series
6. Since the strategy uses daily time series, the positions are also expected on a daily granularity
"""

SPHINX_OUTPUT_CODE_BLOCK = re.compile(r"```(?:python)?\s*(.*?)```", re.DOTALL)


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


@asynccontextmanager
async def lifespan(app: FastAPI):
    await preload_macro()
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


def _infer_macro_columns(code: str) -> list[str]:
    referenced = []
    for column in MACRO_FIELD_NAMES:
        if f"'{column}'" in code or f'"{column}"' in code:
            referenced.append(column)
    return sorted(set(referenced)) or MACRO_FIELD_NAMES.copy()


def _load_macro_sync(columns: Optional[list[str]] = None) -> pd.DataFrame:
    client = create_client(SUPABASE_URL, SUPABASE_KEY)
    rows, page, limit = [], 0, 1000
    requested = list(_normalize_macro_columns(columns))
    select_clause = ",".join(["date", *requested])
    while True:
        res = (
            client.table(TABLE_NAME)
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


def _load_prices_sync(tickers: list, start: str, end: str) -> pd.DataFrame:
    raw = yf.download(
        tickers, start=start, end=end,
        auto_adjust=True, progress=False,
        group_by="ticker"
    )
    if isinstance(raw.columns, pd.MultiIndex):
        closes = raw.xs("Close", axis=1, level=1)
    else:
        col = "Close" if "Close" in raw.columns else "close"
        closes = raw[[col]].rename(columns={col: tickers[0]})
    closes.columns = [str(c) for c in closes.columns]
    return closes.sort_index()


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
            {"role": "system", "content": SYSTEM_PROMPT},
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
    prompt = f"{SYSTEM_PROMPT}\n\nTrading strategy:\n{user_prompt}"

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


def _execute_strategy(code: str, macro: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    namespace = {"pd": pd, "np": np}
    try:
        exec(code, namespace)
    except Exception as e:
        raise RuntimeError(f"Compile error: {e}\n\nCode:\n{code}") from e
    if "generate_signals" not in namespace:
        raise RuntimeError(f"No generate_signals() function found in generated code.\n\nCode:\n{code}")
    try:
        signals = namespace["generate_signals"](macro.copy(), prices.copy())
    except Exception as e:
        raise RuntimeError(f"Runtime error in generate_signals(): {e}\n\nCode:\n{code}") from e
    if not isinstance(signals, pd.DataFrame):
        raise RuntimeError(f"generate_signals() must return DataFrame, got {type(signals)}")
    return signals


def _simulate_portfolio(
    signals: pd.DataFrame,
    prices: pd.DataFrame,
    initial_cash: float,
) -> tuple[pd.Series, pd.Series, pd.DataFrame, pd.DataFrame]:
    tickers = [t for t in signals.columns if t in prices.columns]
    if not tickers:
        raise RuntimeError(
            f"No ticker overlap: signals={signals.columns.tolist()} "
            f"prices={prices.columns.tolist()}"
        )
    px      = prices[tickers].copy()
    returns = px.pct_change()
    weights = signals[tickers].reindex(returns.index, method="ffill").fillna(0)
    row_sum = weights.abs().sum(axis=1).replace(0, 1)
    weights = weights.div(row_sum, axis=0).shift(1).fillna(0)
    port_returns = (weights * returns).sum(axis=1).replace([np.inf, -np.inf], np.nan).fillna(0)
    equity       = (1 + port_returns).cumprod() * initial_cash
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
    referenced = [t for t in ALL_ETFS if f'"{t}"' in code or f"'{t}'" in code]
    if not referenced:
        referenced = ["TLT", "HYG", "GLD", "SPY", "TIP", "SHY"]

    _stream_log(
        stream_log,
        f"Loading price history for {len(referenced)} tickers.",
        stage="load_prices",
        tickers=referenced,
        start=start,
        end=end,
    )
    prices  = _load_prices_sync(referenced, start, end)
    _stream_log(stream_log, "Executing generated strategy.", stage="execute_strategy")
    signals = _execute_strategy(code, macro, prices)

    _stream_log(stream_log, "Simulating portfolio.", stage="simulate_portfolio")
    port_returns, equity, weights, aligned_prices = _simulate_portfolio(
        signals, prices, initial_cash
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
    referenced = [t for t in ALL_ETFS if f'"{t}"' in code or f"'{t}'" in code]
    if not referenced:
        referenced = ["TLT", "HYG", "GLD", "SPY", "TIP", "SHY"]

    _stream_log(
        stream_log,
        f"Loading price history for {len(referenced)} tickers.",
        stage="load_prices",
        tickers=referenced,
        start=start,
        end=end,
    )
    prices = _load_prices_sync(referenced, start, end)
    _stream_log(stream_log, "Executing generated strategy.", stage="execute_strategy")
    signals = _execute_strategy(code, macro, prices)

    _stream_log(stream_log, "Simulating portfolio.", stage="simulate_portfolio")
    port_returns, equity, weights, px = _simulate_portfolio(signals, prices, initial_cash)
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
    try:
        yf_ticker = yf.Ticker(ticker)
        hist = yf_ticker.history(period="1d", interval="30m")
        fast_info = yf_ticker.fast_info
    except Exception as exc:
        logger.warning("Commodity quote fetch failed for ticker=%s: %s", ticker, exc)
        return {
            "price": 0.0,
            "change_abs": 0.0,
            "change_pct": 0.0,
            "history": [],
            "error": str(exc),
        }

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
        "service" : "FI Backtest Engine",
        "status"  : "running",
        "model"   : MODEL,
        "data_range": f"{START} → {END}",
    }


@app.get("/health", tags=["Health"])
def health():
    macro = _cached_macro_all
    return {
        "status"      : "ok",
        "macro_loaded": macro is not None,
        "macro_rows"  : len(macro) if macro is not None else 0,
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

    index_quote = _fetch_commodity_quote(COMMODITY_STRIP_TICKERS["commodity-index"])
    usd_quote = _fetch_commodity_quote(COMMODITY_STRIP_TICKERS["usd-index"])
    wti_item = item_by_symbol("WTI")
    gold_item = item_by_symbol("XAU")

    strip = [
        {
            "id": "commodity-index",
            "label": "Commodity Index",
            "price": index_quote["price"],
            "changePercent": index_quote["change_pct"],
        },
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
