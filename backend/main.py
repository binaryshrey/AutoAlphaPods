import warnings
import numpy as np
import pandas as pd
import yfinance as yf
from supabase import create_client
from typing import Optional
from openai import OpenAI
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uuid
import asyncio
from datetime import datetime
from contextlib import asynccontextmanager

warnings.filterwarnings("ignore")

from dotenv import load_dotenv
import os

load_dotenv()

SUPABASE_URL    = os.getenv("SUPABASE_URL")
SUPABASE_KEY    = os.getenv("SUPABASE_KEY")
TABLE_NAME      = os.getenv("TABLE_NAME", "fi_macro_data")
OPENROUTER_KEY  = os.getenv("OPENROUTER_KEY")
OPENROUTER_BASE = os.getenv("OPENROUTER_BASE", "https://openrouter.ai/api/v1")
MODEL           = os.getenv("MODEL", "anthropic/claude-sonnet-4-5")
START           = os.getenv("START", "2000-05-01")
END             = os.getenv("END", "2025-01-01")

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

SYSTEM_PROMPT = f"""You are a quantitative researcher at a hedge fund.
Translate the plain-English trading strategy into a Python function.

Available DataFrames:
- `macro`: daily macro data, DatetimeIndex. Columns:
{MACRO_COLUMNS}

- `prices`: daily ETF closes, DatetimeIndex. Available ETFs:
{ETF_UNIVERSE}

Write ONLY a function called `generate_signals(macro, prices)` that returns:
- pd.DataFrame with DatetimeIndex
- Columns = ETF tickers (e.g. "TLT", "GLD")
- Values = float weights (1.0=long, -1.0=short, 0=flat)

Hard rules:
1. Raw Python only — no imports, no markdown fences, no explanation
2. pandas=pd and numpy=np are already available — never import them
3. Use .shift(1) on ALL signals to prevent look-ahead bias
4. Handle NaN with .fillna(0)
5. Return a DataFrame, never a Series
6. Resample to monthly (.resample('ME').last()) then reindex to daily prices index
"""


# ─────────────────────────────────────────────────────────────
# IN-MEMORY JOB STORE
# ─────────────────────────────────────────────────────────────

jobs: dict[str, dict] = {}
# Structure: { job_id: { status, prompt, result, error, created_at } }


# ─────────────────────────────────────────────────────────────
# LIFESPAN — preload macro data on startup
# ─────────────────────────────────────────────────────────────

_cached_macro: Optional[pd.DataFrame] = None


async def preload_macro():
    global _cached_macro
    if _cached_macro is not None:
        return
    print("Preloading macro data from Supabase...")
    loop = asyncio.get_event_loop()
    _cached_macro = await loop.run_in_executor(None, _load_macro_sync)
    print(f"Macro data ready: {len(_cached_macro):,} rows × {len(_cached_macro.columns)} cols")


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
    win_rate: str
    max_streak: int
    total_return: str
    end_equity: str
    equity_curve: list[dict]   # [{date, equity}]

class BatchResult(BaseModel):
    job_id: str
    status: str
    results: list[MetricsResult]
    errors: list[dict]


# ─────────────────────────────────────────────────────────────
# CORE ENGINE (sync — run in executor)
# ─────────────────────────────────────────────────────────────

def _load_macro_sync() -> pd.DataFrame:
    client = create_client(SUPABASE_URL, SUPABASE_KEY)
    rows, page, limit = [], 0, 1000
    while True:
        res = (
            client.table(TABLE_NAME)
            .select("*")
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


def _ask_llm_sync(prompt: str, model: str) -> str:
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
    return code.strip()


def _execute_strategy(code: str, macro: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    namespace = {"pd": pd, "np": np}
    try:
        exec(code, namespace)
    except Exception as e:
        raise RuntimeError(f"Compile error: {e}")
    if "generate_signals" not in namespace:
        raise RuntimeError("No generate_signals() function found in generated code")
    try:
        signals = namespace["generate_signals"](macro.copy(), prices.copy())
    except Exception as e:
        raise RuntimeError(f"Runtime error in generate_signals(): {e}")
    if not isinstance(signals, pd.DataFrame):
        raise RuntimeError(f"generate_signals() must return DataFrame, got {type(signals)}")
    return signals


def _simulate_portfolio(
    signals: pd.DataFrame,
    prices: pd.DataFrame,
    initial_cash: float,
) -> tuple[pd.Series, pd.Series]:
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
    port_returns = (weights * returns).sum(axis=1).dropna()
    equity       = (1 + port_returns).cumprod() * initial_cash
    return port_returns, equity


def _compute_metrics(
    port_returns: pd.Series,
    equity: pd.Series,
    prompt: str,
    tickers: list,
    code: str,
) -> dict:
    ann    = 252
    mean_r = port_returns.mean()
    std_r  = port_returns.std()
    sharpe = (mean_r / std_r * np.sqrt(ann)) if std_r > 0 else 0.0

    rolling_max = equity.cummax()
    max_dd      = ((equity - rolling_max) / rolling_max).min()

    n_years = len(port_returns) / ann
    total_r = equity.iloc[-1] / equity.iloc[0] - 1
    cagr    = (1 + total_r) ** (1 / n_years) - 1 if n_years > 0 else 0.0
    calmar  = (cagr / abs(max_dd)) if max_dd != 0 else 0.0

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

    # Equity curve — downsample to monthly for response size
    equity_monthly = equity.resample("ME").last()
    equity_curve = [
        {"date": str(d.date()), "equity": round(float(v), 2)}
        for d, v in equity_monthly.items()
    ]

    return {
        "prompt"        : prompt,
        "tickers"       : ", ".join(tickers),
        "generated_code": code,
        "sharpe"        : round(sharpe, 2),
        "sortino"       : round(sortino, 2),
        "calmar"        : round(calmar, 2),
        "max_dd"        : f"{max_dd*100:.1f}%",
        "cagr"          : f"{cagr*100:.1f}%",
        "win_rate"      : f"{win_rate*100:.0f}%",
        "max_streak"    : max_streak,
        "total_return"  : f"{total_r*100:.1f}%",
        "end_equity"    : f"${equity.iloc[-1]:,.0f}",
        "equity_curve"  : equity_curve,
    }


def _run_single_backtest(
    prompt: str,
    model: str,
    start: str,
    end: str,
    initial_cash: float,
) -> dict:
    """Full pipeline for one prompt. Runs sync — call via executor."""
    global _cached_macro

    macro = _cached_macro
    if macro is None:
        macro = _load_macro_sync()
        _cached_macro = macro

    code       = _ask_llm_sync(prompt, model)
    referenced = [t for t in ALL_ETFS if f'"{t}"' in code or f"'{t}'" in code]
    if not referenced:
        referenced = ["TLT", "HYG", "GLD", "SPY", "TIP", "SHY"]

    prices  = _load_prices_sync(referenced, start, end)
    signals = _execute_strategy(code, macro, prices)

    port_returns, equity = _simulate_portfolio(signals, prices, initial_cash)
    metrics = _compute_metrics(
        port_returns, equity, prompt,
        tickers=signals.columns.tolist(),
        code=code,
    )
    return metrics


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
            errors.append({"prompt": prompt, "error": str(e)})

    jobs[job_id]["status"]  = "done"
    jobs[job_id]["results"] = results
    jobs[job_id]["errors"]  = errors


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
    return {
        "status"      : "ok",
        "macro_loaded": _cached_macro is not None,
        "macro_rows"  : len(_cached_macro) if _cached_macro is not None else 0,
    }


@app.get("/etfs", tags=["Config"])
def list_etfs():
    return {"etfs": ALL_ETFS}


@app.get("/macro/columns", tags=["Config"])
def list_macro_columns():
    if _cached_macro is None:
        raise HTTPException(503, "Macro data not yet loaded")
    return {"columns": sorted(_cached_macro.columns.tolist())}


@app.get("/macro/snapshot", tags=["Data"])
def macro_snapshot():
    """Latest available macro values — useful for prompt context."""
    if _cached_macro is None:
        raise HTTPException(503, "Macro data not yet loaded")
    latest = _cached_macro.iloc[-1].dropna()
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
        raise HTTPException(status_code=500, detail=str(e))


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
