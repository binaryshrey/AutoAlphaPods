# pip install openai supabase pandas numpy rich yfinance

import warnings
import numpy as np
import pandas as pd
import yfinance as yf
from supabase import create_client
from typing import Optional
from rich.console import Console
from rich.table import Table
from rich import box
from openai import OpenAI

warnings.filterwarnings("ignore")
console = Console()

# ── Credentials ───────────────────────────────────────────────
SUPABASE_URL      = "https://oyedcwhvceznyeuuujja.supabase.co"
SUPABASE_KEY      = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im95ZWRjd2h2Y2V6bnlldXV1amphIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM1MDk4MTEsImV4cCI6MjA4OTA4NTgxMX0.xifg2CrZv9330qQTXC0515Gk_fyLnS5KAG2C1a_TQyM"
TABLE_NAME        = "fi_macro_data"

OPENROUTER_KEY    = "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"
OPENROUTER_BASE   = "https://openrouter.ai/api/v1"

# ── Model — pick any from openrouter.ai/models ────────────────
# Free tier options:          "meta-llama/llama-3.3-70b-instruct:free"
# Best for code:              "anthropic/claude-sonnet-4-5"
# Fast + cheap:               "openai/gpt-4o-mini"
# Most capable:               "anthropic/claude-opus-4-5"
MODEL = "anthropic/claude-sonnet-4-5"

START = "2000-05-01"
END   = "2025-01-01"

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
# 1. OPENROUTER CLIENT
# ─────────────────────────────────────────────────────────────

def get_client() -> OpenAI:
    return OpenAI(
        api_key=OPENROUTER_KEY,
        base_url=OPENROUTER_BASE,
        default_headers={
            "HTTP-Referer": "https://github.com/backtester",
            "X-Title": "FI Backtest Engine",
        },
    )


# ─────────────────────────────────────────────────────────────
# 2. DATA LOADERS
# ─────────────────────────────────────────────────────────────

_cached_macro: Optional[pd.DataFrame] = None


def load_macro() -> pd.DataFrame:
    global _cached_macro
    if _cached_macro is not None:
        return _cached_macro

    console.print("  [dim]Loading macro data from Supabase...[/]")
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

    _cached_macro = df
    console.print(f"  [dim]Loaded {len(df):,} rows × {len(df.columns)} columns.[/]")
    return df


def load_prices(tickers: list) -> pd.DataFrame:
    console.print(f"  [dim]Downloading prices: {tickers}...[/]")
    raw = yf.download(
        tickers, start=START, end=END,
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


# ─────────────────────────────────────────────────────────────
# 3. LLM STRATEGY GENERATOR
# ─────────────────────────────────────────────────────────────

def ask_llm_for_strategy(user_prompt: str) -> str:
    console.print(f"  [dim]Asking {MODEL} to write strategy...[/]")
    client = get_client()

    response = client.chat.completions.create(
        model=MODEL,
        max_tokens=2000,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": user_prompt},
        ],
    )

    code = response.choices[0].message.content.strip()

    # Strip markdown fences defensively
    if "```" in code:
        code = "\n".join(
            line for line in code.split("\n")
            if not line.strip().startswith("```")
        )

    return code.strip()


# ─────────────────────────────────────────────────────────────
# 4. STRATEGY EXECUTOR
# ─────────────────────────────────────────────────────────────

def execute_strategy(
    code   : str,
    macro  : pd.DataFrame,
    prices : pd.DataFrame,
) -> pd.DataFrame:
    namespace = {"pd": pd, "np": np}

    try:
        exec(code, namespace)
    except Exception as e:
        raise RuntimeError(f"Code failed to compile:\n{e}\n\nCode:\n{code}")

    if "generate_signals" not in namespace:
        raise RuntimeError(f"No `generate_signals` function found.\nCode:\n{code}")

    try:
        signals = namespace["generate_signals"](macro.copy(), prices.copy())
    except Exception as e:
        raise RuntimeError(f"generate_signals() error:\n{e}\n\nCode:\n{code}")

    if not isinstance(signals, pd.DataFrame):
        raise RuntimeError(f"Must return DataFrame, got {type(signals)}")

    return signals


# ─────────────────────────────────────────────────────────────
# 5. PORTFOLIO SIMULATOR
# ─────────────────────────────────────────────────────────────

def simulate_portfolio(
    signals     : pd.DataFrame,
    prices      : pd.DataFrame,
    initial_cash: float = 100_000,
) -> tuple[pd.Series, pd.Series]:
    tickers = [t for t in signals.columns if t in prices.columns]
    if not tickers:
        raise RuntimeError(
            f"No overlap between signals {signals.columns.tolist()} "
            f"and prices {prices.columns.tolist()}"
        )

    px      = prices[tickers].copy()
    returns = px.pct_change()

    # Reindex weights to daily, forward-fill rebalance dates
    weights = signals[tickers].reindex(returns.index, method="ffill").fillna(0)

    # Normalise so abs weights sum to 1.0
    row_sum = weights.abs().sum(axis=1).replace(0, 1)
    weights = weights.div(row_sum, axis=0)

    # Execute next day — no look-ahead
    weights = weights.shift(1).fillna(0)

    port_returns = (weights * returns).sum(axis=1).dropna()
    equity       = (1 + port_returns).cumprod() * initial_cash

    return port_returns, equity


# ─────────────────────────────────────────────────────────────
# 6. METRICS
# ─────────────────────────────────────────────────────────────

def compute_metrics(
    port_returns: pd.Series,
    equity      : pd.Series,
    prompt      : str,
    tickers     : list,
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

    return {
        "prompt"      : prompt[:58] + ("…" if len(prompt) > 58 else ""),
        "tickers"     : ", ".join(tickers),
        "sharpe"      : round(sharpe, 2),
        "sortino"     : round(sortino, 2),
        "calmar"      : round(calmar, 2),
        "max_dd"      : f"{max_dd*100:.1f}%",
        "cagr"        : f"{cagr*100:.1f}%",
        "win_rate"    : f"{win_rate*100:.0f}%",
        "max_streak"  : max_streak,
        "total_return": f"{total_r*100:.1f}%",
        "end_equity"  : f"${equity.iloc[-1]:,.0f}",
    }


# ─────────────────────────────────────────────────────────────
# 7. DISPLAY
# ─────────────────────────────────────────────────────────────

def display_table(all_metrics: list[dict]):
    table = Table(
        title=f"Backtest Results  [dim](via OpenRouter → {MODEL})[/]",
        box=box.ROUNDED, show_header=True,
        header_style="bold cyan", show_lines=True,
    )
    for name, style, width in [
        ("Prompt",       "cyan",   44),
        ("Tickers",      "white",  22),
        ("Sharpe",       "green",   8),
        ("Sortino",      "green",   8),
        ("Calmar",       "green",   8),
        ("Max DD",       "red",     8),
        ("CAGR",         "green",   8),
        ("Win %",        "white",   7),
        ("Loss streak",  "red",    10),
        ("Total return", "green",  12),
        ("End equity",   "green",  13),
    ]:
        table.add_column(name, style=style, min_width=width)

    for m in all_metrics:
        sc = "green" if m["sharpe"] > 0.5 else "yellow" if m["sharpe"] > 0 else "red"
        table.add_row(
            m["prompt"], m["tickers"],
            f"[{sc}]{m['sharpe']}[/]",
            f"[{sc}]{m['sortino']}[/]",
            f"[{sc}]{m['calmar']}[/]",
            m["max_dd"], m["cagr"], m["win_rate"],
            str(m["max_streak"]), m["total_return"], m["end_equity"],
        )

    console.print(table)


def show_generated_code(prompt: str, code: str):
    console.print(f"\n[bold yellow]── Strategy code for:[/] {prompt[:58]}")
    console.print("[dim]" + "─" * 66 + "[/]")
    for line in code.split("\n"):
        console.print(f"  [dim white]{line}[/]")
    console.print("[dim]" + "─" * 66 + "[/]\n")


# ─────────────────────────────────────────────────────────────
# 8. MAIN
# ─────────────────────────────────────────────────────────────

PROMPTS = [
    "Buy long bonds (TLT) when the yield curve (10y2y spread) is inverted below 0",
    "Go long gold (GLD) when 10-year real rates are falling month over month",
    "Buy TLT and sell HYG when high yield spreads widen more than 50bps in a month",
    "Long TIP when breakeven inflation is above 2.5%, short SHY when below 1.5%",
    "Buy SPY when yield curve is steep above 1%, move to cash when inverted",
    "Rotate into TLT when fed funds is rising, into HYG when fed funds is falling",
]


def run_all(prompts: list[str]):
    all_metrics = []
    macro = load_macro()

    for prompt in prompts:
        console.rule(f"[bold cyan]{prompt[:70]}")
        try:
            # Step 1 — LLM writes strategy
            code = ask_llm_for_strategy(prompt)
            show_generated_code(prompt, code)

            # Step 2 — detect tickers referenced in generated code
            referenced = [t for t in ALL_ETFS if f'"{t}"' in code or f"'{t}'" in code]
            if not referenced:
                referenced = ["TLT", "HYG", "GLD", "SPY", "TIP", "SHY"]
                console.print(f"  [yellow]No tickers in code — fallback: {referenced}[/]")

            # Step 3 — load prices
            prices = load_prices(referenced)

            # Step 4 — execute strategy
            signals = execute_strategy(code, macro, prices)
            console.print(
                f"  [green]✓ Executed.[/] "
                f"Shape: {signals.shape}  "
                f"Tickers: {signals.columns.tolist()}"
            )

            # Step 5 — simulate
            port_returns, equity = simulate_portfolio(signals, prices)

            # Step 6 — metrics
            metrics = compute_metrics(
                port_returns, equity, prompt,
                tickers=signals.columns.tolist()
            )
            all_metrics.append(metrics)

            console.print(
                f"  Sharpe [green]{metrics['sharpe']}[/]  "
                f"Max DD [red]{metrics['max_dd']}[/]  "
                f"CAGR [green]{metrics['cagr']}[/]  "
                f"End equity [green]{metrics['end_equity']}[/]"
            )

        except Exception as e:
            console.print(f"  [red]Failed: {e}[/]")
            continue

    console.print()
    if all_metrics:
        display_table(all_metrics)
    else:
        console.print("[red]No results.[/]")


if __name__ == "__main__":
    run_all(PROMPTS)