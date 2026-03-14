# pip install supabase pandas numpy rich yfinance

import os
import json
import re
import shlex
import shutil
import subprocess
import tempfile
import warnings
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import yfinance as yf
from rich import box
from rich.console import Console
from rich.table import Table
from supabase import create_client

warnings.filterwarnings("ignore")
console = Console()

# ── Credentials ───────────────────────────────────────────────
SUPABASE_URL = "https://oyedcwhvceznyeuuujja.supabase.co"
SUPABASE_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im95ZWRjd2h2Y2V6bnlldXV1amphIiwicm9sZSI6"
    "ImFub24iLCJpYXQiOjE3NzM1MDk4MTEsImV4cCI6MjA4OTA4NTgxMX0."
    "xifg2CrZv9330qQTXC0515Gk_fyLnS5KAG2C1a_TQyM"
)
TABLE_NAME = "fi_macro_data"
SPHINX_API_KEY = "sk_live_eLbVU62dgi_8DkwsidR4KYaUtw2rmNyjJ070N5tUhb4"

START = "2000-05-01"
END = "2025-01-01"

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
    "TLT",
    "IEF",
    "SHY",
    "TIP",
    "AGG",
    "LQD",
    "HYG",
    "JNK",
    "MBB",
    "MUB",
    "EMB",
    "TBT",
    "GLD",
    "SLV",
    "USO",
    "CPER",
    "DJP",
    "SPY",
    "QQQ",
    "XLF",
    "XLU",
    "XLV",
    "XLP",
    "VNQ",
    "XLE",
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

PROMPTS = [
    "Buy long bonds (TLT) when the yield curve (10y2y spread) is inverted below 0",
    "Go long gold (GLD) when 10-year real rates are falling month over month",
    "Buy TLT and sell HYG when high yield spreads widen more than 50bps in a month",
    "Long TIP when breakeven inflation is above 2.5%, short SHY when below 1.5%",
    "Buy SPY when yield curve is steep above 1%, move to cash when inverted",
    "Rotate into TLT when fed funds is rising, into HYG when fed funds is falling",
]

SPHINX_OUTPUT_CODE_BLOCK = re.compile(r"```(?:python)?\s*(.*?)```", re.DOTALL)


# ─────────────────────────────────────────────────────────────
# 1. SPHINX CLI
# ─────────────────────────────────────────────────────────────

def resolve_sphinx_cli() -> str:
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


def build_sphinx_command(prompt: str, notebook_filepath: str) -> list[str]:
    cmd = [
        resolve_sphinx_cli(),
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


def extract_code_from_sphinx_output(output: str) -> str:
    matches = SPHINX_OUTPUT_CODE_BLOCK.findall(output)
    if matches:
        return matches[-1].strip()

    start = output.find("def generate_signals(")
    if start >= 0:
        return output[start:].strip()

    raise RuntimeError(f"Could not find generate_signals() in Sphinx output:\n{output}")


def extract_code_from_notebook(notebook_path: Path) -> str:
    if not notebook_path.exists():
        raise RuntimeError(f"Sphinx notebook was not created: {notebook_path}")

    notebook = json.loads(notebook_path.read_text())
    for cell in reversed(notebook.get("cells", [])):
        source = "".join(cell.get("source", []))
        if "def generate_signals(" in source:
            start = source.find("def generate_signals(")
            return source[start:].strip()

    raise RuntimeError(f"Could not find generate_signals() in notebook: {notebook_path}")


def ensure_sphinx_ready() -> None:
    cmd = [resolve_sphinx_cli(), "status"]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
        env=build_sphinx_env(),
    )
    if result.returncode != 0:
        raise RuntimeError(
            "sphinx-cli status failed. Run `sphinx-cli login` first.\n"
            f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
        )


def ask_sphinx_for_strategy(user_prompt: str) -> str:
    console.print("  [dim]Asking Sphinx CLI to write strategy...[/]")
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

    cmd = build_sphinx_command(prompt, str(notebook_path))

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
        env=build_sphinx_env(),
    )
    if result.returncode != 0:
        raise RuntimeError(
            "sphinx-cli chat failed.\n"
            f"Command: {' '.join(cmd)}\n"
            f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
        )

    stdout = result.stdout.strip()
    try:
        code = extract_code_from_sphinx_output(stdout)
    except RuntimeError:
        code = extract_code_from_notebook(notebook_path)

    if temp_dir and not keep_notebooks:
        shutil.rmtree(temp_dir, ignore_errors=True)

    return code


def build_sphinx_env() -> dict:
    if not SPHINX_API_KEY:
        raise RuntimeError(
            "SPHINX_API_KEY is not set. Export it to authenticate without browser login."
        )

    env = os.environ.copy()
    env["SPHINX_API_KEY"] = SPHINX_API_KEY
    return env


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


def load_prices(tickers: list[str]) -> pd.DataFrame:
    console.print(f"  [dim]Downloading prices: {tickers}...[/]")
    raw = yf.download(
        tickers,
        start=START,
        end=END,
        auto_adjust=True,
        progress=False,
        group_by="ticker",
    )
    if isinstance(raw.columns, pd.MultiIndex):
        closes = raw.xs("Close", axis=1, level=1)
    else:
        col = "Close" if "Close" in raw.columns else "close"
        closes = raw[[col]].rename(columns={col: tickers[0]})

    closes.columns = [str(c) for c in closes.columns]
    return closes.sort_index()


# ─────────────────────────────────────────────────────────────
# 3. STRATEGY EXECUTOR
# ─────────────────────────────────────────────────────────────

def execute_strategy(code: str, macro: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    namespace = {"pd": pd, "np": np}

    try:
        exec(code, namespace)
    except Exception as exc:
        raise RuntimeError(f"Code failed to compile:\n{exc}\n\nCode:\n{code}") from exc

    if "generate_signals" not in namespace:
        raise RuntimeError(f"No `generate_signals` function found.\nCode:\n{code}")

    try:
        signals = namespace["generate_signals"](macro.copy(), prices.copy())
    except Exception as exc:
        raise RuntimeError(f"generate_signals() error:\n{exc}\n\nCode:\n{code}") from exc

    if not isinstance(signals, pd.DataFrame):
        raise RuntimeError(f"Must return DataFrame, got {type(signals)}")

    return signals


# ─────────────────────────────────────────────────────────────
# 4. PORTFOLIO SIMULATOR
# ─────────────────────────────────────────────────────────────

def simulate_portfolio(
    signals: pd.DataFrame,
    prices: pd.DataFrame,
    initial_cash: float = 100_000,
) -> tuple[pd.Series, pd.Series]:
    tickers = [ticker for ticker in signals.columns if ticker in prices.columns]
    if not tickers:
        raise RuntimeError(
            f"No overlap between signals {signals.columns.tolist()} "
            f"and prices {prices.columns.tolist()}"
        )

    px = prices[tickers].copy()
    returns = px.pct_change()
    weights = signals[tickers].reindex(returns.index, method="ffill").fillna(0)

    row_sum = weights.abs().sum(axis=1).replace(0, 1)
    weights = weights.div(row_sum, axis=0)
    weights = weights.shift(1).fillna(0)

    port_returns = (weights * returns).sum(axis=1).dropna()
    equity = (1 + port_returns).cumprod() * initial_cash
    return port_returns, equity


# ─────────────────────────────────────────────────────────────
# 5. METRICS
# ─────────────────────────────────────────────────────────────

def compute_metrics(
    port_returns: pd.Series,
    equity: pd.Series,
    prompt: str,
    tickers: list[str],
) -> dict:
    ann = 252
    mean_r = port_returns.mean()
    std_r = port_returns.std()
    sharpe = (mean_r / std_r * np.sqrt(ann)) if std_r > 0 else 0.0

    rolling_max = equity.cummax()
    max_dd = ((equity - rolling_max) / rolling_max).min()

    n_years = len(port_returns) / ann
    total_r = equity.iloc[-1] / equity.iloc[0] - 1
    cagr = (1 + total_r) ** (1 / n_years) - 1 if n_years > 0 else 0.0
    calmar = (cagr / abs(max_dd)) if max_dd != 0 else 0.0

    downside = port_returns[port_returns < 0]
    sortino = (mean_r / downside.std() * np.sqrt(ann)) if len(downside) > 1 else 0.0

    nonzero = port_returns[port_returns != 0]
    win_rate = (nonzero > 0).sum() / len(nonzero) if len(nonzero) > 0 else 0.0

    losing_streak = max_streak = 0
    for value in port_returns:
        if value < 0:
            losing_streak += 1
            max_streak = max(max_streak, losing_streak)
        else:
            losing_streak = 0

    return {
        "prompt": prompt[:58] + ("…" if len(prompt) > 58 else ""),
        "tickers": ", ".join(tickers),
        "sharpe": round(sharpe, 2),
        "sortino": round(sortino, 2),
        "calmar": round(calmar, 2),
        "max_dd": f"{max_dd * 100:.1f}%",
        "cagr": f"{cagr * 100:.1f}%",
        "win_rate": f"{win_rate * 100:.0f}%",
        "max_streak": max_streak,
        "total_return": f"{total_r * 100:.1f}%",
        "end_equity": f"${equity.iloc[-1]:,.0f}",
    }


# ─────────────────────────────────────────────────────────────
# 6. DISPLAY
# ─────────────────────────────────────────────────────────────

def display_table(all_metrics: list[dict]) -> None:
    table = Table(
        title="Backtest Results  [dim](via Sphinx CLI)[/]",
        box=box.ROUNDED,
        show_header=True,
        header_style="bold cyan",
        show_lines=True,
    )
    for name, style, width in [
        ("Prompt", "cyan", 44),
        ("Tickers", "white", 22),
        ("Sharpe", "green", 8),
        ("Sortino", "green", 8),
        ("Calmar", "green", 8),
        ("Max DD", "red", 8),
        ("CAGR", "green", 8),
        ("Win %", "white", 7),
        ("Loss streak", "red", 10),
        ("Total return", "green", 12),
        ("End equity", "green", 13),
    ]:
        table.add_column(name, style=style, min_width=width)

    for metrics in all_metrics:
        score_color = (
            "green"
            if metrics["sharpe"] > 0.5
            else "yellow" if metrics["sharpe"] > 0 else "red"
        )
        table.add_row(
            metrics["prompt"],
            metrics["tickers"],
            f"[{score_color}]{metrics['sharpe']}[/]",
            f"[{score_color}]{metrics['sortino']}[/]",
            f"[{score_color}]{metrics['calmar']}[/]",
            metrics["max_dd"],
            metrics["cagr"],
            metrics["win_rate"],
            str(metrics["max_streak"]),
            metrics["total_return"],
            metrics["end_equity"],
        )

    console.print(table)


def show_generated_code(prompt: str, code: str) -> None:
    console.print(f"\n[bold yellow]── Strategy code for:[/] {prompt[:58]}")
    console.print("[dim]" + "─" * 66 + "[/]")
    for line in code.split("\n"):
        console.print(f"  [dim white]{line}[/]")
    console.print("[dim]" + "─" * 66 + "[/]\n")


# ─────────────────────────────────────────────────────────────
# 7. MAIN
# ─────────────────────────────────────────────────────────────

def run_all(prompts: list[str]) -> None:
    ensure_sphinx_ready()
    all_metrics = []
    macro = load_macro()

    for prompt in prompts:
        console.rule(f"[bold cyan]{prompt[:70]}")
        try:
            code = ask_sphinx_for_strategy(prompt)
            show_generated_code(prompt, code)

            referenced = [ticker for ticker in ALL_ETFS if f'"{ticker}"' in code or f"'{ticker}'" in code]
            if not referenced:
                referenced = ["TLT", "HYG", "GLD", "SPY", "TIP", "SHY"]
                console.print(f"  [yellow]No tickers in code — fallback: {referenced}[/]")

            prices = load_prices(referenced)
            signals = execute_strategy(code, macro, prices)
            console.print(
                f"  [green]✓ Executed.[/] Shape: {signals.shape}  "
                f"Tickers: {signals.columns.tolist()}"
            )

            port_returns, equity = simulate_portfolio(signals, prices)
            metrics = compute_metrics(
                port_returns,
                equity,
                prompt,
                tickers=signals.columns.tolist(),
            )
            all_metrics.append(metrics)

            console.print(
                f"  Sharpe [green]{metrics['sharpe']}[/]  "
                f"Max DD [red]{metrics['max_dd']}[/]  "
                f"CAGR [green]{metrics['cagr']}[/]  "
                f"End equity [green]{metrics['end_equity']}[/]"
            )
        except Exception as exc:
            console.print(f"  [red]Failed: {exc}[/]")
            continue

    console.print()
    if all_metrics:
        display_table(all_metrics)
    else:
        console.print("[red]No results.[/]")


if __name__ == "__main__":
    run_all(PROMPTS)
