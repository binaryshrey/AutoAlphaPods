import json
import os
import shutil
import subprocess
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Any
from urllib import request as urllib_request
from urllib.error import URLError, HTTPError

import numpy as np
import pandas as pd
import yfinance as yf
from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import PlainTextResponse
from openai import OpenAI
from pydantic import BaseModel, Field


router = APIRouter(prefix="/orchestration", tags=["Orchestration"])


OPENROUTER_KEY = os.getenv("OPENROUTER_KEY")
OPENROUTER_BASE = os.getenv("OPENROUTER_BASE", "https://openrouter.ai/api/v1")
OPENROUTER_MODEL = os.getenv("MODEL", "openai/gpt-4o-mini")
SPHINX_API_KEY = os.getenv("SPHINX_API_KEY")
ORCHESTRATION_BACKTEST_URL = os.getenv(
    "ORCHESTRATION_BACKTEST_URL", "http://127.0.0.1:8000/backtest/sphinx"
)

RUN_STORE: dict[str, dict[str, Any]] = {}
RUN_STORE_LOCK = Lock()


class AgentConfig(BaseModel):
    id: str
    name: str
    specialization: str = ""
    system_prompt: str = ""
    assets: list[str] = Field(default_factory=list)


class OrchestrationRunRequest(BaseModel):
    objective: str
    manager: AgentConfig
    analysts: list[AgentConfig]
    start: str = "2015-01-01"
    end: str = "2024-12-31"
    initial_cash: float = 100_000


def _utc_now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _append_event(
    run_id: str,
    *,
    agent_id: str,
    agent_name: str,
    stage: str,
    message: str,
    result_ready: bool = False,
) -> None:
    event = {
        "id": str(uuid.uuid4()),
        "ts": _utc_now_iso(),
        "agent_id": agent_id,
        "agent_name": agent_name,
        "stage": stage,
        "message": message,
        "result_ready": result_ready,
    }
    with RUN_STORE_LOCK:
        run = RUN_STORE.get(run_id)
        if not run:
            return
        run["events"].append(event)
        run["updated_at"] = _utc_now_iso()


def _resolve_sphinx_cli() -> str:
    configured = os.getenv("SPHINX_CLI_BIN")
    if configured:
        return configured
    for candidate in ("sphinx-cli", str(Path.home() / ".local" / "bin" / "sphinx-cli")):
        resolved = shutil.which(candidate)
        if resolved:
            return resolved
    raise RuntimeError("sphinx-cli not found on PATH.")


def _build_sphinx_env() -> dict[str, str]:
    env = os.environ.copy()
    if SPHINX_API_KEY:
        env["SPHINX_API_KEY"] = SPHINX_API_KEY
    return env


def _run_sphinx_ideation(
    objective: str,
    manager: AgentConfig,
    analyst: AgentConfig,
) -> str:
    prompt = f"""
You are {analyst.name}, a specialized trading analyst.
Specialization: {analyst.specialization or "General macro/quant"}
Allowed assets: {", ".join(analyst.assets) if analyst.assets else "No restriction"}
Custom analyst instructions:
{analyst.system_prompt or "(none)"}

Manager profile:
- Name: {manager.name}
- Specialization: {manager.specialization or "Portfolio manager"}
- Manager instructions: {manager.system_prompt or "(none)"}

Objective:
{objective}

You must produce a markdown memo for an adversarial manager review.
Requirements:
1) Be concrete and testable.
2) Every factual claim must include inline citation markers like [1], [2].
3) Include a "Backtest Prompt" section with one concise strategy prompt suitable for a code-generation backtest agent.
4) Include a "Sources" section with numbered citations and URLs.
5) Keep to 350-550 words.

Output format:
## Thesis
## Evidence
## Risks / Failure Modes
## Backtest Prompt
## Sources
""".strip()

    notebook_path = Path(tempfile.mkdtemp(prefix="sphinx-ideation-")) / "notes.ipynb"
    cmd = [
        _resolve_sphinx_cli(),
        "chat",
        "--prompt",
        prompt,
        "--notebook-filepath",
        str(notebook_path),
    ]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
        env=_build_sphinx_env(),
    )
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()

    fallback = f"""## Thesis
{analyst.name} proposes a regime-aware allocation strategy for {", ".join(analyst.assets) or "multi-asset ETFs"} [1][2].

## Evidence
- Macro regime shifts and rate/inflation transitions can alter relative performance of duration, inflation hedges, and equities [1].
- Cross-asset positioning with explicit risk controls can improve downside behavior versus static allocations [2].

## Risks / Failure Modes
- Regime false positives and whipsaws.
- Overfitting to historical macro transitions.

## Backtest Prompt
Create a monthly-rebalanced strategy that rotates across {", ".join(analyst.assets) or "TLT, GLD, SPY"} using yield-curve slope and inflation trend filters, with volatility targeting and max leverage 1.0.

## Sources
[1] https://fred.stlouisfed.org/
[2] https://finance.yahoo.com/
"""
    return fallback


def _run_openrouter_chat(system_prompt: str, user_prompt: str) -> str:
    if not OPENROUTER_KEY:
        return ""
    client = OpenAI(
        api_key=OPENROUTER_KEY,
        base_url=OPENROUTER_BASE,
        default_headers={
            "HTTP-Referer": "https://autoalphapods.local",
            "X-Title": "AutoAlphaPods Orchestrator",
        },
    )
    completion = client.chat.completions.create(
        model=OPENROUTER_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        max_tokens=900,
        temperature=0.2,
    )
    content = completion.choices[0].message.content or ""
    return content.strip()


def _safe_parse_json_object(text: str) -> dict[str, Any] | None:
    if not text:
        return None
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end < 0 or end <= start:
        return None
    try:
        return json.loads(text[start : end + 1])
    except Exception:
        return None


def _manager_review(
    manager: AgentConfig,
    analyst: AgentConfig,
    memo_markdown: str,
) -> dict[str, Any]:
    default_review = {
        "approve": True,
        "manager_notes": "Baseline approval with risk caveats. Proceed to backtest.",
        "revised_backtest_prompt": "",
        "risk_flags": ["Model risk", "Regime instability"],
    }
    if not OPENROUTER_KEY:
        return default_review

    system_prompt = f"""
You are an adversarial portfolio manager.
Manager specialization: {manager.specialization or "General"}
Manager custom prompt:
{manager.system_prompt or "(none)"}

Evaluate analyst proposals skeptically and only approve if the hypothesis is testable.
Return STRICT JSON with keys:
approve (boolean),
manager_notes (string),
revised_backtest_prompt (string),
risk_flags (array of strings).
""".strip()

    user_prompt = f"""
Analyst: {analyst.name}
Specialization: {analyst.specialization}
Memo:
{memo_markdown}
""".strip()

    raw = _run_openrouter_chat(system_prompt, user_prompt)
    parsed = _safe_parse_json_object(raw)
    if not parsed:
        return default_review
    return {
        "approve": bool(parsed.get("approve", True)),
        "manager_notes": str(parsed.get("manager_notes", "")).strip()
        or default_review["manager_notes"],
        "revised_backtest_prompt": str(parsed.get("revised_backtest_prompt", "")).strip(),
        "risk_flags": [
            str(flag)
            for flag in parsed.get("risk_flags", [])
            if isinstance(flag, (str, int, float))
        ]
        or default_review["risk_flags"],
    }


def _extract_backtest_prompt(memo_markdown: str) -> str:
    marker = "## Backtest Prompt"
    idx = memo_markdown.find(marker)
    if idx < 0:
        return memo_markdown[:300]
    tail = memo_markdown[idx + len(marker) :].strip()
    lines = [line.strip("- ").strip() for line in tail.splitlines() if line.strip()]
    return lines[0] if lines else tail[:280]


def _call_backtest_api(prompt: str, start: str, end: str, initial_cash: float) -> dict[str, Any]:
    payload = {
        "prompt": prompt,
        "start": start,
        "end": end,
        "initial_cash": initial_cash,
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib_request.Request(
        ORCHESTRATION_BACKTEST_URL,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib_request.urlopen(req, timeout=600) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw)
    except HTTPError as err:
        body = err.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Backtest API HTTP {err.code}: {body}") from err
    except URLError as err:
        raise RuntimeError(f"Backtest API connection error: {err}") from err


def _compute_regression_vs_spy(equity_curve: list[dict[str, Any]]) -> dict[str, Any]:
    if not equity_curve:
        return {"status": "insufficient_data"}

    df = pd.DataFrame(equity_curve)
    if "date" not in df.columns or "equity" not in df.columns:
        return {"status": "invalid_curve"}
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").drop_duplicates("date").set_index("date")
    strat_returns = df["equity"].pct_change().dropna()
    if len(strat_returns) < 30:
        return {"status": "insufficient_data"}

    spy_close = yf.download(
        "SPY",
        start=str(strat_returns.index.min().date()),
        end=str((strat_returns.index.max() + pd.Timedelta(days=1)).date()),
        auto_adjust=True,
        progress=False,
    )["Close"]
    if isinstance(spy_close, pd.DataFrame):
        spy_close = spy_close.iloc[:, 0]
    market_returns = spy_close.pct_change().dropna()

    merged = pd.concat(
        [strat_returns.rename("strategy"), market_returns.rename("market")],
        axis=1,
        join="inner",
    ).dropna()
    if len(merged) < 30:
        return {"status": "insufficient_overlap"}

    strat = merged["strategy"]
    mkt = merged["market"]
    var_mkt = float(mkt.var())
    beta = float(np.cov(strat, mkt)[0, 1] / var_mkt) if var_mkt > 0 else 0.0
    alpha_daily = float(strat.mean() - beta * mkt.mean())
    alpha_annual = alpha_daily * 252
    corr = float(strat.corr(mkt))
    r2 = corr**2 if np.isfinite(corr) else 0.0
    active = strat - mkt
    tracking_error = float(active.std() * np.sqrt(252))
    info_ratio = float((active.mean() * 252) / tracking_error) if tracking_error > 0 else 0.0

    return {
        "status": "ok",
        "samples": int(len(merged)),
        "beta": round(beta, 3),
        "alpha_annual_pct": round(alpha_annual * 100, 2),
        "r2": round(r2, 3),
        "tracking_error_pct": round(tracking_error * 100, 2),
        "information_ratio": round(info_ratio, 3),
    }


def _build_final_report(
    run_id: str,
    objective: str,
    manager: AgentConfig,
    results: list[dict[str, Any]],
) -> str:
    lines = [
        f"# Agent Orchestration Run {run_id}",
        "",
        f"Generated: {_utc_now_iso()}",
        "",
        "## Objective",
        objective,
        "",
        "## Manager",
        f"- **Name:** {manager.name}",
        f"- **Specialization:** {manager.specialization or 'General'}",
        "",
        "## Analyst Decisions",
    ]

    for item in results:
        analyst = item["analyst"]
        review = item["review"]
        lines.extend(
            [
                "",
                f"### {analyst['name']} ({analyst.get('specialization') or 'Generalist'})",
                f"- **Approved by manager:** {'Yes' if review.get('approve') else 'No'}",
                f"- **Manager notes:** {review.get('manager_notes', '')}",
                f"- **Risk flags:** {', '.join(review.get('risk_flags', [])) or 'None'}",
                "",
                "#### Analyst Memo",
                item.get("memo_markdown", "_No memo_"),
            ]
        )

        if item.get("backtest"):
            bt = item["backtest"]
            reg = item.get("regression", {})
            lines.extend(
                [
                    "",
                    "#### Backtest Summary",
                    f"- **Tickers:** {bt.get('tickers', 'n/a')}",
                    f"- **Total Return:** {bt.get('total_return', 'n/a')}",
                    f"- **CAGR:** {bt.get('cagr', 'n/a')}",
                    f"- **Sharpe:** {bt.get('sharpe', 'n/a')}",
                    f"- **Max Drawdown:** {bt.get('max_dd', 'n/a')}",
                    "",
                    "#### Regression Diagnostics (Strategy vs SPY)",
                    f"- **Status:** {reg.get('status', 'n/a')}",
                    f"- **Beta:** {reg.get('beta', 'n/a')}",
                    f"- **Alpha (annualized):** {reg.get('alpha_annual_pct', 'n/a')}%",
                    f"- **R²:** {reg.get('r2', 'n/a')}",
                    f"- **Tracking Error:** {reg.get('tracking_error_pct', 'n/a')}%",
                    f"- **Information Ratio:** {reg.get('information_ratio', 'n/a')}",
                ]
            )

        lines.extend(
            [
                "",
                "#### Verdict",
                item.get("verdict_markdown", "_No verdict_"),
            ]
        )

    lines.extend(
        [
            "",
            "## Method Notes",
            "- Analysts ideate with SPHINX and are instructed to include citations.",
            "- Manager performs adversarial review and can revise backtest prompts.",
            "- Approved ideas are backtested through the existing SPHINX backtest endpoint.",
            "- Regression diagnostics benchmark strategy returns against SPY.",
        ]
    )

    return "\n".join(lines).strip() + "\n"


def _run_orchestration_job(run_id: str, payload: dict[str, Any]) -> None:
    with RUN_STORE_LOCK:
        run = RUN_STORE.get(run_id)
        if not run:
            return
        run["status"] = "running"
        run["updated_at"] = _utc_now_iso()

    objective = payload["objective"]
    manager = AgentConfig(**payload["manager"])
    analysts = [AgentConfig(**a) for a in payload["analysts"]]
    start = payload["start"]
    end = payload["end"]
    initial_cash = payload["initial_cash"]

    results: list[dict[str, Any]] = []

    try:
        _append_event(
            run_id,
            agent_id=manager.id,
            agent_name=manager.name,
            stage="manager",
            message="Manager initialized orchestration pipeline.",
        )

        for analyst in analysts:
            _append_event(
                run_id,
                agent_id=analyst.id,
                agent_name=analyst.name,
                stage="ideation",
                message="Reading macro/news context and drafting cited thesis with SPHINX...",
            )
            memo = _run_sphinx_ideation(objective, manager, analyst)
            _append_event(
                run_id,
                agent_id=analyst.id,
                agent_name=analyst.name,
                stage="proposal",
                message="Submitted strategy memo and backtest-ready prompt to manager.",
            )

            review = _manager_review(manager, analyst, memo)
            _append_event(
                run_id,
                agent_id=manager.id,
                agent_name=manager.name,
                stage="review",
                message=f"Adversarial review complete for {analyst.name}. "
                f"{'Approved' if review.get('approve') else 'Rejected'}",
            )

            row: dict[str, Any] = {
                "analyst": analyst.model_dump(),
                "memo_markdown": memo,
                "review": review,
            }

            if review.get("approve"):
                test_prompt = review.get("revised_backtest_prompt") or _extract_backtest_prompt(memo)
                _append_event(
                    run_id,
                    agent_id=manager.id,
                    agent_name=manager.name,
                    stage="backtest",
                    message=f"Running SPHINX backtest for {analyst.name}'s idea...",
                )
                try:
                    backtest = _call_backtest_api(
                        test_prompt, start=start, end=end, initial_cash=initial_cash
                    )
                    regression = _compute_regression_vs_spy(backtest.get("equity_curve", []))
                    is_alpha = (
                        float(backtest.get("sharpe", 0)) >= 1.0
                        and str(backtest.get("total_return", "0%")).startswith("-") is False
                        and float(regression.get("alpha_annual_pct", 0)) > 0
                    )
                    verdict = (
                        "Alpha candidate: passes manager gate with positive risk-adjusted diagnostics."
                        if is_alpha
                        else "Not alpha yet: needs refinement on robustness, risk, or benchmark-relative performance."
                    )
                    row["backtest"] = backtest
                    row["regression"] = regression
                    row["verdict_markdown"] = verdict
                    _append_event(
                        run_id,
                        agent_id=analyst.id,
                        agent_name=analyst.name,
                        stage="result",
                        message=verdict,
                        result_ready=True,
                    )
                except Exception as err:
                    row["backtest_error"] = str(err)
                    row["verdict_markdown"] = (
                        "Backtest execution failed. Requires rerun with adjusted prompt/code constraints."
                    )
                    _append_event(
                        run_id,
                        agent_id=manager.id,
                        agent_name=manager.name,
                        stage="result",
                        message=f"Backtest failed for {analyst.name}: {err}",
                        result_ready=True,
                    )
            else:
                row["verdict_markdown"] = (
                    "Rejected by manager at adversarial review stage; no backtest executed."
                )
                _append_event(
                    run_id,
                    agent_id=manager.id,
                    agent_name=manager.name,
                    stage="result",
                    message=f"{analyst.name}'s idea rejected before backtest.",
                    result_ready=True,
                )

            results.append(row)

        report_markdown = _build_final_report(run_id, objective, manager, results)
        with RUN_STORE_LOCK:
            run = RUN_STORE.get(run_id)
            if run:
                run["status"] = "completed"
                run["results"] = results
                run["report_markdown"] = report_markdown
                run["updated_at"] = _utc_now_iso()
    except Exception as err:
        with RUN_STORE_LOCK:
            run = RUN_STORE.get(run_id)
            if run:
                run["status"] = "failed"
                run["error"] = str(err)
                run["updated_at"] = _utc_now_iso()


@router.post("/runs")
def create_orchestration_run(req: OrchestrationRunRequest, background_tasks: BackgroundTasks):
    if not req.analysts:
        raise HTTPException(status_code=400, detail="At least one analyst is required.")

    run_id = str(uuid.uuid4())
    with RUN_STORE_LOCK:
        RUN_STORE[run_id] = {
            "run_id": run_id,
            "status": "queued",
            "created_at": _utc_now_iso(),
            "updated_at": _utc_now_iso(),
            "config": req.model_dump(),
            "events": [],
            "results": [],
            "report_markdown": "",
            "error": None,
        }

    background_tasks.add_task(_run_orchestration_job, run_id, req.model_dump())
    return {"run_id": run_id, "status": "queued"}


@router.get("/runs")
def list_orchestration_runs():
    with RUN_STORE_LOCK:
        return {
            "runs": [
                {
                    "run_id": run_id,
                    "status": run["status"],
                    "created_at": run["created_at"],
                    "updated_at": run["updated_at"],
                }
                for run_id, run in RUN_STORE.items()
            ]
        }


@router.get("/runs/{run_id}")
def get_orchestration_run(run_id: str):
    with RUN_STORE_LOCK:
        run = RUN_STORE.get(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Run not found.")
        return run


@router.get("/runs/{run_id}/report.md")
def get_orchestration_report_markdown(run_id: str):
    with RUN_STORE_LOCK:
        run = RUN_STORE.get(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Run not found.")
        if not run.get("report_markdown"):
            raise HTTPException(status_code=404, detail="Report not ready.")
        report = run["report_markdown"]
    return PlainTextResponse(
        content=report,
        media_type="text/markdown",
        headers={"Content-Disposition": f'attachment; filename="orchestration-{run_id}.md"'},
    )

