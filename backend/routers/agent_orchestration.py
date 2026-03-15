import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
import uuid
from datetime import datetime
from html import unescape
from pathlib import Path
from threading import Lock
from typing import Any
from urllib.parse import quote
from urllib import request as urllib_request
from urllib.error import URLError, HTTPError

import numpy as np
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import PlainTextResponse
from openai import OpenAI
from pydantic import BaseModel, Field


router = APIRouter(prefix="/orchestration", tags=["Orchestration"])
logger = logging.getLogger("uvicorn.error")

load_dotenv(Path(__file__).resolve().parents[1] / ".env")


def _openrouter_key() -> str | None:
    return os.getenv("OPENROUTER_KEY")


def _openrouter_base() -> str:
    return os.getenv("OPENROUTER_BASE", "https://openrouter.ai/api/v1")


def _openrouter_model() -> str:
    return os.getenv("MODEL", "openai/gpt-4o-mini")


def _openrouter_timeout_seconds() -> float:
    raw = os.getenv("OPENROUTER_TIMEOUT_SECONDS", "45")
    try:
        return max(5.0, float(raw))
    except ValueError:
        return 45.0


def _sphinx_api_key() -> str | None:
    return os.getenv("SPHINX_API_KEY")


def _orchestration_backtest_url() -> str:
    return os.getenv("ORCHESTRATION_BACKTEST_URL", "http://127.0.0.1:8000/backtest/sphinx")


def _orchestration_backtest_stream_url() -> str:
    return os.getenv(
        "ORCHESTRATION_BACKTEST_STREAM_URL",
        _orchestration_backtest_url().rstrip("/") + "/stream",
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
    sphinx_api_key = _sphinx_api_key()
    if not sphinx_api_key:
        raise RuntimeError(
            "SPHINX_API_KEY is not set. Export it for non-interactive Sphinx auth."
        )
    env = os.environ.copy()
    env["SPHINX_API_KEY"] = sphinx_api_key
    return env


def _fetch_news_headlines(query: str, limit: int = 4) -> list[str]:
    rss = (
        "https://news.google.com/rss/search"
        f"?q={quote(query)}&hl=en-US&gl=US&ceid=US:en"
    )
    try:
        req = urllib_request.Request(
            rss,
            headers={
                "Accept": "application/xml,text/xml",
                "User-Agent": "AutoAlphaPods-Orchestrator/1.0",
            },
        )
        with urllib_request.urlopen(req, timeout=25) as resp:
            xml = resp.read().decode("utf-8", errors="ignore")
        matches = re.findall(r"<title>(.*?)</title>", xml, flags=re.IGNORECASE | re.DOTALL)
        cleaned = []
        for raw in matches[1:]:  # first title is feed title
            title = unescape(raw.strip())
            if title:
                cleaned.append(re.sub(r"\s+", " ", title))
            if len(cleaned) >= limit:
                break
        return cleaned
    except Exception:
        return []


def _run_sphinx_ideation(
    run_id: str,
    objective: str,
    manager: AgentConfig,
    analyst: AgentConfig,
) -> dict[str, Any]:
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

    temp_dir = tempfile.mkdtemp(prefix="sphinx-ideation-")
    notebook_path = Path(temp_dir) / "notes.ipynb"
    cmd = [
        _resolve_sphinx_cli(),
        "chat",
        "--prompt",
        prompt,
        "--notebook-filepath",
        str(notebook_path),
    ]
    output_lines: list[str] = []
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env=_build_sphinx_env(),
        )
        if process.stdout is not None:
            for raw_line in process.stdout:
                line = raw_line.rstrip()
                output_lines.append(raw_line)
                if line:
                    _append_event(
                        run_id,
                        agent_id=analyst.id,
                        agent_name=analyst.name,
                        stage="sphinx_cli",
                        message=line[:500],
                    )
        return_code = process.wait()
        stdout = "".join(output_lines).strip()
        if return_code == 0 and stdout:
            return {"memo_markdown": stdout, "sphinx_stdout": stdout, "fallback": False}
    except Exception as err:
        logger.exception(
            "Sphinx ideation failed for run_id=%s analyst_id=%s analyst_name=%s",
            run_id,
            analyst.id,
            analyst.name,
        )
        _append_event(
            run_id,
            agent_id=analyst.id,
            agent_name=analyst.name,
            stage="sphinx_cli",
            message=f"Sphinx ideation error: {err}",
        )
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

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
    return {"memo_markdown": fallback, "sphinx_stdout": "".join(output_lines), "fallback": True}


def _analyst_self_critique(run_id: str, analyst: AgentConfig, memo_markdown: str) -> str:
    system_prompt = f"""
You are {analyst.name}, critiquing your own strategy before manager review.
Return 3 concise bullets:
1) strongest edge
2) main fragility
3) one concrete revision
""".strip()
    output = _run_openrouter_chat(
        system_prompt,
        memo_markdown,
        run_id=run_id,
        agent_id=analyst.id,
        agent_name=analyst.name,
        stage="critique",
        task_label="self-critique",
    )
    if output:
        return output
    return (
        "- Edge: regime-aware signal captures macro transitions.\n"
        "- Fragility: false positives in noisy periods.\n"
        "- Revision: add explicit downside filter and exposure cap."
    )


def _manager_critique_round(
    run_id: str,
    manager: AgentConfig,
    analyst: AgentConfig,
    memo_markdown: str,
) -> str:
    system_prompt = f"""
You are {manager.name}, conducting an adversarial critique.
Give 3 concise bullets:
1) what evidence is weak
2) what could be overfit
3) what test requirement must be added
""".strip()
    output = _run_openrouter_chat(
        system_prompt,
        f"Analyst={analyst.name}\n\nMemo:\n{memo_markdown}",
        run_id=run_id,
        agent_id=manager.id,
        agent_name=manager.name,
        stage="critique",
        task_label=f"manager critique for {analyst.name}",
    )
    if output:
        return output
    return (
        "- Weak evidence: dependence on single-regime examples.\n"
        "- Overfit risk: too many conditional thresholds.\n"
        "- Requirement: include robustness across multiple market regimes."
    )


def _run_openrouter_chat(
    system_prompt: str,
    user_prompt: str,
    *,
    run_id: str | None = None,
    agent_id: str | None = None,
    agent_name: str | None = None,
    stage: str = "review",
    task_label: str = "OpenRouter request",
) -> str:
    openrouter_key = _openrouter_key()
    if not openrouter_key:
        return ""
    timeout_seconds = _openrouter_timeout_seconds()
    client = OpenAI(
        api_key=openrouter_key,
        base_url=_openrouter_base(),
        timeout=timeout_seconds,
        max_retries=1,
        default_headers={
            "HTTP-Referer": "https://autoalphapods.local",
            "X-Title": "AutoAlphaPods Orchestrator",
        },
    )
    try:
        completion = client.chat.completions.create(
            model=_openrouter_model(),
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=900,
            temperature=0.2,
            timeout=timeout_seconds,
        )
    except Exception as err:
        logger.warning(
            "OpenRouter %s failed for run_id=%s agent_id=%s agent_name=%s: %s",
            task_label,
            run_id,
            agent_id,
            agent_name,
            err,
        )
        if run_id and agent_id and agent_name:
            _append_event(
                run_id,
                agent_id=agent_id,
                agent_name=agent_name,
                stage=stage,
                message=(
                    f"{task_label} failed after ~{int(timeout_seconds)}s "
                    f"({type(err).__name__}). Using fallback."
                )[:500],
            )
        return ""

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
    run_id: str,
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
    if not _openrouter_key():
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

    raw = _run_openrouter_chat(
        system_prompt,
        user_prompt,
        run_id=run_id,
        agent_id=manager.id,
        agent_name=manager.name,
        stage="review",
        task_label=f"manager review for {analyst.name}",
    )
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
        _orchestration_backtest_url(),
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


def _call_backtest_api_streaming(
    run_id: str,
    manager: AgentConfig,
    analyst: AgentConfig,
    prompt: str,
    start: str,
    end: str,
    initial_cash: float,
) -> dict[str, Any]:
    payload = {
        "prompt": prompt,
        "start": start,
        "end": end,
        "initial_cash": initial_cash,
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib_request.Request(
        _orchestration_backtest_stream_url(),
        data=data,
        headers={
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
            "Cache-Control": "no-cache",
        },
        method="POST",
    )

    last_event = "message"
    result_payload: dict[str, Any] | None = None
    event_data_lines: list[str] = []

    try:
        with urllib_request.urlopen(req, timeout=900) as resp:
            for raw_line in resp:
                line = raw_line.decode("utf-8", errors="ignore").rstrip("\n")
                if line.startswith("event:"):
                    last_event = line.split(":", 1)[1].strip() or "message"
                    continue
                if line.startswith("data:"):
                    event_data_lines.append(line.split(":", 1)[1].strip())
                    continue

                # SSE event separator (blank line)
                if line.strip() == "":
                    if not event_data_lines:
                        continue
                    data_blob = "\n".join(event_data_lines)
                    event_data_lines = []
                    try:
                        payload_obj = json.loads(data_blob)
                    except Exception:
                        payload_obj = {"raw": data_blob}

                    if last_event == "log":
                        msg = str(payload_obj.get("message", "")).strip()
                        stage = str(payload_obj.get("stage", "backtest")).strip() or "backtest"
                        if msg:
                            _append_event(
                                run_id,
                                agent_id=manager.id,
                                agent_name=manager.name,
                                stage=stage if stage != "generate_strategy" else "sphinx_cli",
                                message=f"[{analyst.name}] {msg}"[:700],
                            )
                    elif last_event == "result":
                        if isinstance(payload_obj, dict):
                            result_payload = payload_obj
                    elif last_event == "error":
                        message = str(payload_obj.get("message", payload_obj))
                        raise RuntimeError(f"Backtest stream error: {message}")

                    last_event = "message"

            if event_data_lines:
                data_blob = "\n".join(event_data_lines)
                try:
                    payload_obj = json.loads(data_blob)
                except Exception:
                    payload_obj = {"raw": data_blob}
                if last_event == "result" and isinstance(payload_obj, dict):
                    result_payload = payload_obj

        if result_payload is None:
            raise RuntimeError("Backtest stream ended without a result payload.")
        return result_payload
    except Exception:
        # Fallback to non-stream endpoint to preserve functionality if stream parsing fails.
        logger.exception(
            "Backtest stream failed for run_id=%s analyst_id=%s analyst_name=%s; falling back to non-stream endpoint.",
            run_id,
            analyst.id,
            analyst.name,
        )
        _append_event(
            run_id,
            agent_id=manager.id,
            agent_name=manager.name,
            stage="backtest",
            message="Backtest stream unavailable; falling back to non-stream endpoint.",
        )
        return _call_backtest_api(prompt, start, end, initial_cash)


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
        headlines_lines = [
            f"- {headline}" for headline in item.get("headlines", [])[:6]
        ] or ["- No fetched headlines recorded."]
        lines.extend(
            [
                "",
                f"### {analyst['name']} ({analyst.get('specialization') or 'Generalist'})",
                f"- **Approved by manager:** {'Yes' if review.get('approve') else 'No'}",
                f"- **Manager notes:** {review.get('manager_notes', '')}",
                f"- **Risk flags:** {', '.join(review.get('risk_flags', [])) or 'None'}",
                "",
                "#### News Context",
                *headlines_lines,
                "",
                "#### Analyst Self-Critique",
                item.get("self_critique", "_No self-critique captured_"),
                "",
                "#### Manager Critique Round",
                item.get("manager_critique", "_No manager critique captured_"),
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
        _append_event(
            run_id,
            agent_id=manager.id,
            agent_name=manager.name,
            stage="manager",
            message=(
                "SPHINX auth mode: API key"
                if _sphinx_api_key()
                else "SPHINX auth mode: missing API key (fallbacks may apply)"
            ),
        )

        for analyst in analysts:
            _append_event(
                run_id,
                agent_id=analyst.id,
                agent_name=analyst.name,
                stage="ideation",
                message="Reading macro/news context and drafting cited thesis with SPHINX...",
            )
            time.sleep(0.6)

            news_query = (
                f"{analyst.specialization} market strategy "
                + (" ".join(analyst.assets[:3]) if analyst.assets else "")
            ).strip()
            headlines = _fetch_news_headlines(news_query, limit=4)
            if headlines:
                _append_event(
                    run_id,
                    agent_id=analyst.id,
                    agent_name=analyst.name,
                    stage="news",
                    message=f"Pulled {len(headlines)} headlines for context.",
                )
                for i, headline in enumerate(headlines, start=1):
                    _append_event(
                        run_id,
                        agent_id=analyst.id,
                        agent_name=analyst.name,
                        stage="news",
                        message=f"[{i}] {headline}",
                    )
                    time.sleep(0.25)
            else:
                _append_event(
                    run_id,
                    agent_id=analyst.id,
                    agent_name=analyst.name,
                    stage="news",
                    message="No fresh headlines retrieved; proceeding with macro priors and market data.",
                )

            time.sleep(0.45)
            ideation = _run_sphinx_ideation(run_id, objective, manager, analyst)
            memo = str(ideation.get("memo_markdown", "")).strip()
            if ideation.get("fallback"):
                _append_event(
                    run_id,
                    agent_id=analyst.id,
                    agent_name=analyst.name,
                    stage="sphinx_cli",
                    message="Sphinx ideation returned non-zero; using deterministic fallback memo.",
                )
            time.sleep(0.4)

            _append_event(
                run_id,
                agent_id=analyst.id,
                agent_name=analyst.name,
                stage="critique",
                message="Running self-critique pass before manager review...",
            )
            self_critique = _analyst_self_critique(run_id, analyst, memo)
            for line in [ln.strip() for ln in self_critique.splitlines() if ln.strip()][:4]:
                _append_event(
                    run_id,
                    agent_id=analyst.id,
                    agent_name=analyst.name,
                    stage="critique",
                    message=line[:500],
                )
                time.sleep(0.2)

            _append_event(
                run_id,
                agent_id=manager.id,
                agent_name=manager.name,
                stage="critique",
                message=f"Manager challenge round for {analyst.name}...",
            )
            manager_critique = _manager_critique_round(run_id, manager, analyst, memo)
            for line in [ln.strip() for ln in manager_critique.splitlines() if ln.strip()][:4]:
                _append_event(
                    run_id,
                    agent_id=manager.id,
                    agent_name=manager.name,
                    stage="critique",
                    message=line[:500],
                )
                time.sleep(0.2)

            _append_event(
                run_id,
                agent_id=analyst.id,
                agent_name=analyst.name,
                stage="revision",
                message="Analyst revised rationale and backtest prompt after critique rounds.",
            )
            time.sleep(0.35)
            _append_event(
                run_id,
                agent_id=analyst.id,
                agent_name=analyst.name,
                stage="proposal",
                message="Submitted strategy memo and backtest-ready prompt to manager.",
            )

            review = _manager_review(run_id, manager, analyst, memo)
            time.sleep(0.4)
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
                "headlines": headlines,
                "self_critique": self_critique,
                "manager_critique": manager_critique,
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
                _append_event(
                    run_id,
                    agent_id=manager.id,
                    agent_name=manager.name,
                    stage="backtest",
                    message=f"Backtest window={start} to {end}, initial_cash=${initial_cash:,.0f}",
                )
                time.sleep(0.7)
                try:
                    backtest = _call_backtest_api_streaming(
                        run_id=run_id,
                        manager=manager,
                        analyst=analyst,
                        prompt=test_prompt,
                        start=start,
                        end=end,
                        initial_cash=initial_cash,
                    )
                    _append_event(
                        run_id,
                        agent_id=manager.id,
                        agent_name=manager.name,
                        stage="backtest",
                        message="Backtest completed. Computing regression diagnostics vs SPY...",
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
        logger.exception("Orchestration job failed for run_id=%s", run_id)
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
