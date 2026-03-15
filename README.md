# AutoAlphaPods

![Next.js](https://img.shields.io/badge/Next.js-black?logo=nextdotjs&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-Backend-009688?logo=fastapi&logoColor=white)
![Sphinx%20AI](https://img.shields.io/badge/Sphinx%20AI-Orchestration-7C3AED)
![Alpaca](https://img.shields.io/badge/Trading-Alpaca-FACC15)
[![Supabase](https://img.shields.io/badge/Supabase-3ECF8E?logo=supabase&logoColor=white)](https://supabase.com/)
[![Render](https://img.shields.io/badge/Render-000000?logo=render&logoColor=white)](https://render.com/)
[![Portals](https://img.shields.io/badge/Portals-Product%20Link-111827)](https://www.makeportals.com/)

![Banner](https://raw.githubusercontent.com/binaryshrey/AutoAlphaPods/refs/heads/main/auto-alpha-pods/public/banner.png)

Auto Alpha Pods is a scenario-driven AI agents workflow that builds the next generation of alpha. Autonomous AI trading agents propose, discuss, and execute trading strategies—turning market chaos into predictable alpha.

## Introducing Auto Alpha Pods

Auto Alpha Pods unifies market data, strategy ideation, backtesting, and execution in one agentic workflow.

Instead of a single assistant, it uses coordinated agents (analysts + manager) that:

- interpret macro and market context,
- debate scenarios,
- test hypotheses through backtests,
- and execute validated actions through broker integrations.

The platform is built to support a fast iteration loop:

`Idea → Debate → Backtest → Refine → Execute`

---

## Built With

### Frontend

- Next.js (App Router)
- React + TypeScript
- Tailwind CSS
- Charting: ApexCharts / Chart.js

### Backend & AI

- FastAPI
- OpenRouter-compatible LLM integration
- Sphinx AI orchestration support
- Python analytics stack (pandas, numpy, yfinance)

### Data & Trading

- SupabaseDB (market + macro data)
- yfinance (market data feeds)
- Alpaca trading integration (positions + orders)

---

## Product Routes

### `/`

![home](https://raw.githubusercontent.com/binaryshrey/AutoAlphaPods/refs/heads/main/auto-alpha-pods/public/home.png)

Auto Alpha Pods is presented as a scenario-driven AI agents workflow that builds next-generation alpha. The home page frames the system as an autonomous research-to-execution engine.

### `/dashboard`

![dashboard](https://raw.githubusercontent.com/binaryshrey/AutoAlphaPods/refs/heads/main/auto-alpha-pods/public/dashboard.png)

The dashboard tracks four asset classes:

- Commodities
- Crypto
- Equity
- Fixed Income

Each class provides:

- Market overview
- Top movers
- News coverage (Google RSS-based feeds)

### `/chat`

The chat interface is the operator console for strategy and execution prompts.

You can ask the agent to:

- test backtesting logic (e.g. buy SPY when the curve is steep, go cash when inverted),
  ![chat1](https://raw.githubusercontent.com/binaryshrey/AutoAlphaPods/refs/heads/main/auto-alpha-pods/public/chat1.png)
- fetch current positions (e.g. NVDA),
  ![chat2](https://raw.githubusercontent.com/binaryshrey/AutoAlphaPods/refs/heads/main/auto-alpha-pods/public/chat2.png)

- and place trade orders through a connected Alpaca account.
  ![chat3](https://raw.githubusercontent.com/binaryshrey/AutoAlphaPods/refs/heads/main/auto-alpha-pods/public/chat3.png)

### `/orchestration`

![orch](https://raw.githubusercontent.com/binaryshrey/AutoAlphaPods/refs/heads/main/auto-alpha-pods/public/orch.png)
The orchestration workspace lets you configure manager and analyst agents and run adversarial strategy sessions powered by Sphinx AI.

Agents ideate, challenge assumptions, and validate strategies before execution.
![opixagentsh](https://raw.githubusercontent.com/binaryshrey/AutoAlphaPods/refs/heads/main/auto-alpha-pods/public/pixagents.png)

---

## Key Features

- **Scenario-driven multi-agent alpha workflow**
- **Cross-asset market dashboard** (commodities, crypto, equity, fixed income)
- **Prompt-based strategy backtesting workflows**
- **Agent-assisted trade operations via Alpaca**
- **Adversarial agent orchestration with Sphinx AI**
- **Macro + market data integration** (Supabase + yfinance)

---

## Core Workflow

1. **Define objective** in chat or orchestration.
2. **Agents propose and debate** strategy variants.
3. **Backtest strategy logic** with market/macro context.
4. **Inspect metrics and narrative output** (risk, return, behavior).
5. **Refine and re-run** until conviction threshold is met.
6. **Execute trade actions** through Alpaca where applicable.

---

## API Overview (High-Level)

### Frontend API Routes (`auto-alpha-pods/app/api/*`)

- `/api/chat`
- `/api/market-overview`
- `/api/news`
- `/api/sentiment`
- `/api/commodity-news`
- `/api/alpaca/portfolio`
- `/api/alpaca/order`

### Backend Endpoints (`backend/main.py` + routers)

- Market data: `GET /yfinance/equity`, `GET /yfinance/commodities`
- Config/data snapshots: `GET /etfs`, `GET /macro/snapshot`, `GET /commodities/snapshot`
- Backtesting: `POST /backtest`, `POST /backtest/sphinx`, stream and batch variants
- Orchestration: `/orchestration` lifecycle endpoints
- Dashboards: `/dashboard?asset=fixed-income`, `dashboard?asset=commodity`, `dashboard?asset=equity`, `dashboard?asset=crypto`

## Development

### 1) Prerequisites

- Node.js 20+
- Python 3.11+
- npm (or pnpm/yarn)

### 2) Frontend setup

```bash
cd auto-alpha-pods
npm install
npm run dev
```

### 3) Backend setup

```bash
cd backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload
```

## Project Layout

```text
AutoAlphaPods/
├── auto-alpha-pods/   # Next.js frontend
├── backend/           # FastAPI + backtesting + orchestration
└── README.md
```
