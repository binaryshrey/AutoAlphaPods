# Backtesting Engine Structure

This package is organized around the trust boundaries of an institutional backtester:

- `data/`: point-in-time ingestion, revision handling, universes, corporate actions
- `core/`: event clock, frozen world state, run orchestration
- `execution/`: fill models, transaction costs, borrow, halts
- `portfolio/`: positions, cash ledger, rebalancing state
- `risk/`: exposures, VaR, drawdown, constraint checks
- `analytics/`: attribution, metrics, regime slicing, robustness analysis
- `validation/`: walk-forward, train/test partitioning, overfit diagnostics
- `reporting/`: tearsheet payloads and renderers
- `strategies/`: sandboxed strategy implementations and adapters
- `storage/`: immutable run metadata, artifacts, reproducibility records
- `config/`: engine configuration, schemas, and environment-specific settings
- `tests/`: fixtures plus unit and integration coverage

Suggested implementation order:

1. `data/`
2. `core/`
3. `execution/`
4. `analytics/` and `risk/`
5. `validation/`
6. `reporting/`

Current high-level tree:

```text
backend/
├── main.py
├── requirements.txt
└── backtesting/
    ├── analytics/
    ├── config/
    ├── core/
    ├── data/
    │   ├── corporate_actions/
    │   ├── ingestion/
    │   ├── pit/
    │   └── universe/
    ├── execution/
    ├── portfolio/
    ├── reporting/
    ├── risk/
    ├── storage/
    ├── strategies/
    ├── tests/
    │   ├── fixtures/
    │   ├── integration/
    │   └── unit/
    └── validation/
```
