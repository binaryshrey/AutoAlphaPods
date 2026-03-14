# Data Layer

This layer should own all point-in-time guarantees.

- `ingestion/`: raw vendor loaders and normalization jobs
- `pit/`: bi-temporal query models and revision-safe accessors
- `universe/`: survivorship-bias-free membership and tradeability rules
- `corporate_actions/`: splits, dividends, mergers, symbol changes, delistings

Upstream code should eventually consume a single PIT-safe interface rather than touching raw tables directly.
