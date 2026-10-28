# NarrowGate — minimal AS teaching edition

[English](README.md) | [简体中文](README.zh-CN.md)

Last materially synchronized: 2026-10-29

## Research availability and contact

Detailed NarrowGate research, experimental plans, production strategy implementations, effective parameters and results are not public. This repository contains only the minimal AS teaching example, synthetic data, generic teaching backtest rules and qualitative research directions. If you are interested in the research or have questions about a topic, please contact me privately by email: [xuantan650@gmail.com](mailto:xuantan650@gmail.com).

This public edition contains a small textbook Avellaneda–Stoikov quotation example, seeded fictional market events, transparent toy backtest rules, and qualitative research directions. It is deliberately independent of the complete private application. It is not a production trading system or a reproduction of private research.

## Run locally

Python 3.11 or newer; standard library only. From this repository root:

```bash
python3 -m as_demo --seed 7 --events 200
python3 -m unittest discover -s tests -v
```

The command generates fictional data in memory and prints a toy accounting summary. It does not connect to a venue, download data, read credentials or write reports. Every numeric example is invented for teaching, not fitted to a real market or copied from an effective production configuration. No recorded research or trading returns are included.

## Read the example

- [AS model and units](docs/model.md)
- [Replay ordering, fill assumptions and accounting](docs/backtest_rules.md)
- [Basic directions of the research families](docs/research_directions.md)

The simulator intentionally omits queue position, latency, market impact, margin, funding and production risk controls. Its fill assumptions are optimistic and must not be used to estimate tradable profitability. Generated output is not economic evidence or investment advice.

## Publication boundary

Only the named educational files are exported from an immutable source snapshot. This edition excludes PoolRun and other orchestration, production strategies, effective parameters, feature/label pipelines, trained models, calibration, research implementation and results, live connectivity, operational runbooks and private data. Research direction notes describe questions, not deployed solutions or successful findings. No hosted workflows are shipped.

Older public content may remain in third-party clones, caches or forks; the reduced current tree does not recall those copies. The [PolyForm Noncommercial License](LICENSE) applies. Commercial use requires separate permission.
