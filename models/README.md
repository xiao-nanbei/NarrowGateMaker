# Models

Last materially modified: 2026-07-29

This directory is the stable import/CLI ABI and the home of shared replay and governance infrastructure. Family-owned implementations and evidence documents are physically organized under [`research/`](../research/README.md). Family modules must be imported from their canonical `research.families.*` package. Removed historical paths are recorded in versioned manifests under `research/governance/migrations/`; they are not import aliases.

Generated model bundles are ignored by Git. A local research checkout should normally retain only:

- the bundle required to reproduce the current live process;
- the latest formally identified causal bundle;
- a scorer artifact that is still referenced by live/replay parity tests.

Do not promote a bundle merely because its files exist. Formal replay requires model metadata with bucket-end feature visibility, causal warmup and label semantics, plus independently identified P3, queue and latency artifacts. Superseded bundles must not enter the parameter-search candidate list. Record them as retention candidates first; remove them only after a reference audit and explicit approval preserve every required frozen identity.

## Current Runtime Boundary

The currently declared runtime model directory is `saved_btcusdc_causal_v7_time_calendar_semantics_20260726`, but 13-head ML inference is disabled. The empirical P3 artifact remains active and is a separate runtime authority even when it is stored inside the same bundle. This is a corrected operational baseline, not a research promotion of the 13-head predictions.

Shared research and governance implementations belong in `models/audit/`. Family-specific implementations belong in their registered `research_*` directory and retain a compatibility path here. `models/backtest_tick.py` is the formal replay authority. The one-second bar entrypoints `models/backtest.py` and `models/backtest_ml.py` are historical/exploratory surfaces and cannot produce promotion evidence.

Before training, replay or tests, use the repository interpreter explicitly, for example `.venv/bin/python -m pytest`. A bundle may be removed only after a separate reference audit confirms that no live config, frozen manifest, parity test or reproducibility contract still names it.

## Canonical Predictive Ablations

`research.families.f03_causal_13_head.ml_model` is the only maintained training entrypoint for the causal 13-head family. Source-profile and taker-feature experiments use the same feature manifest, split, model metadata and training-summary contract as the base bundle. Print the frozen experiment definitions with:

```bash
.venv/bin/python -m research.families.f03_causal_13_head.ml_model --print-experiment-contract
```

A source-profile experiment is trained as a complete, independently named bundle:

```bash
.venv/bin/python -m research.families.f03_causal_13_head.ml_model \
  --feature-dir /absolute/path/to/versioned_causal_features \
  --model-dir models/saved_btcusdc_source_local_only_v1 \
  --source-profile local_only \
  --feature-variant base \
  --experiment-id source_local_only_v1
```

Taker/depth feature variants use `--feature-variant`; source profile and feature variant may be combined in one predeclared experiment. Any non-base experiment requires an explicit model directory and experiment ID, trains all 13 heads, and records `promotion_authority=research_only`. Predictive metrics and replay results cannot independently grant live authority; live bundle validation rejects this marker unless a separate promotion process creates an authorized bundle identity.

## External Information Decay

`research/families/f04_external_market_alpha/external_venue_model.py` is retained as the research-only trainer for Bitget/Bybit/OKX spot/perpetual incremental information. It is separate from the causal 13-head bundle because historical external archives have a trade-time one-second ABI rather than the live receive-time ABI.

Current runs must declare a complete integer-second target grid with `--fast-horizons-s` or `--fast-horizon-max-s`. There is no default 1/3/5-second target set. The trainer derives labels from one causal cached close path, keeps early stopping inside Development, and writes a simultaneous day-cluster horizon-decay selection artifact. The former ten-second heads and explicit `--targets-fast` list are compatibility paths only. No external prediction artifact has live action authority. Test/late scoring requires the frozen Development selection artifact and is restricted to its single selected horizon.
