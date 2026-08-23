# One-Day Data Pipeline

Last materially modified: 2026-08-23

Status: Current public acquisition and diagnostic-replay tutorial.

## Scope

This tutorial takes one UTC day through public Binance trade download, deterministic one-second-bar normalization, raw-trade audit, and a diagnostic replay summary. It does not make the day formal research evidence: strict queue or action work additionally needs an admitted BBO/L2 source, its previous-day warmup when required, a frozen quality manifest, and the study's own evidence contract.

For a zero-network walkthrough of the complete queue, fill denominator, campaign, terminal-accounting, and evidence-gate mechanics, run the [synthetic replay demo](../../examples/replay_demo/README.md) first.

## Install And Paths

Use Python 3.11 or newer and install the public data tooling:

```bash
python -m pip install -e ".[data]"

export NARROWGATE_ROOT="$PWD"
export NARROWGATE_MARKETDATA_ROOT="<local-marketdata-root>"
export NARROWGATE_DATA_ROOT="$NARROWGATE_MARKETDATA_ROOT/NarrowGate_BTCUSDC"
export NARROWGATE_RESULTS_DIR="$NARROWGATE_DATA_ROOT/reports/public_one_day"
export MM_DATA_ROOT="$NARROWGATE_DATA_ROOT"
DAY=YYYY-MM-DD
```

`NARROWGATE_MARKETDATA_ROOT` is the parent directory you control; it is not the repository checkout and must not be assigned to itself. The commands below create the NarrowGate child tree as needed.

## Download

Download the public aggregate tape used for bars and the individual-trade tape used for event-resolution audits:

```bash
python pipeline.py download-agg-trades \
  --symbol BTCUSDC --start "$DAY" --end "$DAY"

python pipeline.py download-raw-trades \
  --symbols BTCUSDC --start "$DAY" --end "$DAY"
```

The expected source files are `${NARROWGATE_DATA_ROOT}/raw/BTCUSDC-aggTrades-${DAY}.csv` and `${NARROWGATE_DATA_ROOT}/raw_trades/BTCUSDC/BTCUSDC-trades-${DAY}.csv`. Download and checksum failure is fatal; a partial file is never admitted as a day.

## Normalize And Audit

Build the one-second bars and then audit raw trade ordering, side coverage, bars, and book availability:

```bash
python pipeline.py bars \
  --symbol BTCUSDC --data-type aggTrades --file "$DAY"

python pipeline.py audit-raw \
  --symbols BTCUSDC \
  --data-dir "$NARROWGATE_DATA_ROOT" \
  --out-dir "$NARROWGATE_RESULTS_DIR/raw-audit" \
  --file "$DAY"
```

Expected outputs are `${NARROWGATE_DATA_ROOT}/bars_1s/BTCUSDC-1s-${DAY}.parquet`, `${NARROWGATE_RESULTS_DIR}/raw-audit/BTCUSDC-raw-trades-audit.csv`, and `${NARROWGATE_RESULTS_DIR}/raw-audit/BTCUSDC-eligible-days.csv`. With trades and bars but no admitted BBO/L2, `raw_ok` and `has_bars` may pass while `eligible` correctly remains false with `missing_bbo;missing_l2`; do not edit that result or substitute another provider silently.

## Diagnostic Replay

Run a one-day, no-ML, aggregate-trade-clock diagnostic against the public template:

```bash
python -m models.backtest_tick \
  --symbol BTCUSDC \
  --config live/config.yaml \
  --day "$DAY" \
  --execution-trade-source aggTrades \
  --replay-event-clock trade \
  --pricing-mode bar \
  --no-ml \
  --summary-json "$NARROWGATE_RESULTS_DIR/replay-${DAY}.json"
```

Success writes the requested scalar summary plus replay diagnostics under `${NARROWGATE_RESULTS_DIR}`. This route is a mechanics diagnostic with fallback queue assumptions; it is not exact-book, live-parity, economic-admission, promotion, or deployment evidence.

## Strict Book Path

CryptoHFTData is an optional authenticated third-party source. An authorized user installs `.[data,provider-cryptohft]`, supplies the provider credential outside Git, and follows the [market-data source and normalization contract](../market_data.md#binance-and-cryptohftdata). The provider client is intentionally not part of `all`, and installation grants neither access nor a data license. A newly normalized day stays in its source-labelled staging root until coverage, sequence, causality, BBO/L2 pairing, warmup, and manifest gates pass; never copy it manually into the immutable formal registry.
