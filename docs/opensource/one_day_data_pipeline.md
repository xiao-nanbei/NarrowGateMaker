# One-Day Data Pipeline

[English](one_day_data_pipeline.md) | [简体中文](one_day_data_pipeline.zh-CN.md)

Last materially modified: 2026-09-12

Last materially synchronized: 2026-09-12

Status: Current public acquisition and diagnostic-replay tutorial.

## Scope

Use this path after the bundled demo when you want to inspect your own day of market data. It takes one UTC day through public Binance trade download, deterministic one-second-bar normalization, raw-trade audit, and a diagnostic replay summary. The public trade-download route needs no exchange trading account or API key, but does require network access to the archives. Strict queue or action work additionally needs a suitable best-bid/ask (BBO) and price-level order-book (L2) source, previous-day warmup when required, and the study's data-quality checks.

The single day is an onboarding/engineering sample, not a rule for choosing economic research dates. New research predeclares a continuous calendar and retains every date, including gaps and blocked inputs; do not assemble a study by selecting only days with a favorable quality score. Data repair does not change previous-use or Validation/holdout permissions.

For a zero-network walkthrough of the complete queue, fill denominator, campaign, terminal-accounting, and evidence-gate mechanics, run the [synthetic replay demo](../../examples/replay_demo/README.md) first.

## Install And Paths

Use an activated Python 3.11-or-newer environment and install both acquisition and replay dependencies. `.[data]` alone covers downloads and normalization, but the full tick replay imports scientific/research modules even with `--no-ml`:

```bash
python -m pip install -e ".[data,research]"

# These help commands do not download data or start replay.
python pipeline.py download-raw-trades --help
python -m models.backtest_tick --help

export NARROWGATE_ROOT="$PWD"
export NARROWGATE_MARKETDATA_ROOT="<local-marketdata-root>"
export NARROWGATE_RAW_DATA_ROOT="$NARROWGATE_MARKETDATA_ROOT/NarrowGate_BTCUSDC/raw"
export NARROWGATE_DATA_ROOT="$NARROWGATE_MARKETDATA_ROOT/NarrowGate_BTCUSDC/derived"
export NARROWGATE_RESULTS_DIR="$NARROWGATE_DATA_ROOT/reports/public_one_day"
export MM_DATA_ROOT="$NARROWGATE_DATA_ROOT"
DAY=YYYY-MM-DD
```

`NARROWGATE_MARKETDATA_ROOT` is the parent directory you control; it is not the repository checkout and must not be assigned to itself. The commands below create the NarrowGate child tree as needed.

`raw/` and `derived/` are separate real directories, without supplier aliases or compatibility symlinks. Every date uses the same filename, fields, column order, types, units and meanings within a channel; distinct channels such as books and trades keep their own schemas. Ingestion adapters do not define separate research-day classes. The [daily-data contract](../../data/README.md#one-fused-daily-orderbook) documents the maintained physical format and current conversion limitations; its existence is not proof that a whole calendar has finished migration.

## Download

Download individual executions for bars and event-resolution audits. BTCUSDC perpetual native aggregates are no longer downloaded; the [data guide](../../data/README.md#individual-union-and-derived-trade-flow) describes optional secondary-source union and locally generated flow groups.

```bash
python pipeline.py download-raw-trades \
  --symbols BTCUSDC --start "$DAY" --end "$DAY"
```

The expected source is `${NARROWGATE_RAW_DATA_ROOT}/binance_futures/BTCUSDC/${DAY}/trades.parquet`. A failed download/checksum must not publish a partial file as complete. Its UTC date nevertheless remains in the calendar manifest as missing/blocked until repaired; failure is not permission to remove the day from a study.

## Normalize And Audit

Build the one-second bars and then audit raw trade ordering, side coverage, bars, and book availability:

```bash
python pipeline.py bars \
  --symbol BTCUSDC --data-type trades --file "$DAY"

python pipeline.py audit-raw \
  --symbols BTCUSDC \
  --data-dir "$NARROWGATE_DATA_ROOT" \
  --out-dir "$NARROWGATE_RESULTS_DIR/raw-audit" \
  --file "$DAY"
```

Expected outputs are `${NARROWGATE_DATA_ROOT}/bars_1s/BTCUSDC-1s-${DAY}.parquet`, `${NARROWGATE_RESULTS_DIR}/raw-audit/BTCUSDC-raw-trades-audit.csv`, and `${NARROWGATE_RESULTS_DIR}/raw-audit/BTCUSDC-eligible-days.csv`.

### Read The Data Status

These are the actual fields produced by [`audit_raw_trades.py`](../../data/audit_raw_trades.py), not a universal yes/no permission to use NarrowGate:

| Field or result | What it means | Next step |
| --- | --- | --- |
| `raw_ok=true` | One nonempty raw-trade file passed the audit's ID-order and side checks | Inspect the detailed audit; this does not certify a full book or all source hours |
| `has_bars=true` | A matching one-second-bar file exists | Continue the bar-priced diagnostic below |
| `has_bbo=false` / `has_l2=false` | Matching book files are absent in the expected data layout | Obtain or normalize those sources if the experiment needs book replay |
| `eligible=false`, `missing_bbo;missing_l2` | Trades/bars are available, but the audit's combined book-input screen is incomplete | Trade/bar diagnostics remain usable; do not claim native queue or fill realism |
| `eligible=true` | Raw checks, bars/BBO/L2 presence, and the audit's known-exclusion check pass | Still check the selected source's coverage, sequences, clocks, warmup, and study window before strict replay |

Missing L2 is a data limitation, not a broken installation. Binance trade archives do not contain historical order-book changes. Preserve this audit's findings instead of editing `eligible`; they describe this input screen, not a command to delete dates from the continuous calendar. Compatible real observations may repair an input. Otherwise a derived book view can only carry a past valid observation with its actual timestamp and increasing age, or remain unknown; it cannot backfill from future observations or imply fresh quotes/queue progress. Unknown collection is not confirmed zero trading. Readability, mechanism support and economic use must be reported separately, and file presence alone does not establish live/replay parity.

## Diagnostic Replay

Run a one-day, no-ML, individual-trade-clock diagnostic against the public template:

```bash
python -m models.backtest_tick \
  --symbol BTCUSDC \
  --config live/config.yaml \
  --day "$DAY" \
  --execution-trade-source trades \
  --replay-event-clock trade \
  --pricing-mode bar \
  --no-ml \
  --summary-json "$NARROWGATE_RESULTS_DIR/replay-${DAY}.json"
```

Success writes the requested scalar summary plus replay diagnostics under `${NARROWGATE_RESULTS_DIR}`. Inspect those fields alongside the data-status report. This explicitly trade-clock, bar-priced route uses fallback queue assumptions; no-trade gaps do not have the same timer behavior as a merged-event replay. Its PnL is a modeled diagnostic, not an exact-book fill estimate or a prediction of live profitability. It cannot establish research promotion or deployment readiness.

## Strict Book Path

CryptoHFTData is an optional authenticated acquisition adapter. An authorized user adds `.[provider-cryptohft]` to the environment above, supplies the credential outside Git, and follows the [current daily-book conversion contract](../../data/README.md#one-fused-daily-orderbook). Acquisition alone can use `.[data,provider-cryptohft]`; strict replay also needs the research dependencies. The provider client is intentionally not part of `all`, and installation grants neither access nor a data license. Downloads are temporary ingestion inputs, not supplier-labelled research datasets: publish verified daily containers under `raw/` and their normalized views under `derived/`. Preserve real observation clocks, reset semantics, missing intervals and current validation status; retire redundant ingestion files only after verified publication. A readable daily container does not itself satisfy a strict queue study's sequence, clock, warmup or mechanism requirements, and must not be manually relabelled as a frozen historical input.
