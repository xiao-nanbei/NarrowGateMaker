# One-Day Data Engineering Walkthrough

[English](one_day_data_pipeline.md) | [简体中文](one_day_data_pipeline.zh-CN.md)

Last materially modified: 2026-09-14

Last materially synchronized: 2026-09-14

Status: Current source-neutral input-layer walkthrough; no training or economic replay.

## Scope

This is a bounded engineering example, not a rule for selecting research dates. The current calendar remains all 407 UTC dates, 2025-08-01 through 2026-09-11, for execution BTCUSDC and reference BTCUSDT perpetual markets. Missing and difficult dates stay in the inventory. Data repair does not change Development, Validation, holdout or previous-use permissions.

Use the [synthetic replay demo](../../examples/replay_demo/README.md) for a zero-network demonstration. It is separate from acceptance of purchased market inputs. The [data guide](../../data/README.md) defines the current facts, observations and remaining integration work.

## Install and configure

Use Python 3.11 or newer. These engineering commands use the data dependency group; they do not invoke a strategy or calculate PnL.

```bash
python -m pip install -e ".[data]"
python -m data --help

export INPUT_ARCHIVES="<private-purchased-archive-root>"
export DATA_OUTPUT="<private-derived-root>/input-engineering"
DAY=2025-08-01
```

Keep licensed compressed originals under raw, including retained purchases below `.incoming`. Outputs belong under the separate derived root. Both roots are real directories, without supplier aliases or symlinks. Delivery endpoints and authorization stay in private configuration, not public issues, logs or Git. Internal source identity remains truthful despite neutral command naming.

## Acquire and inventory

Resume an incomplete configured purchase through the shared entry point:

```bash
python -m data download --config <private-delivery-config.json>
python -m data inventory --root "$INPUT_ARCHIVES" \
  --output "$DATA_OUTPUT/calendar-current.json"
```

Download is archive-only, reusing locks, receipts and integrity checks. It does not retire originals or substitute another source. Pending responses do not become empty successful files. Inventory always retains the complete fixed calendar independently of this single-day example.

Presence and recorded checksum receipts are not full content acceptance. Unknown coverage, gaps and research rights remain unknown until separately established. Missing dates are denominator records, not exclusions.

## Normalize and validate

Choose a new output directory. Normalization refuses to overwrite a bundle and rejects missing or ambiguous selected inputs before building.

```bash
python -m data normalize --root "$INPUT_ARCHIVES" \
  --start "$DAY" --end "$DAY" --symbol BTCUSDC \
  --output "$DATA_OUTPUT/facts-$DAY"

python -m data validate --bundle "$DATA_OUTPUT/facts-$DAY" \
  --output "$DATA_OUTPUT/acceptance-$DAY.json"
```

The default selects both L2 and trades. Repeat `--symbol BTCUSDT` for the reference market or `--channel` for an explicit subset. A private `--plan` may instead specify ordered context files and actual clock mapping evidence. A one-day bundle does not prove cross-day initialization or trade deduplication: include sufficient adjacent context in the same bundle for those checks.

The shared parser preserves contiguous message boundaries and exact decimal values. Snapshots atomically replace the book, deltas set absolute quantities, and trade deduplication uses market/trade identity, not timestamps alone. Supplier receive time is technical grouping only. Unproven exchange-clock provenance remains unknown.

Validation reads the selected bundle, verifies source-bound shard identities and reconstructs books continuously across files. Gaps, regressions and invalid states remain findings. Parsing success is not proof of fresh observations, exact native queues, full-calendar acceptance or economic readiness.

## Next steps and historical tools

Shared observations and minimal features still require production consumer integration and end-to-end causal acceptance before new model use. Labels need actual outcome-end purging and a lawful split manifest. Economics needs explicit fill, latency, fee, funding and terminal-MTM contracts. None is run by the commands above.

Historical acquisition and bar-priced diagnostic recipes are retired from this tutorial. Inspect historical modules through `pipeline.py legacy --help`; they are not current-source fallbacks. Retain historical mechanism documents without treating old results or caches as new-contract evidence.
