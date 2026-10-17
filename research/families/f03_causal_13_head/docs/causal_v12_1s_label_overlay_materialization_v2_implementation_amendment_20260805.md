# F03 1s Label Overlay Materialization v2

Last materially modified: 2026-08-05

Status: `implemented_local_contract_verified_not_materialized_for_training`

## Amendment Boundary

The frozen v1 label design remains the authority for decision time, label horizons, UTC censoring, base weights, and overlap uniqueness. Its `in_memory_join_only` storage rule is superseded only by this materialization amendment. The old design and its evidence are not rewritten.

The successor persists labels independently from the 173-column feature panel:

```text
admitted daily feature panel + matching DailySourceBundle 1s bars
    -> unchanged v1 label generator
    -> label-only daily overlay
```

No feature column is copied into the overlay. Training must later join the two artifacts using `cutoff_exclusive_ms`, `decision_ts_ms`, `feature_ready_ts_ms`, and `feature_row_fingerprint_sha256`.

## Identity And Schema

Every output identity binds the admitted feature-panel manifest and Parquet SHA256, panel cache identity and schema, complete DailySourceBundle identity, label-generator SHA256, quote-config SHA256, and empirical P3-v2 artifact SHA256. A bundle mismatch or an incomplete/non-atomic feature panel fails before labels are generated.

The zstd Parquet output contains exactly four join keys and, for each of 13 heads, its label, validity flag, sample weight, and overlap uniqueness. Exact schema, finite/valid agreement, positive valid-row weights, `(0,1]` valid-row uniqueness, and unique join keys are required. Formal admission requires all 86,400 canonical decisions for one UTC day.

## Atomic Admission

The writer builds a sibling temporary directory, fsyncs Parquet, manifest, and `_SUCCESS`, fsyncs the temporary directory, publishes with `os.replace`, then fsyncs the parent directory. `_SUCCESS` contains the manifest SHA256. An existing artifact is reused only when its full cache identity, manifest, Parquet hash, schema, and row count remain compatible; incompatible output is rejected rather than overwritten.

## Verification And Permission

The implementation is [`causal_v12_1s_label_overlay_materializer.py`](../audit/causal_v12_1s_label_overlay_materializer.py) with tests in [`test_causal_v12_1s_label_overlay_materializer.py`](../../../../tests/test_causal_v12_1s_label_overlay_materializer.py). The focused suite reports `5 passed`; Ruff check and format check pass.

This work does not read predictions, PnL, or other economic outcomes. It does not train a model and grants no action, baseline, or live permission. A real full-day overlay has not yet been admitted for training.
