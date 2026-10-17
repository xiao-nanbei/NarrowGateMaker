# F03 1s Prediction Overlay v1

Last materially modified: 2026-08-05

Status: `implemented_local_contract_verified_awaiting_research_bundle_and_formal_day`

## Boundary

This amendment adds an independent daily prediction-overlay materialization layer. It does not change the 1s feature panel, label overlay, trainer, backtest, or live runtime.

The formal data flow is:

```text
admitted 86,400-row feature panel
    + admitted causal_v12_1s_research_bundle.v1
    -> 13-head research inference
    -> prediction-only daily overlay
```

The output reads no label, PnL, reward, markout, or other economic outcome. It grants no prediction, action, baseline, or live authority.

## Input Validation

The feature panel must retain its exact artifact schema, cache identity, Parquet SHA256, manifest SHA256, `_SUCCESS` binding, and atomic-admission flag. Formal execution requires exactly 86,400 canonical 1s rows. A shorter fixture is accepted only through the explicit `test_only_row_count` function argument; the CLI exposes no such override, and the resulting identity remains test-only.

The bundle must be an atomically admitted `causal_v12_1s_research_bundle.v1` with exactly the frozen 13 heads. For every head, the materializer verifies:

- model and metadata file SHA256;
- exact 173-column feature order and its SHA256;
- `feature_bucket_ms=1000`;
- head name, label, objective, and metric identity;
- `research_only=true` and all prediction/action/live authorities false;
- the feature count and feature names reported by the loaded LightGBM model.

The overlay cache identity binds the feature panel, bundle, every model and metadata hash, materializer code, trainable-schema code and payload, feature order, output schema, and LightGBM runtime version.

## Output And Admission

`prediction_overlay.parquet` is zstd compressed and contains only four join keys plus `prediction__<head>` for the 13 heads. It never copies the 173 input features. Canonical timestamp order, causal feature-ready time, row fingerprints, prediction shape, finite values, and binary probability bounds are checked while writing in bounded batches.

The writer uses a sibling temporary directory, fsyncs Parquet, manifest, and `_SUCCESS`, fsyncs the temporary directory, publishes with `os.replace`, then fsyncs the parent. A failed run removes the temporary output. Existing output is reused only when the complete cache identity, permission boundary, schema, row count, and file hashes still match.

## Verification

The implementation is [`causal_v12_1s_prediction_overlay.py`](../audit/causal_v12_1s_prediction_overlay.py) with tests in [`test_causal_v12_1s_prediction_overlay.py`](../../../../tests/test_causal_v12_1s_prediction_overlay.py).

Focused verification reports `9 passed`. The combined panel, label-overlay, training, and prediction-overlay regression reports `44 passed`. Ruff, format, `py_compile`, and a real LightGBM save/load feature-identity roundtrip pass.

No formal 86,400-row prediction overlay has been generated because the future 1s research bundle is not yet an admitted input to this layer.
