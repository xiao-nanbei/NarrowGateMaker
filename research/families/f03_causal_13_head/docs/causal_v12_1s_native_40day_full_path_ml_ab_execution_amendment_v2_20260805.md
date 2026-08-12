# F03 1s Native 40-Day Full-Path Execution Amendment v2

Last materially modified: 2026-08-05

Status: implementation complete; exact amendment is generated only after the formal candidate overlay panel is admitted. Economic outcomes remain unread.

## Exact amendment

The 40-day runner has no default or optional execution amendment. `prepare`, `run`, and `finalize` each require the same explicit amendment path. Validation rebuilds the amendment from the current files and rejects any path, content, or identity drift.

The amendment freezes:

- the panel runner and dual-overlay ABI;
- their focused test files;
- the operational configuration and Feature DAG;
- the economic precommit;
- the admitted v9 control panel and its model identity;
- the admitted 1s candidate panel and its bundle identity;
- the ordered 40-day denominator;
- the campaign-MAE trace capacity and risk-gate direction.

The amendment must be built after candidate overlay admission:

```bash
.venv/bin/python \
  research/families/f03_causal_13_head/audit/causal_v12_1s_native_40day_full_path_ml_ab_execution_amendment.py \
  build \
  --candidate-overlay-panel-manifest <candidate-overlay-panel-manifest.json> \
  --control-overlay-panel-manifest <v9-control-panel-manifest.json> \
  --precommit research/families/f03_causal_13_head/docs/causal_v12_1s_cadence_full_path_economic_precommit_v1_20260805.json \
  --output research/families/f03_causal_13_head/docs/causal_v12_1s_native_40day_full_path_ml_ab_execution_amendment_v2_20260805.json
```

An existing different output is never replaced.

## Campaign MAE

Both arms receive the same positive `trace_campaign_repair_max=1000000`. The C++ replay remains the economic path. A non-action Python probe emits the campaign-state MAE trace from the same inputs; its fill path must match the C++ path before the trace is admitted. Missing, invalid, or capacity-reaching trace data blocks `campaign_mae_avoidance`. The owner gate consumes the paired candidate-minus-control MAE lower bound and fails closed when it is unavailable or negative.

## Permission boundary

This remains a Development-only, owner-route identity. The raw `action_alpha_v2` result is preserved. The only permitted owner override is fill retention in `[0.80, 1.20]`. Validation, holdout, action, and live authority remain false, and the separate 71-day continuous confirmation is still required.
