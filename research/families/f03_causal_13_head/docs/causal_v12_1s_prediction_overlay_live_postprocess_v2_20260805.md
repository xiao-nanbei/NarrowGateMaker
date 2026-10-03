# Causal-v12 1s Prediction Overlay Live Postprocess v2

Last materially modified: 2026-08-05

Status: implemented and contract-tested; native overlays and economics must be regenerated under the v2 identity.

## Correction

The v1 overlay persisted raw LightGBM outputs. That differed from the live 13-head path, which projects volatility predictions to the nonnegative domain and classification predictions to `[0, 1]` before quote policy consumption. The mismatch was detected before the first 40-day economic replay completed.

The v2 output contract is:

- `vol_10s`, `vol_30s`, `vol_60s`: `max(raw_prediction, 0)`;
- all direction and toxicity classification heads: `clip(raw_prediction, 0, 1)`;
- return heads: identity;
- return demeaning remains a downstream policy-state operation and is not materialized into the overlay.

## Identity Boundary

The materializer and prediction artifact schema are both v2. The 40-day native overlay binding is also v2. Existing v1 overlays, plans, and execution amendments are historical failed-preflight evidence and cannot be reused.

The correction does not retrain any model, read labels or PnL, alter a strategy parameter, or grant prediction, action, or live authority.

## Verification

Focused overlay, replay ABI, native binding, and full-path contract tests: `49 passed`. Ruff passed.
