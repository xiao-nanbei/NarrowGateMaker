# Placement Fill Policy-Clock Race v1

Date: 2026-07-27

Status: the structural contract was frozen before fitting. The subsequent Development implementation failed the complete curve gate and is closed. Validation and sealed holdout remain unread. No action or live deployment is authorized. See `placement_fill_policy_clock_race_v1_development_20260728.md`.

## Why This Is A New Family

v3/v4 correctly placed fill and cancel ACK in one lifecycle. v4 also showed that all six fill curves were calibrated on Development. v5 and v6 then tried to repair cancel calibration with role offsets and interval-hazard Platt maps. Those patches failed because they continued to model the wrong object.

Baseline cancel request is a policy stopping time:

\[
T_{request}=\inf\{t:\pi(\mathcal H_t)=cancel\}.
\]

Cancel ACK is subsequently

\[
T_{cancelACK}=T_{request}+L_{cancelACK}.
\]

The order remains fillable between request and ACK. A full or partial fill can arrive first, and ACK cancels only the remaining quantity. This lifecycle is not equivalent to a stationary cancel hazard active from placement time.

## Frozen Decomposition

1. Replay activation and deterministic baseline cancel-request timing.
2. Fit request-to-ACK latency on past-only data under an explicit environment identity.
3. Continue the dynamic fill process while cancel is pending.
4. Derive the fill/cancel/no-event CIF from that race.

The gates are independent: policy-request parity, ACK-latency parity, and fill CIF calibration/proper score/monotonicity. v4 is the fill benchmark. The v5 and v6 calibrators are not inherited.

The frozen machine-readable contract is `docs/placement_fill_policy_clock_race_v1_spec_20260727.json`.

The fitted family achieved exact request parity, calibrated all 12 marginal ACK-latency curves, and improved joint/fill Brier scores on all six side-by-role curves. Absolute race calibration still failed: three fill curves overpredicted and all six integrated cancel-ACK curves underpredicted. This closes the v1 implementation without rejecting the placement fill estimand.
