# BUY q90 Action Suspension Operational Release v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: deployed baseline-integrity safety correction. This release contains no alpha, threshold, prediction, or economic-promotion claim.

## Decision

The BUY q90 dynamic fill-hazard model remains enabled for shadow observation, but its cancel/re-enter action is suspended in both the live runtime and the rolling backtest control:

```yaml
dynamic_fill_hazard_shadow_enabled: true
dynamic_fill_hazard_action_enabled: false
```

The previous operational identity allowed q90 action while also declaring that q90-ON live equivalence was unsupported. That combination executed an unregistered post-ACK terminal-hold action and is no longer the operational baseline.

## Basis

The frozen Development audit found that cancel ACK did not end the canceled order's active-order fill-hazard risk set. Terminal orders could retain the old depth cursor, be represented as `PENDING_CANCEL`, and use `hold_invalid` as an indefinite policy state. The evidence source is `buy_q90_terminal_hold_riskset_audit_v1_development_20260801.md`, SHA256 `6aae08b0a7c508db0dcba405ec7bdea9294497f726b1d4ee727634cce6581894`.

The causal-v12 ML-ON economic comparison used q90 OFF in both arms. Its result therefore supports neither the old q90 action nor the implicit terminal hold.

## Deployment Identity

- Effective UTC: `2026-08-02T02:25:16Z`
- Runtime Python: `3.12.13`
- Runtime PID after restart: `1721900`
- Private and remote config SHA256: `832e389e44c7d132db3bed10e37d0454b32cad7dca7596b90a1731998f4c7ca8`
- Previous config backup: `deploy_backups/live_config_pre_q90_action_suspend_20260802T022331Z.yaml`
- Previous config SHA256: `93a1a203aacee95466dc032f8b9fc7916b7a2daaf2ee31a1b1142506362ebc4f`
- New immutable baseline identity: `operational_baseline_identity_20260802_v5.json`
- Baseline identity SHA256: `e80aacdad29160e47889d157fdf67a633cb42d8e448bbfe4db70ea7c4435a000`

The 13 causal-v12 heads remained active and passed the remote model, Feature DAG, P3, and Python/C++ ABI preflight. The first post-restart HEALTH record reported 112 valid shadow rows, zero q90 action cancels, zero re-entries, zero invalid holds, and `fillHazardActionAuthorized=0`.

## Deployment Guard

`scripts/preflight_live_deploy.py` now rejects q90 action while the terminal risk-set and independent `POST_CANCEL_RECOVERY` contract is unresolved. An operator can bypass this only with `NARROWGATE_ALLOW_UNREPAIRED_Q90_ACTION_DEPLOY=1`; such a deployment is an explicit owner risk-accepted override and cannot be described as a research-supported baseline.

## Repair Boundary

The mechanics successor must establish all of the following before a new q90 action identity can be evaluated:

- cancel ACK removes the exchange order from the active-order fill-risk set;
- terminal active-depth cursor retention is zero;
- active-order hazard runtime ends at ACK;
- ACK transitions to an independent `POST_CANCEL_RECOVERY` state in 100% of supported cases;
- recovery evaluates a prospective placement with current price, age zero, current queue support, and current causal market state;
- `hold_invalid` is not a policy state;
- Python/C++ event-level mismatch is zero;
- the original valid-fraction, action-rate, and cancel-role-TV gates pass without relaxing thresholds.

The old numerical q90 and recovery thresholds may be carried only into the first mechanics parity run. Because the recovery estimand changes, retaining the same numeric threshold does not grant economic or live authority.

## Permissions

- q90 shadow observation: authorized
- q90 live action: not authorized
- q90 economic-result access during mechanics repair: not authorized
- q90 threshold tuning: not authorized
- causal-v12 ML operational baseline: unchanged and active
- automatic q90 re-enable: not authorized
