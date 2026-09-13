# Model/runtime cleanup and causal retraining (2026-07-17)

> Current status (2026-07-27): the runtime removals in this report remain maintained engineering decisions, but its causal-v3 model, P3 path and exact replay/training numbers are historical. They were superseded first by the normalized-100ms rebuild and then by the time/calendar/unit repair. The replacement model identity is `causal-v7`; 13-head inference is disabled and the active empirical P3 is byte-identical to the v7 copy. Use `docs/time_unit_contract_repair_20260726.md` for current model semantics.

## Decision

This cleanup removes failed system experiments and archived action executors from the maintained runtime. Historical evidence readers remain able to parse old logs, but they cannot reactivate a removed policy.

Removed:

- external-venue Python dispatcher workers and shared dispatcher wiring;
- Transformer training/runtime integration and the live `torch` dependency;
- the async latest-wins order gateway, telemetry ABI, benchmarks, and tests;
- the failed native signal-state experiment and environment switch;
- the dead `depth_data` window ABI; window cache is now v7;
- general-intensity execution, direct quote-EV actions, SELL resiliency live actions, and direct xmarket retreat actions;
- legacy adaptive TTL/simple cooldown configs while retaining `AdaptiveAddCooldownConfig`;
- ret-prediction stacking in training, replay, and live inference;
- C++ replay v1/v2 policy bindings and non-exact queue modes. Formal replay now calls `simulate_tick_arrays_ext_policy_v3` directly; the compact `simulate_tick_arrays` binding remains as an intentional smoke API.

Configuration keys without consumers were removed. The loader now rejects unknown sections and keys instead of silently accepting a no-op setting. Public and current private config snapshots load with `markout_ema_span_fills`; `markout_hl` is no longer an accepted alias.

## Deliberately retained

- quote-EV model training and shadow calibration, without a direct executor;
- xmarket/reference features and historical audit fields;
- BUY fill selection, depth imbalance, adverse/defense guards, dynamic cap, BER, state-conditioned policy, post-fill response, adaptive add cooldown, and queue-v3 calibration;
- calendar legacy aliases in live feature generation because the current deployed model still consumes them;
- the SU-Johnson loader because the deployed bundle still has a legacy four-parameter P3 artifact. New formal work must use empirical survival;
- trade-clock/legacy replay compatibility until all historical runners have moved to merged event clock and native BBO/L2.

These retained compatibility paths are migration dependencies, not approval to use their old evidence for promotion.

## Model impact

The absolute-price variance unit correction changes risk/action outcomes, but does not by itself change raw market labels. The larger model invalidation comes from the repaired 10-second feature-ready timestamp and causal warmup: the old LightGBM and downstream replay evidence saw a different feature distribution and must not support promotion.

A new 13-head candidate was trained from `features_btcusdc_causal_v2`:

`models/saved_btcusdc_causal_v3_calonly_20260717`

Identity:

- causal left-label bucket-end availability;
- chronological good-day split with embargo;
- canonical `cal_*` calendar features only;
- 195 features per head;
- no Transformer or `stacked_ret_*` inputs;
- empirical-survival P3 artifact copied byte-for-byte from the formal causal-v2 calibration (`kappa_eff=0.06743811359073745`).

Test diagnostics:

| Head | Test metric |
| --- | ---: |
| direction 10s / 30s / 60s | AUC 0.5239 / 0.5106 / 0.5174 |
| return 10s / 30s / 60s | IC 0.0169 / 0.0052 / 0.0052 |
| volatility 10s / 30s / 60s | IC 0.5603 / 0.6888 / 0.7464 |
| toxicity bid 5s / ask 5s | AUC 0.5401 / 0.5328 |
| toxicity bid 10s / ask 10s | AUC 0.5275 / 0.5319 |

The candidate is `research_only`. Predictive diagnostics are too weak to replace the live bundle without formal replay A/B, campaign/tail gates, and new later good days.

The 2026-07-06 BUY fill-selection artifact was not refit. Its only available order-level denominator was produced before the replay-time and unit repairs. Refitting the same stale CSV would create a new file without creating valid evidence. It requires a rebuilt causal order-level panel first.

## Verification

- C++ extension rebuilt from the collapsed ABI.
- C++ replay parity/golden: 23 passed, 4 skipped.
- Full suite after cleanup: 345 passed, 4 skipped.
- Public and current private configs load under strict unknown-key validation.
- Current deployed LightGBM bundle has zero `stacked_ret_*` consumers.

The patch removes roughly 7.3k net lines. No live model pointer, strategy parameter, private deployment, or EC2 process was changed.
