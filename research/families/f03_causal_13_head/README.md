# F03 Causal 13-Head Model

Last materially modified: 2026-08-20

Documentation boundary: this README and the unit's tracked `docs/` are public. Owner-only artifact locators, unpublished evidence indexes, and private research context are resolved through this unit's ignored local `private/` catalog and are not distributed with the public repository. See the [public/private research layout](../../PRIVATE_EVIDENCE.md).

Status: active model component inside the v11 operational and backtest default. Causal-v12 semantics-v6 remains enabled, q90 action and BUY fill-selection remain suspended, and the owner-approved SELL Boolean cooldown policy supplies the current action layer. Independent research confirmation of that cooldown policy remains unresolved because its promotion was owner-risk-accepted rather than a research hard-gate pass.

The 1-second cadence successor has completed its frozen 40-day native Development A/B and is closed. Relative to the v9 10-second control, it changed terminal MTM by `-2.116974 USDC`, retained 87.02% of fills, increased inventory time by 888.737 BTC*s, and worsened q10/CVaR. Validation and continuous 71-day confirmation were not opened. See [`causal_v12_1s_native_40day_full_path_ml_ab_v3_development_20260808.md`](docs/causal_v12_1s_native_40day_full_path_ml_ab_v3_development_20260808.md).

The root files own training, feature-profile experiments, and taker-tempo features. Runtime model contracts remain under `strategy/` because they are shared live/replay semantics. Shared dependencies: D, R, S, G.

The sole current operational authority is the mutable [`operational_baseline_current.json`](../f10_live_replay_attribution/docs/operational_baseline_current.json) pointer. It resolves to immutable [`operational_baseline_identity_20260820_v12.json`](../f10_live_replay_attribution/docs/operational_baseline_identity_20260820_v12.json). v12 preserves the v11 strategy/config/model bytes and active owner-risk-accepted SELL cooldown while rebinding the runtime to the successor AWS host epoch; it grants no new research authority. v11, v10, v9, v8, v7, v6, v5, and preceding canary identities remain frozen historical evidence.

Latest post-fit evidence: `docs/causal_v12_postfit_native_oos_20260726_31_20260802.md`. Five Grade-A native dates produced a positive ML-ON terminal-PnL point estimate, but the prediction family passed only 5/13 head gates and the full-path screen failed PnL uncertainty, fill retention, campaign tail, and SELL maker-value gates.

Owner-amended economic sensitivity: `docs/causal_v12_owner_amended_economic_rescore_v2_20260802.md`. The v2 fill band is 80%-120%. Combining the historical late 22 days with five post-fit Grade-A days gives 19/27 positive PnL days, a positive closed-campaign PnL lower bound, and a loss/fill selectivity ratio of 2.925. Campaign q10 non-inferiority remains unresolved. The owner promoted this evidence to the operational and backtest baseline, while research prediction/live authority remains closed.
