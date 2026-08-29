# F03 Causal 13-Head Model

Last materially modified: 2026-08-25

Documentation boundary: this README and the unit's tracked `docs/` are public. Owner-only artifact locators, unpublished evidence indexes, and private research context are resolved through this unit's ignored local `private/` catalog and are not distributed with the public repository. See the [public/private research layout](../../PRIVATE_EVIDENCE.md).

Status: historical causal-v12 semantics-v6 model and replay component. The public tree documents feature, label, training, and replay mechanics; it does not identify a currently deployed model, policy, release, or action state.

The 1-second cadence successor has completed its frozen 40-day native Development A/B and is closed. Relative to the v9 10-second control, it changed terminal MTM by `-2.116974 USDC`, retained 87.02% of fills, increased inventory time by 888.737 BTC*s, and worsened q10/CVaR. Validation and continuous 71-day confirmation were not opened. See [`causal_v12_1s_native_40day_full_path_ml_ab_v3_development_20260808.md`](docs/causal_v12_1s_native_40day_full_path_ml_ab_v3_development_20260808.md).

The root files own training, feature-profile experiments, and taker-tempo features. Runtime model contracts remain under `strategy/` because they are shared live/replay semantics. Shared dependencies: D, R, S, G.

Concrete deployment and backtest identities are private and `private_not_distributed`. This public family grants no live, action, occurrence, or economic authority. A private consumer must verify its own bound identity bytes and fail closed; a deployment-config alias may never substitute for backtest authority.

Latest post-fit evidence: `docs/causal_v12_postfit_native_oos_20260726_31_20260802.md`. Five Grade-A native dates produced a positive ML-ON terminal-PnL point estimate, but the prediction family passed only 5/13 head gates and the full-path screen failed PnL uncertainty, fill retention, campaign tail, and SELL maker-value gates.
