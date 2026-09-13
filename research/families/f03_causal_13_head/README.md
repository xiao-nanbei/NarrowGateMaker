# F03 Causal 13-Head Model

[English](README.md) | [简体中文](README.zh-CN.md)

Last materially modified: 2026-09-13

Last materially synchronized: 2026-09-13

Documentation boundary: this README and the unit's tracked `docs/` are public. Owner-only artifact locators, unpublished evidence indexes, and private research context are resolved through this unit's ignored local `private/` catalog and are not distributed with the public repository. See the [public/private research layout](../../PRIVATE_EVIDENCE.md).

Status: historical causal-v12 semantics-v6 model and frozen replay/operational comparator component. The public tree documents feature, label, training, and replay mechanics; it does not identify a currently deployed model, policy, release, or action state.

The 1-second cadence successor has completed its frozen 40-day native Development A/B and is closed. Relative to the v9 10-second control, it changed terminal MTM by `-2.116974 USDC`, retained 87.02% of fills, increased inventory time by 888.737 BTC*s, and worsened q10/CVaR. Validation and continuous 71-day confirmation were not opened. See [`causal_v12_1s_native_40day_full_path_ml_ab_v3_development_20260808.md`](docs/causal_v12_1s_native_40day_full_path_ml_ab_v3_development_20260808.md).

The root files own training, feature-profile experiments, and taker-tempo features. Runtime model contracts remain under `strategy/` because they are shared live/replay semantics. Shared dependencies: D, R, S, G.

Concrete deployment and backtest identities are private and `private_not_distributed`. This public family grants no live, action, occurrence, or economic authority. A private consumer must verify its own bound identity bytes and fail closed; a deployment-config alias may never substitute for backtest authority.

Historical post-fit evidence: `docs/causal_v12_postfit_native_oos_20260726_31_20260802.md`. Five Grade-A native dates produced a positive ML-ON terminal-PnL point estimate, but the prediction family passed only 5/13 head gates and the full-path screen failed PnL uncertainty, fill retention, campaign tail, and SELL maker-value gates.

## Retired causal-v2 feature bundle: materially distorted / historical reference only

On 2026-09-13 the owner authorized removal of `${NARROWGATE_DATA_ROOT}/features_btcusdc_causal_v2`. This is a feature/dataset directory, not model weights. Its 123 daily feature files, four combined datasets and feature manifest have been deleted; the historical documents remain reference material, not runnable current-input instructions. Do not restore this directory as a default, silently substitute another bundle for its identity, or reuse its old train/validation/test datasets for the new training round.

The retirement label means that these features and their historical diagnostics cannot represent the current data and model contract. The verified reasons and limits are:

- The 2026-07-15 manifest declares 119 daily files, while the directory contained 123. The [2026-07-20 alignment audit](../../../docs/marketdata_good_day_alignment_20260720.md#frozen-features) already identified four unlisted dates, 2026-07-12 through 2026-07-15. Reading by glob versus manifest therefore changes the sample.
- The manifest uses `chronological_good_day_with_embargo`, not the current predeclared complete calendar. Missing prior days stop its causal warmup. Its old selection and warmup support must not be presented as the rebuilt calendar's coverage.
- It uses 10-second bucket-end feature availability and label semantics version 2: return/direction outcomes extend from a fill within horizon `h` to markout `h` after that fill, potentially `2h` after the decision. Old datasets are not interchangeable with newly rebuilt Bar/features and regenerated labels. This is a semantic/support mismatch, not proof that every historical label leaks future information.
- The [July cleanup/retraining report](docs/model_runtime_cleanup_retrain_20260717.md) and [later time/unit repair](../../system_engineering/docs/time_unit_contract_repair_20260726.md) describe superseding model/feature contracts. Their old candidate metrics are historical only. The five-minute metrics exposure defect documented for causal-v3 and the later calendar defect must not be asserted as proven defects in every causal-v2 row without a row-level audit; none was performed during this cleanup.

Both linked July reports carry the **historical reference / materially distorted for current use** qualification through this maintained index; their original experiment records are not rewritten as new tests. Private locator entries record the deletion and remain unavailable for reproduction. Previous-use and locked research boundaries are unchanged. Purchased raw data, current reconstruction inputs, other feature directories, model weights and live state are untouched. This cleanup neither completes the replacement features nor validates a new model.
