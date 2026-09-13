# Causal v4 empirical-P3 retrain and strict replay

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Date: 2026-07-18

> Current status (2026-07-27): the causal-v4 13-head, BUY-scorer and exact replay numbers in this report are superseded. The mixed-L2/trade-side revalidation was followed by additional time/calendar/unit repairs, and the maintained replacement identity is `causal-v7`. The empirical P3 shape was independently revalidated on normalized 100ms BBO and its current artifact is byte-identical between v5 and v7, but that does not restore the old v4 model, fill, campaign or PnL results. The historical no-promotion decision remains.

## 2026-07-20 correction

This report originally treated all 122 feature days as causally aligned. The MarketData alignment audit later found that Binance had changed the timestamp convention in several daily futures-metrics archives. The feature files for 2026-07-12, 2026-07-14, and 2026-07-15 used interval-start metrics five minutes before those observations were feature-ready.

The P3 artifact is unaffected: its own fit/validation/test identity ends on 2026-07-11. The 13-head train and validation panels are also unaffected. However, the old 13-head test metrics, test/all ML A/B values, and BUY scorer bucket values are withdrawn. Those artifacts remain research-only and cannot be used for promotion until rebuilt and reevaluated with normalized metrics.

## Scope

This run replaces the invalid pre-fix ML/replay evidence with one frozen causal identity:

- completed 10-second feature buckets are visible at `bucket_end`;
- volatility is one-second absolute-price variance in `(USDC/BTC)^2 / second`;
- inventory risk converts that variance to USDC without multiplying by mid;
- replay uses the merged event clock;
- P3/effective-kappa, queue and REST latency artifacts are explicit;
- the legacy BUY fill-selection scorer is disabled while constructing the order-level retraining denominator.

This is a research rebuild. No model or parameter in this report is promoted to live automatically.

## Frozen identity

| Component | Identity |
| --- | --- |
| P3 artifact | `models/saved_btcusdc_causal_v3_calonly_20260717/fill_prob_params.json` |
| P3 SHA256 | `f051ed23a5f0508a199164e283fcb8d6fc3170e0385464b587b486d334e0e652` |
| P3 type | `narrowgate_p3_touch_calibration.v2 / empirical_survival` |
| `delta_star` | `13.9990859817` |
| `kappa_eff` | `0.0674381136` |
| Feature manifest | `features_btcusdc_causal_v3_empirical_p3_20260718/causal_feature_manifest.json` |
| Feature manifest SHA256 | `0d5117283378c5f46127a60652eafe8c718365c6357d4e89ab9e4e6c8f7280ac` |
| Order file list SHA256 | `02b10606b3ab8b6ea3650bd30bfa3591438936352c7578b8c90734d691cfe0a2` |
| Queue artifact | queue-v3 q0.70, side/regime multipliers retained |
| Latency profile | AWS Tokyo, Amazon Linux 2023, 2 vCPU / 4 GiB, empirical REST samples |
| Replay engine | C++ with Python/C++ baseline parity gate |

The feature split contains 80 train days, one embargo day, 20 validation days, one embargo day and 20 test days. The order-level denominator contains 122 days, 2,219,633 placed orders and 70,650 fills.

## Unit and time audit

The active cash, inventory MTM and terminal MTM paths are dimensionally closed. `sigma_sq` is absolute-price variance per second; quote horizon is applied explicitly; circuit-breaker and exit-urgency risk use `sqrt(sigma_sq * horizon) * abs(quantity)` and do not multiply by mid. Hypothetical terminal taker liquidation is reported separately and is not deducted from final PnL.

Feature quote labels were corrected to use empirical `kappa_eff`, explicit quote horizon, dynamic-cap interaction and the absolute maker-fee floor. Formal replay ignores the legacy YAML effective-kappa override when strict calibration is enabled.

Regression volatility heads can emit negative raw predictions. Live and replay now clamp volatility to zero at the model-output boundary; the quote core already had the same effective clamp, so this closes the remaining semantic hole without changing valid positive predictions.

## 13-head model

Bundle: `models/saved_btcusdc_causal_v4_empirical_p3_20260718`

The model was fit and selected on unaffected train/validation dates, but its published test table is withdrawn because three test feature files contained the metrics timing error. No current claim is made about its test AUC, IC, or cross-horizon ranking. It remains a research artifact, not action uplift.

## BUY scorer rebuild

The replacement `non_toxic` artifact is:

`models/saved_btcusdc_buy_fill_selection_causal_v4_20260718/fill_selection_model.json`

SHA256: `047dea2d17638b6de812de24992a8d0912da9f7d5304a5ac4bc08bd8e8aebcad`

The separate `beats_opportunity` artifact is:

`models/saved_btcusdc_buy_beats_opportunity_causal_v4_20260718/fill_selection_model.json`

SHA256: `238d539483344b2cdd198e49b6408ef3c2f409de3cbc0102ce4b6a19b211b579`

The old bucket markout, terminal-PnL, and support values for both scorers are withdrawn. Their blocked-day cross-fitting could train on one of the affected feature days while evaluating another day, so excluding only three report rows would not repair the evidence. Both artifacts are invalid for policy use and must be rebuilt from a corrected panel.

## Strict ML A/B

Python/C++ preflight still establishes same-input implementation parity on 2026-01-01. The old test and all-122 ML A/B values are withdrawn. Train and validation runs remain diagnostics, but they are insufficient for promotion without a corrected independent test.

## Queue sensitivity

All queue arms are strict ML-OFF replays. q0.70 is the calibrated reference, not a strategy knob selected by PnL.

| Queue base | Raw PnL | Terminal PnL | Tails | Fills | Inventory time |
| --- | ---: | ---: | ---: | ---: | ---: |
| q0.55 | -679.48 | -335.88 | 166 | 75,056 | 13,664 |
| q0.70 | -651.69 | -337.31 | 167 | 70,457 | 14,509 |
| q0.85 | -634.71 | -295.69 | 169 | 66,733 | 14,804 |
| q1.00 | -631.11 | -275.36 | 167 | 63,200 | 15,285 |

The apparent aggregate improvement at larger queue-ahead values is partly a participation reduction. q1.00 has 10.3% fewer fills than q0.70 and is worse on test raw PnL with five additional test tails. q0.85 is the least fragile later-panel sensitivity point, but replacing q0.70 requires new live order-outcome calibration evidence, not this PnL table.

## Decision

- Keep empirical P3, causal bucket timing, merged clock and strict artifact validation as the new research baseline.
- Keep the causal-v4 13-head bundle out of promotion pending corrected test evidence; rebuild both BUY scorers before any further use.
- Do not enable the rebuilt BUY scorer or promote ML based on this run.
- Do not retune queue calibration from replay PnL.
- Side-specific action uplift was subsequently tested with a frozen 80-day development, 20-day validation and sealed 20-day holdout split drawn from the existing good-day universe. No fixed local add action passed, so the holdout remains sealed and no live change was made. See `docs/side_specific_action_uplift_existing_split_20260718.md`.
- New good days are not a routine prerequisite. They become necessary only after a family-specific holdout is consumed, an action family changes after holdout access, overlap can no longer be identified from existing data, or a material production distribution shift requires confirmation.
