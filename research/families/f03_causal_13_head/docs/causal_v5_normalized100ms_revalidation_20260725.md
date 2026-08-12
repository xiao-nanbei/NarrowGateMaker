# Causal v5 Normalized-100ms Revalidation

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Date: 2026-07-25

> Superseded on 2026-07-26: later calendar, bar-clock, commission, daily-PnL, tick/lot and volatility-interface repairs changed the authoritative identity to `causal-v7`. The causal-v5 deployment statement below is historical; the configured dormant bundle now points to v7, 13-head inference is disabled, and neither v5 nor v7 passed statistical strategy promotion. The normalized P3 artifact remains valid because its v5/v7 copies are byte-identical.

Status: historical normalized-100ms rebuild, superseded by `causal-v7`. This report replaced exact results that depended on the former mixed one-second/100ms L2 roots or on the corrupted 2026-07-04 through 2026-07-11 individual-trade side flags. Its statistical promotion gate did not pass. On 2026-07-25 the corrected causal-v5 bundle was nevertheless explicitly placed in live as an operational trial; that historical deployment did not change its evidence classification.

## Frozen Input Identity

| Input | Identity |
|---|---|
| Normalized L2 | `normalized_l2_100ms_v2/manifest.json`, SHA256 `f47e044b0607d135de713f8cbf13dad82decc051786dd4a896f21e014129b517` |
| Repaired taker-tempo manifest | `trade_features_causal_v2_20260725/manifest.json`, SHA256 `bd7ea637b4e34f98269cd0524a06c30f6922f4a4d86399ce373bbc15ef11583d` |
| Individual-trade quality report | `execution_trade_quality.csv`, SHA256 `23869c15e17809ded9c3a6e8042890a3ea294d5e3b850e0ec6309df8c4d14c11` |
| Empirical P3 | SHA256 `cedec34851454b643be746746a1dd4bcc7e13807985c14e78504643ad6e71714`, `delta*=13.9991`, `kappa_eff=0.0673564` |
| Queue reference | queue-v3 q0.70, SHA256 `7756881704743f7a11a5a7a0f2439bf25cbdd9cbebd3aeb00f1135191148fadd` |
| Formal replay contract | SHA256 `2ccd32327d2236bd3a6af7c82200805c30c502de50f2d05076a3cf5aeea739da` |
| Latency environment | `aws_tokyo_ec2_2vcpu4g_amazon_linux`, fixed keyed seeds |

The q0.70 artifact is a two-day live-conditional queue reference. Queue sensitivity below measures dependence on that reference; it does not select a universal queue multiplier from replay PnL.

## Causal Features And 13-Head Model

The causal feature bundle was rebuilt under:

```text
MarketData/NarrowGate_BTCUSDC/features_btcusdc_causal_v5_normalized100ms_20260725/
```

It contains 128 daily files, 220 features and 13 labels. The frozen split is:

| Panel | Days | Rows |
|---|---:|---:|
| Train | 86 | 742,998 |
| Validation | 20 | 172,798 |
| Test | 20 | 172,799 |

The retrained model is:

```text
models/saved_btcusdc_causal_v5_normalized100ms_20260725/
```

Selected held-panel diagnostics:

| Head | Metric |
|---|---:|
| `dir_10s` | AUC 0.5394 |
| `ret_10s` | IC 0.0429 |
| `vol_10s` | IC 0.6552 |
| `dir_30s` | AUC 0.5097 |
| `vol_30s` | IC 0.7264 |
| `dir_60s` | AUC 0.5112 |
| `vol_60s` | IC 0.7613 |
| `tox_bid_5s` / `tox_ask_5s` | AUC 0.5748 / 0.5764 |

These are predictive diagnostics, not action uplift.

## ML-OFF / ML-ON Replay

The first completed A/B accidentally retained the shared dynamic-hazard action overlay from `feature_config.yaml`. It remains a valid conditional comparison, but it is not the clean current-baseline ranking:

| Conditional panel | Raw PnL | Terminal PnL | InvAdj | Fills retention | Campaign retention | Tail delta |
|---|---:|---:|---:|---:|---:|---:|
| Validation20 | +4.6213 | +0.4815 | +3.9896 | 91.94% | 88.84% | +7 |
| Test17 | +0.8014 | -0.6246 | +1.7457 | 93.84% | 89.51% | -1 |
| Repaired 2026-07-04..11 slice | +0.5290 | -0.6960 | +1.1770 | lower by 216 fills | lower by 76 campaigns | -2 |

The overlay-free rerun uses `formal_clean_config.yaml`: BUY fill selection, dynamic-hazard shadow and dynamic-hazard action are all disabled in both arms. Only `ml_enabled` changes.

| Clean panel | Raw PnL | Terminal PnL | InvAdj | Fills retention | Campaign retention | Inventory-time delta | Tail delta |
|---|---:|---:|---:|---:|---:|---:|---:|
| Validation20 | +3.5564 | -0.2288 | +4.0734 | 91.46% | 88.67% | +271.2 BTC-s | 0 |
| Test17 | +2.1422 | +0.4696 | +1.9945 | 92.64% | 89.96% | +131.4 BTC-s | -1 |
| Repaired 2026-07-04..11 slice | +3.3927 | +2.0769 | +1.2696 | 92.86% | 89.10% | +113.5 BTC-s | -2 |

The paired daily raw and terminal intervals include zero on both clean panels. InvAdj improves, but participation falls and absolute inventory time rises. The 13-head bundle therefore remains a research signal; neither A/B authorizes an unconditional live ML action.

## Queue Sensitivity

Relative to q0.70:

| Panel | q | Raw PnL | Terminal PnL | InvAdj | Fills | Campaigns | Tail |
|---|---:|---:|---:|---:|---:|---:|---:|
| Validation20 | 0.55 | -10.278 | -12.072 | +1.371 | +659 | +236 | +4 |
| Validation20 | 0.85 | -4.383 | -3.258 | -1.173 | -570 | -192 | +3 |
| Validation20 | 1.00 | -2.098 | +0.078 | -2.443 | -986 | -427 | +8 |
| Test17 | 0.55 | -0.452 | -1.250 | +0.498 | +484 | +118 | -5 |
| Test17 | 0.85 | -4.897 | -4.179 | -0.704 | -308 | -118 | +1 |
| Test17 | 1.00 | -8.187 | -6.809 | -1.124 | -645 | -207 | -1 |

The replay is queue-sensitive and the direction does not support replacing the live-fitted q0.70 artifact from replay PnL.

## Formal Order Denominator And Inventory Lifecycle

The maintained formal denominator contains:

- Development: 33 days, 610,915 placed orders;
- Validation: 9 days;
- combined: 42 days, 780,099 placed orders;
- sealed holdout: 9 dates whose order files were not supplied to the scorer.

All 42 input rows bind the same formal replay contract. The combined filelist SHA256 is `c77a94aa24e20534d3730d7eb71ed03e47cfa68169a34621c4729dc733b04bb5`.

The formal and earlier exploratory denominators have equal total fills, BUY/SELL fill counts and filled quantity. Fixed latency sampling changes some order IDs and campaign boundaries: aggregate terminal PnL differs by about `+0.332 USDC`. The formal identity is authoritative.

FIFO lifecycle over Development plus Validation:

| Inventory side | Lots | Median lifetime | Campaign-flat median | Closed rate |
|---|---:|---:|---:|---:|
| LONG | 6,335 | 348.4s | 436.3s | 98.91% |
| SHORT | 3,995 | 261.6s | 306.8s | 99.40% |
| ALL | 10,330 | 308.2s | 381.1s | 99.10% |

The 20s/30s markouts remain early toxicity diagnostics. They are not substitutes for campaign terminal value or inventory survival.

## Repaired 2026-07-04 Through 2026-07-11 Trade Sides

The repaired files contain both maker-side values and are bound by `execution_trade_quality.csv`. A frozen native-lifecycle Validation rerun covers 2026-07-04 through 2026-07-08; 2026-07-09 is an embargo date and the later family holdout remains sealed.

The five affected Validation dates changed far beyond a side-label-only edit:

| Day | Old lifecycle rows | Repaired rows | Old / repaired | Old / repaired fills |
|---|---:|---:|---:|---:|
| 2026-07-04 | 9,637,321 | 792,781 | 12.2x | 97 / 434 |
| 2026-07-05 | 5,164,088 | 618,177 | 8.4x | 165 / 496 |
| 2026-07-06 | 11,947,702 | 629,910 | 19.0x | 145 / 889 |
| 2026-07-07 | 17,022,622 | 530,822 | 32.1x | 125 / 1,077 |
| 2026-07-08 | 9,656,585 | 517,564 | 18.7x | 163 / 1,095 |

All non-runtime daily fields for the unaffected 2026-06-28 through 2026-07-03 dates are exactly equal to the old replay. This isolates the result change to the repaired trade input rather than code drift.

The frozen BUY dynamic-fill model now passes its Validation prediction gate:

| Head | Events | ROC AUC | AP lift | O/E | Brier skill | Gate |
|---|---:|---:|---:|---:|---:|---|
| adverse fill | 2,976 | 0.8963 | 11.60x | 0.931 | +0.02150 | pass |
| favorable fill | 764 | 0.8253 | 9.55x | 0.777 | +0.00256 | pass |
| delayed-entry repair | 1,400 | 0.8740 | 11.20x | 0.934 | +0.01879 | pass |

This admits only a separate randomized keep/cancel experiment. The artifact sets `action_family_allowed=false`, `live_change_allowed=false`, and keeps the sealed holdout closed.

The repaired spread-fill Validation has 169,368 orders and 7,786 first fills, versus 151,158 and 4,250 under the corrupted input. The adjusted spline predicts 5.19% against an observed 4.60%; the old comparison was 5.48% against 2.81%. It now improves both NLL (`0.27332` versus `0.28303`) and Brier (`0.04597` versus `0.04668`) relative to the pooled exponential. This repairs the descriptive fill-probability evidence but still does not identify a spread action.

## Random Opportunity Null

The null samples the same-day/same-side placed-order denominator and evaluates submit-time opportunity markout. It is not a full executable maker strategy.

| Panel | Side | Horizon | Actual fill markout | Random placed opportunity | Gap |
|---|---|---:|---:|---:|---:|
| Development33 | BUY | 20s | -1.2611 | +3.8862 | -5.1473 |
| Development33 | BUY | 30s | -1.3383 | +3.8644 | -5.2027 |
| Development33 | SELL | 20s | -1.2968 | +3.7949 | -5.0917 |
| Development33 | SELL | 30s | -1.2674 | +3.8118 | -5.0793 |
| Validation9 | BUY | 20s | -1.0615 | +3.9183 | -4.9798 |
| Validation9 | BUY | 30s | -1.0869 | +3.9171 | -5.0039 |
| Validation9 | SELL | 20s | -1.4872 | +3.8312 | -5.3184 |
| Validation9 | SELL | 30s | -1.5424 | +3.7993 | -5.3417 |

Actual fills fail to beat the random opportunity reference on every daily comparison in both panels. This confirms a toxic-selection gap; it does not prove that random orders would fill or make money.

## Executable Random-Passive Null

The executable null answers the missing counterfactual. It randomizes quote cadence and flat-state side geometry with 32 keyed seeds while retaining the same queue, latency, lifecycle, cooldown, inventory and terminal accounting as the baseline. Side mirror probability is `0.5` and timing jitter is `0.35`. No seed is selected from Development. The same 32 seeds are read once on Validation9.

Frozen daily artifacts:

| Panel | Daily CSV SHA256 | Null summary SHA256 |
|---|---|---|
| Development33 | `d8935e8d2b58c190e120d8d9b877f47a48158fcb431b47eaca1c0e9b33771858` | `645e7efd3694b86c578ae8842c9838da83d6a4bf7edbcd1154732791fd252c48` |
| Validation9 | `3d58c71024f8103fb5510247f4f6b11bfb32ab10057854fccb5170febfa9d06e` | `392cbe40b8376b122d6ee26c3a568bf616897d871f91cf3d690ca3df59a8446e` |

Representative-day Python/C++ parity is exact on four Development days and three Validation days. Every panel has one baseline row plus 32 random rows per day with no missing or duplicate day/arm identity.

Raw and inventory-adjusted results are:

| Panel | Metric | Baseline | Random mean | Random - baseline | Day-cluster 95% CI |
|---|---|---:|---:|---:|---:|
| Development33 | raw PnL | -127.3036 | -124.3251 | +2.9785 | [-22.0778, +28.4772] |
| Development33 | campaign terminal | -65.9036 | -60.3198 | +5.5838 | [-19.3666, +30.8405] |
| Development33 | InvAdj | -63.0404 | -65.4373 | **-2.3969** | **[-3.7684, -1.0596]** |
| Validation9 | raw PnL | -24.7094 | -33.5628 | **-8.8534** | **[-16.2260, -0.6039]** |
| Validation9 | campaign terminal | -9.9983 | -18.4378 | **-8.4395** | **[-15.5517, -0.5297]** |
| Validation9 | InvAdj | -14.7719 | -15.3478 | **-0.5759** | **[-1.1946, -0.0192]** |

The Development terminal interval crosses zero under both day clustering and the stricter day-by-seed bootstrap. Validation terminal is negative under day clustering; its day-by-seed upper endpoint is approximately `+0.009`, so the stronger conclusion comes from raw PnL and InvAdj, not terminal alone.

Activity and tail attribution are:

| Panel | Metric | Baseline | Random mean | Delta | Day-cluster 95% CI |
|---|---|---:|---:|---:|---:|
| Development33 | fills | 16,804 | 17,274.3 | +470.3 | [+255.3, +693.5] |
| Development33 | campaigns | 4,472 | 4,688.3 | +216.3 | [+90.0, +343.5] |
| Development33 | tails | 65 | 65.4 | +0.4 | [-9.6, +9.7] |
| Development33 | inventory time | 5,271.9 | 5,107.6 | -164.2 | [-391.7, +46.7] |
| Validation9 | fills | 3,811 | 3,881.8 | +70.8 | [-1.3, +169.8] |
| Validation9 | campaigns | 923 | 994.2 | +71.2 | [+1.8, +151.8] |
| Validation9 | tails | 17 | 17.3 | +0.3 | [-3.4, +3.8] |
| Validation9 | inventory time | 1,581.1 | 1,431.2 | -149.9 | [-396.5, +71.8] |

Development seed PnL deltas range from `-23.04` to `+22.43`; 21/32 are positive. On Validation they range from `-16.93` to `+4.23`, only 1/32 is positive, and the median is `-9.20`. No seed beats baseline InvAdj on either panel.

The executable null is therefore rejected. Baseline has stable value relative to this null family, but baseline PnL is still negative. This does not establish profitable alpha; it establishes that the current policy is materially better than randomizing the same executable order lifecycle. The submit-time opportunity gap remains a toxicity diagnostic and must not be interpreted as a random-strategy return.

## BUY Fill-Selection Scorer

Four BUY exposure-increasing scorers use expanding walk-forward Development fits, a one-day embargo and frozen Validation9. Thresholds are selected from Development OOF only. The sealed holdout remains unread.

| Target | Threshold | Delta MO20 | Delta MO30 | Delta terminal PnL | Delta bad rate | Delta tail rate | Action gate |
|---|---:|---:|---:|---:|---:|---:|---|
| `non_toxic` | 0.454401 | +0.6598 | +1.2632 | -0.0388 | +0.0664 | +0.0257 | fail |
| `beats_opportunity` | 0.241349 | +0.3826 | +0.8941 | -0.0671 | +0.0890 | +0.0300 | fail |
| `campaign_repair` | 0.690596 | -0.1046 | -0.6909 | +0.0182 | -0.0609 | +0.0072 | fail |
| `opportunity_and_campaign` | 0.010215 | +0.3328 | +0.4523 | -0.0120 | +0.0246 | +0.0038 | fail |

The high-score buckets preserve useful descriptive ranking evidence, but no target improves fill quality, terminal campaign value and tail together. Direct action remains disabled. Hit/miss campaign sets may overlap when one campaign contains orders from both groups, so campaign comparisons are descriptive and not causal action uplift.

## Python / C++ Replay

The parity audit corrected:

- wall-clock L2 refill/cancel/flip semantics;
- true queue-local rank in BUY scoring;
- keyed random-passive sampling;
- activation-time queue rank, Post-Only checks and order timestamps;
- adverse-pause precedence that preserves reducing quotes;
- cancel-request/ACK ordering, including fills before cancel ACK;
- IOC close activation-book and submit-time accounting semantics;
- Python/C++ tick rounding for mirrored and side-policy prices.

The formal parity matrix now passes exactly on representative low-, medium- and high-activity dates:

| ML mode | Date | Python fills | C++ fills | Absolute PnL difference |
|---|---|---:|---:|---:|
| ON | 2026-04-18 | 379 | 379 | `1.24e-14` |
| ON | 2026-05-30 | 189 | 189 | `5.11e-15` |
| ON | 2026-06-10 | 769 | 769 | `8.88e-16` |
| OFF | 2026-04-18 | 397 | 397 | `<=5.33e-15` |
| OFF | 2026-05-30 | 199 | 199 | `<=5.33e-15` |
| OFF | 2026-06-10 | 823 | 823 | `<=5.33e-15` |

A full June 10 baseline/random-passive lifecycle pair is also exact across engines. Baseline has 769 fills, 236 campaigns and PnL `-3.4770`; the keyed random arm has 783 fills, 231 campaigns and PnL `-3.8676`. Order traces, campaign labels and action summaries match. C++ may therefore run formal screening only when its representative-day parity gate passes first.

## Legacy Arm Rankings

The old 48-arm, 512-arm and 1024-arm generations, together with retained39, blocked71 and late4 exact PnL/ranks, are formally withdrawn. Their panels were repeatedly consumed and their identities depended on superseded clock, feature, P3, queue or mixed-L2 semantics.

They are not rerun under the old IDs. Any future global parameter search must create a new family, new arm IDs and a new frozen causal-v5 split. Reusing an old arm name would incorrectly imply continuity of exact PnL.

The maintained exact ranking in this rebuild is deliberately narrow: overlay-free `ML OFF` versus `ML ON`, with current P3, q0.70, normalized L2, repaired trades, fixed latency and fresh-start state. It does not revive the old global parameter families.

## Decision

- Keep the 13-head model and all four BUY scorers as shadow/research artifacts.
- Do not promote a BUY score threshold or direct action.
- Permit registration of a new randomized BUY keep/cancel experiment from the repaired dynamic-fill prediction gate; do not treat that gate as action uplift.
- Keep q0.70 as the conditional queue reference; do not select queue parameters from replay PnL.
- Treat the random opportunity result as a strong toxic-selection diagnostic, not executable alpha.
- Reject the executable random-passive null: it fails Validation raw PnL and InvAdj, despite a noisy positive raw point estimate on Development.
- The formal 42-day denominator was the maintained causal-v5 research identity for this report. New lifecycle, markout and campaign work must use the corrected causal-v7/time-unit identity or a later explicitly frozen contract.
- Keep the sealed holdout closed.

Historical operational note: the 13-head causal-v5 bundle was explicitly placed in live after this research decision as a user-directed trial rather than a statistical promotion. That state no longer describes the current runtime. The dormant configured bundle now points to restart-safe causal-v7, 13-head inference is disabled, and the four rebuilt BUY fill-selection scorers remain disabled as direct actions.
