# Legacy L2 Evidence Revalidation

[English](legacy_l2_evidence_revalidation_20260725.md) | [简体中文](legacy_l2_evidence_revalidation_20260725.zh-CN.md)

Last materially modified: 2026-09-14

Last materially synchronized: 2026-09-14

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](public_private_documentation_contract.md).

Date: 2026-07-25

Status: evidence-governance decision. No strategy or live-policy change.

Historical completion update (2026-07-27): normalized P3, feature/model, BUY scorer, lifecycle/null and strict replay rebuilds were subsequently executed, followed by the broader time/calendar/unit repair. Their model identity at that time was `causal-v7`, not causal-v5/v6; this does not identify today's deployed or newly trained model. The withdrawal classifications in this document remain authoritative; the plan near the end is historical and must not be read as a current pending-work list.

## July generated-input retirement (2026-09-14)

The owner retired five additional derived bundles. The historical documents and mechanism implementations remain; this is input retirement, not a new economic experiment or a blanket rejection of those research families.

| Retired bundle | Affected research and limitation |
| --- | --- |
| `features_btcusdc_causal_v10_minimal141_context_20260728` | F06 placement-feature context; old feature support/labels cannot stand in for the new calendar's rebuilt inputs. The CLI now requires an explicit compatible context. |
| `trade_features_causal_v3_20260727` | F09 toxicity/P3 action diagnostics; old trade-tempo inputs and results remain historical, not new-source verification. An explicit trade manifest is required, including in spawned workers. |
| `trade_features_causal_v5_expanded_20250801_20260725` | F03 historical source-coverage/profile inputs; dated profile paths remain historical descriptors but the removed payload is unavailable. Never relabel a new-source rerun as the old result. |
| `model_features` | Existing F04 external-venue v2/v4 feature caches, not the current 13-head model bundle. Keep generation/mechanism code; regenerate and bind new inputs before re-evaluating prediction or execution claims. |
| `replay_l2_deep250_snapshot100ms_probe_v1` | Historical deep-book engineering probe; not current queue, feature or economic evidence. |

No additional timing-leakage claim is inferred solely from deletion. Earlier documented distortion/clock/label defects retain their original scope. A new-data study must explain the relevant old defects, rebuild features and labels with actual outcome-boundary checks, and compare simulation/fit/economic changes under its declared protocol before updating public conclusions or blog claims. Previous-use and locked-result permissions are not reset. Current raw data, purchased archives, active book reconstruction and already staged fleet inputs are outside this retirement.

## Retirement and research impact (2026-09-13)

The owner-authorized deletion of the shared derived `bbo/` directory is complete: 256 files, 128 dates per symbol for BTCUSDC and BTCUSDT, spanning 2026-01-01..2026-07-20. Historical documents remain reference material. Current owner-manifest/readability indexes and the active 48-day book rebuild did not reference this directory. Its former optional BTCUSDT reference-feature lookup is not proof that these files should remain current; after deletion, missing optional BBO inputs must not be described as rebuilt reference-book features. Acquired L2/trades require their own derived-feature build and binding. This deletion is not a same-row equivalence validation against the new sources.

| Historical research/result | Status for current use | Recorded cause / surviving scope |
| --- | --- | --- |
| F02 empirical P3 calibrated from the old root on 2026-07-15 | Old input/artifact superseded; do not reuse its identity | Mixed cadence/depth. The later 100ms recalibration reproduced the touch curve within 0.2%; this is not proof that the P3 idea failed. |
| F03 old causal features, 13-head diagnostics and ML A/B built on the mixed book | Materially distorted / historical reference only | Inconsistent book cadence/depth changes book features and execution paths. The retired causal-v2 bundle also has 119 declared versus 123 physical day files and old split/label semantics; see the [F03 retirement note](../research/families/f03_causal_13_head/README.md#retired-causal-v2-feature-bundle-materially-distorted--historical-reference-only). |
| Causal-v4 test/all-122 ML A/B and blocked-cross-fit BUY scorer values | Withdrawn; not current model-selection evidence | Three causal-v3 feature dates exposed five-minute metrics early. This separate verified feature defect is not attributed to every old BBO row. |
| Pre-event-L2 BUY widen, SELL one-cycle skip and side-specific action tables | Exact DR/fill/intervention/campaign values withdrawn | Superseded event/order denominators; historical non-promotion remains. Deletion does not reopen a closed family. |
| Retained111 lifecycle/FIFO statistics and historical random-null/direct quote-EV/cap-compression/markout-sign values | Exact numerical results withdrawn or requiring recomputation | Mixed or superseded book/order/clock paths; short markout is still not full lifecycle value. |
| F04 xmarket/spot/global Stage-0 maker markout and campaign tables using the old local BBO denominator | Maker numeric values withdrawn | External trade-only states, price-direction diagnostics and consensus construction are not invalidated by this book defect. |
| Pre-repair 48/512/1024-arm rankings and gamma/cap/guard/cooldown winners | Inadmissible for current selection | Older book, clock, queue, P3, model and unit identities; old winner IDs are not new candidates. |
| Top20 + q0.70 keep/cancel/rearm/recovery results | Conditional historical evidence, not native deep-queue truth | They avoided the 1s defect but retain their original finite-depth/queue assumptions. |
| A historical model that merely used BTCUSDT BBO reference features | Old-input result; replacement-data revalidation required | A dependency alone does not establish the sign or size of distortion. Do not invent model-specific PnL changes without the bound input/result evidence. |

The detailed support is in the sections below and the [historical backtest revalidation register](../research/families/f10_live_replay_attribution/docs/historical_backtest_evidence_revalidation_20260720.md). These are existing recorded findings, not newly executed economics. Native-input studies, independent trade-only diagnostics, real live observations and implementation-correctness results are not blanket-invalidated. No sealed outcomes were opened, no previous-use records changed, and no new economic comparison ran during this retirement.

## Decision

The former top-level `bbo/` and `l2/` roots were a mixed data identity:

- most early BTCUSDC files were approximately 1-second, top-10 states;
- recent files were approximately 100ms, top-20 states;
- research code could select the mixture through a single default path.

They were removed as defaults for BTCUSDC replay or feature generation. The normalized BTCUSDC default selected at that historical migration was:

```text
${NARROWGATE_RETIRED_DATA_ROOT}/normalized_l2_100ms_v2/
```

Exact visible-level queue research must additionally stream the native CryptoHFTData snapshot/delta source. A 100ms top-20 matrix is not deep queue.

## Measured Distortion

This is not only a metadata problem.

- Rebuilding the same day from approximately 1-second to 100ms states changed observed cancel and refill path counts by about 3.64x and 3.94x.
- The fitted diagnostic adverse/cancel/refill half-lives moved from `1000/1000/1000ms` to `1000/500/101ms`.
- On a one-day probe, changing top-20 queue evidence to native deep-250 changed fills from 1,595 to 2,062; only two decision IDs overlapped.

Therefore old order, fill, queue, campaign, and PnL paths cannot be repaired by renaming the input directory. They are different counterfactual trajectories.

## Must Rebuild

### Empirical P3

The 2026-07-15 artifacts explicitly read the old top-level `bbo/` root, so their input identity remains superseded. Recalibration on the frozen 100ms BBO identity has now completed, however:

- 5s `kappa_eff`: `0.08311357 -> 0.08325351` (`+0.168%`);
- 10s `kappa_eff`: `0.06743811 -> 0.06735643` (`-0.121%`);
- 5s and 10s `delta_star` were unchanged on the 0.1 USDC grid.

Thus the P3 touch conclusion is revalidated under the new input identity, while the old artifact/hash is not. See `p3_touch_recalibration_normalized100ms_v2_20260725.md`.

The controlled fixed-spread probe is not invalidated by this withdrawal: it bypasses the P3 quote floor and assigns distance directly from the 100ms same-side BBO.

### Causal Features And Models

The pre-v2 causal feature bundles contain microprice, L2 imbalance, refresh/cancel, and related book fields generated from the mixed root. Rebuild in this order:

1. causal features with explicit bucket-ready time and v2 data identity;
2. the 13-head model bundle;
3. strict ML-OFF versus ML-ON replay;
4. BUY fill-selection order-level panel, scorer, threshold, and action gate.

The BUY scorer that was current when this audit was written was not revalidated merely because its runtime code was correct; it was subsequently rebuilt and still failed its joint action gate.

### Any Reopened Global Or Action Family

Pre-repair 48/512/1024-arm rankings, retained/blocked/late PnL tables, and old gamma/cap/guard/cooldown winners remain archived. They also contain older clock, queue, P3, model, and unit identities. If a family is reopened, it must be paired against the corrected baseline on the new identity; old arm IDs are not reusable candidates.

## Withdrawn Or Conditional Numeric Evidence

- The 2026-07-18 BUY widen, SELL skip, and earlier side-specific action-uplift tables predate the event-L2 contract. Their DR uplift, fills, intervention rate, and campaign values are withdrawn. Their conservative `do not promote` decisions remain safe.
- The retained111 inventory-lifecycle counts, 5.8-minute FIFO median, and 92.1% 30-second survival estimate should be recomputed. The conceptual conclusion that 30-second markout is early toxicity rather than complete lifecycle value remains plausible.
- Historical xmarket/spot/global Stage-0 maker markout and campaign tables used the old local BBO denominator. Their exact maker values are withdrawn. The external trade-derived 1-second states, 2-of-3 construction, and leave-one-venue-out architecture do not depend on the mixed BTCUSDC book. Stage 0 therefore says only that the old experiment did not establish a maker action; it does not prove that external information has no value.
- Queue keep/cancel, rearm, and recovery experiments using 100ms top-20 avoided the 1-second cadence defect. Their numbers remain valid only under the declared `top20 + q0.70 fallback` counterfactual identity, not as native deep-queue estimates. Their non-promotion decisions remain safe.
- Development dynamic-mechanism attribution that combines top-20 replay with the old P3 is approximate. Its live-log attribution remains live evidence.
- Old random-opportunity/executable-passive null, direct quote-EV, cap-compression, and markout-sign values depended on superseded order denominators, clocks, or replay paths. Their method definitions remain useful, but their historical numeric tables are archived.

## Evidence Not Invalidated

- native snapshot/delta scheduler and native strict-62 universe;
- `queue_value_net_hazard_keep_cancel_v2`, which used native exact-level snapshot/delta plus individual trades;
- `dynamic_fill_hazard_m0_native_strict_nested_cal_v2` and its one-time BUY Validation read;
- the paired `paired_fixed_spread_monotonic_v2` execution-geometry diagnostic;
- external fast 1s/3s price-direction diagnostics built from trade-derived states rather than BTCUSDC L2;
- accounting, variance-unit, terminal-MTM, feature-ready, and merged-clock correctness repairs;
- Python/C++ same-input implementation parity;
- live loss attribution, receive-time capture, and AWS Tokyo latency/soak measurements;
- OPE, SPIBB, scorecard, experiment-registry, and promotion methodology.

## Storage Migration

On 2026-07-25:

- BTCUSDC replay and feature defaults moved to `normalized_l2_100ms_v2`;
- 250 independent legacy `l2/*.parquet` files were deleted;
- about 2.39 GiB was released;
- six 100ms hard-link anchors were retained because frozen strict views still reference them;
- all 62 formal v2 days passed size and SHA256 verification afterward;
- the old top-level `bbo/` was temporarily retained because BTCUSDT bridge and superseded P3 manifests identified it; that temporary retention ended with the owner-authorized 2026-09-13 deletion above.

The migration audit also closed four recurrence paths:

- CryptoHFTData rebuilds default to a versioned staging root, not top-level `bbo/l2`;
- formal replay validates every BBO/L2 context day it actually loads;
- P3 defaults to `normalized_l2_100ms_v2/bbo`;
- `MM_BBO_DIR` and `MM_L2_DIR` must be supplied together.

Separately, eight BTCUSDC individual-trade files for 2026-07-04 through 2026-07-11 were found to have a corrupted all-`true` maker-side column. They were replaced atomically from Binance Vision and now contain both taker directions. The fixed-spread runner records and checks a separate execution trade quality identity before reading outcomes.

The native CryptoHFTData archive was not modified.

## Historical Required Rerun Order

1. Complete the 128-day fixed-spread broad curve.
2. Confirm the broad curve with a separate native deep replay.
3. Rebuild the order-level denominator and lifecycle counts, including FIFO/LIFO survival, random null, and markout/campaign decomposition.
4. Rebuild causal features and the 13-head model.
5. Retrain the BUY fill-selection scorer.
6. Run strict baseline, ML A/B, and queue sensitivity.
7. Rerun only action families still economically relevant.

The maintained rebuild completed under later identities, but completion did not rehabilitate the old exact values. Old non-promotion decisions may still be cited as conservative governance outcomes; old PnL, fill, queue and model calibration values remain inadmissible for current selection.
