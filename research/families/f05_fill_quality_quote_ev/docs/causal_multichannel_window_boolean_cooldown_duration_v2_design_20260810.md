# Causal Multichannel Boolean Cooldown Duration v2

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: `execution v7 bound; 2025 predicates and 41-day strict-native source union admitted; formal labels running`

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Identity: `causal_multichannel_window_boolean_cooldown_duration_v2`

Spec SHA256: `7064c12f1872c0ac7f9d07d15dd60dcc9053360256f309fe5d67fd96834b784f`

Feature semantics amendment SHA256: `5d24db38bd0511db441ded022b7e402ea15dd26ea5f5df22e988cf50ec0de685`

Execution amendment chain SHA256:

```text
v1  70f765b50cb6271aa9033f7a236d3f416d07303aedb56830c1cbe1636085ee41
v2  1086b39cd5f26f0e678c47d1885bbb4b99c88d129866a6a5ce4d21b0953993a7
v3  58949b176c9f810881e887a4fcab56debd6941ca22ce9b31d66d065bc0e7e829
v4  5ab2371de9e736d2f1d45e79a931ebe6073a83bb17fbbcb3cb148c8b343ceb9f
v5  67436ebe25da5963c881f665b96cedc79ebc6a8bee93620149d6657b20dd20ed
v6  7e333deee8e42df61889bbc42753a0fc26000f013df5b523a0e70aaf59e8ebea
v7  5b9ee2732a837e13817c2adf2581e6488dd74114df00942a0960ed5e38710c12
```

## Question

This successor asks:

\[
\text{causally visible market, order, and inventory state}
\rightarrow
\text{cooldown duration}
\rightarrow
\text{complete campaign USDC value}.
\]

It is not a direction classifier, a single EMA-pair search, or an `ADD NOW / WAIT ONE EPOCH` study. BUY and SELL are learned and promoted separately. Reducing quotes are never changed.

## v1 Boundary

The immutable v1 result closes only:

```text
BBO-mid-only
+ 10 EMA / 45 pairs
+ 360 fixed predicates
+ bounded sparse DNF
+ 8 durations
+ one-shot continuation
+ normalized-L2 modeled queue labels
```

It does not close multichannel EMA state, strict-native labels, repeated-policy economics, or prove that 85 seconds is optimal. The v1 reports and negative OOF numbers remain unchanged.

## Frozen Action

An opportunity occurs at each strategy-visible exposure-increasing fill callback. Each callback, including separate partial-fill callbacks at the same exchange timestamp, creates its own ordinal and immutable lineage revision. Its quantity is the callback's incremental fill quantity.

The deployed control is accurately represented as:

\[
\tau_0=85\text{s}\times\max(1,n),
\]

where \(n\) is the consecutive same-side fill units after the callback. The one-unit floor matters for an initial sub-unit partial fill. Any opposite-side fill retains the current reset ABI.

The eight v1 duration arms are reused without re-selection. A fixed arm is the total duration from the target fill-visible timestamp and is not multiplied by \(n\). Expiry restores exposure-quote eligibility; it never forces an order.

## Causal Window

The base window is 100ms because this is the admitted BBO/L2 data grid. It is not an economic horizon or a policy cadence.

```text
window:             [left, right)
book source row:    final accepted native event timestamp inside the bucket
canonical mapping:  floor(source_ts / 100ms) * 100ms + 100ms
partial window:     excluded
visibility:         feature_ready_ts_ns <= fill-visible cutoff
explicit audit N:   864,000 windows (24h)
EMA:                standard continuous-time recursive state
gap/stale:          UNOBSERVED, no forward fill
restart:            restore hash-bound checkpoint, otherwise CONTROL_85N
```

The normalized book artifact does not store the canonical right edge in its `timestamp` column. It stores the final accepted source event timestamp in each nonempty bucket. The extractor therefore maps that timestamp to the bucket's right edge. A missing bucket remains unobserved and is never forward-filled; an event exactly on a boundary belongs to the next bucket.

The explicit 24-hour buffer is an audit/source-support boundary. Older state may enter only through the bound recursive checkpoint. Every EMA fast/slow pair is formed within one physical signal; cross-unit EMA pairs are forbidden. Separately normalized channel predicates may still appear in one AND clause when they are evaluated at the same admitted 2026 assignment cutoff. The 2025 provider-book and exchange-trade reference frames are never joined to create that authority.

## Feature Blocks

`R0` reproduces the narrow BBO-mid Boolean interface and is diagnostic only.

`M0` binds action magnitude and lifecycle context: side, opener/add, pre/post inventory, fill quantity, consecutive units, control duration, campaign age, add count, MAE-to-date, inventory-time-to-date, same/opposite fill ages, and current cooldown owner/deadline.

`M1` adds same-generation BBO state: mid, spread, top quantities, imbalance, microprice deviation, EMA levels/slopes, all-pair ordering, cross age, persistence, distance, curvature, expansion, and convergence.

`M2` adds visible individual-trade and depth state: aggressive BUY/SELL volume, signed flow, trade count, terminal run/age, top-20 depth, depth imbalance, side-specific cumulative-depth slope/convexity, and consecutive-complete-bucket displayed-depth increases/decreases. `is_buyer_maker=false` maps to aggressive BUY. Exact depletion/refill attribution, price impact/absorption, target-price displayed quantity, and provider queue state remain explicitly unobserved until their causal formula and source authority are frozen; they are not emitted as invented zeroes.

`M3` is reserved for separate ablations of causal-v12, q90, BER/markout, and external venues. Those blocks cannot be silently mixed into the first v2 candidate.

## Boolean Semantics

Every literal is three-valued:

```text
TRUE / FALSE / UNOBSERVED
```

In particular, `NOT UNOBSERVED = UNOBSERVED`. Missing cross history can never become a true death-cross condition through negation. The policy remains an ordered first-match list: literals form AND clauses, clauses form OR rules, and each rule has a side-specific duration consequence.

Outcome-blind 2025 BBO/L2/trade distributions may set scales, missingness support, and predicate candidates. Inner chronological folds may select among predeclared candidates. Economic outcomes may not influence the 2025 source manifest or threshold universe.

The frozen 2025 book/trade materialization completed all 134 source parts and was atomically admitted as artifact `25b8918a45989972d21d27304faff493ef9d6f4ea36e824d3372619ab044a9b2`. Its admission manifest SHA256 is `542407f30e7d105657b58bf3a9fb57068b759ac9b1ebcc187dacefbf5cec81d8` and the study predicate bundle SHA256 is `ba4c1bac2380564aa24d47d12796f3be5c0312cc88d28218ce84bd20e4170f37`. This artifact is outcome-blind and grants no action or live authority.

## Source Readiness

`CooldownAssignmentSnapshotV2` is now emitted atomically by the Python replay at the actual strategy-visible exposure-fill callback. It binds M0/M1/M2, source cursors, market/depth generations, exchange/feature-ready clocks, and per-field support/fallback state. The historical profile remains exchange-time exploratory evidence because private fill receive time is unavailable.

The frozen 50-day denominator currently separates into:

| Identity | Days | Permission |
|---|---:|---|
| `full_D_minus_1_D_D_plus_1` | 41 | formal one-shot strict-native labels |
| `reduced_source_diagnostic` | 9 | separate diagnostic only; never pooled silently |

The nine reduced dates are `2026-04-20`, `2026-04-23`, `2026-05-06`, `2026-05-13`, `2026-05-31`, `2026-06-03`, `2026-06-26`, `2026-06-29`, and `2026-07-10`.

Execution amendment v6 binds the existing outcome-blind native sequence audit to this denominator. All 41 formal target days have strict sequence support on both D and D+1. `2026-04-20` and `2026-04-23` remain reduced because their D+1 days contain sequence gaps; `2026-05-06` and `2026-05-13` remain reduced because their D+1 rows were not confirmed by the upstream audit. The gap on `2026-04-21` does not remove formal target `2026-04-22`: that date is D-1 warmup only, and the book must be snapshot-seeded again before the target boundary. Current cache bytes are still revalidated during execution.

Historical 50-day exact AWS receive-time depth does not exist. Sampled Tokyo visibility remains useful exploratory transport, but it cannot grant action or live authority.

## Strict Labels

The one-shot label stage must use Python, raw CryptoHFT snapshot/delta, individual trades, a complete previous-natural-day 24-hour warmup, strict queue mode, and sampled top-N cancel-ahead disabled. The baseline parent stops creating new assignments at the target-day UTC boundary. D+1 is appended only so already-forked children can reach washout; no child is force-terminated at midnight.

All target descendants must be terminal, both compared paths must be flat, and no submit/cancel/ACK, queue cursor, age, or hazard ownership may remain. A path still open at the D+1 boundary is retained as right-censored. Its contemporaneous mid/executable marks are diagnostics, not terminal bounds and not point-label OOF inputs. Complete-case deletion is forbidden.

The strict runner prebuilds each unique D-1/D/D+1 raw hourly artifact under one owner, validates every artifact and source counter through a read-only strict scheduler, and then forks strategy arms with cache fallback disabled. The current formal union contains 57 unique source days and 1,368 hours in eight segments. Only overlapping closed D-1/D/D+1 windows may coalesce; calendar adjacency alone may not. At every target boundary the book must already be initialized and snapshot-seeded, after which all strict source-counter deltas through D+1 must remain zero. Per-hour locks and atomic rename preserve one canonical artifact when segments overlap or an interrupted run resumes.

Execution amendment v3 hardens the formal join identity. The opportunity key now includes the partial-fill ordinal and incremental fill quantity. Each assignment row retains the complete immutable snapshot payload and its SHA256, and admission recomputes that digest rather than trusting projected columns. Source contract, source bundle, execution hashes, side, role, quantity, baseline duration, exchange/visible/assignment clocks, and replay order identity must agree across the opportunity, snapshot, and all eight arms. Joined feature state is not allowed to overwrite explicit opportunity identity.

## Compute Boundary

The Python baseline reaches each opportunity once. A POSIX copy-on-write supervisor forks the eight side-specific duration arms at that exact fill frame and admits their files atomically, with at most two arm children resident at once. This is a local POSIX execution mechanism, not a claim of portable checkpoint serialization.

The admitted parent-stop benchmark found 551 target-day opportunities: 207 BUY opener, 199 SELL opener, 76 BUY add, and 69 SELL add. The first 48 opportunities produced 384 arm paths and covered all four side-role cells: 19 BUY opener, 15 BUY add, 13 SELL opener, and 1 SELL add. Twenty-two opportunities were exact on all eight arms. The other 26 remained in the mechanics denominator but were excluded from exact-label learning because one or more arms failed closed on queue ambiguity or invalidation. Missing queue seeds, source gaps, fail-open bundles, pending supervisors, and post-boundary assignments were all zero. The durable benchmark receipt is [`causal_multichannel_window_boolean_cooldown_duration_v2_benchmark_v10_admission_20260811.json`](causal_multichannel_window_boolean_cooldown_duration_v2_benchmark_v10_admission_20260811.json). It is engineering evidence only. Because it predates the v3 formal join identity, none of its opportunity or arm files may be reused as a formal label or formal identity artifact.

Formal economics is generated once from the `M2` superset snapshot on the 41 full-support dates. R0, M0, and M1 reuse the exact same opportunity and eight economic labels; they do not replay the order path again. UTC day is the outer parallel unit. The first parent benchmark used about 12.4 GiB, so the formal multi-day runner is frozen at one day worker; only the two copy-on-write arm children may overlap. A formal day-worker cap of two remains available only for a separately measured execution, and was not changed by prebuild amendment v2. Both intra-day event progress and per-day atomic status are persisted.

## Exploration and Deployment

Nested chronological exploration compares R0, M0, M1, and M2. Every outer fold must execute the best supported nonbaseline rule frozen by its inner folds. Deployment-level LCB gates are applied only after untouched outer OOF; they may not erase the candidate before the test.

Feature-family selection is hierarchical rather than an unrestricted best-of- four search. R0 is reproduction-only. M0 must first pass its absolute post-OOF gate; M1 may replace M0 only when M1 also passes and the paired day-clustered `M1 - M0` lower bound is positive; M2 is treated analogously against M1. The two incremental comparisons use a frozen Bonferroni familywise confidence contract, separately for BUY and SELL. Statistical gates and effective gates are reported separately, so unresolved partial identification cannot be hidden by a positive exact-subset result.

A continuous-state comparator is required before any unified Boolean policy can be frozen. Execution amendment v4 implements it as a raw-state, multi-output regression-tree diagnostic over the same eight-arm labels and outer OOF opportunities. Its two frozen capacity candidates are selected only inside inner chronological folds. It cannot replace the Boolean policy or grant action/live authority; a positive paired lower bound for continuous minus Boolean value instead blocks freezing the weaker Boolean policy. The implementation is complete, but the comparator remains economically unrun until the formal labels and predicate bundle are admitted.

All opportunities remain in the mechanics denominator. Point-label OOF is explicitly restricted to opportunities where all eight side-specific arms have strict-native labels and common washout. This is a disclosed common-support filter, not evidence that unsupported outcomes are ignorable. If any unsupported opportunity remains, exact-subset OOF is exploratory only until a frozen partial-identification sensitivity or equivalent identifying evidence exists.

One-shot labels cannot be summed into policy PnL. If a BUY or SELL policy passes OOF, freeze one unified policy bundle and then run a separate repeated-policy engine in which every later eligible fill invokes that policy. Required runs are single-day mechanics, BUY-only, SELL-only, joint BUY+SELL, paired 50-day, and restart-aware continuous replay.

## Current Permissions

```text
economic outcomes read:       false
engineering benchmark labels: generated, not read
formal 41-day labels:          false
nested OOF run:               false
unified policy frozen:        false
repeated policy run:          false
Validation/holdout read:      false / false
research supported:           false
action/live authorized:       false / false
```

The 48-opportunity engineering gate is complete and durably admitted. Every opportunity produced exactly one complete side-specific eight-arm bundle. An arm with invalidated or ambiguous queue evidence remained unsupported with the matching reason and cannot enter the learner; it did not invalidate unrelated exact opportunities. The benchmark remains engineering-only under amendment v3. Execution amendment v5 now binds raw D-1 warmup admission instead of inferring it from a calendar cutoff, makes the bounded same-side cooldown owner a real M0 learner input, requires acted campaign/day support in outer OOF, and preserves same-cutoff book/trade clauses. The interrupted pre-v5 manifests are not admissible under the new identity; their raw hourly source bytes remain reusable. Execution amendment v6 additionally binds the 41/9 sequence-support mapping and replaces the erroneous adjacency-coalesced source prebuild with the eight-segment overlap-only plan. Execution amendment v7 binds the strict-label consumer to the same v3 target-receipt ABI; v2 receipts now fail closed instead of being accepted after the producer moved to v3. Fresh 2025 book/trade predicate materialization is now atomically admitted on `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}`, and the real `2026-04-22` v3 source receipt passed all strict counters. Neither step read economic outcomes. The full 41-day source union is now admitted under manifest SHA256 `e8de50439966ddce23561fed16ab8b47f6e35789bb4b7a5f64e125236bc07df5`. All 8 overlap-only segments, 57 source days, 1,368 hours, and 41 target receipts validated, with every frozen strict source counter equal to zero. The source run executed no arms and read no economic outcomes. Formal labels continue to use the v4 opportunity and v2 label-panel schemas. Side-specific nested OOF follows only after both artifacts are admitted. No repeated policy, action, or live permission exists yet.
