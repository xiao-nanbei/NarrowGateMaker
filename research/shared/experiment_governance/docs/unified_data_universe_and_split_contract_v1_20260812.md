# Unified Data Universe And Split Contract v1

Last materially modified: 2026-08-12

## Decision

NarrowGate has one append-only research data universe, but it does not have one universal experiment day list. Every UTC day is a row in a capability ledger. An experiment declares the capabilities it needs, and its denominator is the deterministic intersection of those requirements with the ledger.

This separates five objects that were previously conflated:

1. physical source archive;
2. daily capability and quality ledger;
3. experiment-eligible denominator;
4. chronological evidence split and OOF folds;
5. full-path execution denominator.

Changing any object after reading its outcomes creates a new identity. A runner must not contain an unexplained private date list.

## Daily Capability Ledger

The master ledger is append-only and records at least:

- UTC day and immutable source hashes;
- source authority: native exchange, provider-normalized, or deployment-host receive-time;
- trade, BBO, depth, external-venue, and private lifecycle availability;
- exchange, receive, and feature-ready clock support;
- native sequence continuity and exact queue support;
- previous-natural-day warmup validity where required;
- grade and rejection reasons per capability, rather than one universal grade;
- permissions for prediction fitting, provider sensitivity, native mechanics, strict queue economics, and live transport.

Artifact presence alone never grants a permission. In particular, the Tardis provider-normalized book has no Binance `U/u/pu` sequence authority and no AWS receive-time authority.

## Mandatory Experiment Binding

Every new experiment must emit a `narrowgate_experiment_dataset_binding.v1` manifest and pass `models.audit.dataset_governance`. The binding records:

- master-universe manifest path and SHA256;
- required capabilities;
- eligible and excluded days with reasons;
- Development, embargo, Validation, and sealed-holdout dates;
- every chronological OOF train/test fold;
- training-window rule and source-pooling rule;
- full-path execution denominator, if applicable.

The report must state the number and exact dates for all of these. `OOF=40` or `OOF=50` without an emitted fold manifest is not an admissible statement.

## Canonical 50-Day Rule

For every new daily-fresh-start full-path action, fill, queue, campaign, or PnL study, the execution denominator is the frozen 50-day successor in:

`research/families/f10_live_replay_attribution/docs/current_live_held_ber_replay_baseline_50d_spec_20260810.json`

The immutable prefix 40, added 10, and pooled 50 must be reported separately. Historical 40-day studies remain historical facts and are not rewritten.

If a required source is absent on part of the 50-day panel, the experiment may use only the qualifying subset when all exclusions and capability reasons are frozen before outcomes. That result is a reduced-support identity and must not claim to be the current 50-day baseline.

OOF is not synonymous with this execution denominator. OOF is a prediction or policy-learning protocol inside Development. A frozen policy is subsequently run on the canonical execution denominator. If the OOF labels themselves require strict native support, their test-day count may be below 50, but the manifest must say exactly why.

## Training-Window Rule

The default training candidate is:

```text
all eligible historical days strictly before the current fold cutoff
```

That is an expanding chronological window. It is the default because it uses all causally valid information without selecting a calendar boundary from outcomes.

`2025 only`, `2025 plus January 2026`, and other year/month cutoffs have no automatic authority. They are legal only when the boundary is caused by a source, feature, label, market-structure, or baseline epoch change, or when a window/decay alternative is selected entirely inside nested chronological training folds.

When drift is material, compare expanding history with recency-weighted or rolling alternatives inside inner chronological OOF. Freeze the selected rule before outer OOF. Do not choose the window after inspecting outer OOF, Validation, holdout, or full-path PnL.

## 2025 Data Rule

Use 2025 automatically whenever it satisfies the experiment's required capabilities. Do not wait for a user instruction to include it.

For source-compatible prediction targets, 2025 and 2026 eligible days may enter an expanding source-aware training panel. When 2025 provider data and 2026 native data differ in source authority, they must not be naively pooled as one source. Permitted uses include:

- unsupervised feature or predicate normalization;
- source-stratified prediction fitting;
- auxiliary pretraining followed by current-source calibration;
- provider-specific sensitivity and transport analysis.

Provider data cannot supply native exact-queue, order-lifecycle, action-value, or live-transport labels. Final current-live calibration and action economics must use the authority required by the target.

The 2025 archive is already used. F03's expanded source-aware causal training spec includes 2025 provider days, and F05 used 112,090,884 admitted 2025 rows for unsupervised source-aware normalization. It is not merely idle storage.

## Chronological OOF Rule

OOF folds are expanding and chronological:

```text
past train -> horizon-derived embargo -> future test
```

All OOF train and test days belong to Development. Validation and sealed holdout never participate in model, threshold, rule, feature-family, duration, or training-window selection. Test days cannot repeat across outer folds, and the fold manifest must emit its actual test-day denominator.

When model class, source weighting, feature block, or training window is being selected, use nested chronological OOF. The outer fold evaluates the candidate chosen using only its inner training history.

Embargo length follows the maximum future information used by the labels, reward, lifecycle washout, or feature construction. It is not copied from an unrelated experiment.

## Storage Rule

Raw and normalized source archives are evidence, not cache. They are immutable and are not deleted by LRU. Frozen manifests and source hashes are also not LRU objects.

Hard-linked research unions do not duplicate source payload blocks. Derived feature panels, model overlays, assembled windows, and reproducible training caches are cache and belong under tiered LRU governance.

The 2025 Tardis archive contains 306 source files and 37,870,428,980 bytes. Current derived source-aware feature roots add material storage, including an approximately 12 GiB trade-feature panel and a 2.5 GiB causal-feature panel. Those derived artifacts may be migrated or evicted when unreferenced; the raw 2025 archive remains retained evidence.

## Current Snapshot

The current provider-normalized coverage ledger spans 2025-08-01 through 2026-07-30. The owner excluded 2026-07-30 because its source payload is absent:

- 363 complete normalized days;
- 180 provider-normalized replay candidates across the full period;
- zero provider days with exact-queue or policy-visible authority.

The canonical current full-path Development baseline contains 50 native days: 34 Grade A and 16 Grade B. These numbers describe different capability layers and must not be substituted for one another.

## Migration Rule

Historical experiments retain their frozen panels and conclusions. This contract governs new identities and successors. A successor may reuse an old artifact only when its source, clock, feature, label, and permission hashes are compatible; otherwise the experiment regenerates the affected layer from the master universe.
