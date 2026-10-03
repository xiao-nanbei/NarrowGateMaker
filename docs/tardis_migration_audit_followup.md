# Tardis migration audit follow-up

[English](tardis_migration_audit_followup.md) | [简体中文](tardis_migration_audit_followup.zh-CN.md)

Last materially modified: 2026-09-22

Last materially synchronized: 2026-09-22

This is an implementation follow-up to the owner-supplied static review of revision `09c13a98`, not a claim that every research family or economic replay is accepted. The review document is owner-local and is not distributed here. Historical mechanisms remain research questions; changing input sources neither validates old positive results nor invalidates all old negative findings.

## Additional audit: dbd8588 (2026-09-22 follow-up)

The later owner-supplied all-family audit uses different D-number meanings from 09c13a98. Do not combine their numbering. It is not fully closed.

| dbd8588 item | Current status |
| --- | --- |
| D01 calendar admission | Consumer now rejects interior missing, duplicate or reordered dates and missing required symbol/channel pairs; selected subintervals remain allowed. Five new synthetic cases cover these boundaries and relocated paths. |
| D02 mixed head provenance | Publisher/loader bind per-head provenance and hashes; the new frozen F03 package is separate from historical models. |
| D03 missing toxicity outcomes | Nonfinite future marks retain unknown labels and invalid outcome ends; regression coverage retained. |
| D04 individual versus packet count | New opt-in live protocol uses native individual trades; actual deployment acceptance is separate and still pending. |
| D05 F05 zero imputation | New public-input route is separate; the historical cleaner still zero-imputes. Do not admit that legacy route as new-protocol training. |
| D06 implicit label liquidity branch | Still requires an explicit successor policy and acceptance. Frozen completed F03 labels/models are not silently regenerated to hide this limitation. |
| D07 peak memory | Bounded readers exist, but replay still materializes arrays; no universal bounded-RSS claim. |

F03 has actual new training and economic evidence, summarized in its [current report](../research/families/f03_causal_13_head/README.md); this does not complete other families' experiments. Native message/queue equivalence, full-calendar/cross-day acceptance, matcher non-double-counting, checkpoint equivalence and the original U-item evidence requirements must retain their scoped status until verified. No synthetic suite establishes those real-data claims.

This follow-up also restored early reference-market rejection in the replay convenience wrapper after its prepare-once refactor had moved the check too late. The rejection regression now runs before input loading again.

## First verified fixes

| Review item | Current action | Evidence boundary |
| --- | --- | --- |
| D01: empty trade bundle clears the ID guard | Preserve the selected market's last nonempty ID high-water mark; reject unreconciled overlap even across an empty bundle | Synthetic overlap/nonoverlap and other-market isolation tests; no new global deduplication claim |
| D05: silently disabled reference market | Reject an explicit reference-market request before loading execution-only inputs | Rejection test plus existing execution-only consumer tests; no new F04 adapter |
| Q04: unknown toxicity markout becomes zero | Nonfinite outcome prices retain NaN and no valid outcome end | NaN and both infinities tested for both sides; finite zero label remains valid |

These corrections do not change the live configuration, launch research runs or overwrite existing artifacts. Tests use controlled fixtures, not licensed market samples. Previously running jobs retain their original source identity; publication is not retrospective validation of their outputs.

## Calibration and model-package binding

D02 is now checked at P3 fitting, label generation and training identity loading: the exact training support, source-manifest identities including prior-day context, delivery parameters, market, touch horizon, distance units and quote tick must agree. Measured latency relocation preserves its content hash. P3's one-second frame timer versus the panel's ten-second timer is intentionally excluded from delivery identity; publication cadence, delay, processing, phase, staleness, seed and tie policy are not excluded. Existing daily receipts already carry the evidence needed by the new fitter; no automatic recalculation follows from this validation change.

D03 is now checked at publication and model loading. All thirteen metadata files must identify their own head and the same input/label/split contracts, source parents, train-only selection specification and weighting policy as the requested training contract. Per-head outcome targets and valid-row counts may differ. The model root is atomically published only after all heads validate, binds both model and metadata bytes, and is rechecked on loading. Missing old metadata is not promoted to a compatible package. Negative tests include a different half-life, selection specification, source, label contract and a metadata substitution with an updated file hash.

## Confirmed work still pending

D04 now has a strict default F05 loader with explicit source/training/label identity, ordered booster columns and reject/native-NaN semantics. Old defaults require `load_legacy()`; the historical shadow evaluator requires explicit legacy selection. This does not complete opportunity-feature generation or train a new F05 model. Family-specific boundaries and pending adapters are recorded in [Research input migration](../research/INPUT_MIGRATION.md).

The review's B01 accounting integration, B02 five directly consumed outputs, Q01 provider-group atomicity, Q02 event-count equivalence, Q03 producer semantic identity and Q05 complete order-lifecycle checks are not certified by this patch. Unknown funding/valuation must remain unknown; do not force thirteen heads into quote channels merely to claim usage. Historical entrypoints and family/blog applicability annotations still require scoped follow-up, not a blanket rewrite of frozen conclusions.

The next F03 implementation priority is execution/accounting integration after the new calibration, label and model checks. The active experiment's own plan remains authoritative; no old B0, ML-OFF or reference-market requirement is restored by this review.
