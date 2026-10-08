# Exact-Opener P2 v2.1 Collection Preflight Failure

Last materially modified: 2026-08-02

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Status

`collection_preflight_failed_before_tape_enable`

The prospective exact-opener tape was not enabled. A read-only production-path audit found that the frozen v2.1 validator proves the schema and analysis contract, but does not yet prove the runtime producer or storage contract. No prospective tape, PnL, reward, Validation, or sealed holdout was read.

## Why v2.1 Cannot Collect Formal Evidence

The v2.1 validator still returns `prospective_collection_eligible=true`, but it does not hash-bind `strategy/maker_engine.py` or `strategy/order_manager.py`, which create the decision and lifecycle rows. It also does not validate the deployment configuration that supplies the external venues and stablecoin bridge.

Five runtime gaps remain:

1. The public baseline has external venues, the bridge, fair-price shadow, and exact tape all disabled; enabling only the tape is not rejected at startup.
2. Tape writes use the generic CSV logger. A write exception is logged and swallowed, so a formal denominator can lose rows without failing admission.
3. An inherited active order without exact decision context is silently skipped. Hot-start collection therefore needs an explicit quarantine.
4. REST reconciliation can restore a `PENDING_CANCEL` order after a cancel rejection without emitting the lifecycle callback consumed by this tape.
5. The amendment names `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` as the authoritative destination, but the runtime currently writes one append-only CSV without bounded daily rotation, an atomic manifest, or hash-verified transfer/admission.

The exact tape remains intentionally separate from the seven-tape bounded AWS capture. It needs its own admission path; adding it to that capture would alter the frozen P1 collection contract.

## Decision

The v2.1 bytes remain frozen, but their prospective collection eligibility is withdrawn. A v2.2 execution amendment must bind the actual producers and deployment config, fail closed on source and writer-health errors, quarantine hot-start orders, journal every lifecycle branch, and atomically admit bounded daily parts before side-specific 5% support can be counted.

This is an infrastructure blocker, not a negative result for the external edge guard. P1 continues collecting independently toward 30 valid UTC days. P2 has no prediction, action, or live authority.

The machine-readable record is [`external_adverse_quote_edge_guard_exact_opener_mechanics_v2_1_collection_preflight_failure_20260802.json`](external_adverse_quote_edge_guard_exact_opener_mechanics_v2_1_collection_preflight_failure_20260802.json).
