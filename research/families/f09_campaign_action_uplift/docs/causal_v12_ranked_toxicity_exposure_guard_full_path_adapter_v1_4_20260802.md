# Ranked-Toxicity Full-Path Adapter v1.4

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

v1.4 is an execution-only successor. It was frozen before any 40-day mechanics result, reward, markout, PnL, Validation, or holdout was read. The BUY/SELL action, side-specific past-only p90, 0.5/0.5 behavior policy, frozen seeds, q90-action-OFF baseline, BUY fill-selection parity, reducing-quote invariance, and selective-v3 scorecard remain unchanged.

## Corrected execution contracts

- `on_prediction_bucket()` accepts each completed 10-second prediction once. Duplicate or inconsistent prediction buckets fail immediately.
- `on_quote_decision()` runs for every authoritative quote loop and legally reuses the held score within the same prediction bucket.
- The first untreated baseline-eligible opportunity fixes a stable `prospective_campaign_side_id`. The assignment PRF hashes that identity, so partitioning, checkpoint resume, and runner start order cannot change the assigned arm.
- Untreated eligibility controls assignment and the opportunity denominator; the regenerated candidate role controls actual permission. If path divergence makes the side reducing, the reducing quote remains open.
- Candidate campaign terminals are treatment-dependent and never rerandomize an assignment. Assignment boundaries come only from prospective-lineage changes in the untreated baseline tape. A boundary crossed with a live exposure order fails closed as unsupported mechanics.
- Unknown exchange terminal reasons fail immediately. Cancel-before-activation, activation, partial/full fill, cancel request/ACK, reject, expiry, and shutdown are routed through the authoritative lifecycle hooks.

## Authoritative replay binding

The Python tick replay now supports two explicit passes per UTC day:

1. untreated baseline shadow records the exact decision denominator;
2. candidate replay consumes every baseline decision while regenerating submissions, cancels, ACKs, queue state, fills, inventory, and campaigns.

The baseline index rejects multi-day tapes. A 40-day driver must therefore run 40 independently bounded daily pairs. Journals are written as atomic, hash-verified Parquet parts on the local cache disk; formal adapters do not retain the full decision journal in memory. C++ full-path authority is not claimed.

## Verification and permissions

- targeted execution contracts: `36 passed`;
- full repository suite: `1399 passed, 4 skipped, 1 warning`;
- frozen v1.1 adapter SHA256 remains `5cc1ded739ea6a186026a5759537f8ba306235bb96c008d9b48abc62659ae044`;
- v1.2 and v1.3 artifacts remain unchanged;
- `mechanics_execution_eligible=true`;
- `mechanics_read=false` and the 40-day mechanics run has not been executed;
- economic, prediction, action, Validation, holdout, and live permissions are all false.

- [v1.4 machine amendment](causal_v12_ranked_toxicity_exposure_guard_full_path_adapter_v1_4_execution_amendment_20260802.json)
- [v1.4 machine audit](causal_v12_ranked_toxicity_exposure_guard_full_path_adapter_v1_4_audit_20260802.json)
- [v1.3 predecessor](causal_v12_ranked_toxicity_exposure_guard_full_path_adapter_v1_3_20260802.md)
