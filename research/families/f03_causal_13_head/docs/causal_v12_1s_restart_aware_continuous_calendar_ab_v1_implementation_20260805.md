# F03 1s Restart-Aware Continuous-Calendar A/B v1

Last materially modified: 2026-08-05

Status: `preflight_and_runner_skeleton_implemented_candidate_unbound_results_closed`

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Scope

This successor prepares a paired comparison between:

- control: current v9 causal-v12 at the existing 10-second cadence;
- candidate: the full-schema 13-head F03 1-second successor.

It covers every UTC day from 2026-04-17 through 2026-06-26. All 71 dates are trading dates. The daily source resolver currently admits 52 native days and 19 provider-normalized sensitivity days; no whole day is converted into a maintenance placeholder.

The implementation is outcome blind. It does not call the tick engine, define an economic result schema, aggregate a PnL field, or grant execution, action, live, Validation, holdout, or baseline-replacement authority.

## Continuous Semantics

Both arms consume one frozen restart timeline with 107 merged intervals. At a frozen gap, the executor contract requires observable cancel drain when the left boundary exists, then clears orders, queue state, pending cancel state, and runtime hazard state. Cash, inventory, average entry price, cumulative PnL, and economic campaign identity remain arm-local and survive the gap. Quoting may resume only after a fresh source snapshot and past-only warmup.

UTC midnight is an accounting boundary only. It does not flatten inventory, reset a campaign, or create a fresh-start replay. The 71 daily execution requests are source admission units for one continuous engine timeline; they are not independent simulations. Orders and queue state carry across midnight unless the frozen restart manifest places a real gap there.

Control and candidate use separate mutable state namespaces. They share the calendar, restart events, market source timeline, latency/random path contract, and continuous accounting semantics, but independently generate orders, queue, fills, inventory, cooldown, and campaign state.

## Reused Contracts

- `scripts/run_full_calendar_71d_baseline.py` supplies the exact 71-day source resolution, including the provider-normalized sensitivity fallback.
- `continuous_accounting_contract.v1` remains the accounting authority and is hash-bound with its implementation.
- `causal_v12_1s_ml_ab_replay.py` remains the candidate overlay admission ABI; every formal daily overlay must revalidate its 13 heads, causal ready time, bundle identity, and atomic admission.
- `calendar_continuity_manifest_20260417_20260730_v1.json` supplies the observed gap evidence. The new plan deliberately does not inherit its grade-based whole-day exclusion rule.

## Fail-Closed State

The frozen preflight intentionally contains null bindings for:

- candidate bundle metadata;
- candidate Feature DAG;
- candidate 71-day overlay root and overlay index.

Running the CLI now exits before source loading or engine execution with `candidate identity is not bound`. A successor execution amendment must bind all three artifacts and their SHA256 values, plus an exact chronological 71-row overlay index, before execution requests can be materialized.

## Verification

- New contract tests: 10 passed.
- Continuous accounting/calendar/restart plus F03 replay regression set: 37 passed.
- Ruff check and format check: passed.
- Real `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` source-resolution preflight: 71/71 days, 52 native and 19 provider-normalized sensitivity, 107 restart intervals.
- Default frozen CLI: failed closed at the missing candidate identity.

No prediction, fill, campaign value, terminal MTM, or PnL result was read.
