# Carryover-Safe V2.1 Execution Failure

Last materially modified: 2026-08-03

Status: `one_day_authoritative_smoke_failed_baseline_warmup_parity`.

The bounded `2026-04-17` smoke reached authoritative replay with cache writes disabled. The current loader read the existing components-v1 market-context and model-overlay artifacts, while the native order-book cache was read through a read-only adapter. No cache-write contract exception occurred.

The replay then failed during the no-treatment prediction-warmup phase with:

```text
AdapterContractViolation:
prediction-warmup candidate diverged from untreated baseline
```

Before the first completed 10-second prediction, v1.5 requires the regenerated candidate snapshot to equal the frozen untreated baseline snapshot across its decision identity, clocks, side/role, eligibility, exposure permission, active order, quote coordinate, blocker fingerprint, and policy fingerprint. That equality no longer holds under the v2.1 execution identity. The exception does not serialize both snapshots, so this run cannot safely assign the mismatch to one particular loader or DAG node.

No journal part, manifest, or report was produced; the output directory is `0B`. Consequently the required `29,072` decisions and BUY/SELL episode, carryover, role-transition, and zero-tolerance counts are unassessed. V2.1 is closed fail-safe as incompatible with the frozen v2 baseline tape; it does not inherit the successful v2 smoke.

The targeted suite remains `10 passed`. No PnL, economic result, Validation, or sealed holdout was read. The 40-day run did not start, and no action or live permission is granted.
