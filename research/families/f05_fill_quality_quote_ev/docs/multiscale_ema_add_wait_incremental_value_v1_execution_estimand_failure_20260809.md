# Multiscale EMA ADD/WAIT v1 Execution Estimand Failure

Date: 2026-08-09

Last materially modified: 2026-08-12

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Status

`multiscale_ema_add_wait_incremental_value_v1` is closed as diagnostic-only execution evidence. It has no F09 registration, action, Validation, holdout, or live authority.

The frozen prediction question remains open under the successor identity `multiscale_ema_add_wait_incremental_value_v1_1`. The successor does not reuse the v1 opportunity panel, fork labels, arm checkpoints, or outcomes.

## Failure

The v1 implementation released `WAIT_ONE_EXTERNAL_EPOCH` at the next scheduled requote. That is not the preregistered external clock

\[
\tau^+=\inf\{u>t:G_u^{market}>G_t^{market}\land readiness(u)=1\}.
\]

It therefore estimated `WAIT_UNTIL_NEXT_SCHEDULED_REQUOTE`, not `WAIT_ONE_EXTERNAL_MARKET_GENERATION`. Candidate/order state did not define the v1 release, but the strategy requote clock inserted an unintended roughly five-second policy duration into the estimand.

On the 2026-04-17 outcome-blind census, the supported v1 release delay had median 5,211.5ms and p99 8,951.0ms. The corrected v1.1 raw-event release had median 71ms and p99 153ms; all 4,903 eligible opportunities were supported, and the raw event-index gap was 1-3. A later exogenous event in the same millisecond is ordered by the raw event index and is valid only when the six-field market-content generation strictly changes.

## Consumed Evidence

The full v1 40-day census and frozen panel were created. Fork outcomes were completed for 2026-04-17, 2026-04-18, and 2026-04-19 before the estimand mismatch was identified. Additional atomic arm checkpoints may exist for later days.

Those files remain immutable diagnostic evidence in:

`${NARROWGATE_DATA_ROOT}/reports/multiscale_ema_add_wait_incremental_value_v1_20260809`

They must not be finalized, pooled with v1.1, or used for M0/M1, policy selection, or promotion.

## Successor Boundary

The v1.1 successor changes only execution-identification safeguards:

- release at the first ready exogenous raw market event with a strict market-content generation change;
- verify the frozen locator and all six generation fields before arm-dependent order logic;
- allow same-millisecond successor events only when raw event order and content both advance;
- fail closed rather than train M0/M1 when any selected fork label is right-censored.

The 40-day Development denominator, EMA basis, M0/M1 schema, sampling cap, folds, embargo, purge, Ridge model, and economic threshold remain frozen. SELL and BUY remain separate. F09 identities remain unregistered until the corresponding side-specific M1 passes chronological OOF gates.
