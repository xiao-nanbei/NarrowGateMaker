# BABEL External-Market Research Map

Last materially modified: 2026-08-20

Status: active evidence routing; no external-market action or live authority.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Purpose

`BABEL` separates work that was previously grouped under "external alpha" into four different authorities:

| Layer | Meaning | What it can prove |
|---|---|---|
| D | Data translation | Prices, symbols, basis and clock fields share one causal language. |
| E | Engineering/mechanics | A signal is visible in time and can change the intended quote surface. |
| P | Prediction evidence | External state adds out-of-sample information for one exact value target and denominator. |
| A | Action evidence | One frozen intervention improves post-assignment economics under randomized replay/OPE. |

An artifact from one layer never inherits the authority of the next layer.

## Current Parallel Route

```text
BABEL-P1 receive-time first-add M0/M1 -----------------\
    30-day collection complete; lifecycle/row gates block \
                                                          > same exact surface
BABEL-E6/P2 adverse-edge mechanics --------------------/
    outcome-blind clock, LOO, role, quote and cap audit
                                                          |
                                                          v
                                             independent action identity
                                                          |
                                                          v
                                             randomized replay / OPE
```

P1's 30-day count gate is complete and does not block E6/P2 mechanics. The closed capture ledger contains 31 valid full windows over 30 distinct UTC days; the duplicate 2026-07-21 window counts once. Collection ended on the reactivated-AWS predecessor, the automation is deleted, and the current host has no inherited capture authority. P1 fitting remains blocked by the exact-lifecycle successor and every frozen chronological, side-specific, Grade-A/Grade-B, common-row, source-transport, true-LOO, causal-clock, and late-panel gate.

## Identities

### D1: Cross-Instrument Price Translation

Binance BTCUSDT, USDCUSDT and BTCUSDC plus Bitget/Bybit/OKX spot/perpetual are converted into a common BTCUSDC price language with past-only basis. D1 is data translation, not alpha.

### E5: Historical One-Second Sensitivity

Historical provider-normalized one-second bars can test coverage and second-scale sensitivity. Their one-second grid is a data-resolution boundary, not a claim that one second is the best market horizon and not AWS receive-time transport evidence.

### E6/P2: External Adverse Quote Edge Mechanics

This means `BABEL-P2`; it is not the older roadmap's historical "Phase 4 / P2: Post-Fill Campaign Moderator" label.

[`external_adverse_quote_edge_guard_mechanics_v1`](external_adverse_quote_edge_guard_mechanics_v1_spec_20260802.json) runs now without reward. It measures all/LOO agreement, negative-edge duration, role support, 10-500ms state survival, outward quote movement, spread-cap clipping and potential replace/queue-reset leverage.

The current historical quote logs expose millisecond post-decision log-write time rather than exact decision-start nanoseconds. Therefore their first run is clock-sensitivity evidence. Exact sub-second transport requires native `decision_ts_ns` instrumentation. The immutable seven-tape capture also lacks a complete order/ACK/queue journal, so replace and queue-reset counts remain upper bounds until full-path replay.

The [Development result](external_adverse_quote_edge_guard_mechanics_v1_development_20260802.md) found 26 outward coordinate changes among 7,786 guard-eligible opener/add side-opportunities. The signal survived 500ms for 61.1% of sampled BUY triggers and 75.0% of sampled SELL triggers, but support was sparse and only six triggers were add opportunities. The candidate rate is 0.334%, about fifteen times below the frozen 5% selective-action floor. At 0.5 assignment probability, only about 13 paths would actually change. The 18 BUY and 8 SELL survival samples are descriptive and do not establish side-specific transport. This is useful mechanics evidence, not an action gate.

P2 v1 currently covers observed paired quote-log opportunities. This is a historical sensitivity denominator, not yet the formal exact opportunity tape: the legacy log has no stable decision ID, its clock is a post-decision write, and two ledger days have no quote rows. It derives opener/add/reducing from the latest strictly-prior signed inventory state; the quote log's `inventory_ratio` is an unsigned magnitude and is forbidden for role inference. Only opener/add are guard eligible and reducing is unchanged. Twenty of the 26 changes were opener opportunities, so P2 cannot borrow P1's first-add prediction or merge into its denominator.

The infrastructure-only [`external_adverse_quote_edge_guard_exact_opener_mechanics_v2`](external_adverse_quote_edge_guard_exact_opener_mechanics_v2_spec_20260802.json) now defines a prospective native tape with stable decision IDs, exact signed inventory, feature-ready time, baseline/candidate coordinates, and submit, activation, cancel/ACK, partial/full-fill and queue-reset lineage. It reads no economic outcome and does not enter F09. The [`v2.1 execution amendment`](external_adverse_quote_edge_guard_exact_opener_mechanics_v2_1_20260802.md) accurately discloses that operational lifecycle outcomes are read for native linkage and executed-action support, while PnL, reward, markout, and campaign labels remain forbidden. It also freezes exact-schema validation and requires BUY and SELL each to pass the unchanged 5% candidate-rate floor; pooled support is diagnostic only. If either side remains below that floor, the guard closes for action support. An outcome-blind rank budget or removal of LOO would require a new identity and cannot rewrite v1, v2, or v2.1.

The later [`v2.1 collection preflight`](external_adverse_quote_edge_guard_exact_opener_mechanics_v2_1_collection_preflight_failure_20260802.md) withdrew prospective collection eligibility before the tape was enabled. The frozen validator does not bind the runtime producers or deployment config, and the current append-only CSV path lacks fail-closed writer health, hot-start quarantine, complete cancel-reject journaling, and atomic `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` admission. A v2.2 execution amendment must close those infrastructure gaps before the side-specific 5% denominator is counted. This does not change P2's estimand or the now-closed P1 capture denominator.

### P1: First-Add External Incremental Value

[`first_add_external_incremental_value_m0_m1_v1`](first_add_external_incremental_value_m0_m1_v1_preregistration_20260730.md) targets F10's direct first-add-to-campaign-terminal USDC outcome. The 30-distinct-day count gate is complete; it remains blocked until the corrected exact-lifecycle successor and every frozen chronological, side, Grade A/B, common-row, source-transport, true LOO, causal-clock, and late-panel denominator pass. A passing result is prediction evidence only.

### A1: Symmetric Fair-Center Shift

The completed action moved both quotes around an external center. It is closed on Development because reward uncertainty crossed zero and leave-Bybit-out direction reversed. This closes only the symmetric center-shift action; it does not close external information or an outward-only adverse-edge guard.

### P7: Future Action Surfaces

KEEP/CANCEL, ADD/NO-ADD and bounded re-center are separate actions with separate estimands and experiment identities. They cannot be bundled into one tunable external strategy.

## Model Boundary

`causal_v12` is the operational local/source-aware 13-head model. It is not an AWS receive-time external live-alpha model and cannot substitute for P1 or P2.

## Authority

E6/P2 may emit mechanics evidence only. P1 may emit prediction evidence only. The current P1 first-add and P2 opener denominators are different and must not be merged. A future F09 action would need its own exact prediction surface and separate preregistration. Randomized replay still would not itself grant live authority; the promotion controller must separately verify live parity and safety.
