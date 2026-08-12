# Dynamic Fill Hazard M0 v2

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Date: 2026-07-23

Status: closed on Development; Validation and sealed holdout unread

Live impact: none

## Research contract

`dynamic_fill_hazard_m0_v2` replaced the rejected static exponential competing-risk specification with explicit lifecycle semantics:

- BUY and SELL are fit and accepted separately;
- fill uses a dynamic discrete-time start/stop risk set;
- cancel request is a policy action/censor, while cancel ACK remains a later lifecycle outcome;
- native exchange-book price jump is a non-absorbing state transition;
- campaign repair enters its risk set only after inventory is nonzero and an eligible reducing quote is active;
- policy features are restricted to exact-safe L2, queue, price, side, microprice, refill/recovery and causal clock state;
- historical trade quantity/notional and aggregate child-count fields are excluded.

The registered follow-up action identity was `queue_value_keep_cancel_dynamic_fill_m0_v1`, with one campaign-level 50/50 `K0=keep` / `K1=cancel_then_reenter_on_recovery` intervention. Prediction was allowed only to register that later randomized experiment; it could not authorize an action or a live change.

## Frozen identity

| Artifact | SHA256 |
|---|---|
| `evidence_split_v8.json` | `7f24e6b522ced7f8d2e398bdb60ac7ae25fbfff7629f0d2157a772347a783da7` |
| `family_spec_v8.json` | `6f91fac9dcd59fe98a8eac52e54eba741d8322e78e703625f3d4caad912efe52` |
| lifecycle partition identity | `4f4575b94f0dae0da7d279fbc351591d4ec6714a8f88ff5ca11ed825ea526c3b` |
| `risk_build.json` | `b8a589c55adf1be2fb9a140bf7a28938fee22de28e1d20fc0c40b86a51b9ba7d` |
| Development summary | `25914ca8e8b77dea4255750165961cefa9d21f3b610afdd7740aea114c422608` |

The split contains 17 Development days through 2026-06-14, one embargo day, eight locked Validation days, a second embargo day and eight sealed-holdout days. Only Development lifecycle partitions were read.

## Mechanics gate

The first full-day preflight exposed a real delayed-entry bug: replay order state is numeric, but the lifecycle recorder compared it with string state names. This made every reducing quote inactive and produced zero repair-risk intervals. The recorder now uses its own authoritative lifecycle state.

The corrected 2026-06-05 preflight passed with:

- 413,612 lifecycle events and 394,430 start/stop intervals;
- 58,415 repair-risk entries and 200,425 formal repair intervals;
- 194,640 dynamic snapshots, of which 191,850 had strict queue support;
- zero future-feature rows and zero duplicate order/event sequence rows;
- native jump retained as a non-absorbing transition.

Multi-day identities now use `(day, order_id)` and `(day, campaign_id)`. This prevents daily ID reuse from joining unrelated orders or campaigns. The formal fit reads frozen daily partitions and binds every partition hash, avoiding the memory-heavy `list[dict]` expansion used by the former merger.

## Development denominator

The dynamic fill panel contains 3,057,751 formal rows from 291,695 orders:

- 11,660 adverse fills;
- 4,072 favorable fills;
- 3,042,019 censored intervals;
- 262,364 cancel-request censor rows;
- 181,621 native jump transitions;
- zero future-feature rows.

The delayed-entry repair panel contains 1,469,566 rows across 6,200 campaigns and 6,152 repair events. BUY-long and SELL-short campaign repair support both passed.

## Chronological OOF result

Four expanding folds use 8 or more past train days, one embargo day and two future test days. Both adverse heads passed every frozen prediction gate. Both favorable heads failed only the positive Brier-skill gate.

| Side / cause | OOF rows | Events | AP lift | ROC AUC | O/E | Brier skill | Top-20 lift | Result |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| BUY adverse | 746,266 | 2,647 | 12.012 | 0.889 | 0.781 | +0.00285 | 4.142 | pass |
| BUY favorable | 746,266 | 886 | 7.226 | 0.831 | 0.767 | **-0.00565** | 3.484 | fail |
| SELL adverse | 734,319 | 2,534 | 11.307 | 0.871 | 0.820 | +0.00106 | 3.963 | pass |
| SELL favorable | 734,319 | 968 | 9.173 | 0.828 | 0.867 | **-0.00131** | 3.488 | fail |

The favorable heads have useful rank separation but do not improve calibrated probability loss over the frozen exposure-only baseline. A keep/cancel value model needs both adverse-fill cost and favorable queue-option value. Promoting only the successful adverse head would recreate the failure mode seen in older cancel policies: removing toxic fills by also discarding valuable fills and queue priority.

## Decision

No side passed the complete prediction contract:

```text
prediction_gate_passed_sides = []
validation_access_allowed = false
decision = close_prediction_family_on_development
```

Therefore:

- Validation and sealed holdout remain unread;
- no randomized K0/K1 panel was generated;
- DR uplift, ESS, campaign terminal/tail and nonlinear toxic-fill selectivity were not estimated for this family;
- the missing selectivity metrics are `not_applicable`, not zero;
- no live or baseline parameter changed.

The exact v8 family is closed. A future attempt must register a materially new estimand/model identity and a new frozen Development contract. It must not rescue v8 by dropping the favorable head, weakening Brier skill, changing a threshold on the consumed Development panel, or opening Validation.
