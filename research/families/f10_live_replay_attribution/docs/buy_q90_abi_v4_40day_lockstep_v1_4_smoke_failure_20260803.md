# BUY q90 ABI v4 40-Day Lockstep v1.4 Smoke Failure

Last materially modified: 2026-08-03

## Decision

The `2026-04-22` Development mechanics-only smoke failed closed before a day result was admitted. The 40-day run is **not complete and was not restarted** after this failure.

No PnL, markout, campaign economics, Validation, or sealed holdout data was read. No deployment occurred, q90 operational action remained off, and no threshold changed.

## Failure

At `now_ts_ms=1776818327400`, BUY order `513` disagreed at the evaluation boundary:

| Runtime | Valid | Reason | Action |
|---|---:|---|---|
| Python | false | `same_ms_exchange_book_ambiguity` | `none` |
| C++ | true | `ok` | `none` |

The visibility scheduler had a coalesced same-millisecond ordering ambiguity. Python retained that invalid state on the active order path; the C++ runtime did not. The frozen requirement is exact Python/C++ mechanics parity, so this is a hard failure even though both action strings were `none`.

## Process State

The obsolete v1.3 full-run PID `64058` and its workers were terminated. No lockstep or `backtest_tick` process was left running.

The successor persistence contract removes the full lifecycle journal from day JSON and worker return values. Only aggregate event, transition, terminal-route, terminal-reason, lifecycle-audit, mechanics, and parity counters may be persisted.

## Authority

`development_mechanics_supported=false`. Action, Validation, holdout, and live authority remain false. The 40-day denominator must not be reported as completed.
