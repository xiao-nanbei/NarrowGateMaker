# R: Replay, Queue, And Lifecycle

Last materially modified: 2026-08-25

Documentation boundary: this README and the unit's tracked `docs/` are public. Owner-only artifact locators, unpublished evidence indexes, and private research context are resolved through this unit's ignored local `private/` catalog and are not distributed with the public repository. See the [public/private research layout](../../PRIVATE_EVIDENCE.md).

Canonical single-day replay code remains at `models/backtest_tick.py`, `models/exchange_book_replay.py`, `models/audit/order_lifecycle.py`, and `models/queue_calibration.py`.

The shared continuous-calendar substrate is implemented in:

- `models/replay/continuous_calendar.py`
- `models/replay/restart_boundary.py`
- `models/replay/replay_state_checkpoint.py`
- `models/replay/continuous_accounting.py`
- `data/quality/calendar_gap_manifest.py`

Live lifecycle estimation now has two additional fail-closed layers:

- `models/replay/baseline_epoch_manifest.py` binds code, configuration, artifacts, action permissions, clocks, data sources, and initial runtime state before any live rows are pooled;
- `models/replay/order_lifecycle_clock_registry.py` reports epoch-specific calendar-time and risk-time competing-risk curves without economic inputs.

The shared time-scale taxonomy is frozen in `docs/time_scale_evidence_classification_v1_20260804.md`. It separates estimand horizons, policy clocks, transport limits, feature bases, and governance thresholds so one class cannot silently supply parameters or authority to another.

The first 360-hour epoch draft is intentionally unauthorized: its historical identity files omit required state/source/clock evidence and the restart audit is incomplete. See `docs/baseline_epoch_manifest_v1_contract_20260804.md` and `docs/order_lifecycle_clock_registry_v1_contract_20260804.md`.

It preserves arm-specific cash, inventory, entry price, economic campaign, fees, and cumulative PnL across UTC midnight and planned restart intervals. Process-local orders, queues, cursors, cooldowns, and feature runtime are cleared only at an explicit restart boundary after observable order terminality. UTC midnight is an accounting slice, not a flatten or reset.

The current implementation contracts are [`continuous_replay_state.v2`](docs/continuous_replay_state_v2_contract.json) and [`continuous_accounting_contract.v2`](docs/continuous_accounting_contract_v2.json). They add fail-closed v1 checkpoint rejection, finite signed fee/rebate carry, and a zero-inventory campaign boundary for fills that cross from long to short or short to long. The v1 contracts remain immutable historical predecessors; results and checkpoints that depend on their old fee/campaign semantics are stale until rerun under v2. This correctness repair grants no strategy, economic, action, exact-queue, or live authority by itself.

See `docs/versioned_continuous_replay_substrate_v1.md` for modes, state ownership, and the distinction between continuity effects and tail-governance effects.

The live quote-input atomicity successor is documented in `docs/quote_decision_snapshot_atomicity_v2_implementation_20260804.md`. It binds depth mid, the optional completed-bar pricing anchor, depth-derived quote features, bookTicker BBO, generations, split visibility/source clocks, and lock timing to one immutable requote snapshot. It is locally verified and deployment-eligible, but carries no economic or action authority.

The independent [unknown-submit-ACK correctness amendment](docs/order_lifecycle_unknown_submit_ack_correctness_v1_amendment_20260813.md) is `implemented_local_predeploy_blocked` and has not been deployed. Only a structured exchange `-5022` response that explicitly says the order was not recorded may establish exact-zero exposure; timeout, response loss, malformed submit or query replies, REST `-2013`, and a reconciled fill with an unknown activation prefix remain censored while preserving same-side ownership. This amendment is production lifecycle correctness work, not an F05 companion, shadow, action, or research writer, and it grants no strategy or live authority by itself.
