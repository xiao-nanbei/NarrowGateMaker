# R: Replay, Queue, And Lifecycle

Last materially modified: 2026-08-04

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

The current substrate is contract-complete but is not yet bound to the authoritative tick runner (`full_tick_runner_binding=false`). It grants no strategy, action, exact-queue, or live authority by itself.

See `docs/versioned_continuous_replay_substrate_v1.md` for modes, state ownership, and the distinction between continuity effects and tail-governance effects.

The live quote-input atomicity successor is documented in `docs/quote_decision_snapshot_atomicity_v2_implementation_20260804.md`. It binds depth mid, the optional completed-bar pricing anchor, depth-derived quote features, bookTicker BBO, generations, split visibility/source clocks, and lock timing to one immutable requote snapshot. It is locally verified and deployment-eligible, but carries no economic or action authority.
