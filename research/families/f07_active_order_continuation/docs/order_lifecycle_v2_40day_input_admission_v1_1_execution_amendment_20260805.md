# F07 Order Lifecycle v2 Admission Execution Amendment v1.1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

## Status

`implementation_blockers_cleared_pending_legacy_dual_clock_data`

This mechanics-only amendment preserves the frozen 40-day denominator and the bytes of `order_lifecycle_v2_40day_input_admission_v1_contract_20260805.json`. It does not run the 40-day lockstep, read economic outcomes, train CIF, or grant q90/live authority.

## Cleared Implementation Blockers

The following three items can be removed from the admission implementation blocker list:

1. Cancel reject now resumes the real fill-risk set in both legal phases: `ACTIVE` and `PARTIALLY_FILLED`.
2. Terminal remainder uses the frozen `order_lifecycle_terminal_remainder_zero_abs_1e-12.v1` contract. Numerical residue at or below `1e-12 BTC` is persisted as exact zero; a positive `0.0004 BTC` remainder remains active and cannot be classified as full fill.
3. The native C++ journal-v2 mirror is bound to the complete Python schema and checks each event's sequence, phase, terminal route, quantity, and risk-set state. Its ABI is `order_lifecycle_journal_v2_cpp_event_stream_mirror.v1`.

These remain fail-closed per-day admission gates. Clearing the implementation blockers does not permit a daily artifact to omit their evidence.

## Remaining Blocker

The only remaining **input-data blocker** is authoritative dual-clock coverage in the retained legacy 40-day traces:

- `event_visibility_ts_ns` must be present on every event;
- `event_exchange_ts_ns` must be present on activation, partial/full fill, cancel reject/ACK, reject, and expiry;
- exchange time must not exceed visibility time;
- the missing clock cannot be inferred from journal-v2 or provider timestamps.

Until those legacy traces are rebuilt or independently admitted with both clocks, `lockstep_execution_eligible=false`. The absence of an admitted panel is execution state, not a fourth implementation blocker.

## Verification

- C++ event-stream binding: `7 passed`.
- Binding + admission + event-lockstep focused suite: `24 passed`.
- Scoped Ruff: passed.
- Old v1 canonical identity remains `3458cb12a9fcc9b65608018b9324fbb30de02e208ee3b26d000257800102dbd7`.
- This amendment's canonical SHA256 is `dd7b16a087f4ba96d8b331fa20672ca0bc167dfd668733f2b95d26328e2be16f`.
- All implementation SHA256 values are frozen and checked in the companion machine-readable amendment.

No 40-day execution, PnL read, CIF training, or live deployment was performed.
