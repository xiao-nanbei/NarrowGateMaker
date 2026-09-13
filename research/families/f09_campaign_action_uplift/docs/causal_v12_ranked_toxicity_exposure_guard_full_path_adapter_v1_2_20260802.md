# Ranked Toxicity Guard Full-Path Adapter v1.2

Last materially modified: 2026-08-02

## Status

This execution-only amendment supersedes v1.1 as the current dependency identity after the shared order lifecycle gained immediate fail-closed exchange-clock validation and the authoritative live/replay lifecycle journal. The v1.1 amendment, note, and audit remain byte-for-byte historical evidence.

No ranked-toxicity action semantics changed. BUY/SELL heads, past-only p90, random seeds, 0.5/0.5 assignment, operational baseline, zero-tolerance gates, scorecard, and economic gates are identical to v1.1. No mechanics or economic result has been read.

## Dependency Refresh

An activation without an exchange timestamp now immediately records:

- `exchange_exposure_valid=false`;
- `exchange_exposure_complete=false`;
- `quantity_time_exposure_exchange_btc_s=null`;
- `exchange_exposure_invalid_reason=missing_exchange_timestamp:activate`.

Live and authoritative Python replay persist the separate visible and exchange quantity-time exposures through `order_lifecycle_journal.v1`, including remaining quantity, terminal route, validity, completeness, and invalid reason. The ranked-toxicity adapter continues to use its execution-only mechanics journal; this amendment binds the repaired shared lifecycle dependency and does not reinterpret its historical v1.1 output.

The pybind q90 kernel still does not emit either quantity-time exposure. Python/C++ parity remains limited to terminal route, path, score, cancel, and recovery transitions. v1.2 makes no three-runtime \(E_q\) parity claim.

## Permissions

The refreshed adapter is eligible only for a future mechanics run. PnL, markout, toxic-fill, campaign-tail, Validation, holdout, action, and live permissions remain closed. The future mechanics run must still pass every original v1 hard gate and all zero-tolerance lifecycle checks.

The machine amendment is [`causal_v12_ranked_toxicity_exposure_guard_full_path_adapter_v1_2_execution_amendment_20260802.json`](causal_v12_ranked_toxicity_exposure_guard_full_path_adapter_v1_2_execution_amendment_20260802.json).
