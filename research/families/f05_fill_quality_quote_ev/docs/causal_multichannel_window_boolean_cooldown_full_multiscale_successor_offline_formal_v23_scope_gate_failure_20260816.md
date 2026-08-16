# F05 Offline Formal V23 Scope-Gate Failure

Last materially modified: 2026-08-16

Status: `failed_closed_first_opportunity_cpp_runtime_scope_invalid`.

## Admitted Progress

Formal-v23 was bound from clean commit `8a1d6e73` and its annotated tag. Its v22-to-v23 research-contract invariance receipt passed, and its unchanged zero-economic builder walk accepted all 3,516 mechanics opportunities with zero economic evaluator calls.

## Failure

The first-real-opportunity, all-eight-arm quick gate began after the builder receipt. Python completed the single shared-prefix opportunity, but the first C++ arm was rejected before a completed arm could be admitted because both the C++ runtime validator and Python dispatch validator accepted real-day qualification scopes only through v22 while the bound runtime correctly declared `real_day_all_arm_full_replay_v23`.

The atomic failure receipt records phase `first_opportunity_all_arm_preflight`, error `cpp_qualification_scope_invalid`, zero completed C++ arms, no persisted or exposed economic value, no use of economic values for selection, no Validation or sealed-holdout read, and false action/live permissions. The full 81-opportunity, 648-arm qualification day and nested OOF never started.

## Gate Audit

This identity mismatch should have been caught by the all-panel zero-economic builder gate. That gate constructed the runtime configuration and validated target rows, but did not instantiate the exact `F05RepeatedBooleanCooldownRuntime` used by arm execution. The local correction now makes the builder gate instantiate that runtime and require `parity_qualified=true` before reading panel rows; it also adds v23 to both C++ and Python allowlists and adds regression coverage for the exact real-day scope.

## Closure

Formal-v23 is permanently closed and must not be rerun or overwritten. Its quick gate did its intended job by preventing another full-day late failure, but the missing runtime-instantiation check remains a preflight design defect. The local correction does not itself create a new formal execution identity. A subsequent formal identity requires explicit owner authorization, a new clean commit and annotated tag, a new manifest and invariance receipt, and fresh immutable qualification receipts.
