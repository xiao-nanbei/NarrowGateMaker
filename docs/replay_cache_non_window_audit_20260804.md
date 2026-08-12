# Non-window Replay Cache Audit

Last materially modified: 2026-08-04

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](public_private_documentation_contract.md).

This is a read-only audit. No cache file was modified or deleted.

| Node | Size GiB | Classification | Migration | Deletion |
|---|---:|---|---|---|
| native_exchange_book_hour_v1 | 9.152 | `current_reusable` | `safe_with_atomic_copy_hash_verify_and_compatibility_symlink` | `preserve` |
| cross_venue_fair_price_trade_1s_v1 | 1.634 | `closed_but_frozen_referenced_with_reusable_base_variants` | `safe_with_atomic_copy_hash_verify_and_compatibility_symlink` | `prune_only_unreferenced_superseded_implementation_duplicates` |
| external_adverse_quote_edge_guard_mechanics_v1 | 0.005 | `closed_but_frozen_referenced` | `preserve_in_place_absolute_hash_chain` | `preserve` |
| p3_touch_window_context_v1 | 0.088 | `closed_but_frozen_referenced` | `safe_with_atomic_copy_hash_verify_and_compatibility_symlink` | `preserve` |
| p3_touch_reaches_v1 | 0.008 | `closed_but_frozen_referenced` | `safe_with_atomic_copy_hash_verify_and_compatibility_symlink` | `preserve` |
| p3_conditional_quote_overlay_v1 | 0.003 | `deletion_candidate` | `migration_not_worthwhile_small_closed_adapter_cache` | `conditional_after_frozen_reproduction_export` |

## Findings

### `native_exchange_book_hour_v1`

This is the active action-independent hourly native-book DAG node; it is not tied to a closed strategy action.

- Files: 3384; distinct days: 47; size: 9.152 GiB.
- Manifest checks: 1128 parsed; 0 payload hash mismatches; 0 identity mismatches.
- NPZ key checks: 0 checked; 0 mismatches.
- Repository references: {'code_or_test': 4, 'documentation': 1}.
- Migration: `safe_with_atomic_copy_hash_verify_and_compatibility_symlink`.
- Deletion: `preserve`.

### `cross_venue_fair_price_trade_1s_v1`

The fair-center action is closed and this 1s provider-clock adapter does not replace AWS receive-time BABEL inputs. Current frozen implementation variants remain reproducible inputs; byte-identical older implementation variants may be pruned separately.

- Files: 648; distinct days: 40; size: 1.634 GiB.
- Manifest checks: 324 parsed; 0 payload hash mismatches; 0 identity mismatches.
- NPZ key checks: 0 checked; 0 mismatches.
- Repository references: {'code_or_test': 3, 'frozen_spec_or_evidence': 3}.
- Migration: `safe_with_atomic_copy_hash_verify_and_compatibility_symlink`.
- Deletion: `prune_only_unreferenced_superseded_implementation_duplicates`.

### `external_adverse_quote_edge_guard_mechanics_v1`

The report binds exact mechanics payload paths and SHA256 values. This is frozen evidence, not the reusable external receive-time source.

- Files: 4; distinct days: 0; size: 0.005 GiB.
- Manifest checks: 0 parsed; 0 payload hash mismatches; 0 identity mismatches.
- NPZ key checks: 0 checked; 0 mismatches.
- Repository references: {'code_or_test': 90, 'documentation': 40, 'frozen_spec_or_evidence': 52, 'other_text': 1}.
- Migration: `preserve_in_place_absolute_hash_chain`.
- Deletion: `preserve`.

### `p3_touch_window_context_v1`

The fixed-10s context is not the new reach-time authority, but frozen Specs bind exact files and hashes. Its small footprint does not justify breaking historical reproduction.

- Files: 222; distinct days: 210; size: 0.088 GiB.
- Manifest checks: 0 parsed; 0 payload hash mismatches; 0 identity mismatches.
- NPZ key checks: 222 checked; 0 mismatches.
- Repository references: {'code_or_test': 1, 'frozen_spec_or_evidence': 1}.
- Migration: `safe_with_atomic_copy_hash_verify_and_compatibility_symlink`.
- Deletion: `preserve`.

### `p3_touch_reaches_v1`

These fixed-horizon reach labels are superseded for authoritative F02 work by the full reach-time surface, but remain cheap frozen evidence.

- Files: 210; distinct days: 210; size: 0.008 GiB.
- Manifest checks: 0 parsed; 0 payload hash mismatches; 0 identity mismatches.
- NPZ key checks: 210 checked; 0 mismatches.
- Repository references: {'code_or_test': 2}.
- Migration: `safe_with_atomic_copy_hash_verify_and_compatibility_symlink`.
- Deletion: `preserve`.

### `p3_conditional_quote_overlay_v1`

This overlay is specific to the failed scalar compression mapping, is not consumed by the full reach-time successor, and is deterministically regenerable from frozen inputs. It is a candidate, not deletion-ready, until a reproduction bundle or explicit owner receipt is recorded.

- Files: 24; distinct days: 24; size: 0.003 GiB.
- Manifest checks: 0 parsed; 0 payload hash mismatches; 0 identity mismatches.
- NPZ key checks: 24 checked; 0 mismatches.
- Repository references: {'code_or_test': 2, 'frozen_spec_or_evidence': 1}.
- Migration: `migration_not_worthwhile_small_closed_adapter_cache`.
- Deletion: `conditional_after_frozen_reproduction_export`.

## Candidate Pruning

The audit found 160 exact superseded file groups totaling 0.806 GiB. They were not deleted. Each group still requires a fresh content/reference audit and explicit owner-authorized deletion receipt.

The entire `p3_conditional_quote_overlay_v1` node is only a conditional candidate. Its small closed-adapter payload remains in place until a frozen reproduction bundle or owner receipt exists.

## BABEL Boundary

`cross_venue_fair_price_trade_1s_v1` is a transport-unsupported historical provider-clock adapter. It can reproduce closed fair-center and historical sensitivity work, but it is not a substitute for F04 BABEL AWS receive-time tapes. The P2 mechanics cache is frozen derived evidence; the reusable BABEL base remains the receive-time source tapes and their ledger on `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}`.
