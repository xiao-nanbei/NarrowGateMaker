# F05 Formal V10 Exact-Owner Action Schema Amendment

Last materially modified: 2026-08-14

Status: `formal_v10_preflight_blocked_schema_provenance_fixed_locally_v11_clean_binding_pending`.

Evidence availability: this Markdown report and its repository-relative JSON receipt are public. The formal-v10 execution manifest, preflight receipt, admitted panel, source receipts, owner policy, predicate bundle, private configuration, and underlying market data are retained in the owner-private evidence store and are not distributed with the public repository; their SHA256 values are public integrity metadata only.

## Formal V10 Result

Formal-v10 bound the exact-owner bridge v3 mechanics admission to clean commit `1a306e803cbf00d7ffe473b0fdd0b2fb7a6ca76a` and its annotated tag. Its preflight failed closed with exactly one reported missing field: `exact_owner_action`. No one-day mechanics, outer-train label generation, candidate selection, outer-test replay, PnL report, Validation read, or sealed-holdout read occurred.

The admitted panel was not missing this field. It stores `exact_owner_action` in the separately bound `exact_owner_actions` table, and the mechanics loader already validates and joins that table into replay inputs before execution. The schema-only preflight inspected only metadata and replay-input schemas, so it incorrectly required the duplicate column to exist in `replay_inputs` before the admitted join.

## Fix

The canonical preflight interface now receives the bound `exact_owner_actions` schema explicitly. `exact_owner_action` is considered present only if it exists in either the replay-input table or the separately admitted owner-action table. The backend verifies the latter table's manifest schema before invoking the adapter. If both sources omit the field, preflight remains blocked. The later mechanics loader still verifies row identity, side-specific owner-action vocabulary, and equality if both sources carry the column.

This is a provenance correction, not a relaxed field contract. It neither injects an action nor changes the 30 dates, 3,516 opportunity IDs, feature tables, owner actions, replay inputs, policy, predicate bundle, configuration, queue identity, or ambiguity censoring rule.

## Verification And Boundary

The replay-adapter, repeated-policy backend, and orchestrator tests completed with 105 passed. Ruff passed. Formal-v10 remains immutable and cannot be reused because its clean tag contains the pre-fix interface. A new clean commit and annotated tag are required before binding formal-v11.

The active owner policy and EC2 runtime remain unchanged. Action and live authority remain false for the successor. The Unknown ACK lifecycle amendment remains independent and `implemented_local_predeploy_blocked`.

## Next Gate

After the clean formal-v11 tag is created, bind the same exact-owner bridge v3 mechanics canonical SHA to a new formal-v11 manifest, rerun preflight, and require `formal_offline_replay_mechanics_ready`. Only then run the fixed first-day single-worker exact-owner no-op mechanics diagnosis.
