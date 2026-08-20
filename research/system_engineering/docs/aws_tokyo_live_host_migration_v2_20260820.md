# AWS Tokyo Live Host Migration V2

Last materially modified: 2026-08-20

Evidence availability: Public host and instance identifiers are represented by logical placeholders. Exact endpoints, cloud identifiers, archive locators, health snapshots, and migration receipts are owner-side evidence in the private store and are not distributed with this repository.

Status: `completed_current_active_host_only_migration`

## Question

Can the active NarrowGate BTCUSDC runtime move from the reactivated AWS Tokyo predecessor to a new AWS Tokyo instance without changing strategy, model, config, or owner-policy semantics?

## Result

Yes. The predecessor maker stopped at `2026-08-19T18:55:39Z`; the successor prospective epoch started at `2026-08-19T18:59:09.278806684Z`. The public routing boundary records the explicit maintenance gap as `[2026-08-19T18:55:39Z,2026-08-19T18:59:09Z)`, while the exact stop-to-epoch-start interval is `210.278806684` seconds. The current operational host is `<current-live-host>` / `<current-live-instance>`, resolved only through the ignored private current-host pointer. The predecessor is now `<retired-reactivated-aws-host>` / `<retired-reactivated-aws-instance>` and must be queried through its verified local retirement archive rather than treated as current authority.

The successor reused the exact deployed runtime/config/model/P3/F05 artifact identities. The operational config SHA256 remains `800f4c025663ce6b54cfcf16d02ce510ccaf52545332ca4c19b1fbdf37f0cf85`; the deployed model identity remains `65792e9b672813ddf9ccb8a3504f998a828cd3c893e970fb0a073747685ee83c`; the F05 policy and predicate identities remain `877a20033ff678bd7aa9b58069f37c3dc459b18db78c316b7e50023248f15a29` and `ba4c1bac2380564aa24d47d12796f3be5c0312cc88d28218ce84bd20e4170f37`. The host migration did not change BUY, SELL, reducing-quote, quote-price, BER, P3, q90, inventory-limit, or action semantics.

## Successor epoch and lifecycle evidence

The current bound epoch is `prospective-1787165949278806684-4bc13c4b0f9d`, identity SHA256 `4bc13c4b0f9d6284736b0b19f8b2858bebb0ecd8b92de7bda59feb46f77a6adc`. Its owner-local admitted lifecycle evidence contains 3,323 rows with zero writer drops and zero writer errors; its admission-manifest SHA256 is `21c5bc40747b91fc3b24760dd27febf69f15bdb74148ad88dda36e0866c09a0e`. The remote spool remains source evidence rather than formal local authority; formal validity belongs to the verified owner-local admission.

The successor completed the 2,048-second Boolean-cooldown warmup and passed single-process, deep-book, external-feed, lifecycle-writer, model, config, and policy identity checks. These checks establish the host migration and active operational epoch. They do not establish research uplift, exact historical queue authority, receive-time transport equivalence, or a new action permission.

## Receive-time capture boundary

The prior seven-tape collection program closed on the predecessor source with 31 valid full windows covering 30 distinct UTC days. Its automation is completed and deleted. The successor host has no inherited capture authority: no script may silently resume the old automation, append successor rows under the predecessor source key, or describe the new host as a fourth admitted F04 capture stratum without a new source amendment and explicit owner authorization.

The 30-day denominator closes only the collection-count gate. F04 M0/M1 remains blocked until the corrected unknown-submit-ACK lifecycle implementation is deployed and a new exact-lifecycle session is admitted, followed by all chronological, side-specific, source-transport, common-row, true-LOO, and late-panel gates.

## Authority and history

The current machine-readable operational identity is [`operational_baseline_identity_20260820_v12`](../../families/f10_live_replay_attribution/docs/operational_baseline_identity_20260820_v12.json), resolved through [`operational_baseline_current.json`](../../families/f10_live_replay_attribution/docs/operational_baseline_current.json). v12 is a deployment-only successor to immutable v11. v11 remains the frozen identity that activated the owner-risk-accepted SELL cooldown; v12 only rebinds those bytes and permissions to the successor host epoch.

The complete four-epoch/three-gap query contract is [`docs/live_host_and_historical_data_access_20260811.md`](../../../docs/live_host_and_historical_data_access_20260811.md). Exact endpoints, archives, cloud-resource identifiers, and receipts remain private. Historical reports are not rewritten to pretend they were produced on the successor host.

## Permissions

- operational baseline active: `true`;
- owner-risk-accepted F05 live authority inherited: `true`;
- host migration changed strategy or action semantics: `false`;
- research prediction authority granted by migration: `false`;
- research live authority granted by migration: `false`;
- F04 successor-host capture authority: `false`;
- Validation or sealed holdout read: `false`.
