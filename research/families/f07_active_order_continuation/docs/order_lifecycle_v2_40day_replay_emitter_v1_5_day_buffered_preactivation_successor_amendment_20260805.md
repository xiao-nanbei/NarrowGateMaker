# F07 Lifecycle-v2 v1.5 Day-Buffered Replay Successor

Date: 2026-08-05

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Status: formal execution restarted; first strict-native day admitted

This amendment does not modify the frozen v1.3, v1.4, or earlier v1.5 documents. It binds the replay-only performance successor and the explicit pre-activation cancel routing discovered during formal execution.

## Execution Boundary

The live and historical journal writers remain unchanged. Formal replay now validates callbacks and lifecycle cursors in memory inside one exclusively owned, disposable day staging directory. At day close it publishes one content-addressed Parquet part. The parent independently validates the journal, health, clocks, terminal routes, exact-native eligibility, hashes, and manifest before atomically admitting the day directory.

A crash or worker failure cannot resume or admit a partial buffer. The entire unadmitted day staging directory is discarded and the frozen day is replayed.

Pre-activation cancel requests do not create fill-risk before activation. If activation follows, the lifecycle enters `ACTIVE` and immediately `CANCEL_PENDING` at activation visibility time. If cancel ACK arrives first, the lifecycle terminates from `SUBMITTED` and is explicitly censored as `no_activation`. Pre-activation cancel reject remains fail-closed because ABI v2 has no supported transition for it.

## Failed Attempts

- `run2`: stopped before admission because callback-per-part Parquet commits made the 40-day execution operationally impractical.
- `run3`: failed closed before admission on the previously unsupported `SUBMITTED -> cancel request` branch.

Both roots contain `DIAGNOSTIC_NOT_ADMITTED.json` and zero admitted days.

## First Admitted Day

The formal root is:

`${NARROWGATE_DATA_ROOT}/cache/replay_dag/f07_order_lifecycle_v2_40day_v1_5_20260805_run4`

Plan SHA256: `6a83e033cd143b20aeefc657938a3c32fb5eae237783e351b58023512e717d0a`

For `2026-04-17`, the parent admitted 68,861 rows covering 17,492 lifecycles and exactly 17,492 terminal observations. There were 17,143 exact-native eligible lifecycles and 349 explicit queue-path censors. The single 4,931,007-byte Parquet part has SHA256 `b9356d3d5f053284e0db2445aa6964f90a209a9d0cb19a004b5023a29b070ccb`.

Writer drops, writer errors, missing exchange clocks, clock-order violations, invalid exchange exposure, and terminal positive remainders were all zero. The staging directory was empty after parent admission.

The loaded C++ module remains unchanged at SHA256 `4ceeb79b7f6c1f50a8d5f40824cac95e8452b1569c0dd3ed5569a684305dcac0`.

## Permissions

This is mechanics-only evidence. No PnL, reward, markout, or campaign economic outcome was read. Forty-day emission and lockstep are incomplete. CIF training, q90 action, live transport, and live deployment remain unauthorized.

The machine-readable authority for this amendment is the sibling JSON file.
