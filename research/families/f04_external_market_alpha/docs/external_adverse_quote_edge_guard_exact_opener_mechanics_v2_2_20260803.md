# External adverse quote-edge exact opener mechanics v2.2

Last materially modified: 2026-08-12

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Status

This is an execution-only successor to frozen v2/v2.1. It closes the runtime, health, crash recovery, and atomic admission contracts. Prospective collection remains disabled. No economic result was read and no prediction, action, or live authority is granted.

The collection preflight was run on 2026-08-03 with a temporary copy of the private operational config whose only change was enabling this tape. It passed the three-venue, stablecoin-anchor, shadow-only, local-staging, mounted-`${NARROWGATE_PRIVATE_EVIDENCE_ROOT}`, and runtime-identity checks. The public and private operational configs remain disabled; passing preflight is collection eligibility, not evidence that a prospective denominator already exists.

## Runtime boundary

When explicitly enabled by a future reviewed private config, the producer binds the active config artifact, fair-state config, Feature DAG identity, tape schema, MakerEngine, OrderManager, and guard implementation. Collection fails closed unless Bitget, Bybit, and OKX are enabled, the Binance stablecoin anchor is enabled, and all external inputs remain shadow-only.

The writer publishes per-session health with queue depth, heartbeat, row drops, errors, and last flush. A hot reload with existing orders begins in quarantine; formal rows start only after every pre-existing order is terminal. A rejected cancel restores the active or partially-filled lifecycle and journals a `cancel_rejected` event.

## Storage boundary

Runtime chunks stay on local temporary storage as session-specific UTC-day files. A crash leaves only `.partial` bytes, which cannot be admitted. A normal zero-drop, zero-error close creates a ready CSV and hash-bound manifest.

The separate admission command validates row count, schema hash, canonical row hash, file hash, runtime identity, UTC day, and event interval before an atomic copy to:

`${NARROWGATE_DATA_ROOT}/exact_opportunity_tape`

Admission is idempotent by chunk identity. Overlapping session intervals are rejected; the collector never combines partial windows into an apparent full window. The tape is intentionally outside the seven-tape bounded capture.

## Permissions

- Public config: disabled.
- Prospective collection: disabled.
- Prospective collection preflight: passed.
- Economic outcome read: forbidden.
- F09 action registration: forbidden.
- Live deployment: forbidden.
