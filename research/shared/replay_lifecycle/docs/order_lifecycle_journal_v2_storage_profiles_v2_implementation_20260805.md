# Order Lifecycle Journal-v2 Storage Profiles v2

Date: 2026-08-05

Last materially modified: 2026-08-12

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Status: deployed and first bounded session atomically admitted

## Scope

The prospective baseline epoch and lifecycle journal-v2 producer previously required every enabled output root to live below `${NARROWGATE_STORAGE_ROOT}`. That is a valid local replay/admission contract but is impossible on the EC2 live host. This implementation separates producer storage from local formal admission.

The storage profile has now been exercised on the live host and its first bounded session has been transferred and atomically admitted to the private evidence store. No economic outcome was read and the seven-tape bounded receive-time capture contract was not modified.

## Profiles

`local_orico_replay_admission` remains the default profile and the feature remains disabled by default. When enabled, the required mount must be exactly `${NARROWGATE_STORAGE_ROOT}`; journal and prospective epoch roots must be distinct strict children of that mount. This is the only profile whose validated writer health may set top-level `formal_collection_valid=true`.

`bounded_remote_spool` is the live producer profile. Both roots must be absolute, distinct siblings below one explicit allowlisted root. The default allowlist is `${NARROWGATE_REMOTE_ROOT}/formal_collection`. The filesystem root, broad home or mount roots, system trees, and temporary trees are rejected. Remote paths are normalized lexically so a local preflight cannot rewrite a remote-home identity through a host-specific filesystem alias.

The remote profile requires a finite duration and combined session/epoch byte bound; both values are frozen in the epoch manifest and writer identity. Reaching the duration bound stops accepting journal callbacks without blocking trading. Exceeding the byte ceiling invalidates the tape. Producer queue or writer errors retain the existing behavior: the tape becomes invalid, while the strategy continues running.

## Authority Boundary

Remote health emits `remote_spool_valid`, but always emits:

```text
formal_collection_valid=false
local_admission_complete=false
formal_collection_valid_reason=remote_spool_requires_rsync_and_local_orico_admission
```

The prospective epoch manifest and writer identity bind the storage profile. Session ID and baseline epoch ID are the same bounded ownership identity.

## Collector Interface

[`scripts/lifecycle_journal_v2_collector.py`](../../../../scripts/lifecycle_journal_v2_collector.py) retains read-only inspection. Formal transfer is owned by [`scripts/admit_prospective_lifecycle_remote_session.py`](../../../../scripts/admit_prospective_lifecycle_remote_session.py): it seals a stable bounded session, transfers the exact allowlist in one rsync files-from session, validates all parts and cursor chains, then atomically admits the staging directory. This data remains separate from the seven-tape capture.

## Verification

Targeted tests cover default-off behavior, local `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` enforcement, remote operation without an `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` mount, unsafe path rejection, finite bounds, nonblocking bound completion, authority fields, closed-session manifests, and live startup propagation into both the epoch and writer.

Machine-readable record: [`order_lifecycle_journal_v2_storage_profiles_v2_implementation_20260805.json`](order_lifecycle_journal_v2_storage_profiles_v2_implementation_20260805.json)
