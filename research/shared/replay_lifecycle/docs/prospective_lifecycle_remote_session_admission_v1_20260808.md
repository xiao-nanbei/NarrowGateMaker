# Prospective Lifecycle Remote Session Admission v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Status: `first_bounded_remote_session_atomically_admitted_to_orico`

The bounded AWS Tokyo journal-v2 session `prospective-1785924596631939323-4e487a4a8d53` was sealed remotely, transferred in one `rsync --files-from` session, deeply validated on `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` staging, fsynced, and atomically admitted. The collection interval was 2026-08-05 10:09:56 through 11:09:56 UTC.

Validation covered 5,922 frozen files, 2,664 Parquet parts and rows, and 589 durable lifecycle cursors. Part manifests, Parquet SHA256 values, event IDs, checkpoint chains, cursors, epoch identity, runtime identity, writer health, and row accounting all agree. Producer drops and errors were zero; queue HWM was 9, enqueue p99 was 133.17 microseconds, and worker write p99 was 272.93 milliseconds.

The admitted artifact is at `${NARROWGATE_DATA_ROOT}/formal_collection/prospective_lifecycle_journal_v2/session-prospective-1785924596631939323-4e487a4a8d53`. Its admission identity is `cc1d4ad466f0cd096ae0e30067b63d9240c21f4eb1baa08038fd035289a46db4`. The remote source payload remains intact.

This admission changes no quote, order, q90 action, model, or live policy. It reads no economic outcome and grants no action or live authority.

Machine-readable record: [`prospective_lifecycle_remote_session_admission_v1_20260808.json`](prospective_lifecycle_remote_session_admission_v1_20260808.json)
