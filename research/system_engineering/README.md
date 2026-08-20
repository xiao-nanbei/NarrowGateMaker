# SYS Low-Latency And Replay Parity

Last materially modified: 2026-08-20

Documentation boundary: this README and the unit's tracked `docs/` are public. Owner-only artifact locators, unpublished evidence indexes, and private research context are resolved through this unit's ignored local `private/` catalog and are not distributed with the public repository. See the [public/private research layout](../PRIVATE_EVIDENCE.md).

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../docs/public_private_documentation_contract.md).

Status: active system-engineering evidence line with no alpha authority.

`audit/` owns latency and receive-time analysis. C++ quote/replay/feature cores, live telemetry, and order management remain in their production directories; `docs/` contains latency, dispatcher, replay-clock, and deployment evidence. Shared dependencies: R, S.

The dated [`vultr_tokyo_live_host_migration_v1_20260811.md`](docs/vultr_tokyo_live_host_migration_v1_20260811.md) and [`aws_tokyo_live_host_reactivation_v1_20260811.md`](docs/aws_tokyo_live_host_reactivation_v1_20260811.md) are frozen predecessor evidence, not current deployment pointers. The current host-only cutover is [`aws_tokyo_live_host_migration_v2_20260820.md`](docs/aws_tokyo_live_host_migration_v2_20260820.md). The original AWS, intermediate Vultr, and reactivated-AWS predecessor logs are read only from verified local `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` archives. Historical latency evidence retains its exact host/epoch label and is never current-host calibration. The current authority and four-epoch/three-gap routing contract is [`docs/live_host_and_historical_data_access_20260811.md`](../../docs/live_host_and_historical_data_access_20260811.md).
