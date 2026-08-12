# Live Host And Historical Data Access

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](public_private_documentation_contract.md).

Status: `current_operational_pointer`

## Current and retired hosts

The current NarrowGate BTCUSDC live host is AWS Tokyo:

- instance: `<current-live-instance>`;
- Elastic IP: `<current-live-host>` (`<current-live-eip-allocation>`);
- SSH target: `<current-live-ssh-target>`;
- repository root: `${NARROWGATE_REMOTE_ROOT}`;
- architecture: `x86_64`;
- maker start: `2026-08-11T15:48:00Z`;
- first bound prospective epoch: `2026-08-11T15:48:02Z`, `prospective-1786463282415286221-dca0650f299a`.

Two predecessors are retired. The original AWS Tokyo instance `<retired-original-aws-instance>` and former Elastic IP `<retired-original-aws-host>` were terminated and released. The intermediate Vultr Tokyo host `<retired-intermediate-vultr-host>` stopped at `2026-08-11T15:17:38Z` and was destroyed after AWS health passed; deletion was confirmed by `2026-08-11T16:02:02Z`. Neither retired endpoint may be used for status, telemetry, capture, deployment, SSH fallback, or routine historical-log queries. The terminated misprovisioned address `<never-authoritative-host>` never had live authority and must not appear in a current or historical live epoch.

The private machine-readable pointer is `docs/private/live_remote.current.local.json`. Operational scripts must resolve `NARROWGATE_LIVE_REMOTE` or that pointer rather than copying a historical IP from a dated report.

## Query routing

| Question | Authoritative source |
| --- | --- |
| Current maker status or current logs | AWS `<current-live-host>` |
| Original AWS live data before `2026-08-11T11:43:10Z` | Local stopped AWS archive on `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` |
| Intermediate Vultr live data | Local stopped Vultr archive on `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` |
| Historical AWS receive-time capture denominator | Local `capture_ledger.v1.jsonl` and its admitted tape directories |
| Retired Vultr receive-time capture | Host-bound v2 ledger row and `vultr_tokyo_<retired-intermediate-vultr-epoch>_*` tape directory |
| New AWS receive-time capture | Host-bound v2 rows under transport-source amendment v2; first window not yet started |
| Frozen research conclusion | The report/spec and exact artifact named by the family registry |

For fill/trade queries, partition the requested half-open UTC interval by host epoch before reading rows. Only the current AWS partition may use SSH. Both retired partitions must use their verified local `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` archives.

The mandatory lookup order is:

1. normalize the request to `[start_utc,end_utc)`;
2. read current AWS rows for the intersection with `[2026-08-11T15:48:02Z,end_utc)`;
3. read the local Vultr retirement archive for the intersection with `[2026-08-11T11:47:12.288Z,2026-08-11T15:17:38Z)`;
4. read the local original-AWS retirement archive for the portion before `2026-08-11T11:43:10Z`;
5. retain `source_host` and `epoch_key`, preserve both maintenance gaps, then filter and union the rows.

The frozen routing boundaries are:

- AWS live authority ended at `2026-08-11T11:43:10Z`;
- the first Vultr state-sync row is `2026-08-11T11:47:12.288Z`;
- `[2026-08-11T11:43:10Z, 2026-08-11T11:47:12.288Z)` is a planned maintenance gap with no live authority;
- the Vultr signed API first returned `-2015` at `2026-08-11T15:11:22Z`; preserve `[15:11:22Z,15:17:38Z)` as a degraded subinterval rather than erasing fills from already-resting orders;
- Vultr shutdown completed at `2026-08-11T15:17:38Z`;
- the new AWS prospective epoch starts at `2026-08-11T15:48:02Z`;
- `[2026-08-11T15:17:38Z, 2026-08-11T15:48:02Z)` is the second planned maintenance gap.

For an interval spanning either cutover, query every intersecting partition and union source-labelled rows while preserving both gaps. A missing row inside a host epoch is a host-specific data gap; data from either adjacent epoch must not be used as a substitute. The original AWS `logs/trades.csv` covers from `2026-06-26T16:52:45.937Z`; earlier fill requests are unsupported by this frozen archive and must not silently fall back to other old snapshots.

Use `logs/trades.csv` as the economic fill table, but exclude rows where `trade_type=SYNC_ADJUST` from fill counts and fill PnL analysis; those rows carry restart position continuity rather than executions. Use `logs/order_outcomes.csv` with `event_type=filled` when order identity, age, reason, target, or lifecycle context is needed. Do not sum the per-row `realized_pnl` field as though it were an incremental reward.

The complete stopped AWS log and formal-collection archive is:

`${NARROWGATE_DATA_ROOT}/remote_live_retirement/aws_<retired-original-aws-epoch>_20260811/final_stopped`

Use its local `logs/` files for old `trades.csv`, `order_outcomes.csv`, `quote_decisions.csv`, `live_perf_telemetry.csv`, snapshot diagnostics, and maker logs. Use its local `formal_collection/` tree for old lifecycle-journal and prospective-epoch evidence. Routine analysis must not SSH to the retired host to read those files.

The final stopped archive contains 16,696 SHA256-verified files with zero missing, extra, or mismatched files. Credentials were excluded.

The intermediate Vultr stopped archive is:

`${NARROWGATE_DATA_ROOT}/remote_live_retirement/vultr_<retired-intermediate-vultr-epoch>_20260811/final_stopped`

Its 6,834 files/59,361,363 bytes passed local SHA256 verification. The source manifest SHA256 is `d3b27e2b0bca3299ab219a1934078cf5d99c205a290b7b440814089794b86e30`. Use this archive, not SSH, for the intermediate host's `trades.csv`, `order_outcomes.csv`, telemetry, maker log, capture markers, and lifecycle diagnostics.

## Retired AWS instance deletion record

The owner updated the Binance IP allowlist and then explicitly requested direct deletion of both the predecessor instance and its Elastic IP. At approximately 2026-08-11 13:34 UTC, AWS showed instance `<retired-original-aws-instance>` as terminated, Elastic IP allocation `<retired-original-aws-eip-allocation>` as released, and no remaining EBS volumes in `ap-northeast-1`. The former root volume `<retired-original-aws-root-volume>` had `DeleteOnTermination` enabled and was deleted with the instance.

No private AMI or EBS rollback snapshot was created. The owner explicitly waived the fast AWS rollback path before deletion. Consequently, the verified local archives are the sole recovery and historical-query source for the old host; neither the instance nor `<retired-original-aws-host>` can be used as a fallback.

The additional unique files outside the repository archive were preserved under the local `supplemental_home_admitted` archive: 166 files/660,908,196 bytes, including the 351-MB BUY-q90 export, the separate home-level logs, deployment backups, and six release generations. Its remote-relative manifest has SHA256 `707a5c2ce54cc99bf14720b51e3ca723fbc5988f63dc252b6c42e9dc479cfe16`; all 166 local hashes passed with zero missing or extra files. Together with the repository-level 16,696-file archive, the historical-data preservation gate is complete.

### Checks independent of the completed AWS deletion

The first Vultr bounded capture and lifecycle journal are intermediate-host data-quality checks. Neither depended on nor could have blocked the completed original-AWS deletion:

- **Post-capture health passed.** Capture `20260811T123346Z` recorded 3,600.207 seconds, seven tapes, 1,680,170 events and 11 maker fills. The recording flags were disabled, baseline config restored, strategy hash unchanged, recorder drops zero, gzip/SHA/event checks valid, and the v2 ledger row was admitted. It validates that Vultr capture only, not current AWS transport or the trading strategy.
- **Lifecycle health** means a journal session has zero writer errors/drops, callback counts converge, terminal/exposure state is internally consistent, and the remote spool subsequently passes local `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` admission. The retired Vultr session failed on a never-activated GTX `-5022` rejection whose snapshot encoded exchange exposure as exact zero while its last event encoded it as unavailable. Trading continued; only this evidence session is invalid.

The new AWS session reproduced the same evidence-only defect at `2026-08-11T16:00:06Z` on rejected BUY order `mm_B_1786464006030_148`. Binance `-5022` explicitly said the order was not recorded; live trading continued and the exchange order path was safe, but the AWS lifecycle session is also invalid for formal admission. This unresolved gate blocks eventual M0/M1 eligibility, not capture collection or live maker authority.

The final AWS capture `20260811T080319Z` is preserved locally with seven valid tapes, 832,949 events, zero recorder drops, six maker fills, and no severe log. It missed canonical admission before cutover. Because the frozen AWS v1 ledger must remain immutable, its disposition is `retained_unadmitted_diagnostic_not_counted`; it is not a missing-data or EC2 termination blocker.

## AWS evidence reuse after migration

The strategy source, config, model, and P3 identities were preserved across the two cutovers. All three live epochs are Tokyo/x86_64 and use the same broad 2-vCPU/4-GiB resource class. Therefore admitted historical market, live, fill, and latency data can be reused without recollection as:

- immutable historical live evidence for their AWS epochs;
- a frozen empirical transport prior or replay sensitivity;
- a separately identified source stratum in a source-aware research panel;
- regression evidence for code/config/model behavior that is not provider-network-specific.

Direct reuse does not mean relabelling or unlabelled pooling. Every reuse must retain its provider, region, IP/instance or host identity, original capture/runtime epoch, and original profile hash. Do not relabel an old AWS or Vultr row as a current-AWS measurement. Equal CPU/RAM class does not make cloud routing, kernel scheduling, WebSocket stalls, or REST/user-stream transport identical.

Current-host transport claims require measurements bound to AWS instance `<current-live-instance>`, EIP `<current-live-host>`, and the current runtime/config epoch. Old AWS and Vultr profiles remain historical sensitivities; neither is current-host live parity.

## Receive-time capture and F04 boundary

The legacy AWS ledger is:

`${NARROWGATE_DATA_ROOT}/receive_time_tape/capture_ledger.v1.jsonl`

At cutover it contains 23 admitted rows, including 22 valid full windows over 21 distinct UTC days. Same-day duplicates count once and captures shorter than 3,500 seconds remain diagnostics. Its cutover SHA256 is `6cb9c4729b179fa1b9c0959e408d8f0b0ab860ab1f6f293c8bda2dfba243e2ac`.

The stopped archive also contains a 2026-08-11 AWS capture candidate with seven tapes and a near-one-hour observed interval. It was not admitted into the canonical ledger before cutover and therefore does not count unless a separate local validation/admission proves every frozen gate. Its mere presence in the retirement archive is not admission.

Every capture must carry provider, region, SSH target, public IP, runtime/config identity, and a host-specific directory prefix. The admitted Vultr row remains immutable historical evidence. A new AWS row must use the new EIP/source identity and cannot be appended under a Vultr prefix. Rows from different hosts may be read together only through an explicit source-aware admission contract; they must never be pooled by an unlabelled day counter.

Across immutable v1 and source-bound v2, the admitted total is currently 23 valid full windows over 22 distinct UTC days: 21 original-AWS days and one Vultr day. The new AWS epoch begins from that frozen denominator and does not recount either historical source.

The frozen `first_add_external_incremental_value_m0_m1_v1` preregistration explicitly names AWS Tokyo receive-time state. Its target, M0/M1 definition, chronology, side split, Grade A/B panels, true leave-one-venue-out rebuild, common-row denominators, and prediction-only permissions remain unchanged. The frozen v1 transport amendment admitted original-AWS and Vultr as separate strata. The outcome-blind v2 amendment adds `aws:ap_northeast_1:<current-live-epoch>` without modifying either historical stratum or reading the target.

The bounded-capture automation is data collection only. When authorized, it may start the detached one-hour recorder and admit valid tapes; it must not create, enable, or test a strategy arm. Once the genuinely admissible panel reaches 30 distinct full-window UTC days and every frozen denominator passes, it may run only the preregistered prediction audit and must then delete itself.

Automation ID `collect-vultr-tokyo-bounded-market-tapes` retains its historical ID but is now named `Collect AWS Tokyo bounded market tapes` and is `ACTIVE`. It resolves the current pointer, requires source key `aws:ap_northeast_1:<current-live-epoch>`, and is governed by the v2 transport-source amendment. It must never run with the retired Vultr prompt or recollect/relabel the admitted Vultr capture.

Frozen AWS REST-latency evidence is under `${NARROWGATE_DATA_ROOT}/reports/formal_recalibration_20260715/`. Frozen AWS WebSocket profiles are under `live/profiles/latency/aws_tokyo_*`. Use their original hashes and environment labels when selecting a historical sensitivity; do not copy their millisecond values into a current-host profile ID without a new host-bound measurement.

## Migration evidence

See [`vultr_tokyo_live_host_migration_v1_20260811.md`](../research/system_engineering/docs/vultr_tokyo_live_host_migration_v1_20260811.md) for the frozen intermediate cutover evidence. It is historical and must not be rewritten as the current pointer. The machine-readable current identity and three epoch boundaries are in `docs/private/live_remote.current.local.json`.
