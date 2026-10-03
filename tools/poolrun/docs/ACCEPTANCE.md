# Local acceptance record

[English](ACCEPTANCE.md) | [简体中文](ACCEPTANCE.zh-CN.md)

Last materially modified: 2026-09-30

Last materially synchronized: 2026-09-30

Environment: macOS arm64, Python 3.12.14. Synthetic data and disposable test SSH keys only. No production host, worker, netdisk account or business data was accessed. This change is local only, not a deployment.

## Baseline and SSH regression

The specified pre-change checkout's **27 tests passed** in an isolated baseline copy (20.90 seconds). That business acceptance remains valid for its version; it was not discarded. The SSH implementation's **46 tests passed** (31.40 seconds, no skips). The production state machine, Store, scheduler, resource rules, runner, validators and data adapters were not redesigned. Integration communication and authentication were replaced, and transport fault/boundary tests were added.

A final independent dependency run containing psutil but no aiohttp also passed all 46 tests (35.33 seconds, no skips). Source compilation, Ruff's F checks, git diff whitespace checks and the repository public-documentation audit passed. No remote CI was dispatched.

The two integration scenarios each execute seven tasks shared by three independent Agents, with future-only rollout and checkpoint/restart upgrade respectively. They use a real temporary loopback sshd, system OpenSSH subprocesses, distinct Agent keys, forced gateway commands and a resident Unix-socket Master. They do not preallocate per-host task counts. The synthetic data/result adapter remains a local-copy mock, not a real netdisk transfer.

| Coverage | Executed evidence |
| --- | --- |
| Finite queue, READY and shared input | Three Agents, seven tasks per mode, one content preparation per Agent, hash checks |
| Persistent SSH | Same SSH process carries consecutive poll/start/event requests |
| SSH disconnect | Kill control SSH only; business PID survives, same Attempt remains, no second launch |
| Master/Agent restarts | Existing runner continues; eventual completed results and release identity match |
| Lost START/result reply | Send and persist without consuming reply; disconnect; same ID returns original Attempt/receipt |
| Permission boundary | Agent key cannot administer or impersonate another host; unauthorized key and changed host key rejected |
| Forced command | A supplied touch command cannot bypass gateway; marker file is never created |
| Protocol | Half lines, consecutive lines, invalid/duplicate JSON, oversized messages, stdout contamination and 2 MB stderr flood |
| Local IPC | Only Unix socket in Master, owner permissions, peer UID rejection, second-writer/live-socket protection |
| CLI | SSH status, durable admin request replay/conflict checks, backup and snapshot-related existing paths |
| Code package | Register and fetch by project/release over SSH, equal bytes, path traversal rejected |
| Updates | Future-only does not change active release; checkpoint and explicit restart preserve generations/output ownership |
| Correctness | Store persistence failure, corrupt state, UNKNOWN, stale generation, immutable replies and result conflict rejection |
| Resources and data policy | Original admission, OOM versus SIGKILL, RESULT_PENDING, drain/GC protections and noncloud netdisk requirements |
| Offline lifecycle | Original local deadline/process-tree, incompatible checkpoint and frozen environment tests retained |
| Dependency removal | Control source import scan and lockfile assert no HTTP stack |

The socket/pipe malformed-stream tests are unit tests, not two-host acceptance. The real SSH tests are loopback networking on one machine, not two physical hosts. If sshd is unavailable on another environment, tests report an explicit skip; a skipped case does not establish SSH acceptance.

## Measured complete control-process footprint

Active-process snapshots from the two SSH integration runs, including the admin client session:

| Role | Processes | Aggregate RSS MiB | Aggregate CPU seconds since process start |
| --- | ---: | ---: | ---: |
| Master | 1 | 27.45–27.58 | 0.056–0.062 |
| Agents | 3 | 85.12–85.16 | 0.181–0.185 |
| OpenSSH clients | 4 | 18.03–18.06 | 0.031–0.036 |
| Temporary sshd listener and session children | 9 | 43.20–43.23 | 0.037–0.042 |
| Gateways (three Agents plus admin) | 4 | 106.81 | 0.146–0.149 |
| Runners/business, reported separately | 6 | 130.77–131.78 | 0.269–0.322 |

RSS sums include shared resident pages and are not unique physical-memory consumption. These are short synthetic-run snapshots, not CPU percentages or steady-state capacity guarantees. Brief package sessions may already have exited at sampling time; peak concurrent footprint is not measured. New SSH/gateway processes are included rather than hidden behind the Master/Agent figures. The isolated sshd itself is included even though a deployed owner might reuse an existing approved SSH service.

Ten persistent-session status requests averaged 2.54–3.09 ms in this small local test; idle one-second samples showed zero snapshot writes. Neither is a remote-network SLA. Private raw receipts are in pytest temporary directories; they include local paths and are not distributed. The old 12-task demo acceptance is historical; this turn reran the seven-task-per-mode SSH scenarios, not that old demo.

## Remaining field acceptance

- Two physical machines, real NAT/firewall/ProxyJump paths, long partitions and high-concurrency load have not been tested.
- Real netdisk login, provider failures, byte resume, upload deduplication, throttling and cloud relays remain untested; policy/adapters were preserved, not replaced.
- Linux cgroup v2, power-loss filesystem behavior, delegated account/group setup and cross-host master migration require their own platform acceptance.
- Production dependency/native inventories, real project checkpoint adapters, checkpoint CONVERT and automatic cross-host checkpoint migration are not validated.
- Prepared caches/external environments remain conservatively retained; there is no new long-running disk-pressure or optimal scheduling guarantee.
- Owner-UID processes remain trusted. Forced keys and UID authorization are basic identity separation, not a hostile multi-tenant sandbox.

No push or production deployment is part of this delivery. Preserve the original research schedulers until separately authorized field acceptance.
