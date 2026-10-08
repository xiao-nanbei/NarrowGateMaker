# PoolRun Lite

[English](README.md) | [简体中文](README.zh-CN.md)

Last materially modified: 2026-10-04

Last materially synchronized: 2026-10-04

Heartbeat loss keeps an Attempt occupied and uncertain, not failed. A fresh authenticated Agent poll may restore `RUNNING` only when both runner and business process are alive, the reported PID/birth/boot identity matches the original process, and the current Attempt/generation is unchanged. Pending stop commands, other uncertainty reasons and terminal/replaced Attempts are never cleared by this recovery. Existing Agents without liveness evidence retain conservative `UNKNOWN` behavior. SSH tunnels need an operating-system supervisor with keepalive/reconnect; changing an idle timeout alone does not restore an exited tunnel.

A general-purpose, finite-batch, data-aware task pool: one Python master, an outbound-polling Agent on each host, and independent runners, using JSON snapshots and local file locks. It is not tied to a cloud provider, research project, host name or machine size. Operators supply machines and resource budgets; the scheduler does not purchase, resize or delete machines.

This independently installed subproject is maintained inside NarrowGate. It has not taken over existing NarrowGate jobs, connected to research machines or transferred production data. It is a runnable first version, not a promise of seamless migration for arbitrary applications.

## 1. SSH and same-owner local control

### Existing prepared-cache locality

Agents scan administrator-configured `prepared_cache_roots`, for example `{"replay-tape":"/approved/shared/prepared"}`, before reporting readiness over the existing SSH session. Each immediate cache child must have a `manifest.json` with an exact `identity` object and a `files` mapping from relative names to byte sizes. Partial directories, symlinks, missing files and size mismatches are not ready. Discovery is read-only, refreshed every 30 seconds, and is not a replacement for the business reader's cache validation.

The submission adapter adds `"cache_affinity":{"namespace":"replay-tape","identity":{"schema":"public-prepared-v2","manifest":"<input-manifest-identity>","tick":"0.1"}}` using the actual cache producer's identity, not an account name or date. All eligible hosts need the same namespace configured, with their own physical root. The Master first collects fresh inventories, then offers the task only to a compatible host holding that identity, even if that host must finish other work first. Offer and START both enforce locality. Missing peer reports wait up to 30 seconds after submission or Master restart; offline/draining hosts do not indefinitely reserve work. A host without a successful local scan cannot claim this task. If no reachable eligible host has the cache, ordinary preparation remains allowed; `why` reports inventory/locality waits. Inventory ages above 60 seconds or host reports older than 30 seconds are not used.

Tasks without `cache_affinity` retain their existing behavior: input objects alone do not describe an application's hidden prepared cache. Adding these bindings is part of the submission adapter, not a retrospective rewrite of running task specifications. This feature neither transfers cache files nor restarts/migrates active Attempts; computation limits and data transport policies are unchanged.

Python 3.12+ on Linux/macOS; runtime dependency `psutil==7.2.2`, tests `pytest==9.1.1`, system OpenSSH client/server for remote control. There is no PoolRun HTTP/HTTPS server, TCP control listener, TLS configuration, Bearer token or HTTP tunnel. Remote control uses Agent/CLI → outbound SSH stdio → fixed gateway → local Unix socket → one resident Master. A same-owner local Agent can instead specify `socket` and its existing `host`, without `ssh`; it sends the agent principal and retains agent-only operation permissions. The OS-owner trust boundary remains unchanged. The gateway never constructs a Master or opens state.json. Prefer a local master for workstation-led runs; remote Agents still need an approved reachable endpoint. Migrating the controller does not migrate or restart its runners.

```bash
cd "${NARROWGATE_ROOT}/tools/poolrun"
python3 -m venv .venv
.venv/bin/python -m pip install '.[test]'
.venv/bin/python -m pytest -q -rs
```

The integration tests create a temporary loopback-only sshd, fresh test host/client keys and isolated directories. They do not modify system sshd, enable remote login, read production keys or contact real netdisk accounts. Missing server/permissions are reported as skipped SSH acceptance, not replaced with a pipe pretending to be a network. Separate socket/pipe fault tests are labelled units. Real loopback SSH is still not two-physical-host or production-network acceptance.

### Server-owned identity

An administrator prepares an approved SSH access account and installed read-only PoolRun environment. Give every Agent its own public key; give administrators a different key. Put fixed commands in administrator-owned authorized_keys (replace all example paths and public-key placeholders):

```text
restrict,command="/opt/poolrun/venv/bin/python -I -m poolrun ssh-gateway --socket /approved/ipc/master.sock --principal agent:host-a" ssh-ed25519 AGENT_PUBLIC_KEY
restrict,command="/opt/poolrun/venv/bin/python -I -m poolrun ssh-gateway --socket /approved/ipc/master.sock --principal admin" ssh-ed25519 ADMIN_PUBLIC_KEY
```

These are complete authorized_keys entries, not client options. Disable password/keyboard-interactive access and user-controlled SSH environment for this dedicated account; grant no unrestricted keys, shell, PTY, forwarding or agent forwarding. The installed code, fixed command, authorized_keys and parent directories must not be writable by an untrusted gateway account. `SSH_ORIGINAL_COMMAND` is ignored. Task argv, filenames and payloads are JSON, never interpolated into the SSH command. This is a single-owner tool, not a hostile multi-tenant sandbox.

The default state root is owner-only (0700), snapshots 0600, socket directory 0700 and socket 0600. The resident Master's OS owner is the trusted local administrator. For separate approved gateway OS accounts, the administrator precreates a short IPC directory with a dedicated approved group (0770), sets `socket`, `socket_gid`, and `gateway_uids` in control.json, e.g. `{"502":["agent:host-a"],"503":["admin"]}`. Keep the state root outside that shared directory, 0700. The Master verifies kernel peer UIDs and permitted principals, not a client-supplied role. Other local UIDs are rejected even if they reach the socket. Members with the same OS UID are inherently trusted; forced SSH commands restrict remote keys, not a malicious local process under the owner UID.

The Master acquires the Store's single-instance lock before touching a stale socket, rejects a live socket, and will not unlink a regular file or symlink. Use a short Unix socket path (OS pathname limits apply).

### Start and connect

Edit [control.json](examples/control.json), [host.json](examples/host.json) and [client.json](examples/client.json). Configure an SSH alias in the user's approved SSH config; it identifies the current control host, not a permanent machine. Agent and CLI select explicit `identity_file` and `known_hosts_file`; an optional `config_file` selects an approved OpenSSH config, including an already-approved ProxyJump. Verify the server host key out of band. No automatic key acceptance, firewall changes or overlay networking are performed.

```bash
python -m poolrun master --root /approved/private-state --config control.json
python -m poolrun agent --config host-a.json
python -m poolrun --config client.json status
python -m poolrun --config client.json submit --file tasks.jsonl
```

A local administrator may use `{"socket":"/approved/ipc/master.sock"}` instead of SSH; this still calls the resident Master and never edits the snapshot directly. Agents require SSH. Legacy endpoint/TLS/token configuration fails with a migration error; there is no compatibility mode. Existing business state, Attempts, runners, caches and release identities are retained; change communication configuration during an authorized service boundary, not by modifying running workers.

Each Agent retains one SSH subprocess for serialized small requests. OpenSSH uses -T, BatchMode, strict host keys, keepalives, connection timeout and no port forwarding. Up to 32 requests may be admitted; each request has a 30-second default exchange timeout and at most four connection attempts with bounded backoff. JSON lines have a 24 MiB ceiling; code packages have a separate 16 MiB ceiling and a short separate SSH session. stdin/stdout carry only protocol; stderr is continuously drained into a bounded diagnostic tail. Runner descriptors are explicitly detached.

Connection loss means unknown execution, not failure or permission to spawn again. Reconnect first reconciles local Attempts; pending START/events/transfers retain request ID and payload. CLI mutations print a request ID and save the exact request/ack in `request_dir` (default `~/.local/state/poolrun/requests`, private, not a publication artifact). After an uncertain CLI exit, repeat the same command with `--request-id ID`; changing payload or endpoint under that ID is rejected. Successful Store persistence precedes the response. Old attempt generations remain fenced.

For diagnosis, inspect Agent stderr, the bounded OpenSSH stderr tail, server sshd logs, gateway stderr and the local socket/UID permissions. Host-key mismatch or unauthorized keys are not bypassed. SSH cannot manufacture a reachable endpoint through arbitrary NAT/firewalls: provide an approved reachable alias/ProxyJump or report the connection blocker. No workstation inbound SSH is required.

The optional `examples/demo.py --root NEW_DIR --init-only` writes templates. An actual demo additionally needs `--connections ISOLATED_CONFIG_DIR` containing admin.json and host-a/b/c.json with fixed commands bound to the demo's control/master.sock; do not point it at a production gateway. The full automated SSH demonstration is the integration test suite.

## 2. Tasks, slots and data

Slots are concurrency ceilings, not a promise of running replay workers. An administrator can explicitly set host `memory_admission: false` through `host-update --resources` to disable estimated-memory and live-memory admission checks for that host. This does not falsify memory measurements, disable slot/CPU/disk checks, change runner termination policy, or automatically retry failed tasks. Defaults remain memory-guarded; use this override only with owner approval. Preparation performed inside a business command still occupies its slot; use the prepared-cache interface for scheduler-visible staging.

Stable host IDs do not depend on IP addresses. Master `hosts` entries specify `slots/cpus/memory_bytes/disk_bytes`, memory/disk headroom, platform and capabilities. Tasks declare independent peak resource budgets; disk budgets must include temporary files, outputs and checkpoint peaks. Budgets use bytes, time uses Unix seconds, and platforms include `linux-x86_64` and `darwin-arm64`.

The global finite queue does not preallocate host quotas. Scheduling combines priority, aging and project rotation. Ready data is preferred; preparation placement considers existing copies, available budgets and link samples. Unknown bandwidth gets a conservative nonzero estimate, not an exact completion prediction. Aging large-memory tasks prevent smaller tasks from continually consuming released capacity. Unready inputs/code/environments do not occupy business slots. Each task has one preparation target, each host at most one prefetch, with bounded prefetch bytes. Preparation and result uploads share a host-local heavy-operation lock.

Register immutable input objects, then reference their SHA256 IDs in task `inputs`:

```json
{
  "id": "actual-64-character-sha256",
  "sha256": "actual-64-character-sha256",
  "size": 12345,
  "reconstructible": true,
  "sources": [{"route":"netdisk","zone":"noncloud","source_id":"workstation","account":"existing-account","object_ref":"existing-tool-object-reference"}]
}
```

```bash
python -m poolrun --config client.json object-register --file object.json
python -m poolrun --config client.json submit --file tasks.jsonl
```

The `task_id/business_spec/resources.memory_gib` form is also accepted. `business_spec.input_refs[].object_id` must resolve to registered content hashes. `project_default` requires a prior project-default `rollout`; missing versions are not guessed. Unfinished or held upstream dependencies block dispatch. Reimporting the same business task is idempotent and does not undo later version policies; conflicting specifications under one ID are rejected.

Optional `prepared` recipes contain `platform/contract/recipe_version/input_ids` and `memory_bytes/disk_bytes/cpus` budgets. Releases declare `prepared_contract/prepare_argv`. Builders receive recipes, input-cache and temporary-output directories on stdin and return `{"status":"READY","files":[{"file":"name","sha256":"..."}]}`. Verified outputs enter a shared prepared cache; recipe identity does not contain the whole repository Git SHA. Prepared caches are not automatically deleted in this version.

## 3. Transfer adapters

`transports` and `result_adapter` use explicit argv arrays, never shell concatenation. Credentials stay in the tools' local configurations. The contract is stdin JSON/stdout JSON; stderr is not uploaded to the control plane. Adapters must obey requested rate limits and cannot launch unbounded internal transfers; audit their actual behavior. For an adapter that only verifies and durably saves results on the same host, set Agent `result_storage` to `host_disk`: no network transfer ticket or bandwidth quota is required. This does not mean results have been copied to another machine. Network-upload adapters must not use this mode. Durable-result validation and idempotent completion still apply.

| Route | Rule |
| --- | --- |
| Cloud to cloud | Configured destination-pull `direct`, or an explicitly approved netdisk source |
| Noncloud to cloud, cloud to noncloud, noncloud to noncloud bulk data | `netdisk` only; no hidden SSH/scp fallback |
| Code and small control files | Restricted SSH packages operation by project/release identity; code packages are limited to 16 MiB |
| No direct connectivity | An explicitly approved single-relay source; no automatic multihop exploration or relay purchase |

Input and result transfers have durable global, account, source, destination and link concurrency reservations; the default is one heavy transfer per destination. Disconnection alone does not release an unknown transfer's reservation. Inputs use `.part` files, verified size/SHA256, fsync and atomic publication before READY. Tasks on one host share one copy. Transfer retries do not consume business recomputation attempts.

Input request example:

```json
{"action":"fetch","object":{"id":"sha256","size":123},"source":{"route":"netdisk","object_ref":"..."},"destination":"/absolute/incoming/hash.part","host":"host-a","rate_bytes_per_second":10485760,"transfer_id":"..."}
```

Return `{"status":"READY"}` or `{"status":"NEED_AUTH"}`. Expired credentials/HTTP 403 must become NEED_AUTH, never fabricated readiness. Existing netdisk tooling owns content-addressed upload reuse and destination-isolated download receipts; the scheduler does not implement provider login. When byte resume is unavailable, redownload the same file; never concatenate unverified partial files from different sources.

Result requests include `action=save_result`, attempt ID, output directory, a file manifest with sizes/SHA256 and rate limit. Return `{"durable":true,"uri":"verified-external-location","files":[...]}` only after durable verification. Repeated requests for one attempt must be idempotent. Computation exit releases CPU slots, while RESULT_PENDING protects disk and artifacts. Failed uploads retry transport, not computation.

Failures back off for at most five rounds; blocking reasons appear in `status.live.*.errors`. After repairing credentials/connectivity, stop the Agent service, not its runners, reset the selected transfer retry budget, and restart the Agent:

```bash
python -m poolrun agent-reset-transfer --root HOST_ROOT --target TASK_OR_ATTEMPT_ID
```

This clears only preparation/upload errors, not business generations, and does not recompute. Real loopback SSH is tested separately; real netdisk, cross-host relay and bandwidth behavior remain unverified. Validate adapters using nonproduction synthetic files first.

## 4. Immutable code, rollouts and checkpoints

```bash
python -m poolrun release create --project calc-demo --root examples/synthetic \
  --include examples/synthetic/include.txt --id r2 \
  --contract examples/release-contract.json --destination packages
python -m poolrun --config client.json release register --file packages/r2/release.json
python -m poolrun --config client.json release prepare --project calc-demo --release r2 --hosts host-a,host-b
python -m poolrun --config client.json rollout --project calc-demo --release r2 --scope pending --mode future-only --missing wait
python -m poolrun --config client.json status --versions
```

Export uses an explicit file allowlist and rejects private artifacts, credentials, `.git/.venv`, symlinks and escaping paths. Mutation during export is rejected. Agents verify and publish read-only `releases/PROJECT/RELEASE`. START freezes release, argv, environment mapping, output contract and business specification. Attempts do not execute working-tree code, follow a `current` link, run `git pull/importlib.reload` or share output directories.

Future-only is the default: change only pending tasks without an Attempt, not authorized STARTING or running tasks. Fallback requires an explicit task `code_policy.missing=fallback` plus an `allowed_fallback` allowlist; otherwise unavailable versions wait.

Create and approve environments at their final paths. Do not copy/rename virtual environments or upgrade packages in place. Agents reject a changed mapping for one env_id. The stdlib demo verifies only the interpreter; dependency/native workloads need complete file manifests:

```bash
python -m poolrun env-seal --root /final/env --python /final/env/bin/python \
  --include environment-files.txt --output environment.json
```

Put the environment descriptor in host `environments[env_id]`, and its `manifest_sha256` in release `environment_manifests[platform]`. Approved manifests must cover actual dependencies, native binaries and configuration. The tool cannot infer semantic equivalence of arbitrary environments. Administrators must not mutate approved environments during execution; read-only permissions are not a sandbox against malicious modification by the same OS account.

Explicit running-task updates:

```bash
python -m poolrun --config client.json upgrade --task demo-001 --release r2 --mode checkpoint
# Use restart only with permission to interrupt and recompute; preserve old outputs.
python -m poolrun --config client.json upgrade --task demo-002 --release r2 --mode restart
```

Checkpoint mode prepares the target release before atomically writing `control/pause.json`. The business program publishes checkpoint/receipt at a valid boundary and exits; writing a checkpoint without exiting is insufficient. The target adapter checks source binding, hashes and contract and returns DIRECT plus `resume_argv`; a new generation then resumes. The example preserves cursor/RNG/output prefix and validates equivalence against from-start computation. Missing/incompatible checkpoints, UNKNOWN or load failure never imply permission to restart from zero.

This version implements DIRECT only. CONVERT is blocked until a project-specific converter is approved and validated; it cannot silently alter pickle/public_binding. Checkpoints stay on their source hosts; automatic cross-host checkpoint/dependency transport is not enabled. After a failed target resume, explicitly resume the old checkpoint with its original version:

```bash
python -m poolrun --config client.json resume --task demo-001 --release r1
```

Normal rollback is another future-only rollout. `hold --project PROJECT` preserves accepted results but blocks further dispatch, result acceptance and dependent tasks. It is not automatic recomputation.

## 5. Hot-plugging, stopping and OOM

```bash
python -m poolrun --config client.json drain --host host-a
python -m poolrun --config client.json drain --host host-a --resume
python -m poolrun --config client.json host-update --host host-a --resources new-budget.json
python -m poolrun --config client.json pending-placement --project demo --hosts host-a,host-b
python -m poolrun --config client.json stop --task TASK --mode checkpoint
python -m poolrun --config client.json stop --task TASK --mode terminate
python -m poolrun --config client.json retry --task TASK --memory-bytes 8589934592
```

`pending-placement` changes placement only for pending tasks that have never started an Attempt. It preserves the immutable business specification and its hash, existing Attempts, retries and results. The explicit host list must contain known distinct hosts; normal environment, release, data and resource checks still apply. It does not copy inputs or increase concurrency.

Reducing slots/budgets does not kill in-flight work; it prevents new starts that exceed the new budget. To add a host, add its host policy to master configuration and bind a separate restricted SSH key, restart only the master service, then start the new Agent. Existing runners do not restart. Agents are outbound-only but need a reachable master endpoint; coordination is impossible without one.

Agent `stop_accepting_at`, `checkpoint_at` and explicitly authorized `hard_deadline` policies persist separately. Runners enforce their local deadlines even while the master is offline. Without hard-stop authorization, a checkpoint request does not fall back to killing the process.

Runners track PID, creation time, boot identity, process groups, discovered descendants and a local attempt lock. A saved spawn intent without a process receipt becomes UNKNOWN conservatively. Agent restarts do not respawn business jobs. Host disconnection does not prove death; UNKNOWN retains reservations.

On Linux, an approved delegated `cgroup_root` allows joining cgroup v2 before exec and inspecting `memory.events`; this path has not been tested on the local macOS host. Otherwise RSS is monitored, but soft memory termination is disabled unless explicitly enabled through the owner policy below. This does not guarantee against host OOM. `requires_hard_isolation=true` tasks are not assigned to soft-isolation hosts. macOS tracking cannot guarantee catching every daemonized grandchild that escapes before discovery. Workloads needing that guarantee require validated cgroup hosts or must prohibit daemonization.

Exit 137/SIGKILL is not automatically OOM: watchdog and cgroup evidence are distinguished. Business failures retry only explicitly and within `max_attempts`. OOM retry excludes the original host and may increase the next reservation without modifying the business spec; the next host must meet the new budget. Unknown execution can revoke a generation only through explicit `retry --uncertain` for `side_effects=false` tasks. External-side-effect tasks remain manual. Network partitions cannot guarantee physical exactly-once computation; with intact state, only one authorized generation's result is accepted.

## 6. State, backups and garbage collection

Master `control/state.json` is authoritative. Commits copy state, fsync a temporary file, atomically replace, fsync the directory, then update memory/ACK. A failed snapshot write makes that master fail closed until restart and inspection. Heartbeats/progress stay in memory, avoiding full snapshots every second. Retry side-effecting requests with global `--request-id`; reusing an ID for different content is rejected.

```bash
python -m poolrun --config client.json backup --destination ./private-backup
python -m poolrun --config client.json gc --host host-a
python -m poolrun --config client.json gc --host host-a --apply
```

GC without `--apply` only plans. Backups retain the latest five master copies and a client-selected copy. Also back up configuration, packages and local credential references separately; original inputs/results retain their own durability policies. GC removes only reconstructible, unreferenced blobs and releases. Running/STARTING/UNKNOWN, pending archive/checkpoint dependencies, pending inputs, current default and the latest two fallback releases stay protected. New START is blocked during host GC. Results, checkpoints, unique originals and external environments are not deleted. Prepared caches/environments remain conservatively retained and require manual cleanup review; historical disk growth is not universally solved.

For planned master migration, stop and prove the old master cannot restart, copy the entire control state/configuration/packages, update the approved SSH alias and verify the new server host key, then start the new master. `flock` prevents same-host duplicates, not cross-host leader election. Corrupt state refuses startup; it does not silently roll back to a backup. Stale restoration can lose generations. If an Agent reports an attempt missing from restored state, enter global reconciliation_required and stop dispatching. Restore later authoritative state or reconstruct evidence manually; there is no ignore-old-processes recovery shortcut.

## 7. Acceptance and untested scope

See the [acceptance record](docs/ACCEPTANCE.md). Original local business acceptance is preserved; SSH-specific results and remaining limits are documented separately. They do not validate:

- Real netdisk authentication, upload deduplication, throttling, byte resume, cloud links or relays.
- Linux cgroup v2, Linux power-loss filesystem guarantees or planned cross-host migration.
- Complete dependency seals for production models/native environments, or real project checkpoints including NarrowGate.
- Checkpoint CONVERT or automatic cross-host checkpoint migration; these requirements block rather than trigger guessed execution.

This version has no web UI, database, automatic machine provisioning, cross-host master election or provider login implementation. Do not replace existing production schedulers before the required field acceptance.

### Owner-selected soft memory enforcement

Admission estimates are distinct from termination policy. The runner defaults to no soft memory termination limit, including for newly named projects: `memory_bytes` is not automatically a kill threshold. An owner may explicitly enable the watchdog in Agent-root `runtime-memory-policy.json` using `{"defaults":{"soft_memory_watchdog":true}}` or `{"projects":{"project-id":{"soft_memory_watchdog":true}}}`; project settings override defaults, and `false` disables it. When enabled, the task's `memory_bytes` is the soft RSS limit. Reservations, host admission, slot caps, deadlines and explicit stop requests remain unchanged; this does not disable a configured cgroup limit or the OS OOM killer. Runners read policy changes while running, but older processes do not acquire new code: explicitly disable affected projects in their supported policy before updating installed code. After correcting the policy, `retry --task ID --override-soft-memory-failure` permits one additional attempt after a confirmed soft-watchdog failure, preserving frozen task bindings and old outputs. It does not override UNKNOWN or kernel OOM failures.
