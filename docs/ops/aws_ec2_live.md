# AWS EC2 Live Runbook

<p><a href="aws_ec2_live.md">English</a> | <a href="aws_ec2_live.zh-CN.md">简体中文</a></p>

Last materially synchronized: 2026-09-03

This runbook describes a reusable AWS EC2 deployment pattern for the public
NarrowGateMaker code. It contains no current host, credential, account state,
active release, strategy parameter, or artifact identity. `203.0.113.10` is an
RFC 5737 documentation address, not a deployment target.

The local synthetic demo is the five-minute quickstart. A live deployment is a
separate, owner-operated process with private configuration and venue authority.

## Public/private boundary

The public repository supplies source publication, runtime verification,
deployment-envelope, reconciliation, and process-entry tools. The operator must
supply and retain outside the Git checkout:

- the actual host and access route;
- credentials and service environment;
- active config, model, policy, and authorization bundles;
- Linux wheels, dependency lock, and wheelhouse;
- deployment, reconciliation, activation, and rollback records;
- current health and account/order/position state.

Never paste those values into this runbook, an issue, a test fixture, or a
tracked example. Commands below use logical placeholders only.

## One-time instance preparation

Use a supported 64-bit Linux image with systemd and CPython 3.12. Build Linux
wheels on a controlled Linux builder or CI runner, not by downloading arbitrary
dependencies during activation.

Never compile the native extension on the EC2 host while `narrowgate.service`,
a legacy `narrowgate-maker.service`, or any maker process is running. The
canonical `make native-live-wheel` entry point enforces that boundary, defaults
to one compile job, and refuses to start below 2,048 MiB `MemAvailable`. A 2 GiB
live host is therefore not an admitted build host. Prefer a controlled
Linux x86_64 Azure builder with at least 16 GiB RAM and exact GNU C++ 11.5.0;
build the `ec2-cascadelake-avx2` wheel there, then transfer the immutable wheel
into the release wheelhouse. Do not use the Azure host's `-march=native`.
Installation on EC2 must still pass the native build receipt, import, and
Python/C++ parity checks; target-host performance measurements remain EC2-only.

Host requirements:

- encrypted root storage;
- no public application port;
- administrative access restricted to an approved route;
- only the outbound venue and operating endpoints needed by the deployment;
- an unprivileged `narrowgate` service user;
- separate release and private-artifact roots;
- systemd as the only live process owner.

For SSH examples, an operator may place an alias in `~/.ssh/config` and use:

```text
Host narrowgate-example
    HostName 203.0.113.10
    User narrowgate
```

The address is documentation-only. Prefer AWS Systems Manager Session Manager
when available, or limit the EC2 security group to the operator's real source
network. Do not commit a proxy, key path, host key, or real address.

Create stable roots once:

```bash
sudo useradd --system --create-home --shell /bin/bash narrowgate
sudo install -d -o narrowgate -g narrowgate -m 0755 /opt/narrowgate/releases
sudo install -d -o narrowgate -g narrowgate -m 0700 /opt/narrowgate/private
sudo install -d -o root -g root -m 0700 /etc/narrowgate/releases
```

The release checkout contains public source only. The private root contains the
release-scoped runtime and private inputs. A root-owned mode-`0600` environment
file selects them for systemd without putting credentials on a command line.

## First release

From a clean operator/build checkout, publish one exact source release:

```bash
export NARROWGATE_RELEASE_TAG="<annotated-release-tag>"
export NARROWGATE_DEPLOY_TARGET="narrowgate@narrowgate-example"
export NARROWGATE_RELEASE_DIR="/opt/narrowgate/releases/<release-id>"

make publish-source-dry
make publish-source
```

When native inputs changed, produce the immutable native wheel on the controlled
builder before staging the runtime closure:

```bash
make native-live-wheel
```

The target writes to `dist/native` by default. It is intentionally independent
of `publish-source`, deployment preflight, installation, and activation; none of
those live-host operations may compile source.

`publish-source` transports source only. It must not transfer credentials,
private config, models, policy artifacts, runtime receipts, or process-control
authority, and it must not start or restart live.

Use the operator's approved private channel to place the complete Linux runtime
closure under `/opt/narrowgate/private/<release-id>`. Then:

1. create `venv-<execution-commit>` with `live.deployment_runtime install`;
2. run `verify-install` and `verify-static-tree`;
3. expose that exact environment through the release's ignored `.venv-active`
   selector;
4. run `live.native_build_receipt` from the installed environment so the native
   ABI and Python/C++ parity smoke are bound to this release;
5. run `make deploy-preflight` against the private active config;
6. build the deployment envelope from the exact config, runtime, native, model,
   and enabled policy bundle;
7. keep all emitted records in the release-scoped private directory.

Use the command help as the canonical field reference instead of copying a
release-specific command transcript into documentation:

```bash
python3.12 -m live.deployment_runtime install --help
python3.12 -m live.deployment_runtime verify-install --help
python3.12 -m live.deployment_runtime verify-static-tree --help
python3.12 -m live.deployment_runtime build-envelope --help
python3.12 -m live.native_build_receipt --help
```

An incomplete artifact closure is a staging failure. Do not repair it by
allowing network dependency resolution on the live host.

## Python-only incremental release

A Python-only change can reuse the exact admitted native wheel bytes only when
all of the following are true:

- no C++ source, compiler/runtime dependency, binding declaration, native ABI,
  or native build option changed;
- the reused wheel is compatible with the target OS, architecture, and CPython;
- the new release still passes the native import and Python/C++ parity smoke;
- the operator builds a new Python root wheel from the new clean commit.

Prove the first condition by comparing the two immutable release trees that
will actually run, including `cpp/`, `pyproject.toml`, the native build options,
and the dependency/toolchain identity. Do not assume `git diff <old> <new>` is
available: a cherry-picked release graph or a minimal Git bundle may not contain
the old commit object. If the release-tree comparison finds any native input
change, rebuild the native wheel.

Reusing the native wheel does **not** authorize reusing the old virtual
environment, install receipt, native receipt, envelope, or reconciliation. Those
records bind the old source/runtime relationship.

The incremental sequence is:

1. publish the new exact source release;
2. compare the old and new immutable native-input trees, then copy or reference
   the already admitted native wheel only when they are equal;
3. build the new root wheel from the new commit;
4. create a new `venv-<new-execution-commit>` from the frozen wheelhouse;
5. generate and verify a new install receipt;
6. rerun `live.native_build_receipt` against the reused native wheel, new root
   wheel, new environment, and new execution commit;
7. rerun parity smoke and preflight;
8. build a new deployment envelope;
9. perform a fresh stopped reconciliation and normal activation.

This path removes unnecessary native compilation from routine Python changes
without weakening the commit-bound runtime proof. If native compatibility cannot
be established, rebuild the native wheel instead of guessing.

## systemd is the sole process owner

Use one systemd unit. `live/run.sh service` verifies startup once and then
replaces itself with the maker process. Do not combine this unit with `nohup`, a
second supervisor, a cron restart, or manual `run.sh start|stop` operations.

```ini
[Unit]
Description=NarrowGate live maker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=narrowgate
WorkingDirectory=/opt/narrowgate/current
EnvironmentFile=/etc/narrowgate/releases/<release-id>.env
ExecStart=/opt/narrowgate/current/live/run.sh service
ExecReload=/opt/narrowgate/current/live/run.sh reload
Restart=no
KillSignal=SIGTERM
TimeoutStartSec=120
TimeoutStopSec=120

[Install]
WantedBy=multi-user.target
```

systemd must send `SIGTERM` directly and retain the full stop grace period. Do
not add a second five-second kill ladder through `ExecStop`.

## Fresh reconciliation before every activation

Every start, restart, rollback, or instance resume needs a newly created stopped
exchange reconciliation. A previous file proves only an earlier stopped moment.

For a release whose source, private environment, active config, locked runtime,
and deployment envelope are already prepared on EC2, use the single transaction
entry point rather than manually stitching the activation steps together:

```bash
python3.12 scripts/live_deploy_common.py activate-prepared-release --help
```

It defaults to a non-mutating dry-run; `--execute` enables the one-SSH remote
transaction. The default service identity is the validated EC2 contract user
`ec2-user`; use `--service-user` for another valid non-root identity. Failure
before stop leaves the old service alone. Failure after stop leaves the host
stopped, and failure after candidate start but before pointer rename stops that
candidate. It never
blindly restarts the old release, and it publishes the current pointer only
after bounded health admission. When the approved control path requires SOCKS5,
pass only the validated `--socks5-proxy HOST:PORT` option; arbitrary SSH options
are not accepted.

The normal prepared transaction admits a running transient
`narrowgate.service`. It proves `active/running`, `Transient=yes`, the exact
previous working directory, positive `MainPID`, matching `/proc/<pid>/cwd`, and
the previous release's `live/main.py` command line before stop. A persistent
unit or ambiguous process fails before stop and is not modified. Use
`--resume-stopped` only after the same transaction has already stopped the
verified previous release but failed before reconciliation. That mode requires
an inactive/absent unit, exact process quiescence, the unchanged previous
current pointer, and absent reconciliation/activation outputs; it re-verifies
the candidate before producing a fresh reconciliation.

The private systemd `EnvironmentFile` uses standalone `NAME=value` records and
must end with a newline before deployment grants are appended. Validate only
key presence and syntax; never print secret values.

Admission also requires `reconciliationPending=false` and a finite
`lastTickAge` in `[0, 1s]`, derived from the existing 100 ms main-loop safety
clock with bounded scheduler allowance. The private user stream must be
connected with the same positive connection generation across advancing health
observations; no private fill or user event is required. Pointer rename is the
commit point. A later parent-directory `fsync` failure is reported as an
uncertain commit while the candidate remains running for manual verification.

The process-level startup boundary is private-first: warm-up and stale-order
cancellation, private-stream readiness, exact reconciliation under a complete
private-callback barrier, prospective epoch publication and asynchronous writer
attachment, callback release, public-market startup, then periodic metrics
polling. This order prevents a market event or half-processed private callback
from crossing the initial-state/evidence boundary.

Safe activation order:

1. verify the prepared candidate and deployment envelope while the old service
   is still running;
2. stop systemd and wait for graceful process exit;
3. confirm no maker process remains;
4. create a unique, previously absent reconciliation output while live is fully
   stopped;
5. require the signed venue reads to show zero open orders and a stable exact
   position under the intended credential/config;
6. start the prepared release through systemd;
7. observe process and runtime health for a bounded interval;
8. build the activation receipt;
9. publish the current pointer last.

Run reconciliation through a bounded transient systemd unit so it receives the
same root-owned environment without exposing credentials in the operator shell.
Use a unique unit and output name for every attempt. A condition-skipped or
collected unit is not fresh success.

## Interpreting `LoadState=not-found`

Use a bounded read-only status query:

```bash
sudo systemctl show narrowgate.service \
  --property=LoadState,ActiveState,SubState,MainPID,ExecMainStatus,StateChangeTimestamp
```

`LoadState=not-found` means systemd currently has no loaded unit definition with
that name. It does **not** by itself prove that:

- the maker process is absent;
- exchange orders are absent;
- the position is reconciled;
- a transient unit completed successfully.

If `narrowgate.service` is intended to be persistent, `not-found` is a host
configuration error: restore/reload the reviewed unit before activation. If a
collected transient unit is expected, confirm its command result and newly
created output separately. In either case, inspect the process family and run a
fresh stopped reconciliation before treating the host as safe to activate or
stop.

## Health admission and current pointer

A running PID is necessary but insufficient. Admission must confirm:

- systemd reports one active main process and no restart loop;
- runtime health is current and the quote loop is advancing;
- position and open-order reconciliation has converged;
- no ownership conflict, fatal runtime, or reconciliation-required latch is
  active;
- expected market-data and private-event clocks are current;
- recent logs contain no unknown execution state.

After health admission, create a new activation receipt and publish the compact
current pointer with `live.deployment_runtime`. The pointer is only a selector:

```json
{
  "release_id": "<release-id>",
  "activation_receipt_sha256": "<activation-receipt-root>",
  "schema_version": "<current-pointer-schema>",
  "status": "selected_activation"
}
```

`selected_activation` is lineage state, not a health assertion. Do not expand
the pointer with leaf artifact inventories, host routing, account state, or live
metrics.

## Live hot-path runtime contracts

The admitted process keeps canonical evidence and health publication off the
decision and private-event threads. One bounded FIFO worker owns the CSV
descriptors and atomic JSON publications. Producers freeze each payload before
admission; accepted items retain one global sequence and are never silently
dropped or reordered. Queue exhaustion or worker/I/O failure is a fatal,
health-visible condition, not a successful collection. Normal shutdown stops
new admission, waits for the FIFO barrier, drains every accepted item, flushes
and closes the descriptors, and reports accepted, committed, and uncommitted
counts. This is an in-process ordering guarantee, not protection against power,
kernel, or storage loss.

USD-M REST traffic is split into persistent single-owner sessions. The hot
order session is used only for new/cancel/close requests; reconciliation,
market snapshots, metrics, and listen-key maintenance use independent cold
sessions. Each pool is bounded and has automatic HTTP retries disabled. This
prevents a slow cold request from occupying the order connection and prevents
an ambiguous order write from being replayed automatically. A timed-out or
otherwise uncertain write still requires exchange reconciliation.

The fill-cooldown checkpoint must never delay the immediate risk response to a
fill. The engine first issues the required cancel of continuing
exposure-increasing orders, then commits the updated checkpoint to the
sequence-numbered, checksummed two-slot WAL. Startup restores the newest valid
slot, cancels stale exchange orders, reconciles any trade gap, and completes
position/open-order admission before quoting resumes. Changes to this ordering
require crash tests at every write boundary, including torn newest-slot,
restart-gap, duplicate-fill, and stale-order cases.

Signal computation has two distinct paths. A request for an already completed
10-second bucket returns from the cache before copying historical bars or L2.
A new bucket updates rolling execution-book state incrementally in the native
C++ ring and materializes only the required features. The Python fallback and
native path must preserve feature, causal-cutoff, prediction, and action parity;
activation must expose which path is active, and latency reporting must keep
`cached`, `new_bucket`, and `catch_up` samples separate.

## Bounded WebSocket order-gateway A/B

Persistent REST remains the production default. `websocket_api_ab` is a
restart-only, short qualification transport for the exact official USD-M
WebSocket API endpoint; it is not a hot-reload switch or an automatic fallback.
Prepare it as a separate immutable release/config envelope while retaining a
fully prepared REST rollback release.

The gateway preconnects, permits at most one in-flight request, assigns a unique
request identity, and emits per-request evidence through the central FIFO. The
evidence must preserve transport request identity, client order identity,
dispatch time, authoritative ACK/error time, outcome, and connection
generation. Once a frame may have been dispatched, timeout or disconnect is
`UNKNOWN`: do not retry the write; stop new authority and reconcile.

Before activation, set a hard `max_runtime_s` and schedule the verified REST
rollback to begin with enough margin to finish **before** that bound. The hard
timer is only a fail-safe that stops the candidate; it is not a rollback
mechanism. If the active rollback cannot complete, leave the host stopped and
reconcile rather than extending the experiment. Compare REST and WebSocket on
the same identity chain from decision to wire, authoritative ACK, and private
visibility; report failure/unknown rates and reconnects alongside latency
quantiles. A lower ACK p99 alone does not authorize the transport.

## Post-activation latency observation

Health admission proves that the release started safely; it does not prove a
latency improvement. When a release changes the live hot path, observe it with
one release and process epoch at a time and exclude restart/warm-up rows.

- For `live_perf_telemetry.csv`, select `event=requote,status=ok`; reject
  negative or non-finite durations and report sample count plus p50, p95, p99,
  and maximum for total requote, quote computation, and order update.
- Split signal timing by its recorded path (`cached`, `new_bucket`, or
  `catch_up`) instead of pooling unlike work.
- Compute REST distributions only from rows where the operation occurred and
  report the request count. Aggregated count/sum health fields support a mean,
  not a p99.
- For terminal-driven replacement, count `arm`, `publish`, `decision`, and
  `drop` markers, split terminal-visible-to-decision latency by side, and report
  every drop reason.
- Record the systemd restart count and non-`ok` outcomes separately; a shorter
  successful-path distribution must not hide failures or restart churn.

Adjacent, non-overlapping market windows are operational observations, not a
controlled causal comparison. Passing a latency target also does not establish
PnL improvement, economic no-harm, or action authority.

## Stop, resume, and rollback

To stop the instance safely: stop systemd, confirm process exit, create a fresh
stopped reconciliation, and only then request the EC2 stop from an approved
control host. If any check is unavailable or uncertain, leave the instance on
and reconcile manually.

Starting an existing instance is not an activation. Confirm systemd remains
inactive, verify the selected release and static runtime, create a fresh stopped
reconciliation, then start and repeat health admission.

Rollback is another verified deployment. It repeats stop, fresh reconciliation,
selector change, startup, health admission, activation receipt, and pointer
publication. Never restart blindly from an old reconciliation or pointer.

## Related documentation

- [Operations index](README.md)
- [Local live dry-run](live_dry_run.md)
- [Public/private documentation contract](../public_private_documentation_contract.md)
- [AWS Session Manager](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager.html)
- [EC2 security-group rules](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/security-group-rules.html)
