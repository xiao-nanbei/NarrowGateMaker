# Operations Documentation

<p><a href="README.md">English</a> | <a href="README.zh-CN.md">简体中文</a></p>

Last materially modified: 2026-09-05

Last materially synchronized: 2026-09-05

This directory contains reusable public operations contracts. It must not contain
current hosts, credentials, account/order/position state, active release
identities, private artifact locations, strategy parameters, or live economics.

## Runbooks

- [Local live dry-run](live_dry_run.md): no-trading integration check using public
  synthetic material.
- [AWS EC2 live](aws_ec2_live.md): one-time host preparation, full and Python-only
  incremental releases, systemd ownership, fresh reconciliation, health
  admission, current pointer, stop/resume, and rollback.
- [Azure Batch private replay](azure_batch_replay.md): persistent pool definition,
  zero-node idle state, one UTC day per task, `_SUCCESS` admission, failure
  handling, scale-down, and final cleanup.
- Repository [README](../../README.md): public quickstart and project-wide
  boundaries.

The five-minute quickstart is the local synthetic demo. It is not a promise that
a credentialed live host or private replay estate can be safely created in five
minutes.

## Authority budget

SHA256 is a byte-identity primitive, not a substitute for deployment, research,
or health semantics. Keep leaf identities in the manifest that owns them and
expose only the smallest root set required by the next boundary.

| Boundary | External identities | Deliberately not copied outward |
| --- | --- | --- |
| Public source | Git commit/tree and one annotated release tag | Per-file hashes of tracked source |
| Build/runtime | Runtime or wheelhouse manifest root | Every dependency, installed `RECORD`, and native leaf |
| Live release | Deployment-envelope root | Config, model, policy, runtime, and native leaves |
| Stopped exchange barrier | Reconciliation root | Repeated account/order/position leaves in service configuration |
| Activated live release | Activation-receipt root | Runtime identity and reconciliation leaves already bound by the receipt |
| Research run | Source identity, runtime root, input-manifest root, and output-receipt root | Cache keys and every input/output leaf |

The mutable current pointer has no self-hash. It contains only the release
selector, activation-receipt root, schema, and status. It is not a health record.
Cache hashes are cache keys only and never grant research or live authority. Do
not copy leaf identities into Python constants, environment variables, tests,
Markdown, current pointers, or multiple receipts.

## Provider-neutral deployment boundary

The public deployment kernel is deliberately split:

- `make publish-source-dry` and `make publish-source` publish only an exact clean
  public source checkout. They do not read or move private config, models,
  credentials, receipts, or process-control authority, and they do not start a
  process.
- Runtime construction and activation consume operator-supplied private material
  outside the checkout. Source publication never authorizes trading.

The safe sequence is:

1. provision a non-root service identity and disjoint release/private roots;
2. publish an exact clean source release;
3. materialize the complete private runtime and artifact closure;
4. install and verify a commit-bound environment without network dependency
   resolution on the target;
5. build one deployment envelope from the admitted runtime/config/model/policy closure, recording only policies explicitly approved by the operator;
6. while live is stopped, create a fresh exchange reconciliation;
7. start through the single process owner and observe runtime health;
8. build the activation receipt and atomically publish the compact current
   pointer only after admission.

Use command help as the canonical field reference instead of copying a private
deployment transcript into public documentation:

```bash
python3.12 -m live.deployment_runtime --help
python3.12 -m live.deployment_runtime install --help
python3.12 -m live.deployment_runtime verify-install --help
python3.12 -m live.deployment_runtime verify-static-tree --help
python3.12 -m live.deployment_runtime build-envelope --help
python3.12 -m live.deployment_runtime verify-envelope-startup --help
python3.12 -m live.deployment_runtime build-activation-receipt --help
python3.12 -m live.deployment_runtime publish-current-pointer --help
python3.12 -m live.native_build_receipt --help
python3.12 scripts/live_deploy_common.py source-release --help
python3.12 scripts/live_deploy_common.py activate-prepared-release --help
```

Policy permission is part of the existing deployment envelope, not an environment switch or a research verdict. For an explicitly approved action, repeat `build-envelope --approve-policy <policy>` using `q90_action`, `f05_boolean_cooldown`, or `f05_buy_e3` as appropriate. Never generate these arguments automatically from enabled config fields. The envelope binds this approval list to the exact config and artifact bundles; startup compares enabled policies with the verified list once, before constructing the engine, and passes the admitted result to runtime identity/logging. No additional receipt or hash chain is introduced.

Config validation and ordinary `preflight_live_deploy.py` check configuration/artifact compatibility; ordinary preflight reports policy admission as not evaluated and cannot authorize startup. The existing `candidate-verify` transaction additionally calls the same admission function with `--check-policy-approval`, so a missing approval fails before the old service is stopped. That pre-stop diagnostic is not a startup credential: the new process admits its own verified release once and never trusts a previous preflight output. BUY E3 loaders do not interpret `research_supported`, `owner_risk_accepted`, or historical `evidence_route` descriptions as permission. Research conclusions remain in their research records. Legacy `NARROWGATE_ALLOW_*_PRIVATE_DEPLOY` flags have no admission effect. An older envelope without `policy_approvals` grants no optional policy: disabled-policy releases remain valid, while enabling one requires a newly approved envelope, not mutation of an existing immutable release. Preparing this refactor does not itself authorize deploying or restarting live.

Once the release, private environment, active config, locked runtime, and
deployment envelope already exist on the host, `activate-prepared-release`
executes the remaining activation as one SSH transaction. It is a dry-run plan
unless `--execute` is present. In normal mode, the transaction verifies the
candidate before stopping the running old service, then performs
stop/quiescence, fresh reconciliation, start, bounded health admission,
activation receipt, and current-pointer publication in that order. Runtime-fatal
recovery instead proves the already stopped selected release's lineage,
fail-closed health, systemd exit, and process quiescence before it rejoins that
same sequence at fresh reconciliation. The pointer is published last. Any failure before
the pointer rename leaves it unchanged; a candidate that was started but fails
admission is stopped, and the command never automatically restarts the old
release. `--service-user`
defaults to the validated EC2 contract user `ec2-user` and may be set to another
validated service identity. Hosts reachable only through a local SOCKS5 proxy
use the bounded `--socks5-proxy HOST:PORT` option; arbitrary SSH options are not
accepted.

The normal transaction accepts a running transient `narrowgate.service` created
by the documented `systemd-run` contract. Before stopping it, the command proves
`active/running`, `Transient=yes`, the exact previous working directory,
positive `MainPID`, matching `/proc/<pid>/cwd`, and the expected
previous-release `live/main.py` command line. A persistent unit or ambiguous
process fails before stop and is left unchanged. If an earlier transaction
already stopped that exact release but failed before creating reconciliation,
`--resume-stopped` is the only continuation path. It requires no maker or
supervisor process, an inactive/absent unit, a current pointer still naming the
previous release, and previously absent reconciliation/activation outputs. It
then repeats candidate verification and creates a fresh reconciliation; it is
not a general bypass for the normal pre-stop proof.

An already selected service that later exits with code 78 is a different case.
Use `--recover-runtime-fatal`, never `--resume-stopped`. This narrowly scoped
mode requires the compact current pointer, its activation receipt, deployment
envelope, stopped reconciliation, and activation-bound runtime identity to form
one valid lineage. The runtime-health snapshot must belong to that PID. Normally it records the final fail-closed reconciliation-required state. If its writer failed before publishing final health, recovery instead requires the same authenticated process and journal invocation to record the final-health publication failure followed by the operator-gated exit. Both paths require the matching systemd `EXIT_STATUS=78`; stale healthy state alone is never sufficient. The
unit must be inactive or absent, and every maker/supervisor process absent. The candidate
must use new, previously absent stopped-reconciliation and activation-receipt
paths; no artifact from the failed release is reused as fresh evidence. After
these checks, fresh signed reconciliation, start, health admission, a new
activation receipt, and pointer-last publication remain mandatory. If the
journal or lineage proof is unavailable, leave the host stopped.

The journal proof is read as two bounded streams: the old PID from its
runtime-identity timestamp to the recovery instant, then the matching systemd
invocation. It is never loaded as an unbounded in-memory history on a small
live host.

Use the ordinary candidate arguments plus the failed current release's three
immutable lineage files:

```bash
python3.12 scripts/live_deploy_common.py activate-prepared-release \
  <normal-candidate-arguments> \
  --recover-runtime-fatal \
  --previous-deployment-envelope <failed-release-envelope> \
  --previous-activation-receipt <failed-release-activation-receipt> \
  --previous-stopped-reconciliation <failed-release-stopped-reconciliation> \
  --execute
```

The attempt-scoped stopped-reconciliation and activation-receipt outputs must
be new and absent. Do not point either of them at the three previous files; the
existing current pointer is replaced only at the final commit step.

The private systemd `EnvironmentFile` must contain standalone `NAME=value` records. Keep a final newline before appending runtime selectors; otherwise the first appended selector becomes part of the preceding secret value. Policy approval belongs in the deployment envelope, not this file. Never print the file during validation.

Health admission requires `reconciliationPending=false` and a finite
`lastTickAge` between zero and one second. This bound follows the existing
100 ms main-loop safety clock with allowance for bounded scheduler jitter; it
also requires a connected private user stream with one positive, stable
connection generation across the advancing health observations. It does not
require a private fill or user-stream event during admission. The pointer rename
is the commit point; if its parent-directory `fsync` then fails, the command
reports an uncertain commit and leaves the candidate running for manual
verification rather than stopping a possibly published release.

Inside the admitted process, stream startup is also ordered. The engine first
warms state and cancels stale orders, then opens and proves the private user
stream. Complete private callbacks are serialized while the exact account,
position, and open-order reconciliation is captured into the prospective epoch
and the asynchronous lifecycle writer is attached. Waiting callbacks are then
released in FIFO order, public market streams are opened, and only after that
does periodic metrics polling begin. Public market input can therefore neither
mutate an incomplete initial checkpoint nor precede its evidence writer.

Before a remote upload or task starts, recursively calculate the complete
materialization closure. Every manifest reference must be inside the admitted
bundle or supplied as an explicit immutable resource. A successful archive
upload is not closure admission.

## Non-negotiable operational invariants

- One process owner: systemd for live; Azure Batch for offline tasks.
- A source upload never restarts or activates live.
- Every live activation uses a newly created stopped reconciliation.
- A PID, `LoadState`, exit code, or current pointer alone is not health admission.
- One formal Batch task represents one registered UTC day.
- `_SUCCESS` is written last and is valid only with its matching output manifest.
- Paid Batch nodes return to zero when the queue is drained.
- Unknown execution state, missing closure, or ambiguous ownership fails closed.
- Private live and replay material remains outside the public Git checkout.

See the [public/private documentation contract](../public_private_documentation_contract.md)
for the publication boundary.
