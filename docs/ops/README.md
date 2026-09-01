# Operations Documentation

<p><a href="README.md">English</a> | <a href="README.zh-CN.md">简体中文</a></p>

Last materially modified: 2026-09-02

Last materially synchronized: 2026-09-02

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
5. build one deployment envelope from the admitted runtime/config/model/policy
   closure;
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

Once the release, private environment, active config, locked runtime, and
deployment envelope already exist on the host, `activate-prepared-release`
executes the remaining activation as one SSH transaction. It is a dry-run plan
unless `--execute` is present. The transaction verifies the candidate before
stopping the old service, then performs stop/quiescence, fresh reconciliation,
start, bounded health admission, activation receipt, and current-pointer
publication in that order. The pointer is published last. A failure never
publishes it; a candidate that was started but fails admission is stopped, and
the command never automatically restarts the old release. `--service-user`
defaults to the validated EC2 contract user `ec2-user` and may be set to another
validated service identity. Hosts reachable only through a local SOCKS5 proxy
use the bounded `--socks5-proxy HOST:PORT` option; arbitrary SSH options are not
accepted.

This transaction currently supports only a running transient
`narrowgate.service` created by the documented `systemd-run` contract. Before
stopping it, the command proves `active/running`, `Transient=yes`, the exact
previous working directory, positive `MainPID`, matching `/proc/<pid>/cwd`, and
the expected previous-release `live/main.py` command line. A persistent unit or
ambiguous process fails before stop and is left unchanged.

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
