# Azure Batch Private Replay Runbook

<p><a href="azure_batch_replay.md">English</a> | <a href="azure_batch_replay.zh-CN.md">简体中文</a></p>

Last materially synchronized: 2026-09-06

This runbook defines a reusable private Azure Batch executor for offline
NarrowGate replay. It contains no subscription, account, region, resource ID,
storage locator, private path, strategy parameter, dataset identity, or result.

Azure is compute, not a new source of truth. The canonical market data, frozen
date set, queue and latency contracts, seeds, research authority, and final
admission remain local and owner-private.

## Public/private boundary

The public repository may document the operational contract and generic Batch
commands. The private submission layer owns:

- Azure tenant/subscription/resource identities and credentials;
- container registry and storage locations;
- input data, manifests, model/policy material, and runtime image identity;
- registered replay dates and task commands;
- outputs, failure logs, receipts, and economic aggregation.

Do not commit generated Azure configuration or output. Use Microsoft Entra/RBAC
and managed identity where possible rather than long-lived account keys.

## Persistent environment, ephemeral compute

Create one bounded resource group containing only the resources needed by the
private executor:

- one storage account with separate input, output, manifest, and failure-log
  containers;
- one container registry or equivalent immutable runtime source;
- one Batch account;
- one managed identity with minimum Blob and registry permissions;
- one persistent pool definition.

The pool definition may remain for the research period, but its normal idle state
is zero dedicated and zero low-priority nodes. Do not keep a control VM, database,
NAT gateway, large logging workspace, or persistent per-node disk merely to keep
the executor available.

Use a Linux image and VM architecture compatible with the frozen Python/native
runtime. Set one task slot per node unless a measured memory/copy-on-write
qualification proves a different setting safe. The pool start task may run with
elevated privileges only for bounded host preparation; replay tasks themselves
run as a non-admin user.

One-time pool bootstrap must:

1. materialize the exact runtime and complete manifest closure;
2. create real canonical directories required by the frozen contract;
3. use bind mounts, not symlinks, only when an immutable contract requires a
   specific absolute directory;
4. normalize ownership and modes after extraction;
5. reject group/world-writable private material;
6. run a closure/import/native smoke probe;
7. write the pool-ready marker last.

Keep Batch's own node-shared and task-working directories distinct from any
compatibility mount. A missing closure is a bootstrap failure, not permission to
download dependencies from the network inside a formal task.

## Controlled native wheel builder

A separate, bounded build task may reuse a Linux x86_64 node with at least
16 GiB RAM to produce the EC2 native artifact. It is a build task, not a replay
day, and must not run concurrently in a replay task slot. The task must enter an
Amazon Linux 2023 or manylinux_2_34-compatible glibc 2.34 container/rootfs;
generic Ubuntu 24.04/glibc 2.39 output cannot be deployed to EC2. Materialize
the exact source commit, CPython 3.12 build environment, and GNU C++ 11.5.0
toolchain. Before disabling task network access, populate that dedicated build
environment from a controlled local wheelhouse with the requirements declared
by `cpp/pyproject.toml` and their transitive dependencies. Then run:

```bash
make native-live-wheel
```

The entry point defaults `CMAKE_BUILD_PARALLEL_LEVEL` to `1`, checks available
memory, selects the live-only surface, and fixes
`NARROWGATE_LIVE_CPU_PROFILE=ec2-cascadelake-avx2`. It uses
`--no-build-isolation --check-build-dependencies` with `PIP_NO_INDEX=1`, so a
missing build dependency fails locally instead of reaching a package index.
Build-tool versions remain measured builder inputs until the Amazon Linux 2023
qualification freezes them. Upload the resulting
`dist/native/live/<full-git-commit>/*.whl` as an immutable build output before
releasing the node. Do not substitute `-march=native`, a portable wheel, or a wheel from a
different compiler. The target EC2 release must still verify the installed
wheel with its native build receipt and Python/C++ parity smoke; Azure does not
replace target-host performance qualification.

## Scale from zero for a research batch

Authenticate to the intended Batch account using the operator's approved Azure
CLI context, then resize the persistent pool:

```bash
az batch account login \
  --resource-group <resource-group> \
  --name <batch-account>

az batch pool resize \
  --pool-id <pool-id> \
  --target-dedicated-nodes <bounded-node-count> \
  --target-low-priority-nodes 0
```

Wait until the pool reports the requested usable nodes and the start-task ready
marker exists on every node. A resize request is asynchronous; submission must
not assume that the accepted request means nodes are ready.

Before broad submission, run one representative UTC day and compare its input,
action counts, terminal accounting, wall time, peak memory, and output contract
with the same local run. Only that compatibility check authorizes fan-out; it
does not authorize a strategy.

## One independent replay segment per task

For daily fresh-start research, each task represents one registered UTC day.
For continuous-account research, one task runs an entire contiguous segment in
one simulator invocation: orders, cooldowns, campaigns and risk state cross
midnight in memory. Do not split dependent days across nodes or concatenate
non-contiguous dates and call that continuous replay. Parallelize only independent
segments or arms, with their own execution state.

- task ID is a deterministic day identity within one job;
- one task uses one task slot;
- no task starts nested per-day multiprocessing;
- start/end, warmup, initial state, queue/latency contract, and seeds are frozen;
- inputs are read-only;
- output uses a unique attempt-specific directory;
- the task has a bounded wall-clock time and retry policy.

Create the job once and create each day task once. Before submission, list
existing task IDs and refuse a duplicate. A retry uses Batch's retry mechanism or
a new opaque attempt namespace under the same logical day; it must not create a
second concurrent day task.

Generic command shape:

```bash
az batch job create \
  --id <job-id> \
  --pool-id <pool-id>

az batch task create \
  --job-id <job-id> \
  --task-id <utc-day-task-id> \
  --max-task-retry-count <bounded-retry-count> \
  --max-wall-clock-time <bounded-duration> \
  --json-file <private-task-json>
```

The private runner must verify the runtime root, input-manifest root, registered
plan, frozen date, warmup, and every referenced closure before entering the event
loop. Public code does not resolve the private command or its artifacts.

## Persist task files before node release

Configure Batch `outputFiles` on **every** task, including development checks.
Task `retentionTime` does not preserve files after a node is removed. Monitoring
must never be the only way to retrieve logs before autoscale deletes a node.

The task writes test XML, a non-secret environment summary (source revision,
Python, package versions and CPU), and command exit codes under `artifacts/`.
Do not dump the environment: it can contain credentials. Before exiting zero,
the command must require its expected reports to exist and be nonempty; a glob
matching no files does not itself make Batch upload fail.

Use these fields in the private task JSON, resolving the placeholders locally:

```json
{
  "outputFiles": [
    {
      "filePattern": "artifacts/*",
      "destination": {"container": {
        "containerUrl": "<existing-output-container-url>",
        "path": "<job-id>/<task-id>/<attempt-id>",
        "identityReference": {"resourceId": "<pool-managed-identity-resource-id>"}
      }},
      "uploadOptions": {"uploadCondition": "taskCompletion"}
    },
    {
      "filePattern": "../std*.txt",
      "destination": {"container": {
        "containerUrl": "<existing-failure-log-container-url>",
        "path": "<job-id>/<task-id>/<attempt-id>",
        "identityReference": {"resourceId": "<pool-managed-identity-resource-id>"}
      }},
      "uploadOptions": {"uploadCondition": "taskCompletion"}
    }
  ],
  "exitConditions": {
    "fileUploadError": {"jobAction": "terminate"},
    "default": {"jobAction": "terminate"}
  }
}
```

The job uses `onTaskFailure=performExitOptionsJobAction`. The existing pool must
have the referenced managed identity, scoped Blob write permission, and a network
route permitted by the storage firewall. Do not put a SAS or storage key in Git.
Batch uploads on task completion, including when the command fails, before the
task reaches its completed state. Upload failure is reported in
`executionInfo.failureInfo`; `exitCode=0` alone is **not** success. Require
`executionInfo.result=success`, no `failureInfo`, command exit zero, and the
expected accessible Blob files. Parse the uploaded XML before reporting test
counts. Missing reports are incomplete evidence, not passing tests.

Batch does not promise upload order between `outputFiles` entries. Do not use
this unordered upload list to publish a replay `_SUCCESS` marker ahead of its
results; the existing replay publisher must still write that marker last.

Qualify the setup with a small task: retrieve its XML and logs **after** the pool
has returned to zero. Also test an upload failure and confirm it cannot be
reported as a successful validation. See Microsoft's
[task output persistence documentation](https://learn.microsoft.com/en-us/azure/batch/batch-task-output-files).

## Output publication and `_SUCCESS`

A completed process is not automatically an admitted replay day. Publish output
in this order:

1. write results to the attempt-specific temporary namespace;
2. validate schema, day boundary, terminal accounting, and required diagnostics;
3. write an output manifest covering the admitted result files;
4. atomically publish or promote the attempt output;
5. write `_SUCCESS` **last**.

The success marker is meaningful only with a matching output manifest and the
expected logical day/input identity. A zero exit code without that marker is
incomplete. A marker copied from another attempt is invalid.

Resume logic may skip a day only when both the marker and manifest match. While a
registered multi-day run is incomplete, monitoring reads task states, failures,
elapsed time, node health, and success-marker counts only; it does not read or
report partial economics.

## Failure handling without duplicate work

If infrastructure or closure fails:

1. disable the job so no new task starts;
2. choose explicitly whether active tasks wait, terminate, or requeue;
3. inspect task state, stderr/stdout, node state, and missing closure only;
4. retain initialized nodes for a short, bounded repair window when economical;
5. supply a small immutable missing resource directly only if the base runtime
   and input bundle are unchanged;
6. update the pool start task and reimage nodes when shared base materialization
   changed;
7. resume only after proving no duplicate logical day task exists.

For example, disabling a job while allowing active work to finish uses the
operator-selected task policy supported by Azure Batch:

```bash
az batch job disable \
  --job-id <job-id> \
  --disable-tasks wait
```

Do not rebuild or upload a large base bundle for one omitted small receipt, and
do not mutate an already admitted input in place.

## Scale to zero after a batch

Configure bounded autoscale with `taskcompletion` deallocation. Once the queue
drains, nodes return to zero after Batch has finished output upload; local
download is not a prerequisite for releasing compute. An upload failure must
remain visible in task metadata, not keep paid nodes alive indefinitely.

1. confirm completed task results, upload status and required Blob objects;
2. verify current and target node counts automatically reach zero;
3. download outputs, manifests and failure logs from Blob, not node-local files;
4. run the same local finalizer and aggregation boundary;
5. retain completed task metadata until failures and outputs are accounted for;
6. verify no unexpected compute, disk, public IP, load balancer, or logging
   resource remains allocated.

```bash
# Manual fallback only if an idle pool has not automatically returned to zero:
az batch pool resize \
  --pool-id <pool-id> \
  --target-dedicated-nodes 0 \
  --target-low-priority-nodes 0

az batch pool show --pool-id <pool-id>
```

Keep the pool definition, Batch account, registry, and storage only when another
registered batch is expected within the approved period. Zero nodes avoids VM
compute charges; it does not imply that storage, registry, network, or retained
job data is free.

## Final teardown

Before the budget or subscription window closes:

1. stop new submissions;
2. wait for or explicitly terminate active tasks;
3. download and locally verify all retained outputs;
4. resize the pool to zero and confirm completion;
5. delete completed jobs and the pool;
6. delete the Batch account, registry, storage account, and finally the resource
   group;
7. verify the subscription has no residual VM, managed disk, public IP, load
   balancer, network interface, or logging workspace from the executor.

Resource-group deletion is intentionally last. Never use a cleanup command until
the output download and local verification are complete.

## Related documentation

- [Operations index](README.md)
- [AWS EC2 live runbook](aws_ec2_live.md)
- [Public/private documentation contract](../public_private_documentation_contract.md)
- [Azure Batch CLI quickstart](https://learn.microsoft.com/en-us/azure/batch/quick-create-cli)
- [Azure Batch pool commands](https://learn.microsoft.com/en-us/cli/azure/batch/pool?view=azure-cli-latest)
- [Azure Batch job commands](https://learn.microsoft.com/en-us/cli/azure/batch/job?view=azure-cli-latest)
- [Azure Batch task commands](https://learn.microsoft.com/en-us/cli/azure/batch/task?view=azure-cli-latest)
