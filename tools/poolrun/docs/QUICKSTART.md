# Run a finite batch yourself

[English](QUICKSTART.md) | [简体中文](QUICKSTART.zh-CN.md)

Last materially modified: 2026-10-23

Last materially synchronized: 2026-10-23

Once the existing Master, Agents, environments and transfer adapters are configured, use one batch file instead of manually registering each release/input and submitting each account. This is a client workflow over the same PoolRun; it does not start a second scheduler, change host budgets or rewrite active tasks. Install the client with `python3 -m pip install ./tools/poolrun` from the repository root, using your chosen Python 3.12+. Updating the client does not require restarting an existing Master or Agent.

## One plan, one command

Create `batch.json` next to your existing task JSONL and manifests:

```json
{
  "schema": "poolrun.batch.v1",
  "releases": ["packages/r1/release.json"],
  "objects": ["inputs/object.json"],
  "tasks": "tasks.jsonl"
}
```

All referenced paths resolve against this plan, not your shell directory. `code.zip` stays beside each release manifest. Empty `releases` and `objects` lists reuse already registered identities. Task JSONL, release and input schemas remain the existing [PoolRun contracts](../README.md); do not put API keys or private credentials in the plan.

```bash
poolrun --config client.json apply --plan batch.json
poolrun --config client.json status --summary
poolrun --config client.json status --summary --project my-project
```

`apply` reads all local submission files first, verifies source packages, then registers releases and input manifests and submits the whole finite task list. There is no need to run a separate manual release-prepare command for ordinary queued tasks: the existing scheduler and Agents handle readiness and completion-driven dispatch. It does not preload the whole input calendar, assign arbitrary hosts, increase concurrency or run economic work itself. Business-specific preparation hidden inside external scripts must still be exposed through the existing task/prepared interfaces; this command does not infer or convert those scripts.

An `ACKNOWLEDGED` response means registration/queueing, not completed input transfer or running replay. Summary separates controller `RUNNING`, `STARTING`, uncertain and result-pending Attempts from preparation, and shows host capacity, last reported resource observations, errors and pending task reasons. `RUNNING` alone is not proof of advancing business progress. For one task, use `poolrun --config client.json why TASK_ID`; detailed `status` remains available unchanged.

Optional offline inspection, without reading client configuration or contacting any host:

```bash
poolrun apply --plan batch.json --check
```

This checks the local submission files only, not remote credentials, capacity, business semantics or data readiness. Ordinary `apply` already performs these local checks; you do not have to invoke `--check` first.

## Interrupted command: repeat it, not the economics

Repeat the same `apply` command after a connection failure. Request IDs are derived from exact operation payloads; the existing Master deduplicates accepted operations. A batch is not one atomic transaction: registration can succeed before a later stage fails. Repetition continues those control operations without creating new task IDs or restarting completed accounts. Changing an existing task's business specification is still rejected. A conflicting release/input is not silently replaced. Application and resource checks remain at their existing owners.

For any older control command whose ACK was lost, reuse its saved request instead of reconstructing its arguments or rereading changed files:

```bash
poolrun --config client.json request-retry --id ORIGINAL_REQUEST_ID
```

The command loads the original payload from the client's existing `request_dir`, verifies the selected endpoint and request ID, and resends exactly that operation. Missing records or endpoint differences are errors. This is not `retry --task`: it does not create a new computation generation. Keep the original record until the uncertain outcome is reconciled. The existing Master remains the authority; no state file is edited directly.

## What remains manual

Initial machine/SSH setup, environment/release creation, business input definitions, expired transfer credentials and scientific decisions still need explicit configuration. The client does not bypass transfer verification, lower disk/RAM protection, automatically retry failed economic work, turn on a drained host or activate trading. Transport integrity and business/account identity checks are internal correctness checks, not a sequence of approvals you should have to perform for every account. This delivery simplifies submission and recovery; it does not claim that every historical research launcher is already a one-plan application.
