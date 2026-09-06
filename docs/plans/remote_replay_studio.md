# Remote Replay Studio

[简体中文](remote_replay_studio.zh-CN.md)

Last materially modified: 2026-09-06
Last materially synchronized: 2026-09-06

## What is available

Replay Studio now provides a browser workbench, a durable control service and an independent HTTP worker. The first adapter executes the existing [public synthetic replay](../../examples/replay_demo/README.md); it does not implement another matching engine. A submitted experiment survives a browser or SSH disconnect. Control state and published artifacts survive a control-process restart when its state directory is retained.

This is a working first delivery, not the completed research platform. Completed owner-private B0 results can now be imported for read-only inspection, separately from synthetic jobs. Real-market F01/F10 execution and learned E/C candidates are still disabled. Creating two demo arms runs the same fixture twice with separate output directories; their names do not create different economic policies.

The service never starts a maker, reads live credentials, imports a current-live pointer, creates cloud resources or promotes a strategy. Job submission accepts only the built-in synthetic dataset. Do not upload private market, account or research artifacts through this demo adapter. B0 result import is an owner-local CLI operation, not an HTTP upload or arbitrary-path endpoint.

## Run the closed loop

Use the same tested checkout or installed wheel on the control host and workers. Python 3.11 or newer is required. An installed wheel includes the built frontend; readers do not need Node.js.

```bash
python -m pip install ".[studio]"
python -m narrowgate.studio serve --state-dir ./results/studio-control --port 8080
```

In another terminal or service:

```bash
python -m narrowgate.studio worker \
  --url http://127.0.0.1:8080 \
  --worker-id worker-a \
  --work-dir ./results/studio-worker-a
```

Open `http://127.0.0.1:8080`, create a demo experiment, and inspect its orders, event trace, campaign, ledger and original logs. A second independent worker uses a different worker ID and work directory. Each worker owns at most one unfinished job. Two workers can execute two independent arms concurrently; do not interpret this as permission to split continuous inventory paths into fresh-start UTC days.

Frontend development and reproducible builds are described in [frontend/README.md](../../frontend/README.md). The public [one-day data tutorial](../opensource/one_day_data_pipeline.md) remains the route for real-data diagnostics outside Studio.

## Import completed B0 results without replay

The owner supplies an existing private `baseline_summary.json` beside its `input_plan.json` and selected final segment outputs. These are private evidence store inputs, not distributed with the public repository. Use the same owner-only state directory (mode `0700`) as the control service:

```bash
.venv/bin/python -m narrowgate.studio import-b0 \
  --state-dir "${NARROWGATE_RESULTS_DIR}/studio-control" \
  --summary "${NARROWGATE_PRIVATE_EVIDENCE_ROOT}/<tag>/baseline_summary.json"
```

The importer follows only explicitly selected segment stems and reads the small summary, input plan, segment metadata and aggregate CSV. It checks coverage, completed baseline/config metadata and amount consistency. Raw fill, campaign and funding files are checked for existence only, not reread or rehashed. Missing, partial, overlapping and out-of-root sources are rejected. An allowlisted compact report is stored in the existing private SQLite database, without raw artifacts or source paths; one summary-derived ID makes repeat imports idempotent. Changes to subsequent research phase, training or execution-permission descriptions do not block viewing an existing B0. Import creates no job and starts no replay, worker, Azure synchronization or cloud resource.

The separate “真实 B0” view uses read-only `/api/results` endpoints. It shows covered UTC days, continuous segments, accounting amounts, selected local/Azure execution origins and the source summary's existing cross-host verification statement. Origin is provenance, not current cloud liveness. Import consistency checks do not repeat the original fill/funding or cross-host qualification. `daily.csv` rows remain segment aggregates; the UI does not invent daily returns, Sharpe or an account equity curve. Trading PnL already includes fees and terminal MTM; funding is added exactly once. Missing native queue coverage and modeled-runtime limitations remain visible. Synthetic demo jobs and their workers stay separate.

## One remote host first, then more workers

```text
Browser ── HTTP over SSH tunnel ── Control API + local SQLite + published outputs
                                      ▲
                                      │ claim / heartbeat / publish over HTTP
                                  Worker process
                                      │
                               canonical replay CLI
```

The browser talks HTTP, not SSH commands. On your workstation, forward the control host's loopback port:

```bash
ssh -NT -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=30 -o ServerAliveCountMax=3 \
  -L 127.0.0.1:18080:127.0.0.1:8080 research@control-host
```

Open `http://127.0.0.1:18080`. `control-host` is an operator-configured SSH alias, not a bundled server. The service intentionally listens only on loopback. Do not expose it by changing cloud firewall rules or binding to all interfaces.

For a worker on another host, establish an equivalent tunnel from that worker host to the control host, then point `--url` at its local forwarded port. Workers communicate through the API; they never mount or write the control SQLite file. The built-in dataset is shipped with each worker. Arbitrary private dataset registration and data-aware scheduling are future adapters, not current features.

The optional `NARROWGATE_STUDIO_TOKEN` environment variable enables Bearer authentication. Set the same operator-generated token on control and workers; the browser's “访问凭据” dialog keeps it only in page memory. Tokens must not appear in URLs or source control. With authentication the UI uses authenticated polling rather than putting tokens into an SSE URL. This is a single-owner SSH-tunnel service, not a public multi-user service.

## Process ownership and storage

Run control and worker as separate OS-managed services for unattended use. Do not attach their lifetime to an SSH shell. A minimal Linux user-service template for the worker is:

```ini
[Unit]
Description=NarrowGate synthetic replay worker
After=network-online.target

[Service]
Type=simple
WorkingDirectory=%h/narrowgate
ExecStart=%h/narrowgate/.venv/bin/python -m narrowgate.studio worker --url http://127.0.0.1:8080 --worker-id worker-a --work-dir %h/narrowgate/results/studio-worker-a
Restart=no
KillMode=control-group
TimeoutStopSec=20

[Install]
WantedBy=default.target
```

Adapt installation paths once. Use a separate control unit with the `serve` command and a separately supervised tunnel where needed. `KillMode=control-group` matters: a forcibly killed bare worker may otherwise leave its independent replay child alive. Normal worker termination sends TERM, waits, then kills and reaps its child if necessary. A host reboot or forced kill is not a resumable economic checkpoint; preserve the old task and inspect its process/logs before deciding what to do next.

Keep the control state directory and worker work directories on durable private storage. SQLite is local to the control host, not a shared network database. Published outputs are stored with that database. Worker scratch and outboxes must not be deleted while a job is running, uncertain or awaiting upload. A cloud temporary disk disappearing also removes any unuploaded artifacts; a browser reconnection cannot recover them. This first release has no automatic cloud-resource provisioning, Blob lifecycle or deletion action.

## Failure semantics

| Event | Behavior |
| --- | --- |
| Double-click or lost submit response | The same `Idempotency-Key` and request return the original experiment. A different request with that key is rejected. |
| Lost worker claim response | Reusing the worker's persisted session resolves to its original unfinished job, never a second job. |
| Browser/SSH disconnect | The control queue and worker process remain independent of the browser. |
| Temporary control outage | Network/5xx retries are bounded. A running child is not immediately killed solely because the API is unavailable. |
| Missed heartbeat | The task is `lost`, not automatically failed or requeued. Only its original worker/session can reconnect and publish. |
| Cancel queued task | It becomes canceled without executing the runner. |
| Cancel running task | `cancel_requested` remains until the worker terminates/reaps the process and publishes its terminal logs. |
| Cancel races with successful computation | Cancellation wins before result publication; logs are preserved, but no completed report is admitted. |
| Upload failure | No completed state. The exact publication payload and logs remain in the worker directory; restarting the same worker resumes that upload, not the computation. |
| Worker restarts with an existing execution directory but no outbox | Stops with an explicit uncertain-execution error. It cannot safely infer that the old child is dead or start a duplicate. |
| Repeated publication | Identical same-attempt publication is idempotent; changed or other-attempt content cannot overwrite a terminal result. |

The control marks a result completed only after required summary, trace, receipt, stdout, stderr and environment information have been saved and synchronized, and the database transaction commits. Reference bytes are checked once when the synthetic output enters the result store, not on every chart read. No additional research SHA/permission hierarchy is created.

Demo execution has a 600-second worker-enforced timeout and a bounded upload size. Running logs currently remain on the worker; the browser shows terminal archived logs, not a live stdout stream. Task progress is lifecycle status, not an invented completion percentage.

## Result and accounting contract

The display-only `backtest_report.v1` wrapper retains the original `summary.json` and ordered `trace.jsonl`. It is not research authority. The UI reads existing cash, inventory, fees, campaign and terminal PnL fields; it does not recalculate them. Missing fields remain unknown. All three demo orders are displayed, including the unfilled canceled order.

Real-market adapters must preserve these boundaries before being enabled:

- `replay_pnl` already includes trading fees; do not subtract fees twice. Funding must be separately reported and included exactly once in the primary net value.
- A continuous segment summary is not one observation per UTC day. Reuse the canonical continuous ledger's daily slices rather than labeling the entire segment as its first day.
- A PnL ledger beginning at zero is not funded account equity. Do not fabricate returns, annualized performance or a capital-scaled Sharpe ratio.
- Queue positions and counterfactual fills are modeled. Matching two hosts proves implementation reproducibility, not exact exchange queue or live-economic equivalence.
- Compare only complete compatible environments. Preserve each arm's own orders, inventory, funding, endogenous gateway queue and RNG state; a common seed alone is insufficient to prove shared exogenous latency draws.
- Real event queries need bounded time/order/campaign windows. Any chart decimation must not change original event order, accounting or statistical calculations.

## Remaining delivery sequence

| Workstream | Current status / next acceptance |
| --- | --- |
| Public onboarding | Product-first no-account demo; worked order example; data-state table; research status separated from tooling availability. |
| Remote execution foundation | Implemented for synthetic demo: durable queue, independent workers, cancel/lost states, output publication and browser result inspection. |
| Current B0 integration | Completed private B0 summaries are importable for read-only viewing with segment accounting and import consistency checks. Real-market job execution remains disabled. |
| E/C research | The separate research branch collects complete E/C opportunities and assembles single-intervention paired labels. This does not yet constitute a trained or validated selection policy. |
| Real dataset registry | Required before market-run submission: explicit Development/Validation/holdout roles, immutable logical dataset mapping, source coverage and no silent backend/feed substitution. |
| Bounded real execution | Add an allowlisted canonical runner adapter, CPU/RSS/disk budgets and artifact streaming. Never accept arbitrary shell commands from the browser. |
| Full analysis | Reuse existing campaign, funding, scorecard and chronological statistics. Add real trace pagination, source-aware latency views and compatibility explanations. |
| Multi-host qualification | Verify the packaged service, SSH tunnels, node loss and original finalizer acceptance on target hosts before declaring remote real-market readiness. |

The current research work remains independent of this UI. Do not train/select strategies from incomplete baseline segments, start duplicate Azure baselines, restart live, or read sealed outcomes to make the interface look populated.

## Checks

```bash
python -m pip install ".[studio,dev]"
python -m pytest tests/test_replay_studio.py tests/test_public_replay_demo.py tests/test_public_onboarding.py
python -m ruff check narrowgate examples --select E,F,I,UP,B
```

The Studio tests exercise concurrent claims, idempotency, control-store reopening, lost/canceled workers, artifact failures, original demo CLI execution and loopback/API boundaries. They are not native queue qualification, an economic backtest or proof of arbitrary host-crash recovery. Browser verification should create two tasks, run two workers, reopen the page, inspect the unfilled order and campaign, cancel a queued task, and confirm the API cannot submit a real-market or live runner.
