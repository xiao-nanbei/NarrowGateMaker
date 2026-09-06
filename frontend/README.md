# Replay Studio frontend

Last materially modified: 2026-09-07
Last materially synchronized: 2026-09-07

This English page is canonical; [简体中文](README.zh-CN.md).

React + TypeScript workspace for the packaged Studio UI. The public default runner is `replay-demo` with the built-in `synthetic-demo` dataset. An optional owner-registered offline adapter queues already-prepared research, training or data-processing plans without exposing commands, paths or parameters in the browser. No real plan is enabled by a public clone. A separate default tab displays existing private B0 summaries imported through the [owner-local CLI](../docs/plans/remote_replay_studio.md#import-completed-b0-results-without-replay); viewing them never starts a new baseline. The interface does not provide arbitrary shell execution or cloud provisioning.

Real results use read-only `/api/results` endpoints and remain separate from synthetic jobs, reports and comparison. The browser displays saved amounts and continuous-segment coverage without manufacturing daily PnL or Sharpe; fees already in trading PnL are not deducted again, and funding is added once. Local/Azure origin and existing cross-host verification describe historical provenance, not current cloud connectivity. Missing queue coverage and modeled-evidence limitations stay visible. No private evidence ships in the frontend bundle.

## Market review and data quality

The market-review page selects an imported B0, continuous segment, UTC date, and 1s/5s/1m/5m candle interval (default 1m). It reuses the existing React/SVG stack, with keyboard/button zoom and bounded drag navigation; requests stay within one UTC day, align to complete candle boundaries, and display at most 1,000 candles. Candles come only from retained market bars, never from strategy fills. The current source is explicitly **historical market context, not verified as the exact original replay input**. A missing bar second has an unknown cause, not an automatic source-outage classification.

BUY/SELL markers identify simulated fill direction, not opening/closing trades. Each fill keeps its exact physical and local-visible timestamp, execution price, quantity, original/scoped order identifiers, inventory, signed fee, and recorded campaign fields. Same-candle and same-millisecond fills remain independent rows; incomplete pagination is disclosed and can be continued. Missing fields stay unknown, and a submit-time campaign is not relabeled as final attribution. Inventory observations use the local-visible callback clock and original sequence, retaining same-clock changes without drawing an invented path across different timestamps. Missing inventory clocks are not replaced by exchange fill time. Order details are partial fill snapshots: the UI does not manufacture active-order bands or continuous PnL.

The separate UTC quality calendar includes the entire requested inclusive date range, including absent leading/trailing dates. Source, market, symbol, dataset, node, pending-review-day, and explicitly missing-replica filters are available. It separates raw/processed artifacts, current canonical-file audit applicability, per-use conclusions and node copies. Current-use cards show reasons for candles, feature input, modeled/strict-queue replay and funding; historical checks stay under their original scope/time. Unknown is explained as not observed, no processed-data check, an unbound report or another recorded cause. A feature check is not training admission, and missing native sequence does not disqualify every use. Expanded entries show only recorded counts, byte sizes, coverage, gaps and timestamps. A registered size match links an existing audit; it does not claim newly verified content or that forward filling proves no data loss.

**刷新本机登记清单** submits only the date range, registered dataset ID and node ID to `/api/data-quality/refresh`. It performs bounded local metadata observation, not raw-content reads, whole-disk SHA, downloads, quality calculations or replay. Remote-copy timestamps remain separate. **重新读取页面** only rereads the projection. Export still creates a bounded JSON download/recheck checklist without executing it. See [quality evidence and refresh scope](../docs/plans/remote_replay_studio.md#view-existing-data-quality-evidence). Following a day into market review does not bind the current quality catalog to an older result.

## Developer build

The compute page uses `/api/compute-resources` for owner-configured physical/cloud resources, not the number of workers. Friendly labels, fixed background probes, external job observations and placement roles use `--resources-manifest`; see the [resource connection guide](../docs/plans/remote_replay_studio.md#actual-compute-resources-separate-from-demo-workers). A scaled-to-zero pool, stale observation and unreachable host are distinct from an execution worker heartbeat. External research remains read-only, without takeover or duplicate execution.

With an owner `--execution-manifest` on control and workers, the compute page reads `/api/execution-plans` and submits only `plan_id` and `resource_id` to `/api/executions`. The plan/revision and target retain one idempotency key across retries. Once a plan/revision has an attempt, the UI links to that job and disables another submission; a different request key receives HTTP 409, not a retry of failed or lost execution. A permitted but unready/offline worker leaves the job queued; this does not start Azure nodes or silently fall back to the Mac. Training prefers configured training resources and excludes LAN, while replay/data plans follow their registered resource order. See [registered execution setup](../docs/plans/remote_replay_studio.md#queue-an-operator-registered-offline-plan).

The task queue explicitly separates synthetic and operator-registered jobs. Only synthetic jobs enter demo trace/order/campaign views or the synthetic comparison. Completed offline jobs use `registered_execution_report.v1` and show registered JSON summaries, output metadata, bounded terminal logs and environment; empty summaries stay empty and are not converted into PnL. Large outputs and complete logs remain on the original worker's persistent disk. Missing worker readiness and an offline resource are not the same state.

Use Node.js 22.12+ and the pinned pnpm 11.19.0. Commit `pnpm-lock.yaml` together
with dependency changes. `pnpm-workspace.yaml` explicitly permits the standard
esbuild binary installation hook; no interactive approval is required in CI.

```bash
cd frontend
pnpm install --frozen-lockfile
pnpm run lint
pnpm test
pnpm run format:check
pnpm run build
```

`lint` performs strict TypeScript checking, including unused locals and parameters.
The production build is written directly into `narrowgate/studio_static/` for wheel
packaging. End users of the built Python wheel do not need Node.js. Builds use the
already-installed local TypeScript and Vite executables and never install or
upgrade dependencies themselves. After dependency installation, `npm run lint`
and `npm run build` are equivalent script entrypoints. To bypass package-manager
environment checks entirely, `node scripts/build.mjs` performs the same build.

For frontend development, start the API on `127.0.0.1:8080`, then run `npm run dev`.
Vite proxies `/api` to that local server. Production uses same-origin API requests.

SSE updates are supplemented with a four-second status refresh. Create requests
retain their idempotency key across retries for identical input. Only completed
synthetic reports can be compared. Metrics are rendered from runner output rather
than recomputed, and missing values stay missing. Orders and campaigns link to the
matching original trace events. No mock jobs or economic values ship in the UI.

Overview charts only plot fields already present in the original trace, with at
least two finite timestamped observations of the same field. The synthetic
fixture currently provides three post-fill inventory observations but not a PnL
series: no PnL curve is shown. Equity is not relabeled as PnL, absent values are
not filled, and different field definitions are never spliced together. Lines
only connect recorded observations; they do not establish the state in between.
Selecting a point opens its original trace event.

Optional bearer authentication is entered through “访问凭据” and retained only in
page memory. Authenticated mode uses polling instead of putting tokens in the SSE
URL. Refreshing the page clears the token. Log panels currently show published
terminal logs, not a live worker log stream.

Run `pnpm run format` to apply the standard Prettier formatting to source and
documentation. `pnpm run format:check` checks formatting without editing files.

## Suggested CI coverage

- On `frontend/**` or Studio API/static-packaging changes: install using
  `pnpm install --frozen-lockfile`, then run lint, trace-series tests, format check,
  and build once on a Linux runner.
- Start the loopback API with a temporary state directory and one synthetic
  worker. Browser-smoke create, cancel, completed report, order/trace linkage,
  comparison, and mobile overflow. Separately use a harmless registered offline
  check to test plan-only submission, unavailable resources remaining queued,
  real-worker heartbeats and non-demo reporting. Do not start an economic run or
  cloud resource just to populate the UI.
- Build the Python wheel after the UI build, then verify the installed wheel
  serves its HTML and hashed CSS/JS assets without a frontend development server.

There is intentionally no second dependency lockfile or separate pnpm approval
step in the default build path.
