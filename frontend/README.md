# Replay Studio frontend

Last materially modified: 2026-09-07
Last materially synchronized: 2026-09-07

This English page is canonical; [简体中文](README.zh-CN.md).

React + TypeScript workspace for the packaged Studio UI. The only executable runner is `replay-demo` with the built-in `synthetic-demo` dataset. A separate default tab displays existing private B0 summaries imported through the [owner-local CLI](../docs/plans/remote_replay_studio.md#import-completed-b0-results-without-replay). This interface does not submit real F01, current B0 or E/C research, invoke shell commands, or provision cloud resources.

Real results use read-only `/api/results` endpoints and remain separate from synthetic jobs, reports and comparison. The browser displays saved amounts and continuous-segment coverage without manufacturing daily PnL or Sharpe; fees already in trading PnL are not deducted again, and funding is added once. Local/Azure origin and existing cross-host verification describe historical provenance, not current cloud connectivity. Missing queue coverage and modeled-evidence limitations stay visible. No private evidence ships in the frontend bundle.

## Market review and data quality

The market-review page selects an imported B0, continuous segment, UTC date, and 1s/5s/1m/5m candle interval (default 1m). It reuses the existing React/SVG stack, with keyboard/button zoom and bounded drag navigation; requests stay within one UTC day, align to complete candle boundaries, and display at most 1,000 candles. Candles come only from retained market bars, never from strategy fills. The current source is explicitly **historical market context, not verified as the exact original replay input**. A missing bar second has an unknown cause, not an automatic source-outage classification.

BUY/SELL markers identify simulated fill direction, not opening/closing trades. Each fill keeps its exact physical and local-visible timestamp, execution price, quantity, original/scoped order identifiers, inventory, signed fee, and recorded campaign fields. Same-candle and same-millisecond fills remain independent rows; incomplete pagination is disclosed and can be continued. Missing fields stay unknown, and a submit-time campaign is not relabeled as final attribution. Inventory observations use the local-visible callback clock and original sequence, retaining same-clock changes without drawing an invented path across different timestamps. Missing inventory clocks are not replaced by exchange fill time. Order details are partial fill snapshots: the UI does not manufacture active-order bands or continuous PnL.

The separate UTC quality calendar includes the entire requested inclusive date range, including absent leading/trailing dates. Source, market, symbol, dataset, node, problem-day, and explicitly missing-replica filters are available. File availability, audit status, per-task usability, and node-copy verification remain separate; an offline or unchecked copy is not silently missing or usable. Expanded entries show only recorded counts, byte sizes, coverage, gap intervals, timestamps, reasons, and evidence scope. Export creates a bounded JSON download/recheck checklist; it never launches a download, synchronization, audit, or replay. Following a day into market review does not bind the current quality catalog to an older result.

## Developer build

The compute page uses `/api/compute-resources` for owner-configured physical/cloud resources, not the number of synthetic workers. Friendly labels, fixed background probes, external job observations and placement roles are configured through the control service's `--resources-manifest`; see the [resource connection guide](../docs/plans/remote_replay_studio.md#actual-compute-resources-separate-from-demo-workers). A scaled-to-zero pool, stale observation and unreachable host are distinct from an online worker. The page observes existing research; it does not add a real-market submit adapter or cloud provisioning.

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
  comparison, and mobile overflow. Do not submit real research or cloud tasks.
- Build the Python wheel after the UI build, then verify the installed wheel
  serves its HTML and hashed CSS/JS assets without a frontend development server.

There is intentionally no second dependency lockfile or separate pnpm approval
step in the default build path.
