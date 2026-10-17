# Live / Dry-Run Boundary

[English](live_dry_run.md) | [简体中文](live_dry_run.zh-CN.md)

Last materially modified: 2026-09-06

Last materially synchronized: 2026-09-06

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

The tracked `live/config.yaml` is a public template. It is safe to inspect and load, but it is not a private live parameter snapshot.

Private runtime configs should live outside published docs, for example:

```bash
export NARROWGATE_LIVE_CONFIG="$PWD/docs/private/live_config.current.local.yaml"
bash live/run.sh start
```

Use the repository preflight as a local diagnostic before preparing a start, restart, or deployment:

```bash
.venv/bin/python scripts/preflight_live_deploy.py \
  --config "$NARROWGATE_LIVE_CONFIG" \
  --repo-root .
```

Preflight validates the selected local config and resolves its model and policy artifacts, but it does not prove that a remote process is running the same release or authorize activation. Live startup independently consumes the deployment envelope and stopped-exchange reconciliation. Use `live/run.sh status`, `live/run.sh profile`, and the startup logs to verify the running release after every activation.

In an admitted live deployment, the complete config bytes are restart-only and bound to the release, not merely the fields listed in individual hot-reload guards. Changing even a descriptive config field produces different bytes: running preflight again cannot make those bytes hot-reloadable. Prepare a new deployment envelope with the intended config, perform the required stopped reconciliation, and activate through the normal deployment transaction. The generic SIGHUP handler retained in the library does not override the deployed config binding. See the [operations workflow](README.md).

`make deploy-preflight` rejects a config marked `PUBLIC TEMPLATE`. With ML enabled, it requires all 13 model heads, their schema/feature contract, and a hash-bound private model authorization permitting live use; public synthetic, `public_dry_run_only`, `research_only`, missing-authority, and `authority.live=false` model bundles fail closed. With ML disabled, it validates the P3 fill-probability artifact still used by quoting, without requiring unused model heads or a model-authorization bundle. P3 identity, format, and runtime numerical checks remain in force, and a `public_dry_run_only` P3 fixture cannot enter deployment. This remains a local validation result, not deployment approval. Source publication is a separate operation performed by `make publish-source-dry` or `make publish-source`; it does not inspect private deployment inputs.

Public examples:

```bash
narrowgate doctor
narrowgate paths
bash live/run.sh dry-run
```

## Formal dry-run

`bash live/run.sh dry-run` is the only public operational dry-run. With no environment override it uses the minimal [`live/formal_dry_run_public.yaml`](../../live/formal_dry_run_public.yaml) helper and the checked-in synthetic P3 artifact. It loads the config through the strict live config parser, requires the P3 file to exist, and records its SHA256. ML is disabled in the public example, so no model heads or model-authorization bundle are required; with ML enabled, it additionally validates all 13 heads and their schema/feature contract through `strategy.model_contract.validate_model_bundle`. This public local check does not supply the private live authorization or complete P3 deployment validation required by preflight and startup.

The command does not source `live/.env` or a runtime profile. It exits before logging setup, exchange or network client construction, `MakerEngine`, WebSocket setup, worker threads, and every order path. The default deadline is 30 seconds; a timeout terminates with exit code 124. A different positive deadline can be selected explicitly:

```bash
NARROWGATE_DRY_RUN_TIMEOUT_S=10 bash live/run.sh dry-run
```

The command emits one JSON object on stdout. `status=passed` and exit code 0 mean only that local config checks, P3 file/identity checks, and any enabled model-contract validation completed before the deadline. Validation failure returns exit code 1; deadline expiry returns 124. The summary includes config/P3 identities, ML enablement, required and validated head counts (both zero for ML-OFF), and explicit zero counts for exchange clients, threads, and submitted orders. It never includes API keys or the complete config.

To validate another local config without changing the command contract:

```bash
NARROWGATE_LIVE_CONFIG="${NARROWGATE_ROOT}/docs/private/live_config.current.local.yaml" bash live/run.sh dry-run
```

The public synthetic artifact carries no research, action, baseline, live, or deployment authority. A successful local dry-run does not invoke, weaken, or replace `scripts/preflight_live_deploy.py`, the remote deployment gate used by `start` and `restart`, or any owner-side evidence requirement.

`bash live/run.sh status` is not a dry-run. It only reports whether a maker process is already running and returns nonzero when no process is found.

## Runtime profile

Do not start `live/main.py` with a one-off shell export. `live/run.sh` loads the untracked credentials file first and then one checked-in compute profile:

```bash
NARROWGATE_LIVE_PROFILE=python bash live/run.sh profile
NARROWGATE_LIVE_PROFILE=native bash live/run.sh profile
```

The native profile is strict: startup validates the extension source and every required quote/signal/routing API before connecting the order path. Use `run.sh status` and the startup `NATIVE_PROFILE` log line to verify the running implementation after every restart.

## Order gateway

Persistent REST with one globally serialized order-write lane remains the
default live adapter. The former async latest-wins gateway remains removed
after its target-host soak worsened p99/p99.9 and produced almost no useful
coalescing. The current bounded async-response lane, cross-side lane, and
USD-M WebSocket API adapter are separate restart-only experiments and are all
disabled by default; none is a latest-wins queue or a hot-reload switch. They
require matched-host latency and economic qualification before activation.

Do not publish hostnames, process ids, raw live PnL, account size, or complete private parameter snapshots.

## External venue credential boundary

Bitget, Bybit, and OKX public trades and public BBO/order-book channels do not require authentication. NarrowGate therefore holds no external-venue API key: only Binance execution credentials remain in `live/.env`.

This boundary still permits the intended receive-time research feeds:

- Bitget `publicTrade`/`trade` and `books1` or public `books`;
- Bybit `publicTrade` and `orderbook.1` or public depth;
- OKX `trades`, `bbo-tbt`, and public `books`.

OKX's VIP-only 10ms `books-l2-tbt` and `books50-l2-tbt` are deliberately out of scope because they require login and fee-tier eligibility. Do not introduce credential management unless a separately reviewed private-account or VIP depth requirement appears.
