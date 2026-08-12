# Live / Dry-Run Boundary

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

The tracked `live/config.yaml` is a public template. It is safe to inspect and load, but it is not a private live parameter snapshot.

Private runtime configs should live outside published docs, for example:

```bash
export NARROWGATE_LIVE_CONFIG="$PWD/docs/private/live_config.current.local.yaml"
bash live/run.sh start
```

Before a start, restart, hot reload or deployment, run the repository preflight with the project virtual environment:

```bash
.venv/bin/python scripts/preflight_live_deploy.py \
  --config "$NARROWGATE_LIVE_CONFIG" \
  --repo-root .
```

The 2026-07-27 local declared-config preflight reports 13-head ML disabled, model directory `models/saved_btcusdc_causal_v7_time_calendar_semantics_20260726`, empirical P3 SHA256 `cedec34851454b643be746746a1dd4bcc7e13807985c14e78504643ad6e71714`, and `quote_horizon_s=1`. Those values validate the named local config and artifacts only; they do not prove that a remote process is running the same hashes. Verify the remote config/model/P3 identity with `live/run.sh status`, `live/run.sh profile`, and the startup identity logs after every restart.

`make deploy` refuses to deploy a config marked `PUBLIC TEMPLATE`. This prevents accidentally pushing the public template to a private live host.

Public examples:

```bash
narrowgate doctor
narrowgate paths
NARROWGATE_LIVE_CONFIG=examples/live_dry_run_config.yaml bash live/run.sh status
```

## Runtime profile

Do not start `live/main.py` with a one-off shell export. `live/run.sh` loads the untracked credentials file first and then one checked-in compute profile:

```bash
NARROWGATE_LIVE_PROFILE=python bash live/run.sh profile
NARROWGATE_LIVE_PROFILE=native bash live/run.sh profile
```

The native profile is strict: startup validates the extension source and every required quote/signal/routing API before connecting the order path. Use `run.sh status` and the startup `NATIVE_PROFILE` log line to verify the running implementation after every restart.

## Order gateway

The live order adapter is synchronous. The former async latest-wins gateway was removed after its target-host soak worsened p99/p99.9 and produced almost no useful coalescing. A future gateway redesign must arrive as a new experiment; there is no disabled switch to re-enable.

Do not publish hostnames, process ids, raw live PnL, account size, or complete private parameter snapshots.

## External venue credential boundary

Bitget, Bybit, and OKX public trades and public BBO/order-book channels do not require authentication. NarrowGate therefore holds no external-venue API key: only Binance execution credentials remain in `live/.env`.

This boundary still permits the intended receive-time research feeds:

- Bitget `publicTrade`/`trade` and `books1` or public `books`;
- Bybit `publicTrade` and `orderbook.1` or public depth;
- OKX `trades`, `bbo-tbt`, and public `books`.

OKX's VIP-only 10ms `books-l2-tbt` and `books50-l2-tbt` are deliberately out of scope because they require login and fee-tier eligibility. Do not introduce credential management unless a separately reviewed private-account or VIP depth requirement appears.
