# Causal-v12 1s Daily Physical Materialization v2

Date: 2026-08-05

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: real source path executable for an admitted 2025 provider day; bulk materialization, training, and live authority remain closed.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Scope

This implementation adds a read-only physical source adapter and an atomic daily Parquet materializer for the F03 1s cadence successor. It does not modify the frozen 13-head estimands, live code, strategy code, C++, registry, or an existing frozen Spec. It does not accept an old 10s feature panel as input and does not forward-fill old 10s feature rows onto a 1s grid.

The implementation files are:

- `research/families/f03_causal_13_head/audit/causal_v12_1s_daily_sources.py`
- `research/families/f03_causal_13_head/audit/causal_v12_1s_panel_materializer.py`

## Target-day Decision Clock

For target day `D`, authoritative decision timestamps are exactly:

```text
[D 00:00:00, D+1 00:00:00)
```

The automatic full-day set contains exactly 86,400 canonical 1s timestamps. Every decision at `t` requires the completed local bar with `start_ts_ms == t - 1000`.

- `D 00:00:00` is supported by the `D-1 23:59:59` warmup bar.
- `D 23:59:59` is the final decision assigned to `D`.
- `D+1 00:00:00` is excluded from `D`.
- Missing predecessor bars fail closed before a panel is admitted.

This clock contract is included in the panel schema, cache identity, and artifact manifest. It corrects the earlier target-bar-derived selection that could omit `D 00:00:00` and include the next midnight.

## Physical Source Contracts

### Local and reference 1s bars

Real trade-tempo and reference Parquet files may store `timestamp` as a pandas index in Arrow metadata. The reader uses `to_pandas(ignore_metadata=True)` so the Arrow field remains an ordinary column, with a safe index restoration fallback. Tests use a fixture with the same pandas index metadata shape.

Sparse no-trade seconds are reconstructed from the previous observed close using the live-compatible flat-bar rule: OHLC equals the prior close and all trade quantities and counts are zero. The lag state is explicitly recorded as `synthetic_flat_no_trade_1s_from_prior_observed_close`. A gap cannot be synthesized without a prior close, and a consecutive missing run above the frozen 30-second support fails closed. Synthesized counts and maximum run length are written to the source audit and daily manifest.

This reconstruction is a physical 1s lag-state rule. It is not feature-row forward filling.

### Metrics

Metrics authority is the strict raw CSV layout `raw_metrics/BTCUSDC-metrics-YYYY-MM-DD.csv`. The parser requires the exact eight-column schema, 288 rows, symbol `BTCUSDC`, finite numeric values, and one of two complete 5-minute timestamp layouts:

- interval-start stamps are shifted to the completed interval end;
- interval-end stamps remain unchanged.

Both normalize to `feature_ready_ts == completed_5m_interval_end`. Parquet or an incomplete/ambiguous clock is rejected.

### L2 and source authority

The execution-L2 reader accepts the real `bid_px_1..20` and corresponding quantity/ask layout while materializing the frozen execution features from the required levels. The per-day quality JSON is parsed, validated, and bound to the actual L2 Parquet size and SHA256; merely hashing an unvalidated JSON is not sufficient.

Every bundle must provide exact `D-1` and `D` coverage for local tempo, L2, quality, metrics, BTCUSDT reference bars, and their source manifests. The BTCUSDT metadata is validated for day, symbol, completeness, 1s cadence, causal-ready semantics, row count, size, and SHA256.

## Materialization Contract

The materializer requires an explicit output path. Full-day output additionally requires `--allow-full-day`; otherwise an explicit cutoff file is required. It writes a temporary directory containing:

```text
panel.parquet
source_probe.json
manifest.json
_SUCCESS
```

The panel and probe are hashed, the manifest binds all physical inputs and code identities, and `_SUCCESS` contains the manifest SHA256. Admission is one atomic directory rename. Existing artifacts are reused only when cache identity, panel hash, and admission marker all match.

## Executable Real-source Probe

A read-only `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` probe and a two-row atomic smoke materialization completed for target day `2025-08-02`, using `2025-08-01` as `D-1`:

- source bundle identity: `bfda22b3b523ce0c4b36e25b2b4e2acdddf67f19c5f645682369fe51cd6b1986`;
- physical materialization eligible: true;
- local bars: 172,798 observed, 1 synthesized, maximum missing run 1s;
- BTCUSDT reference bars: 170,543 observed, 2,256 synthesized, maximum missing run 3s;
- both metrics files were accepted as interval-end stamped;
- smoke decisions included `2025-08-02 00:00:00 UTC`, supported by the prior-day bar;
- output cache identity: `d0ed561236adec7529c7b194745bc12b89c0a483db58dcb748e45368e4ec492b`;
- output panel SHA256: `bba932df9feeca0c81bc316ecc38bef95c13781858a2342fdc5fb76259d26cd3`.

The diagnostic smoke artifact uses `${NARROWGATE_EPHEMERAL_ROOT}/f03_1s_real_source_probe_20260805_v2`. It is not a training panel or durable research authority.

## Honest Blockers

The reader/materializer code path is executable, but real source mapping is not universally admitted yet:

- the `2026-07-29` provider-L2 probe found both `2026-07-28` and `2026-07-29` quality records ineligible under their internal-gap/provider-normalized replay authority;
- the available native `2026-07-28/29` L2 path does not provide matching per-day quality JSONs, so exact quality authority cannot be bound;
- an earliest physical source file that begins after midnight cannot synthesize its own leading second without a prior close. Target-day decisions remain supported when the required `D-1` tail is present, but the earlier day itself is not silently upgraded.

No bulk day was materialized and no model was trained. Python/C++ feature parity, chronological training identity, ML-OFF/ON full-path economics, and deployment remain separate blockers.

## Verification

Using `.venv/bin/python`:

```text
ruff format --check: 3 files already formatted
ruff check: All checks passed
pytest: 28 passed
```

The tests cover pandas-index Arrow metadata, strict metrics CSV semantics, bounded flat-bar reconstruction, D-1/reference authority, L2 quality hash binding, rejection of old 10s feature panels, atomic admission, and the exact target-day decision boundary.
