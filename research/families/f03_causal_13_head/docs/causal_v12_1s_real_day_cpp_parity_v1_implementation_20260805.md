# F03 causal-v12 1s real-day Python/C++ parity v1

Last materially modified: 2026-08-05

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Status

The runner and its contracts are implemented and pass their regression suite. Real-source parity is pending the final stable `cpp_batch` and panel-materializer identity. No complete day was materialized or audited, and this work grants no training, economic, action, baseline, or live authority.

## Input boundary

The runner accepts exactly two identity inputs:

1. an atomically admitted `causal_v12_1s_daily_feature_panel_artifact.v2` manifest; and
2. that manifest's hash-bound `source_probe.json` source-bundle identity.

Before feature computation it verifies the `_SUCCESS` marker, manifest SHA256, panel SHA256/size/schema/row count, source-probe SHA256, bundle canonical hash, all physical source sizes and SHA256 values, current Python source-reader and feature-code hashes, feature order, source manifest, and the C++ ABI. Any identity disagreement still fails immediately in the production runner.

The panel is expected output only. Each cutoff rebuilds local 1s bars, execution L2, metrics, and BTCUSDT reference views from the bound physical sources. Panel features are never passed into C++.

## Compared contract

All 173 fields are checked in frozen order for:

- value;
- validity;
- source timestamp;
- feature-ready timestamp;
- observation count; and
- lag state.

The cutoff, decision time, row-ready time, unsupported count, physical observed/synthetic lag metadata, feature-order hash, and row fingerprint are also checked. Panel-to-Python row fingerprints must be bitwise exact. The C++ numeric gate uses the frozen `rtol=2e-12`, `atol=2e-12`; C++ bitwise float identity remains diagnostic because reduction order can change only the last bits without violating numerical parity.

## Local probe policy

The local temporary integration fixture currently uses `${NARROWGATE_EPHEMERAL_ROOT}/f03_1s_real_source_probe_20260805_v3`. The test executes parity only when the probe's bound Python code identity exactly equals the current code identity. If it differs, pytest reports:

```text
stale local diagnostic: probe code identity does not match current code identity
```

and skips only that local integration test. This does not relax the runner: calling the runner directly with the same stale artifact still fails closed.

Both earlier probes are retained as stale diagnostics:

- v2 binds daily-source reader SHA256 `22d00155...`;
- v3 binds daily-source reader SHA256 `03782d3e...`.

The transient v4 artifact produced while implementation identities were moving is preserved but is not selected or cited as parity evidence. A final probe will be regenerated only after `cpp_batch` and the panel materializer stabilize.

## Streaming behavior

The panel is read with `ParquetFile.iter_batches`. A complete day can be processed without retaining 86,400 output rows: only 173 field-error summaries, the cutoff hash, and the comparison-stream hash remain online. This implementation record did not run a complete-day artifact.

Example invocation after the implementation identity is stable:

```bash
.venv/bin/python \
  research/families/f03_causal_13_head/audit/causal_v12_1s_real_day_cpp_parity.py \
  --panel-manifest /path/to/admitted/day/manifest.json \
  --source-bundle-identity /path/to/admitted/day/source_probe.json \
  --report-json /path/to/parity-report.json
```

## Verification

- Focused runner tests: 11 passed, 1 skipped as stale local diagnostic.
- Runner plus C++ and materializer regressions: 37 passed, 1 skipped.
- The two stale-probe cases explicitly verify production fail-closed behavior.
- Ruff, JSON validation, formatting, and diff checks: passed.

The runner reads no labels, predictions, markouts, rewards, or PnL. The next step is a newly admitted two-row probe bound to the final stable implementation, followed by complete-day parity only after that probe passes.
