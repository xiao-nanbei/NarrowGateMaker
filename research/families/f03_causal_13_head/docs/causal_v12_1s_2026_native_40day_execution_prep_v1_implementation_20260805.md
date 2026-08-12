# F03 causal-v12 1s 2026 native 40-day execution preparation v1

Date: 2026-08-05

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Status: `feature_panels_complete_model_bundle_unbound`

## Scope

This delivery prepares the frozen 2026 native 40-day feature inputs only. It does not train a model, materialize predictions, read labels or economic outcomes, start a replay, or grant action/live authority.

No change was made to `models/backtest_tick.py`, live code/configuration, or an existing frozen Spec. The implementation is isolated to a new F03 audit module, its tests, and this new implementation record.

## Frozen denominator and authority

The runner requires the exact ordered 40 days from the existing F03 1s economic precommit, from 2026-04-17 through 2026-06-26. It independently requires the same days in the existing source-profile evidence and rejects any day-order, count, profile, or SHA drift.

The source profile is exactly:

```text
native_historical_minimal141_individual_reference_v1
```

Each target uses its previous natural UTC day as warmup. Source fallback, substitute warmup, aggregate reference bars, and glob discovery remain forbidden.

The minimal141 registry expresses different sequence requirements for D-1 warmup and target days. The older daily materializer probe does not represent that role distinction. The new runner therefore serially injects the already validated exact-profile probe into each worker's materializer call, checks the bundle identity, and restores the generic probe afterward. It does not rewrite the source bundle or synthesize a misleading per-day quality identity.

## Cache and execution

The feature cache is stored under `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}`:

```text
${NARROWGATE_DATA_ROOT}/cache/replay_dag/
  f03_causal_v12_1s_2026_native_40d_execution_prep_v1
```

The formal run used `.venv/bin/python` 3.12.13, the C++ batch engine, and four isolated worker processes. Daily artifacts use the existing atomic `panel.parquet + manifest.json + _SUCCESS` contract. The parent process writes resume-safe atomic progress and reorders completed work back to the frozen day ordinal before top-level admission.

```bash
.venv/bin/python -m \
  research.families.f03_causal_13_head.audit.causal_v12_1s_2026_native_execution_prep \
  --market-data-root ${NARROWGATE_DATA_ROOT} \
  --cache-root ${NARROWGATE_DATA_ROOT}/cache/replay_dag/f03_causal_v12_1s_2026_native_40d_execution_prep_v1 \
  --batch-rows 4096 \
  --workers 4
```

## Result

- 40/40 days admitted.
- 86,400 rows per day; 3,456,000 total rows.
- 1.934 GiB of Parquet feature panels.
- All daily panel hashes, manifest hashes, Parquet row counts, and `_SUCCESS` markers verified.
- A second 40-day run reused 40/40 panels and reproduced the same top-level identity and manifest SHA; zero panel was rebuilt.
- No incomplete temporary panel directory remains.
- Top-level execution-prep identity: `ce936e22719da52f284576aae0ce74b1e780fd18f6b02dd9c6487659f53f2d67`.
- Top-level manifest SHA256: `ddbd4bbcb4344483a9e20538fc5291bfffff14877c89624b361289dd810cb1b8`.

The full machine-readable record is `causal_v12_1s_2026_native_40day_execution_prep_v1_implementation_20260805.json`.

## Fail-closed boundary

No causal-v12 1s research bundle is bound yet. The admitted state therefore is:

```text
feature_panels_complete_model_bundle_unbound
blocker = model_bundle_identity_unknown
execution_input_eligible = false
training_performed = false
```

Passing `--require-bound-model-bundle` without a bundle fails before source access or materialization. The module has no training command or training entrypoint. A future admitted 13-head bundle may be hash-bound by a separate execution step, but this delivery cannot train it or proceed into prediction or PnL by itself.

## Verification

- New module tests: 8 passed.
- Module plus source-resolver and panel-materializer regression: 38 passed, 2 skipped.
- Ruff check and format check: passed.
- Python compile check: passed.

Permissions remain closed for prediction execution, economic replay, Validation, sealed holdout, action, and live deployment.
