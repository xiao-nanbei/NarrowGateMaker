# F03 causal-v12 1s 2026 Native Overlay Binding v1

Date: 2026-08-05

Last materially modified: 2026-08-12

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Status: implemented, result-blind, future bundle not yet bound

## Scope

This layer binds the future atomically admitted `causal_v12_1s` 13-head research bundle to the existing ordered 40-day 2026 native feature prep. It creates only a compact prediction overlay. It does not copy the 173 market features, read labels or economic outcomes, call the full-path replay, or grant action/live authority.

The implementation is [`causal_v12_1s_2026_native_overlay_binding.py`](../audit/causal_v12_1s_2026_native_overlay_binding.py).

## Bound Contracts

Before an execution plan is admitted, the module re-resolves and hashes:

- the complete 13-head bundle, every model and metadata artifact;
- the canonical 1s cadence, 173-column order, full Feature DAG and per-node source clock;
- all 66 training feature artifacts named by the bundle training identity;
- the existing 40 ordered native Development feature panels;
- every native day's canonical timestamp and feature-row fingerprint identity;
- the implementation, overlay materializer, schema, DAG and native-prep code.

A missing candidate bundle fails before the plan or any large overlay is written. A plan also fails when a code byte, model artifact, feature panel, source-clock contract, day order or row identity changes.

## Materialization

The default output is on `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}`:

```text
${NARROWGATE_DATA_ROOT}/cache/replay_dag/
  f03_causal_v12_1s_2026_native_40d_model_overlay_v1/
```

Each UTC day is written by the existing atomic prediction-overlay materializer. The new layer verifies that the four join columns reproduce the feature panel's daily row identity, writes an atomic progress file after every day, and publishes the 40-day panel manifest only after all daily overlays pass. Interrupted runs reuse already admitted hash-compatible days.

Once training provides the bundle path, the intended commands are:

```bash
.venv/bin/python -m research.families.f03_causal_13_head.audit.causal_v12_1s_2026_native_overlay_binding prepare \
  --research-bundle-dir ${NARROWGATE_DATA_ROOT}/<bundle-dir>

.venv/bin/python -m research.families.f03_causal_13_head.audit.causal_v12_1s_2026_native_overlay_binding run \
  --plan ${NARROWGATE_DATA_ROOT}/cache/replay_dag/f03_causal_v12_1s_2026_native_40d_model_overlay_v1/execution-plan.json
```

## Verification

```text
.venv/bin/python -m ruff check <implementation> <test>
All checks passed

.venv/bin/python -m pytest -q tests/test_causal_v12_1s_2026_native_overlay_binding.py
5 passed
```

No prediction values or economic outcomes have been read in this step. The remaining blocker is only the future trained bundle path; after it exists, this module can freeze the execution plan and produce the 40 daily overlays without rebuilding or duplicating the native market features.

The existing `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` prep was also physically revalidated through this new loader: 40/40 days passed, the ordered interval remained 2026-04-17 through 2026-06-26, and all 40 daily row-identity hashes were distinct. This check read only feature artifacts.
