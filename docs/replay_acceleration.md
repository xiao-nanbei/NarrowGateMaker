# Replay acceleration: implementation and measurement

[English](replay_acceleration.md) | [简体中文](replay_acceleration.zh-CN.md)

Last materially modified: 2026-09-22
Last materially synchronized: 2026-09-22

This is a partial implementation report, not completion of P0–P4. The reviewed and starting checkout was `ed8b4991f64a26786139a8475214dd06a66df557`; an immutable reference archive and per-run metrics/results remain in the private evidence store, not distributed with the public repository. Existing frozen research workers were not replaced. No live service or strategy parameter was changed.

## Actual entry and environment

The active F03 wrapper calls `simulate_public_inputs` → `load_public_replay_inputs` / `public_predictions` → Python `simulate_tick` → `settle_public_replay`. Its four models previously repeated input decoding. The target server reports two Xeon Gold 6254 sockets, 18 cores/socket, two threads/core (36 physical cores, 72 logical CPUs), two NUMA nodes, approximately 188 GiB RAM and CPU affinity 0–71. Inspected user cgroups report unlimited CPU/memory, not exclusive resources. Other research jobs occupy the host; available memory was approximately 85 GiB at inspection. Its research Python is 3.12.3 and no native extension was found in that environment. Local measurements below are on the macOS development host, not target-server throughput.

## Implemented changes

- P1: persistent ABI/compiler/profile/configuration-specific CMake/Ninja research build; optional ccache, IPO disabled, existing floating-point options retained. The production live-wheel entry is unchanged. Explicit fresh-process extension loading bypasses an observed editable-install hook that otherwise loaded the old installed binary despite `PYTHONPATH`.
- P2: `prepare_public_inputs` and `simulate_prepared_inputs`, compatible public wrapper, read-only book/variance arrays and independent tape cursors. F01 prepares once and settles each candidate immediately; its compatible materialized API still retains returned traces. Public-model raw predictions are batched, with one model thread, all 13 heads retained and the original clipping/EMA/state update implementation reused. Native model backends retain their original prediction path.
- P3: opt-in atomic numeric cache with read-only NumPy mappings, flattened variable-length book levels, source/content validation at admission and fresh cursors per account. Bounded spawn workers exchange descriptors and write results locally, not giant trace objects through IPC. One prepared shard is retained per worker. This is not yet a production memory-admission scheduler; select workers conservatively from measured memory and existing host load.

## Measurements and limits

The fixed real short window was selected from an existing engineering input before optimization, not by PnL. It contains 1,717 execution trades, 7,697 merged-clock rows, and four strategy fills. The stronger comparison retained bounded decision/quote/fill traces and actual funding input; the shared accounting reported complete. All three reference and three cached result documents were byte-identical (including economic output). This does not establish full two-day, dense-day or native parity.

| Local measurement | Reference | Optimized | Conditions |
| --- | ---: | ---: | --- |
| Short-window processing, median | 4.935 s | 2.922 s | Three runs; includes loading/model work, ledger and serialization; these recorded runs exclude final file write |
| Range | 4.802–4.971 s | 2.879–3.500 s | Uncontrolled OS page cache and concurrent background work |
| Prediction plus state processing, median | 0.626 s | 0.155 s | Same Python LightGBM backend; all heads, original EMA |
| Event loop, median | 3.782 s | 2.653 s | Cached numeric book instead of repeated fact decoding |
| Process RSS high-water | 497–544 MB | 259–280 MB | Separate runs; not USS/PSS and not summed across workers |
| Four-task elapsed time | 14.584 s, one worker | 11.214 s, two workers | Earlier diagnostic-output run without funding; not an all-in economic benchmark |
| First numeric cache preparation | — | 2.486 s | Earlier same-window cache creation, separate cost, not hidden in hot-run timing |
| No-change native build | — | 0.320 s | Configure additionally 0.597 s; Ninja reports no work |
| Registration-unit incremental build/link | — | 7.104 s | One object plus link; configure additionally 0.597 s |

The first native compile/link had a Ninja timeline of 133.589 s; its initial post-build import check correctly failed on the old editable-installed extension. This is not a clean first-build end-to-end timing. The loader was repaired and a fresh process verified the correct artifact. No ccache was installed. Current measurement commands also record result-file write separately; the earlier values above are not retroactively relabelled as including that write. Preparation, process startup, model loading and cache generation must be included in campaign totals before claiming an overall speedup. PSS/USS was unavailable locally.

The sampled reference profile placed substantial cost in exchange-book fact decoding/conversion and per-frame model calls. The target-server engineering check is isolated from active workers and uses existing server inputs. Target results, full-shard comparisons, dense/gap cases and a server concurrency scan remain outstanding; do not multiply these short-window gains by 72.

## Stage acceptance

| Stage | Actual status |
| --- | --- |
| P0 | Partial: actual entry/environment and local short baseline measured; representative full-shard and target throughput acceptance pending |
| P1 | Implemented and locally verified; target-server native build not verified |
| P2 | Implemented core reuse/batching and local parity tests; full-shard acceptance and frozen F03 worker integration pending |
| P3 | Implemented opt-in cache and spawn entry, local parity and 1/2-worker trial; target memory admission/NUMA/concurrency validation pending |
| P4 | Not implemented: existing native entry rejects diagnostic public book mode, explicit BBO invalidation and actual private-fill/runtime compute latency. The resumable native execution/strategy boundary and parity tests remain required |
| P5 | Not implemented: no measured shared native strategy callback yet; cannot justify JIT selection |
| P6 | Not implemented: current workload is fixed-model independent accounts, not the counterfactual branch workload |

Do not remove native rejection checks, turn off latency/invalidations, or call the Python loop from a C++ wrapper and label that P4. The current Python implementation remains the execution authority; this change does not create a second full strategy implementation. No production result should silently switch to an unvalidated optimized release.

## Executable interfaces

Executed locally (use the directory printed by the first command):

```bash
.venv/bin/python scripts/build_replay_native.py --jobs 2
.venv/bin/python scripts/run_replay_native.py --build-dir <printed-build-directory> --module pytest tests/test_cpp_signal_features.py -q
.venv/bin/python -m pytest tests/test_replay_prepared.py tests/test_research_public_inputs.py tests/test_signal_compute_telemetry.py tests/test_signal_feature_cutoff.py -q
```

The following interfaces were exercised with private inputs; substitute your admitted locators, not historical defaults. Output directories are create-only. The cache command reports preparation separately; do not compare its hot run with an uncached run without including this cost.

```bash
.venv/bin/python -m models.replay.benchmark --bundle <consumer-bundle> --params <params.json> --model <model-directory> --funding <funding.json> --output <new-output-directory> --repeat 3
.venv/bin/python -m models.replay.benchmark --bundle <consumer-bundle> --params <params.json> --model <model-directory> --cache <cache-directory> --output <new-output-directory> --repeat 3
.venv/bin/python -m models.replay.batch --tasks <tasks.json> --workers 2 --output <new-summary.json>
```

The task file is a JSON list of `{ "id": "unique-id", "bundle": "...", "params": "...", "model": "...", "funding": "...", "cache": "...", "output": "..." }`. Model/funding are optional; omitted funding stays incomplete, not zero. Every task is a whole independent account with its existing interval; the runner never slices accounts into days. This CLI is an engineering executor, not permission to open F or select candidates. Strict quote-parameter candidate restrictions remain in F01. Failed tasks raise and cannot produce a complete batch summary.

## Maintenance

| Change | Owner | Build needed |
| --- | --- | --- |
| Parameters | Explicit replay config / family candidate contract | No |
| Models | Frozen public model loader; all-head raw prediction | No C++ build; new engine and prediction identity |
| Market features/observation | `data` and feature contract | Invalidate affected input cache schema/identity |
| Strategy rules | Existing strategy modules and Python replay authority | No duplicate C++ strategy maintenance introduced; P4 still pending |
| Matching/order/time semantics | Existing execution engine plus focused parity tests | Native modifications require incremental build and a fresh worker |
| Ledger/reporting | Shared `public_accounting` and benchmark module | No |

Tests executed: 127 prepared/public-input/signal tests and 46 native feature tests passed; these suites overlap and are not a full repository audit. Cache corruption, A→B→A isolation, independent tape cursors and all-head EMA parity are included. Full target acceptance remains explicitly incomplete.
