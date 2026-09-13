# Live execution-feature migration

[English](execution_feature_migration.md) | [简体中文](execution_feature_migration.zh-CN.md)

Last materially modified: 2026-09-22

Last materially synchronized: 2026-09-22

## Change live, preserve the model

The owner selected a live-input migration, not retraining or rewriting frozen models. The explicit `ml.feature_protocol: execution_v1` path subscribes to native BTCUSDC `@trade` messages and routes partial-depth snapshots through [LiveExecutionFeatures](../../live/feature_protocol.py) into the existing [shared execution calculator](../../data/observation.py). Existing releases retain an isolated legacy path. Model/protocol mismatches and cross-protocol hot reloads fail closed. This source implementation is not an activated release.

The [live signal consumer](../../strategy/live_public_signal.py) verifies all 13 unchanged head files, metadata, feature schema, training identity and variance units. It requires a separate hash-bound live-input authorization rather than rewriting frozen metadata; only the transport binding is projected in memory. Live input retains actual exchange publication, reception and readiness clocks and its own identity, not Tardis identity or historical simulated delays. The adapter uses a 100 ms lateness allowance and 1 s maximum book age; deployment must bind these settings to the model/P3/config closure.

## Source granularity and safety

An aggregate packet spanning nine execution IDs is still one observed event. It cannot substitute for individual-source `observed_trade_count_10s` or `trade_intensity_burst_guard`; the compatibility guard continues to reject that route. A bounded read-only venue probe observed native BTCUSDC `trade` events with individual `t` identities. The implementation consumes those events without inventing aggregate children. Endpoint availability alone does not prove ongoing completeness, target-host timing or deployment readiness.

Duplicate identities do not add volume; conflicting or regressing identities fail. Disconnects and detected ID gaps clear rolling features and compatibility price history. Warmup requires a complete observed 60-second window and fresh book. Missing depth stays unknown, stale books cannot provide fresh prices, and REST aggregate prefill is disabled. Loss of new-protocol support cancels existing quotes and blocks direct requotes. Stopped research and frozen model bytes remain unchanged.

## Verification and activation boundary

[Adapter tests](../../tests/test_live_feature_protocol.py) cover granularity, identities, causal clocks, invalid payloads, gaps, disconnects, book staleness and historical/shared feature parity. [Consumer tests](../../tests/test_live_public_signal.py) use synthetic models to verify all-head prediction equality with the offline consumer, unchanged artifact hashes, authorization rejection, subscription/dispatch isolation, warmup and cancellation guards. Existing live/feed/model regressions remain part of qualification.

The current private live config, runtime pointer and remote service have not been switched. The real-feed probe also detected exchange publication timestamps ahead of local reception; clock alignment therefore remains an explicit qualification blocker, not permission to clamp timestamps or retrain. A candidate still needs exact private model/P3/config authorization, full quote/action and target-runtime latency qualification, then the existing [deployment transaction](aws_ec2_live.md) with fresh reconciliation and health admission. These local tests are not deployment or profitability evidence.
