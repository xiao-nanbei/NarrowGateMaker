# Formal Replay Contract Guide

[English](formal_replay_contract_20260722.md) | [简体中文](formal_replay_contract_20260722.zh-CN.md)

Original guide date: 2026-07-22

Last materially modified: 2026-09-06

Last materially synchronized: 2026-09-06

Status: Maintained reader guide to the current implementation; not an immutable run manifest. [`models/replay_contract.py`](../../../models/replay_contract.py) currently emits `narrowgate_formal_replay_contract.v4`. Historical v1 executions retain their original source and artifact identities.

## Purpose

Strategy evidence and live alignment now use separate replay identities.

- `formal`: compares baseline and candidate arms under a frozen, causal replay contract. It requires `--strict-calibration` and is the only purpose that can be promotion-eligible.
- `live_alignment`: diagnoses unit conversion, event clocks, state-machine transitions, and gate ordering. It does not require campaign, fill, or PnL identity with one historical live window.
- `diagnostic`: evaluates explicitly labelled runtime or modeling assumptions without promotion eligibility. In particular, the current measured REST async GLOBAL FIFO path uses Python with this purpose; it must not be relabelled as formal or assumed supported by the complete C++ scheduler.

The split prevents a same-day live warm start, paired receive-time trace, or historical process stall from becoming an implicit strategy parameter.

## Initial State

Formal replay defaults to UTC daily `fresh_start`:

- inventory = 0;
- entry price = 0;
- no inherited active orders, cooldown counters, markout EMA, or campaign state.

An experiment may instead use `frozen_standard`, but must name a checked-in or archived `narrowgate_standard_initial_state.v1` JSON artifact. The artifact may contain only inventory and entry price. The separate partial diagnostic restore interface is not a complete live checkpoint: [`replay_state_checkpoint.py`](../../../models/replay/replay_state_checkpoint.py) rejects canonical live snapshots whose required runtime domains it cannot restore, and C++ rejects additional nonempty order/control domains. Selecting `live_alignment` does not make those unsupported domains restorable.

## Frozen Identity

`models/replay_contract.py` writes a canonical JSON contract and SHA256 covering:

- live config file;
- active model directory and BUY fill-selection artifact;
- empirical P3/effective-kappa artifact and horizon;
- queue-v3 artifact, fit days, and final runtime queue multipliers;
- execution trade source, merged event clock, historical BBO requirement, and feature-ready-before-decision semantics;
- initial-state mode and artifact;
- hard-risk limits, fee rates, and exchange-filter declarations, with explicit modeling limitations;
- latency environment, distribution, seeds, scenario, and sample hashes.

Every arm inherits the same contract. An arm that changes the model, P3, queue calibration, event clock, initial state, latency samples, or latency seed fails before replay. Strategy action overrides remain free to vary.

## Latency Semantics

The baseline latency distribution carries an operator-defined environment/version label. A machine migration requires a new profile identity and a new replay contract; concrete host identities are private and not distributed.

The current default `--latency-baseline-clip-quantile 1.0` preserves the complete observed sample arrays, including their tails. A lower quantile explicitly selects a trimmed sensitivity or an exact historical reproduction; it is not the current stable-environment default. The measured async GLOBAL FIFO input requires `1.0`. Rare synthetic stalls are added only by `--latency-scenario stress`; stress artifacts are explicitly not promotion-eligible. Preserving an observed tail does not prove that a short sample estimates p99 accurately.

The shared Python/C++ new/cancel sampler uses `keyed_splitmix64_v1`. Its latency is a deterministic function of:

```text
latency_seed, event timestamp, originating quote timestamp, side, operation
```

It is not a sequential random-number stream. Therefore an extra cancel in one arm does not shift every later latency draw, and common decisions continue to use a common random path. Sampler parity covers that shared mechanism, not every runtime option or an entire asynchronous scheduler. Baseline and candidate retain independent order, queue, fill, and inventory state.

## Causal Event Contract

Formal evidence requires:

- individual execution trades when the frozen data identity declares them;
- `replay_event_clock=merged` with a positive timer interval;
- historical BBO/L2 visible only at or before the decision boundary;
- model features available only when `feature_ready_ts <= decision_ts`;
- maker-close circuit-breaker behavior;
- terminal equity as `cash + inventory * terminal_mark`, without a synthetic end-of-window taker close.

Paired live receive-time observations, empirical live requote clocks, nonzero source-time offsets, and full live warm-start requests are rejected by `formal`. Supported partial alignment inputs use `live_alignment`; measured async timing uses the explicit `diagnostic` route described above. Do not bypass an unsupported backend or clock by changing only its evidence label. Snapshot age is not a per-message delivery delay and must not be substituted for one.

## Example

From a source checkout with Python 3.11+ and `.[research]`, first inspect the current runner without reading market data:

```bash
python -m research.families.f01_fixed_parameter_racing.campaign_outcome_replay_audit --help
```

The following is a command template, not a runnable public-data fixture. Replace the config, day, and environment placeholders with the study's frozen inputs; the public live template is not a calibrated baseline:

```bash
python -m research.families.f01_fixed_parameter_racing.campaign_outcome_replay_audit \
  --config "${NARROWGATE_PRIVATE_CONFIG_ROOT}/<frozen-replay-config>.yaml" \
  --days YYYY-MM-DD \
  --strict-calibration \
  --replay-purpose formal \
  --initial-state-mode fresh_start \
  --latency-profile-id "<operator-defined-profile-id>" \
  --latency-environment "<operator-defined-environment-id>" \
  --latency-scenario baseline \
  --latency-baseline-clip-quantile 1.0 \
  --rng-seed 42 \
  --latency-seed 59 \
  --engine python
```

The runner writes `*.replay_contract.json` beside the daily/rollup artifacts. The contract hash is also present in the replay results. Use `--engine cpp` only when the selected policy and runtime options are implemented and covered by the corresponding Python/C++ parity checks; shared-kernel test success alone is insufficient.

## Interpretation

F01 campaign attribution uses the simulated execution price (`quote_px`), not
the triggering public trade price (`fill_trade_px`). It includes signed
commission, preserves physical `fill_sequence`, and splits cross-zero fills
into closing/opening economic legs with proportional fees. Opening fees belong
to the new campaign. A completed-window mark values residual inventory without
creating a liquidation; open campaigns remain open. Physical fill counts and
economic-leg counts are different quantities.

These campaign paths are marked at observed fills and at the final window mark,
not continuously between every market event. Their path extrema are therefore
fill-marked diagnostics. The current runner does not book funding or operating
costs, and `economic_pnl_complete` describes its local fill-ledger coverage, not
all-in account economics. Summing independent fresh-start days is not continuous
deployment PnL. New action-value labels must use a shared absolute endpoint and
the actual cost/state coverage required by the study, not silently inherit old
campaign labels after an attribution correction. Old result files are preserved;
regenerated attribution is a new derived result, not a new execution history.

Live alignment is successful when it explains or catches structural errors in units, clocks, state transitions, or gate order. Residual differences caused by unobservable queue priority, asynchronous ACK/fill ordering, inherited campaign state, or random network stalls do not require one-to-one campaign or PnL replication. Strategy comparisons use the frozen formal baseline instead.
