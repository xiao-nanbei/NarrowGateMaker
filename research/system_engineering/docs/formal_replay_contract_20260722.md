# Formal Replay Contract v1

## Purpose

Strategy evidence and live alignment now use separate replay identities.

- `formal`: compares baseline and candidate arms under a frozen, causal replay contract. It requires `--strict-calibration` and is the only purpose that can be promotion-eligible.
- `live_alignment`: diagnoses unit conversion, event clocks, state-machine transitions, and gate ordering. It does not require campaign, fill, or PnL identity with one historical live window.

The split prevents a same-day live warm start, paired receive-time trace, or historical process stall from becoming an implicit strategy parameter.

## Initial State

Formal replay defaults to UTC daily `fresh_start`:

- inventory = 0;
- entry price = 0;
- no inherited active orders, cooldown counters, markout EMA, or campaign state.

An experiment may instead use `frozen_standard`, but must name a checked-in or archived `narrowgate_standard_initial_state.v1` JSON artifact. The artifact may contain only inventory and entry price. Full live active-order state remains a Python-only `live_alignment` input.

## Frozen Identity

`models/replay_contract.py` writes a canonical JSON contract and SHA256 covering:

- live config file;
- active model directory and BUY fill-selection artifact;
- empirical P3/effective-kappa artifact and horizon;
- queue-v3 artifact, fit days, and final runtime queue multipliers;
- execution trade source, merged event clock, historical BBO requirement, and feature-ready-before-decision semantics;
- initial-state mode and artifact;
- latency environment, distribution, seeds, scenario, and sample hashes.

Every arm inherits the same contract. An arm that changes the model, P3, queue calibration, event clock, initial state, latency samples, or latency seed fails before replay. Strategy action overrides remain free to vary.

## Latency Semantics

The baseline latency distribution carries an environment/version label, for example `aws_tokyo_ec2_2vcpu_4g_amazon_linux_20260718_v1`. A machine migration requires a new profile identity and a new replay contract.

The stable baseline clips the frozen sample arrays at the declared quantile. Rare synthetic stalls are enabled only by `--latency-scenario stress`; stress artifacts are explicitly not promotion-eligible.

Python and C++ use `keyed_splitmix64_v1`. New/cancel latency is a deterministic function of:

```text
latency_seed, event timestamp, originating quote timestamp, side, operation
```

It is not a sequential random-number stream. Therefore an extra cancel in one arm does not shift every later latency draw, and common decisions continue to use a common random path. The same sampler, rounding, stress draw, and parameters are covered by Python/C++ parity tests.

## Causal Event Contract

Formal evidence requires:

- individual execution trades when the frozen data identity declares them;
- `replay_event_clock=merged` with a positive timer interval;
- historical BBO/L2 visible only at or before the decision boundary;
- model features available only when `feature_ready_ts <= decision_ts`;
- maker-close circuit-breaker behavior;
- terminal equity as `cash + inventory * terminal_mark`, without a synthetic end-of-window taker close.

Paired live receive-time observations, empirical live requote clocks, nonzero source-time offsets, and full live warm starts are rejected by `formal` and must use `live_alignment`.

## Example

```bash
python3 models/campaign_outcome_replay_audit.py \
  --config /path/to/frozen-live.yaml \
  --strict-calibration \
  --replay-purpose formal \
  --initial-state-mode fresh_start \
  --latency-profile-id aws_tokyo_ec2_2vcpu_4g_amazon_linux_20260718_v1 \
  --latency-environment aws_tokyo_ec2_2vcpu_4g_amazon_linux \
  --latency-scenario baseline \
  --rng-seed 42 \
  --latency-seed 59 \
  --engine cpp
```

The runner writes `*.replay_contract.json` beside the daily/rollup artifacts. The contract hash is also present in every Python and C++ replay result.

## Interpretation

Live alignment is successful when it explains or catches structural errors in units, clocks, state transitions, or gate order. Residual differences caused by unobservable queue priority, asynchronous ACK/fill ordering, inherited campaign state, or random network stalls do not require one-to-one campaign or PnL replication. Strategy comparisons use the frozen formal baseline instead.
