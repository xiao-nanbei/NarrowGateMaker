# F05 Offline Sequential Replay Input V2 Contract

Last materially modified: 2026-08-13

Status: `frozen_pre_economics`.

This contract repairs the mechanics boundary that blocked the first offline panel. It creates a new `offline_sequential_panel_v2` identity and a new ORICO-backed artifact root; it does not modify the previously admitted 30-day panel, its blocked receipt, or either existing annotated tag. The selected 30 dates, 3,516 opportunities, and exact opportunity-ID set must remain unchanged. Any denominator drift requires a new admission and stops the formal path.

## Field Sources

The 23 formerly missing fields are not treated as 23 missing datasets. Seven are deterministic projections of the existing assignment or contract, twelve bind existing canonical sources or frozen replay parameters, and three require a fresh outcome-blind B0 assignment mechanics run: `campaign_id`, `campaign_cluster_id`, and `assignment_equity_usdc`. No action economics are read to produce them.

`exposure_fill_ordinal` and `order_id` are parsed from the already frozen `cooldown-v2` assignment, fill-event, and replay-client-order identities and must agree across all three strings. `d_plus_1_utc_day`, the three continuation booleans, and formal replay parallelism are frozen contract values. Formal day replay uses six workers; the first diagnostic mechanics materialization uses one worker. Worker count is an execution resource parameter and cannot affect policy semantics.

Every SHA field must rehash a real bound source or a canonical receipt that recursively binds real source bytes. A lowercase 64-character string is not sufficient evidence. In particular, `day_input_sha256` is the canonical day-projection receipt, the portable binding contains all 30 day projections, and its source paths are portable placeholders resolved through the governed data-path contract. `d_plus_1_native_observation_sha256` is the admitted D+1 cache observation identity; the separate observation-receipt SHA verifies that identity and must never be substituted for it. The D+1 market identity binds the source-day receipt, including individual and aggregate trades, together with normalized BBO/L2 and native observation identities.

## Observation End

`observation_end_ts_ns` is frozen as the start of D+2 UTC, which is the end of the common D+1 administrative observation interval. It is common to every action for a target-day opportunity, contains no realized arm outcome, and is used only as a conservative purge bound. It does not assert that an order, campaign, or arm terminated at that time and does not force terminalization.

Actual arm termination and common washout are later replay outputs named `actual_arm_terminal_ts_ns` and `actual_common_washout_ts_ns`. If common washout is not observed before the canonical source boundary, the replay row remains right-censored; the mechanics panel never fills that uncertainty with an invented terminal time.

## Evidence Boundary

The new panel remains exchange-time, modeled-queue historical Development evidence. It has no receive-time, strict queue, live lifecycle, Validation, sealed-holdout, action, or live authority. The current owner policy remains the exact B0, no F05 companion or shadow is permitted, and no EC2 operation is part of this contract.

The machine-readable field-by-field producer, granularity, type, clock, nullability, and validation rules are frozen in [the JSON contract](causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_sequential_replay_input_v2_contract_20260813.json).
