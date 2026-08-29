# Cross-Venue Reference to Alpha Roadmap - 2026-07-11

Last materially modified: 2026-08-02

Status: The retained111 one-second result is frozen historical evidence. The receive-time M0/M1 workstream remains infrastructure/diagnostic-only and has no live-action authority.

> Security boundary: Bitget, Bybit, and OKX reference books/trades use public channels and NarrowGate stores no external-venue API keys. The market-data upgrade is receive-time BBO/L2 reconstruction, not WebSocket authentication.

## Current Decision

The trades-derived causal one-second Stage 0 is now frozen. No further one-second threshold sweep is part of this roadmap. Its result is retained as a second-scale diagnostic baseline, not as evidence against event-driven cross-venue information.

The three-venue architecture remains valid as research infrastructure:

- Bitget, Bybit, and OKX are independent venue inputs;
- spot and perpetual are separate factors;
- 2-of-3 consensus, median aggregation, outlier rejection, and leave-one-out are the first robust baseline;
- Binance BTCUSDT perpetual is a local level bridge, not an external vote;
- Binance `USDCUSDT` spot converts the bridge into USDC terms;
- BTCUSDC spot is a cross-check/fallback.

Stage 0 did not show enough short-horizon, daily, leave-one-out, campaign, and repair stability to promote global residual into re-center, cancel, widen, size, or lifecycle policy. External markets therefore remain diagnostic and shadow-only.

The next Stage 0 is a different experiment: public WebSocket BBO/trades at local receive time, event-driven L1 OFI/depletion/refill proxies, and 10/25/50/100/250/500ms maker-signed fill markouts. It targets fill toxicity, not a one-second future-price forecast.

The long-run authority order remains intact, but evidence production is not a single blocking queue. The current BABEL route is:

```text
P1 receive-time first-add M0/M1 -- background collection to 30 distinct days
                                  \
                                   -> same exact quote surface -> randomized action
                                  /
E6/P2 adverse-edge mechanics ----- outcome-blind work can run now
```

The 30-day gate blocks P1 fitting only. BABEL-P2 may inspect clock survival, support, LOO agreement and quote mechanics without reward. If P2 covers every exposure-increasing quote rather than exact first-add, it requires a new value- prediction denominator before the two branches can merge.

`BABEL-P2` here is the adverse quote-edge mechanics layer. It is distinct from the older roadmap label "Phase 4 / P2: Post-Fill Campaign Moderator" retained below as historical design context.

`fill_toxicity_incremental_v1` is implemented as an observational M0/M1 audit. M0 uses causal Binance execution/bridge flow, order context, and campaign state; M1 adds separately reconstructed Bitget/Bybit/OKX spot/perpetual consensus. It reports BUY/SELL and opener/add/reducing targets, chronological folds with embargo, explicitly labelled blocked-day cross-fit, family-specific later-panel results, latency mode, and true leave-one-venue-out recomputation. It does not change quote actions.

The current corrected strategy control is the causal-v7, 13-head-ML-disabled baseline described in `time_unit_contract_repair_20260726.md`. The historical BTCUSDT level bridge has migrated from CryptoHFTData BBO to causal one-second bars built from official Binance individual trades; live continues to use the Binance WebSocket book ticker. Neither change reopens the frozen retained111 Stage 0 result.

## Workstream Boundary

Phase 0 and experiment metadata are system-nature work. They prove event-time integrity, receive-time observability, latency capability, and reproducibility; they do not prove alpha.

Phases 1-5 are strategy-nature work. They require retained daily evidence, chronological walk-forward, side/inventory-role labels, campaign outcomes, and late holdout. A lower latency or smaller log gap cannot promote a strategy arm.

## Governance Prerequisite

Chronological splitting and the experiment registry are prerequisites for all new strategy experiments, not cleanup steps at the end.

Every experiment must record:

```text
experiment_id
code_commit
config_hash
dataset_manifest_hash
feature_schema_version
model/label version
train interval
embargo interval
validation interval
late holdout interval
arm/action definition
metrics and artifact paths
```

The existing hypothesis registry remains useful, but it must not substitute for immutable dataset/config/code identity.

The canonical implementation is `models/audit/experiment_manifest.py`. A spec produces a write-once `experiment_manifest.json` containing the current commit, tracked dirty-patch hash, untracked-file hashes, config and dataset manifest hashes, feature/model/label versions, exact split definitions, action definitions, metrics, and artifact hashes. With `--checkpoint-dir`, it also writes a restorable code bundle:

```text
checkpoint.json
tracked.patch
untracked_files.tar
```

The bundle is based on one Git commit and can reconstruct the dirty research workspace without pretending the dirty tree was a clean commit. Existing manifests and checkpoint directories are never overwritten. Use `--verify` to detect later code, config, dataset, input, or artifact drift.

## Phase 0: Receive-Time Data Layer

### Objective

Determine whether an external signal survives long enough to be observed, computed, and acted on. Historical causal 1s trades can sort second-scale outcomes but cannot establish 10-100ms execution alpha.

### Unified Tape Schema

Every event uses `market_tape.v1`:

```text
market_id
transport
event_type
exchange_event_ts_ns
local_receive_ts_ns
feature_ready_ts_ns
transport_lag_ms
feature_latency_us
sequence_number
previous_sequence_number
gap_flag
bid / bid_size
ask / ask_size
price / size / aggressor_side
```

`gap_flag=null` means the transport does not promise contiguous sequence numbers. A REST snapshot id jump must not be mislabeled as a feed gap.

### Required Markets

- Binance BTCUSDC perpetual;
- Binance BTCUSDT perpetual;
- Binance BTCUSDT/BTCUSDC/USDCUSDT spot anchors where used;
- Bitget BTCUSDT spot/perpetual;
- Bybit BTCUSDT spot/perpetual;
- OKX BTCUSDT spot/perpetual.

### Current Implementation

- external recorder rows are normalized to `market_tape.v1`;
- Binance callbacks capture receive time before decode/routing and feature-ready time after SignalEngine consumption;
- Binance depth sequence gaps are recorded only when the stream supplies a comparable previous-update id;
- recorder writes asynchronously and reports written/dropped/invalid counts in HEALTH;
- receive-time files are losslessly streamed as `.jsonl.gz`; audit readers accept both legacy plain JSONL and gzip;
- Bitget spot/perpetual use public v3 `books1 + publicTrade`, Bybit uses public `orderbook.1 + publicTrade`, and OKX uses public `bbo-tbt + trades`;
- `strategy/global_flow.py` maintains receive-time 10/25/50/100/250/500ms aggressive-flow, L1 OFI, depletion, refill, agreement, and local/global-gap state from `feature_ready_ts_ns`;
- `models/audit/fill_toxicity.py` samples that state at fill time and labels BUY/SELL maker-signed markout at the same 10-500ms horizons;
- `models/audit/market_data_latency.py` freezes environment-labeled 3600-second visibility distributions and supports captured, exchange-zero, empirical, and p50/p95/p99/p99.9/max replay modes;
- `models.audit.runner --reports receive_time_tape` produces per-market/event/ transport latency, cadence, gap, and leader-survival diagnostics.

### Capability Warning

The promoted shadow capture uses public WebSocket for all six external spot/perpetual sources and records every BBO/trade callback. REST remains only for bootstrap, recovery, and a deliberately slower comparison source. Older REST and 100ms-throttled files keep their original capability labels.

Therefore a 10/25/50ms row is admissible only when the empirical event cadence and transport support that horizon. REST polling and 100ms-throttled tapes may appear in latency tables but cannot vote on sub-100ms leader survival.

An EC2 zero-key preflight received BBO and trades from all six sources with no connector error or reconnect. End-of-window transport lag was roughly 5-13ms for Bitget, 28-30ms for OKX, and 38-40ms for Bybit. These are operational observations, not stable latency guarantees or alpha evidence. Full live capture reported zero dropped/invalid rows and `globalFlow100Valid=1`.

Lossless gzip reduced the observed write projection from about 5.9GB/day to about 0.54GB/day without time aggregation. Capture remains shadow-only and must still be monitored for CPU, disk, gaps, and queue drops.

Latency profiles are not portable strategy parameters. A profile must include the cloud region, instance type, vCPU/memory, OS/kernel, compute profile, gateway mode, transport, measurement timestamps, and source/event group. `captured` evidence already contains live visibility delay and must not receive another injected profile. Profile modes start from exchange timestamps and are for archive/replay counterfactuals. Apparent one-way delay remains exchange-clock-sensitive.

Host-bound receive-time profiles and their measurements are owner-private and are not distributed. Public research code may consume an explicitly supplied private profile, but it must fail closed when that dependency is unavailable; the interface alone grants no action authority.

The legacy 2026-07-11 event files are not an admissible tape: independent `gzip -t` validation failed for six of seven files. They are quarantined and must not be used by fill-toxicity or leader-survival evidence. A new bounded AWS Tokyo capture ran from `2026-07-12T05:51:53Z` for 3,600 seconds with an unchanged strategy hash. Recording was disabled after the window, all seven unique-session gzip files passed independent CRC validation, and the admitted payload is `161,753,276` bytes. The latest in-window HEALTH row reported `2,993,611` Binance writes with zero dropped/invalid rows and zero dropped external rows. Binance queue high-watermark reached `9,272/20,000` with a `2,695ms` maximum queue age; external recording reached HWM `441` and `354.3ms` maximum age. Those burst tails are capture-overhead diagnostics rather than a normal live latency baseline. This paragraph records the first admitted capture; it is not the current ledger count.

One valid hour can verify schema, causal merge, latency modes, LOO rebuilding, and M0/M1 wiring. It cannot satisfy chronological or late-panel denominators. An insufficient-data result from this first window is therefore an infrastructure smoke, not evidence for or against cross-venue alpha.

The collection ledger uses a separate receive-time universe. It must not reuse the Binance local-replay good-day manifest or the historical three-venue Stage 0 manifest. The default incremental runner needs 20 chronological training days, one embargo day, chronological test days, and four late days. That gives an absolute floor of 26 distinct valid UTC capture days and a 30-day floor for a complete five-day test block. Side and inventory-role row minimums may push the effective requirement higher. Until those day and row denominators pass, daily bounded captures update only the integrity/latency ledger; M0/M1 remains frozen and no policy arm is created.

As of the last completed 2026-07-29 ledger check, receive-time evidence contains `16` valid full-window captures over `15` distinct UTC days. Two captures belong to 2026-07-21 and therefore count as one statistical UTC day. The 2026-07-29 background capture was not yet admitted when this snapshot was written. This remains below the predeclared `30`-day complete denominator, so the collection process may extend the integrity ledger but M0/M1 must not be run or used to create an action arm yet.

That paragraph is a dated historical snapshot. The current 2026-08-02 ledger contains `19` valid full-window captures over `18` distinct UTC days. This is still below P1's frozen 30-day denominator. Outcome-blind E6/BABEL-P2 mechanics have since completed under a separate, clock-limited identity; they did not read reward or alter P1 eligibility.

The registered successor is now `first_add_external_incremental_value_m0_m1_v1`. Its M0 is campaign state plus local causal microstructure; M1 adds Bitget/Bybit/OKX state under the recorded AWS receive and feature-ready clock. Its target is F10's direct decision-to-campaign-terminal USDC value, not future price direction. The old `maker_lifecycle_screen.v1` targets and the historical direction screen are not valid substitutes for this identity.

The local historical data universes answer a separate question. The minimal raw-complete audit has `141` days, `84` BTCUSDC native-sequence-valid days, `76` days satisfying the historical 99% whole-day formal gate, and `46` Grade-A days satisfying the additional five-second maximum-gap budget required by continuous queue/order/campaign studies. Broader dates may be used only for pointwise or segmented diagnostics with explicit masks and lifecycle censoring. The independent-venue historical Stage 0 universe remains the frozen retained111 panel. None of these denominators may be merged into one generic "good day" count.

That wiring smoke loaded and labelled all `13` maker fills under captured, profile-p95, and profile-p99 visibility, and matched order context for all `39/39` mode-expanded rows. The latency stress is active: nonzero global-flow pressure at fill time fell from `8 -> 5 -> 2` rows at 10ms and from `10 -> 7 -> 3` rows at 25ms for captured/p95/p99. All 13 rows retained a valid global-flow snapshot because validity also covers fresh zero-flow state. With only one UTC day, M0/M1 produced zero eligible chronological metric rows and the canonical status `insufficient_chronological_data`; no prediction or policy conclusion was drawn.

### Phase 0 Outputs

```text
*.receive_time_latency.csv
*.receive_time_leader_survival.csv
*.fills.csv
*.sorting.csv
```

Required horizons:

```text
10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s
```

The first leader-survival table is deliberately single-source and diagnostic. It measures whether an external-local pending move remains unabsorbed or is followed by the Binance bridge. It is not a 2-of-3 alpha result and not a cancel/re-center counterfactual.

### Phase 0 Exit Gate

- no recorder drops during the selected window;
- feature-ready timestamp is never earlier than local receive timestamp;
- negative transport lag is reported as clock-quality evidence, not clipped;
- sequence/gap semantics are transport-specific and documented;
- empirical cadence supports the claimed horizon;
- leader survival exceeds total feed + feature + decision + gateway latency at p95 and remains visible under p99 stress.

## Phase 1: Global Flow Reference

### Naming Rule

Trade-only features are `aggressive_flow_pressure` or `trade_imbalance`. They must not be called OFI.

True order-flow imbalance/depth depletion requires book events with auditable size changes and sequence continuity. Top-of-book snapshots can support L1 depletion/refill proxies; full depth OFI requires exact depth updates.

### Features

Per venue/instrument:

```text
aggressive_buy_volume_{50ms,100ms,250ms,1s}
aggressive_sell_volume_{50ms,100ms,250ms,1s}
trade_imbalance
L1 bid/ask depletion
L1 refill
book pressure
source freshness and gap state
```

Cross-venue state:

```text
global_buy_pressure
global_sell_pressure
global_spot_flow
global_perp_flow
venue_agreement
venue_dispersion
local_global_flow_gap
leader and leader_confidence
```

The output target is not “BTC goes up/down.” It is whether the current local passive fill is likely to be informed/toxic or locally absorbable.

## Phase 2: Side-Specific Fill/Campaign Targets

Models are split by side and inventory role:

```text
BUY / SELL
opener / add / reducing
```

Targets:

- 5s/20s/30s maker-signed markout;
- probability of extreme adverse fill;
- tail-before-repair probability;
- campaign terminal PnL and repair;
- incremental campaign inventory/MAE cost.

Feature families:

```text
local exact-L2/queue/microprice/refill
local aggressive flow
inventory and campaign state available at quote time
global flow pressure
spot/perp divergence
venue agreement/dispersion/leader
global residual and stablecoin basis
```

Required comparison:

```text
M0 = local-only causal state
M1 = M0 + external global state
```

External data has incremental information only if M1 improves chronological walk-forward and family-specific sealed evidence, survives leave-one-venue-out and latency stress, and improves side/campaign labels rather than only future mid.

## Phase 3: Action Uplift

Observational outcome differences are not action uplift. Keep/cancel, add/no add, and re-center/unchanged require overlap and a causal counterfactual from:

- replay with queue/latency/order-lifecycle parity;
- randomized shadow exposure where safe;
- or a predeclared propensity/uplift design with adequate overlap.

### Keep vs Cancel

Estimate:

```text
V_keep - V_cancel
```

Include future fill value, queue value, cancel success/latency, false cancel, replace churn, and campaign effect. Submit-time opportunity markout alone is not a cancel counterfactual.

### Add vs No Add

For exposure-increasing fills, estimate incremental terminal campaign value:

```text
terminal_PnL(add) - terminal_PnL(no_add)
```

Reducing fills remain separate because blocking them can destroy natural repair.

### Re-center vs Unchanged

The first admissible action remains a bounded one-tick reservation shift with unchanged size/inventory limit. Evaluate side fill value, fill retention, queue loss, campaign terminal outcome, and tail.

## Phase 4: Priority Mechanisms

### P1: Local Shock Without Global Confirmation

This is the first strategy hypothesis:

```text
strong Binance aggressive flow
+ weak external spot/perp confirmation
+ local depth refill
+ microprice recovery
-> local liquidity event / absorptive fill candidate
```

The initial policy question is whether existing guards over-widen or cancel these orders. Do not immediately tighten or increase size.

### P2: Post-Fill Campaign Moderator

After a fill:

```text
external adverse consensus
+ inventory adverse direction
+ falling repair probability
-> higher campaign outcome risk
```

Candidate shadow actions are conditional stop-add, small inventory-side skew, or repair urgency. This path has a longer usable horizon than 50ms cancel and directly targets current campaign-tail risk.

## Phase 5: Existing Policy Attribution

### Adverse/Defense Guards

For each blocked side decision, delayed replay labels must report:

```text
blocked opportunity count
would-fill count under the calibrated queue/latency model
would-have-been 5s/20s/30s markout
true toxic block rate
positive-fill false-block rate
campaign impact
```

This remains a replay counterfactual, not observed exchange truth.

### BUY Fill-Selection Score

Run score buckets low/mid/high with only small policies:

```text
baseline
soft keep / prevent extra widening
soft widen
```

Do not tighten or increase size until action-level uplift, campaign terminal, tail, and late holdout pass.

## Promotion Order

```text
data integrity
-> receive-time/latency capability
-> prediction evidence and outcome-blind action mechanics may develop in parallel
-> merge only on one exact opportunity/quote surface
-> action counterfactual/uplift
-> daily walk-forward + late holdout
-> tiny shadow arm
-> replay parity
-> live shadow counter
-> limited live policy
```

No global reference state currently skips any of these gates.

## Current Authority Links

- `minimal_marketdata_good_day_reaudit_20260727.md`: local source contracts, per-day grades, and exact continuous-lifecycle denominator;
- `btcusdt_trade_bridge_migration_20260727.md`: official-trade historical bridge and retirement of BTCUSDT CryptoHFTData as a current dependency;
- `time_unit_contract_repair_20260726.md`: corrected causal-v7 baseline and disabled 13-head runtime inference;
- `global_reference_stage0_retained111_20260711.md`: frozen historical one-second Stage 0 evidence.
