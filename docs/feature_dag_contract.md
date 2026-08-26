# Feature DAG Contract

Last materially modified: 2026-08-26

## Status

NarrowGate uses static runtime implementations, not a dynamic graph executor. `features/feature_dag.py` is the canonical machine-readable source for graph shape and causal metadata that have been migrated so far.

The first registered graphs are:

- `live_10s_signal_cutoff.v1`
- `buy_q90_visibility_lifecycle_path_score.v3`
- `p3_touch_volatility_conditioned.v4`

Replay-window materialization is governed separately by `models/replay_cache_dag.py`. This separation is intentional: the Feature DAG defines causal feature dependencies, while the replay-cache DAG decides which strategy-independent nodes may be persisted and reused. See [`replay_cache_materialization_contract.md`](replay_cache_materialization_contract.md).

Their SHA256 identities are computed from canonical manifests at import time. Graph validation fails on duplicate nodes, missing dependencies, cycles, missing unit/clock/cadence metadata, and any direct feature or decision dependency on a label node.

## Ten-Second Cutoff

Every completed ten-second bucket uses one immutable boundary:

```text
cutoff_exclusive_ms = bucket_start_ms + 10_000
source_clock = exchange_time_ms
availability_clock = finalized_bar_time
```

Only finalized one-second bars with `bar.ts < cutoff_exclusive_ms` are visible. The bar exactly at the cutoff belongs to the next bucket. Python, warmup, and the persistent C++ engine use this same strict view. If several buckets become available between calls, they are processed once each in chronological order. An incomplete one-second grid fails rather than silently changing a bucket's denominator.

This fixes a prior target-time leak in which the first finalized bar from the next bucket had already entered Python rolling state and the persistent C++ engine before features for the previous bucket were computed.

## Q90 Graph

The registered order is:

```text
exchange book event
  -> causally visible book state
exchange order event
  -> order lifecycle + remaining quantity
  -> exchange-time quantity-weighted exposure (BTC*s)
  -> visibility-time quantity-weighted exposure (BTC*s)
book state + fill-risk lifecycle
  -> active-order depth path
  -> model feature vector
  -> adverse-fill score
```

Exchange time remains market truth. Receive/feature-ready time controls what the strategy can observe. An exchange-terminal order must leave the active fill-hazard risk set; post-cancel recovery is a separate state and cannot keep using a terminal order's old queue path.

The lifecycle authority is:

```text
SUBMITTED -> ACTIVE -> PARTIALLY_FILLED -> CANCEL_PENDING
          -> EXCHANGE_TERMINAL -> POST_CANCEL_RECOVERY
          -> REENTRY_ELIGIBLE
```

Only `ACTIVE`, `PARTIALLY_FILLED`, and `CANCEL_PENDING` accrue fill risk. Two exposure estimands are reported: exchange-time physical exposure and strategy-visible exposure. Missing, future, or regressed exchange timestamps invalidate the physical statistic rather than borrowing the visibility clock. Their reported difference `quantity_time_exposure_visibility_minus_exchange_btc_s` measures lifecycle information delay. First-fill latency is reported on both clocks as a separate statistic.

`POST_CANCEL_RECOVERY` is legal only after `cancel_ack` (including reconciled ACK) with positive remaining quantity. Full fill is terminal-complete and cannot request same-direction re-entry; reject/expiry returns to baseline resubmit routing; shutdown never creates re-entry. Unknown terminal reasons remain fail-closed at `EXCHANGE_TERMINAL`. The current q90 score consumes the active queue path at 100ms cadence; it does not yet consume BTC*s exposure as a model feature, and no prospective-placement recovery estimator is implemented after terminal.

## Label Boundary

Labels occupy the `label` namespace. A feature or decision node cannot depend on a label node. Offline code may construct labels from frozen feature/source state after feature materialization, but labels cannot be written back into a feature dependency path.

## Conditional P3 Graph

The research-only P3 graph makes the 10-second touch estimand explicit:

```text
causal BBO + official aggressive trades
  -> immutable 10s window start
  -> fast/slow past-only volatility and spread state
  -> raw distance + volatility-normalized distance
  -> conditional touch probability

future aggressive reach
  -> touch label namespace only
```

The touch label cannot feed the feature vector. Both raw distance and volatility-normalized distance carry decreasing structural constraints; source identity and calendar year are transport metadata rather than trading features. The graph emits a conditional probability curve, not a scalar kappa, queue fill probability, or quote action.

Its first Development identity remained fail-closed because three historical native days violated the frozen 98% context-coverage gate. The explicit v4.1 owner successor lowered only that gate to 95%; its minimum observed coverage was 96.85%, so historical Development prediction is supported. The override is outcome-informed and does not independently confirm the model or permit the graph output to replace the current P3 artifact. A separate full-path economic identity is still required before the conditional curve can affect quotes.

## Current Limit

The registry currently validates graph-level stages. The 88 ten-second base feature fields still have an existing explicit Python order, and the C++ overlay still has its checked-in enum/name table. They have parity tests but are not yet generated from one per-field registry. A later implementation identity may migrate each field's dependencies and unit, then generate Python column order, C++ identifiers, and model-manifest schema from that source.

Until that migration is complete, do not claim automatic dimensional propagation for all 88 fields.

Native logical order-book hours and component window caches are now reusable DAG nodes. Existing `tick_window_v13` pickles remain read-only compatibility artifacts; new misses publish a model-independent market-context component and a separately keyed model overlay, then assemble `WindowData` in memory. The remaining limit is that market context still groups trades/rolling arrays and BBO/L2 into one component rather than per-field feature nodes.

## Runtime Cadence Boundary

The current operational baseline runs the causal-v12 semantics-v6 13-head bundle. Its prediction node refreshes once per completed ten-second bucket and is sample-and-held between refreshes. Market events and lower-level feature state continue to update during that interval, but the 13-head output is not a 100ms order-lifecycle model.

The q90 subgraph evaluates active-order state at 100ms cadence. Its faster cadence does not grant post-ACK authority: after exchange terminal it must leave the old fill-risk graph and wait for a separately specified prospective placement/re-entry estimator. These two cadences therefore share lifecycle and visibility contracts without pretending to be one prediction horizon.
