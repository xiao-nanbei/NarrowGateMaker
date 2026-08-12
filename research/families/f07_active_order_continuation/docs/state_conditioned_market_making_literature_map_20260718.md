# State-Conditioned Market Making Literature Map

Date: 2026-07-18

> Current status (2026-07-27): the literature mapping and state-conditioned research direction remain useful. The causal-v4 numerical examples are historical and withdrawn under the normalized-L2 and time/unit repairs. The maintained model identity is `causal-v7` with inference disabled; current execution-probability work is the separate placement/active-order CIF line, whose first placement Development result is diagnostic-only after failing absolute calibration.

## Decision

The current evidence does not prove that every fixed parameter is universally useless. Fixed safety limits, exchange constraints, and a neutral quote coordinate system remain necessary. What the project has rejected is the idea that one global `gamma`, `kappa`, cooldown, spread cap, one-tick widening rule, or one-cycle skip rule can supply the missing fill-selection edge across all states.

The historical causal-v4 experiments originally sharpened that conclusion; their exact values are no longer current evidence:

- BUY add `baseline` versus `widen_1tick` had valid 50/50 support and almost no activity drift, but the learned chronological action rule had negative reward uplift and a confidence interval crossing zero;
- SELL add `baseline` versus `skip_one_cycle` also had valid 50/50 support, but the skipped quote would have filled only 3.52% of the time, so the action was usually too weak to change the campaign path;
- both families failed in Development, so locked Validation and holdout dates were not read.

The next generation should therefore estimate the value of an existing order and its queue position, then choose a state-dependent action with enough treatment strength to change the execution path.

## What Is Already in the Project

### Active structural foundations

| Literature | Project use | Exact boundary |
|---|---|---|
| Avellaneda and Stoikov, *High-frequency trading in a limit order book* | Reservation price, inventory-aware quote center, spread-risk coordinate system | A neutral quote skeleton, not a source of fill-selection alpha |
| Guéant, Lehalle, and Fernandez-Tapia, *Dealing with the Inventory Risk* | GLFT-style fill-intensity and inventory-constrained quote-distance ideas | Current production research uses an empirical P3 artifact and effective kappa; it does not rely on legacy fixed `kappa=0.073` |
| Guéant, *Optimal market making* | General state-dependent intensity functions and quote approximations | Supports estimating `Lambda(side, state, distance)`; it does not identify one globally optimal kappa |
| Cartea, Jaimungal, and Penalva | Short-horizon alpha, adverse selection, inventory and execution costs | The project has the state and labels, but stable action uplift is not yet established |
| Stoikov, *The Micro-Price* | Book-conditioned fair-value direction | Current `microprice_from_book` is a size-weighted BBO/top-N proxy, not the paper's learned limiting expected-mid estimator |
| Cont, Kukanov, and Stoikov, *The Price Impact of Order Book Events* | OFI, depth-normalized local pressure, depletion/refill features | Existing features are ingredients; they are not yet a calibrated order-value model |
| Easley, Lopez de Prado, and O'Hara | Volume-time toxicity/VPIN proxy | The code intentionally implements a proxy, not an exact reproduction of the original estimator |
| Zhao and Linetsky, *High Frequency Automated Market Making Algorithms with Adverse Selection Risk Control via Reinforcement Learning* | Book Exhaustion Rate feature and spread-defense concept | BER is implemented/configurable; the paper's RL policy is not the current project policy, and the paper does not validate NarrowGate's thresholds |

Relevant code:

- `strategy/quote_core.py`
- `features/feature_engineer.py`
- `strategy/signal.py`
- `models/audit/p3_touch_calibration.py`

### Implemented research and shadow layers

| Literature | Project use | Current status |
|---|---|---|
| Huang, Lehalle, and Rosenbaum, queue-reactive LOB | Exact-L2 replay, queue-state features and state-dependent event interpretation | Simulator/calibration foundation exists; event intensities are not yet fitted as a joint queue-reactive model |
| Jusselin, *Optimal market making with persistent order flow* | Hawkes-style post-fill excitation and decaying add-side defense | Implemented in `post_fill_quote_response.py`, but tested quote-response families were rejected and remain off |
| Albers et al., *The Market Maker's Dilemma* | Treat easy fills as potentially toxic; evaluate fill probability jointly with post-fill return | Directly consistent with the project's random-null and maker-signed markout evidence |
| Albers et al., *Fragmentation, Price Formation, and Cross-Impact in Bitcoin Markets* | Receive-time external BBO/trade tape, venue consensus and latency-stressed global-flow state | Infrastructure exists; current external state is diagnostic/shadow, not promoted quote alpha |
| Bennett and Kallus; Kallus and Uehara | Propensity, overlap, action-specific Q, IPS/SNIPS/DR and chronological OPE | Contextual OPE is implemented; full efficient policy-parameter estimation and sequential DR are not |

Relevant code:

- `strategy/global_flow.py`
- `strategy/global_reference.py`
- `strategy/post_fill_quote_response.py`
- `models/audit/offline_policy_evaluation.py`
- `models/audit/state_conditioned_policy_artifact.py`

### Analogy only

These references must not be presented as direct parameter derivations:

- Milionis et al. LVR is an AMM/CFMM stale-price-loss result. It may motivate volatility sensitivity, but it does not prove a CLOB `vol_power` exponent.
- Gatheral and Oomen concerns realized-variance estimation under microstructure noise; it is not the source of the size-weighted microprice helper.
- reaction-diffusion, Le Chatelier, and response-kernel discussions motivate shock/refill/recovery features, but do not prove that a local shock will revert.
- Hawkes persistence supports a state-dependent decay clock; it does not derive fixed 41-second or 85-second cooldowns.

### Retired or non-active directions

- RL and Transformer execution paths have been removed or are not active.
- direct quote-EV tighten/widen/pause execution has been retired; quote EV remains useful as a label/model/shadow object.
- maker-taker hybrid and general-intensity variants that failed project gates are historical evidence, not current policy.
- the fixed BUY one-tick widen and SELL one-cycle skip families are closed on their frozen causal-v4 identities.

## The Main Missing Mechanism

The project currently estimates many state labels:

\[
P(\text{fill}\mid x),\quad
E[\text{markout}\mid \text{fill},x],\quad
P(\text{repair}\mid x),\quad
P(\text{tail}\mid x).
\]

The missing object is the value of a specific live order and action:

\[
V(x,q,a)
=
P(\text{fill before adverse move}\mid x,q,a)
\cdot E[\text{fill value}\mid x,q,a]
-C_{\text{campaign}}(x,a)
-C_{\text{latency/reset}}(x,q,a)
+V_{\text{queue optionality}}(x,q,a).
\]

Here `q` is queue position and queue-ahead state. This term matters because canceling an order destroys a real option: the order may later become valuable without paying the queue-reset and gateway-latency costs again.

Moallemi and Yuan explicitly decompose queue-position value into:

1. a static trade-off between spread capture and adverse selection at fill;
2. a dynamic option value from retaining priority for future states.

This is the most important mechanism currently missing from NarrowGate's action learner.

## Literature-Driven Optimizations

### 1. Fit a true local order-value state

Build `local_order_value_panel_v1` for each active order. At each predeclared decision point, record:

- exact price level, queue ahead, estimated queue rank and order age;
- market-order consumption, cancellation and refill intensities;
- multi-level OFI, spread state, depth and event type;
- empirical microprice innovation and adverse-move hitting probability;
- cancel/new ACK latency and probability of a fill while cancel is pending;
- inventory role and campaign state available at the decision time.

Targets should be competing events:

- favorable fill;
- adverse fill;
- cancel/depletion before fill;
- mid-price transition;
- campaign repair or censoring.

This is stronger than predicting future return because it models the exact race the passive order participates in.

Primary sources:

- [A Model for Queue Position Valuation in a Limit Order Book](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2996221)
- [Limit Order Strategic Placement with Adverse Selection Risk and the Role of Latency](https://arxiv.org/abs/1610.00261)
- [Fill Probabilities in a Limit Order Book with State-Dependent Stochastic Order Flows](https://arxiv.org/abs/2403.02572)

Time-to-fill should be represented as a right-censored survival target rather than a binary fill in an arbitrary window. Deep survival models are a possible later benchmark, but the first production candidate should remain a calibrated low-capacity hazard model whose failure modes can be audited.

### 2. Replace global event rates with state-dependent intensities

Estimate separate limit, cancel, and market-order intensities by:

\[
(\text{side},\text{spread},\text{queue imbalance},\text{distance},
\text{event history},\text{volatility regime}).
\]

A queue-reactive Hawkes model is a suitable first model:

- queue state explains the current event-rate surface;
- Hawkes excitation explains event clustering and decay;
- side-specific kernels replace fixed cooldown clocks;
- the fitted intensities feed both replay calibration and order-value estimation.

The first version should be shallow and auditable: piecewise-constant queue states plus exponential kernels, not a neural point process.

Primary sources:

- [Simulating and analyzing order book data: The queue-reactive model](https://arxiv.org/abs/1312.0563)
- [State-dependent Hawkes processes and their application to limit order book modelling](https://arxiv.org/abs/1809.08060)
- [Optimal market making with persistent order flow](https://arxiv.org/abs/2003.05958)

### 3. Learn an empirical microprice instead of using weighted mid

The current top-N weighted price is a useful feature, but Stoikov's microprice is a learned conditional expected future mid. Fit a causal transition model using:

- spread and queue-imbalance state;
- multi-level OFI;
- event type and recent event sequence;
- depletion/refill/recovery;
- a fixed event-time or hitting-time target.

Use the result as a bounded fair-value correction and adverse-move hazard, not as an unconstrained direction bet.

Primary sources:

- [The Micro-Price: A High Frequency Estimator of Future Prices](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2970694)
- [The Price Impact of Order Book Events](https://arxiv.org/abs/1011.6402)

### 4. Test an action with real treatment strength

The next family should not be another fixed quote-distance grid. Freeze:

```text
family: queue_value_keep_cancel_v1
surface: already-resting exposure-increasing order
unit: at most one intervention per campaign
actions:
  K0 = keep current order and queue position
  K1 = cancel, then block re-entry until the adverse state exits
reducing side / size / inventory limit: unchanged
external reference: excluded in v1
```

`K1` is intentionally stronger than skipping one 5-10 second quote cycle. Its re-entry condition is a state transition, not a new fixed number of seconds. The replay must include cancel-pending fills, queue reset, empirical gateway latency and subsequent campaign evolution.

Only after this binary family has support should `replace_at_new_price` become a third action:

\[
V_{\text{replace}}
>
V_{\text{keep}}
+C_{\text{queue reset}}
+C_{\text{latency}}.
\]

### 5. Make baseline fallback part of the policy definition

The current DR/OPE layer should add a safe-improvement rule:

- candidate action is allowed only where action support and ESS pass;
- its day-clustered lower confidence bound must be positive;
- unsupported or high-uncertainty states copy the rolling baseline;
- candidate-rate and campaign-tail budgets remain explicit constraints.

This follows the baseline-bootstrapping principle rather than asking one model to act everywhere.

Primary sources:

- [Safe Policy Improvement with Baseline Bootstrapping](https://proceedings.mlr.press/v97/laroche19a.html)
- [Efficient Policy Learning from Surrogate-Loss Classification Reductions](https://proceedings.mlr.press/v119/bennett20a.html)

### 6. Upgrade campaign evaluation from a contextual row to a stateful process

One intervention per campaign is the right first causal unit, but campaign management is sequential. Once a single action family passes:

- define decision epochs at fill, material queue-state transition, shock transition and repair threshold;
- assign incremental rewards between epochs;
- model repair, trend-through and day-end censoring as competing risks;
- use stateful/sequential OPE rather than copying terminal MTM to many rows.

Primary sources:

- [Stateful Offline Contextual Policy Evaluation and Learning](https://proceedings.mlr.press/v151/kallus22a)
- [Estimating heterogeneous treatment effects with right-censored data via causal survival forests](https://arxiv.org/abs/2001.09887)

### 7. Add external venues only as incremental order-value information

Bitget, Bybit and OKX should not be averaged into a permanent superior price. Research shows that Bitcoin leadership changes, and Binance itself is often a primary source of price discovery.

For a later `M1` extension, add receive-time external features only to the same frozen local order-value model:

- external spot/perp OFI and trade pressure;
- 2-of-3 venue agreement and dispersion;
- dynamic leader confidence;
- Binance-local unabsorbed residual;
- captured/p95/p99 latency stress and leave-one-venue-out results.

The required test is:

\[
V_{\text{order}}^{M1}(x_{\text{local}},x_{\text{external}})
-
V_{\text{order}}^{M0}(x_{\text{local}}),
\]

not whether external returns alone sort a 30-second markout.

Primary sources:

- [Fragmentation, Price Formation, and Cross-Impact in Bitcoin Markets](https://arxiv.org/abs/2108.09750)
- [Where is the Price of Bitcoin Determined?](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4983566)

### 8. Optimize for regime robustness, not pooled historical mean

Days, sessions, volatility states and venue-leadership states are different environments. A policy should maximize a conservative mixture or lower bound, not the pooled sample winner.

After the ordinary DR estimator is stable, add:

- per-regime policy value;
- worst-mixture or distributionally robust value;
- explicit baseline fallback under covariate shift;
- no promotion when the improvement comes from one regime only.

Primary source:

- [Doubly Robust Distributionally Robust Off-Policy Evaluation and Learning](https://proceedings.mlr.press/v162/kallus22a)

## Recommended Order of Work

1. Build `local_order_value_panel_v1` with exact queue and competing-event labels.
2. Fit a side-specific queue-reactive Hawkes/intensity artifact and an empirical microprice transition artifact on chronological Development.
3. Verify that they improve fill/adverse/cancel hazard calibration over current P3 plus static queue calibration.
4. Freeze and replay `queue_value_keep_cancel_v1` with known 50/50 propensity.
5. Evaluate DR uplift, lower confidence bound, campaign tail, fill retention and queue-reset attribution.
6. Add SPIBB-style baseline fallback.
7. Only after local `M0` passes, add external `M1` receive-time flow and run leave-one-venue-out plus latency stress.
8. Only after one single-decision family passes should campaign control become a sequential/stateful policy.

## Stop Rules

Close the family without retuning locked evidence when:

- the action changes fewer than a predeclared fraction of fills or campaigns;
- conditional uplift is positive only in-sample;
- queue-value calibration does not beat a static queue baseline;
- reward improves while repair/trend-through or tail outcomes worsen;
- the candidate needs unsupported regression extrapolation;
- the lower confidence bound is non-positive;
- external improvement disappears under one-venue removal or realistic receive-time latency.

The immediate research target is therefore not another optimal fixed number. It is a calibrated, side-specific estimate of the value of retaining versus abandoning a live queue position.
