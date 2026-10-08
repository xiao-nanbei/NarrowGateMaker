# Trade–book response action value: completed research, separate live admission

[English](response_baseline.md) | [简体中文](response_baseline.zh-CN.md)

Last materially modified: 2026-10-08

Last materially synchronized: 2026-10-08

## Identity and completed economics

`B0_RESPONSE_20261007` is the subsequent research reference selected from the frozen `TRADE_BOOK_RESPONSE_VALUE` candidate. The original B0, candidate and `RESPONSE_HISTORY_ABLATION` results retain their original names, inputs and accounting. Promotion neither overwrites those records nor activates trading. This response study is separate from the historical side-flow/hazard studies indexed by [F08](README.md) and the evaluation-timing study [F07 U0/U1](../f07_active_order_continuation/README.md).

The completed coverage is 204 independent accounts: 150 two-day Development accounts and 54 Final accounts (53 two-day accounts plus one day), totaling 407 days. The candidate's 54 Final results were reused exactly once, not rerun as a reproduction test. This is not a continuous 407-day account. All full results were collected into the local private archive; model, authorized source overlay, compute calibration, input identity, account boundaries, terminal cut and accounting bindings passed closure checks. Research archival remains local; runtime storage admission is a separate contract.

The following figures were recomputed from all 204 locally retained complete account ledgers, not from the earlier 169-account progress snapshot. Ratios divide aggregate net PnL by aggregate executed BTC or USDC turnover; they do not average account ratios.

| Panel | Accounts / days | Net PnL (USDC) | Net USDC / BTC | Net USDC / 10,000 USDC turnover |
| --- | ---: | ---: | ---: | ---: |
| Development | 150 / 300 | -1414.486011 | -6.518970 | -0.716277 |
| Final, reused once | 54 / 107 | -188.545171 | -4.222452 | -0.616426 |
| Total | 204 / 407 | -1603.031182 | -6.127022 | -0.702886 |

Total executed volume is 261.633 BTC and turnover is 22,806,427.3226 USDC. These are modeled all-in account economics, including fees, actual funding and terminal inventory MTM. All totals remain negative. A less negative value is a smaller loss, not profitability. Development includes training dates; Final evaluation and promotion have previous use. The model artifact's `final_used=false` describes fitting scope only and does not erase later Final evaluation. Neither the full calendar nor reference selection establishes a fresh out-of-sample or online causal result.

## Model, action and clocks

The frozen standardized Ridge model consumes 47 features and predicts modeled action-label value, not full-account PnL. The information ablation keeps 39 features, removing four recovery-history quantities and their four missing indicators; it does not remove all trade history. Missing recovery anchors have explicit missing encoding, whereas unavailable necessary market/context inputs cause fallback. See the shared [feature contract](../../../strategy/response_action_features.py) and [inference consumer](../../../strategy/response_action_value.py).

The model queries the actual old order price, not a replacement touch-price history. On a qualified optional update, a positive score chooses UPDATE, a negative score KEEP and zero retains the original price decision. Unsupported inputs retain the original decision. Thus the original 15-tick price condition is not an absolute lower bound on every candidate update. Age, pending state, quantity, inventory, hard risk and forced-action rules remain independent constraints. An intent is not a submitted request, accepted cancellation or fill.

Research windows use strategy-visible ready clocks. Live consumes observed receive/commit clocks; timestamps must not be shifted after the fact to force agreement. The offline response-compute budget belongs to its measured simulation contract; live must not add an artificial sleep for the same cost. `legacy_v12` versus `execution_v1` is neither proof of disagreement nor proof of equivalence: actual consumers, values and clocks must be compared.

## Existing live path and evidence boundary

The implementation already routes execution-market individual trade messages through `WSHandler` to `LivePublicSignalEngine`, updates `LiveResponseHistory`, binds the response sequence and depth generation to the quote snapshot, queries an old-price frame, scores the frozen model and enters the original optional replacement-price gate, OrderManager and request lifecycle. Ordinary `SignalEngine.on_agg_trade` is not this route. Aggregate messages are not split into invented individual trades. Artifact bytes, feature order, scale, roles and target scope are checked; changing model path or identity requires restart.

The response candidate requires the supported Python order-planning path. Leave `NARROWGATE_CPP_LIVE_ROUTING`, `NARROWGATE_CPP_ORDER_ACTION_PLAN` and `NARROWGATE_CPP_FINAL_ORDER_PLAN` disabled; incompatible selections are deliberately rejected. This is not a prohibition on every native component. Other native consumers require their own existing compatibility checks.

Code existence, synthetic checks and production admission are different evidence layers. Existing tests cover input parsing/history, score-to-price intent, order routing, original guard preservation and restart-only configuration. They are not, by themselves, one complete raw-input-to-exchange lifecycle proof. Required acceptance must compare the same event record, visible watermark, depth version and order identity through all 47 raw features/missing flags, standardized inputs, score and KEEP/UPDATE. Quote context must come from the real prediction/quote consumer, not manually filled constants. A fake gateway may replace the network boundary, but not features or the order state machine. Cancel/fill races, private ACK before HTTP, unknown outcomes, rejection, terminal continuation, changing order identity and disconnect/re-warmup require explicit coverage.

Snapshot sequence and depth-generation rejection prevents future state from leaking into an older decision. Do not remove it to improve coverage, substitute touch history or relax the approximately one-second book freshness and ten-second response warmup. Measure qualification, market support, old-price coverage, version agreement, scoring, intent change, request and downstream blocking on the same opportunity; marginal counts cannot be multiplied or added to infer their intersection.

Actual USD-M BTCUSDC subscription delivery/reconnection, deployment-environment compatibility, journal/storage mount admission and restart reconciliation still need separately authorized evidence. No exchange collector, testnet order, real order, AWS change or deployment readiness is asserted here. Research closure cannot satisfy a live storage or execution gate.

## Offline integration acceptance in this source revision

[Network-free integration tests](../../../tests/test_live_response_integration.py) now construct the real MakerEngine, LivePublicSignalEngine, quote consumer, OrderManager, inventory callbacks and durable cooldown store. Raw-shaped synthetic trade/depth messages reach the actual quote context and response scorer; a controllable gateway is the network boundary. They cover HTTP cancellation, private cancellation before HTTP, partial fill during cancellation, cancel refusal/unknown result, terminal continuation with a newly computed quote, private NEW before HTTP, rejected/unknown NEW and reconnect warmup. A separate same-visible-record check compares all 47 extracted features, missing flags, normalization, score and intent against the reference response-state implementation. It does not replace the model with a mocked scorer or fill context with manual constants.

The published revision fixes floating-point rejection of an exactly one-tick movement and rejects response-model admission for a stale captured order object that is not the active order. These are local live-adapter changes, not retrospective changes to frozen replay evidence. Existing pending, age, quantity and risk restrictions remain tested. The focused offline suite passed 295 tests; no hosted CI or exchange test is claimed.

These fixtures use synthetic models and messages. They do not prove all 47 features equivalent on recorded production inputs with the frozen production models. Production same-opportunity joint coverage rates (including downstream requests and blocks), actual subscription behavior, storage admission and restart reconciliation remain unmeasured; existing marginal counters are not their substitute. Keep these gaps explicit rather than describing the candidate as deployable.

## Availability and reproduction scope

Source and tests are public; purchased events, frozen model weights, complete parameters, account ledgers, run identities and operational locators are retained in the private evidence store and are not distributed with the public repository. Private evidence availability does not mean the economic experiment is incomplete. Reproduction requires the admitted private inputs and original frozen identity; this documentation update does not rerun economics or relabel historical evidence. The subsequent [F01](../f01_fixed_parameter_racing/README.md) experiments retain separate frozen identities and admissions.
