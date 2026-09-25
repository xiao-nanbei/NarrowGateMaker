# Quote units and semantics

Last materially modified: 2026-09-25

The controller is AS-shaped and empirical, not an AS/GLFT optimum. Classify literature relationships as exact derivation, adapted proxy, analogy or archived research. Citations do not prove action value.

Trace actual callers in `strategy/quote_core.py`, `strategy/maker_engine.py` and `strategy/signal.py`, including price/base/notional units, order size, risk horizons and final orders.

- Inventory: `n=q/q_ref` uses matching base units.
- Center: `fair - n * eta_inventory_eff * sigma_sq_per_s * risk_horizon_s`.
- Empirical pair spread before dynamic multipliers: `risk_per_order * sigma_sq_per_s * risk_horizon_s + (2/risk_per_order) * log(1+risk_per_order/kappa_spread)`. Trace the effective distance slope into `kappa_spread`; do not substitute a touch-curve slope or time hazard solely because units resemble one another.
- Quote coefficients and risk horizon are explicit inputs. There is no current gamma input or inheritance from kappa, ber_spread_mult or quote_horizon_s. Historical coefficient conversion belongs outside the application. Do not transfer parameters across size/capital/markets without an explicit experiment.
- P3 is fixed-horizon same-side-BBO touch probability. Preserve horizon, distance unit, side scope, training support, origin and identity. `touch_log_probability_distance_slope` is local `-d log(P_touch)/d distance`, not arrival intensity, fill hazard or GLFT kappa. `distance_touch_product_argmax` maximizes distance times touch probability, not account net profit. A pair-spread floor twice this distance does not ensure each final side has that distance after skew/caps/rounding.
- Touch is not fill. Queue, arrival, post-only, cancel races and fill eligibility need separate assumptions/tests.
- Use truthful weighted-mid proxy, clock-volume imbalance and trade-intensity-burst guard names. Retired business aliases must fail, not silently reinterpret another estimator; external supplier protocol parsing remains separate.
- Forecast origin/horizon/conditioning must match its consumer. Fill-conditioned returns are not automatically decision-time alpha; metadata cannot enable incompatible skew.
- Base-quantity and notional/loss/drawdown hard fuses are distinct. Unit refactors preserve final quotes/safety; new scaling/floors/guards are behavioral candidates.

Parity inspects actual Python/native/replay paths and emitted activation, not flags alone. Preserve existing freshness, reducing protection and order-state behavior unless explicitly in scope.

Quote-audit output gives applicable equations, units/clocks, actual consumers, action propagation and tests. Candidate economics uses the selected research method, not universal DR requirements.
