# Quote units and semantics

Last materially modified: 2026-09-19

The controller is AS-shaped and empirical, not an AS/GLFT optimum. Classify literature relationships as exact derivation, adapted proxy, analogy or archived research. Citations do not prove action value.

Trace actual callers in `strategy/quote_core.py`, `strategy/maker_engine.py` and `strategy/signal.py`, including price/base/notional units, order size, risk horizons and final orders.

- Inventory: `n=q/q_ref` uses matching base units.
- Center: `fair - n * eta_inventory_eff * sigma_sq_per_s * inventory_risk_horizon_s`.
- Empirical pair spread: `a_spread * sigma_sq_per_s * quote_horizon_s + (2/a_spread) * log(1+a_spread/kappa_spread)`.
- Legacy gamma compatibility is not a portable CARA coefficient. Do not transfer parameters across size/capital/markets without an explicit experiment.
- P3 is fixed-horizon same-side-BBO touch probability. Preserve horizon, distance unit, side scope, training support, origin and identity. `effective_kappa` is local `-d log(P_touch)/d distance`, not arrival intensity, fill hazard or GLFT kappa. A pair-spread floor `2*delta_star` does not ensure each final side has that distance after skew/caps/rounding.
- Touch is not fill. Queue, arrival, post-only, cancel races and fill eligibility need separate assumptions/tests.
- Use truthful weighted-mid proxy, clock-volume imbalance and trade-intensity-burst guard names. Legacy microprice/VPIN/BER ABI names do not establish theoretical estimators.
- Forecast origin/horizon/conditioning must match its consumer. Fill-conditioned returns are not automatically decision-time alpha; metadata cannot enable incompatible skew.
- Base-quantity and notional/loss/drawdown hard fuses are distinct. Unit refactors preserve final quotes/safety; new scaling/floors/guards are behavioral candidates.

Parity inspects actual Python/native/replay paths and emitted activation, not flags alone. Preserve existing freshness, reducing protection and order-state behavior unless explicitly in scope.

Quote-audit output gives applicable equations, units/clocks, actual consumers, action propagation and tests. Candidate economics uses the selected research method, not universal DR requirements.
