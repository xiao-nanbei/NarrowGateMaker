# P3 Aggressive-Reach Time Surface v1

Date: 2026-08-04

Last materially modified: 2026-08-12

Status: mechanics contract implemented; model identity and split not frozen.

## Decision

The fixed 10-second P3 estimand is historical. It remains unchanged in the operational v2 loader and calibration code so frozen artifacts remain exactly reproducible, but it is no longer the authoritative definition for new F02 research.

The successor estimand is the side-specific first-passage surface:

\[
F_{\mathrm{reach},s}(t,d\mid x)
=
P(T_{\mathrm{aggressive\ reach},s}(d)\le t\mid x).
\]

It describes aggressive market-price reach. It does not describe activation, queue conversion, fills, cancel ACK, inventory, campaign value, or PnL.

## Label Contract

The reusable label kernel records, for every explicit causal decision origin, the farthest BUY- and SELL-side aggressive-trade reach observed by each 100ms upper endpoint through 30 seconds. A distance/horizon label is derived from that one cumulative path.

- BUY-maker reach uses aggressive sells relative to the decision bid.
- SELL-maker reach uses aggressive buys relative to the decision ask.
- Label intervals are `(decision, decision + h]`; trades at the decision timestamp are excluded and trades at the right endpoint are included.
- First-passage times are 100ms interval upper bounds, not exact timestamps.
- No reach by 30 seconds is right censoring, not a negative lifetime outcome.
- Invalid BBO rows remain distinct from valid-but-censored rows.
- Prices and distances use integer ticks; float equality does not define a touch boundary.

The kernel accepts any strictly increasing set of causal decision origins. The formal model cadence is deliberately not frozen here. The one-day engineering smoke sampled canonical 10-second origins only to keep the test small; that does not authorize use at either canonical 10-second or 100ms live decisions.

## Parameter Provenance

`100ms` is the discrete label resolution inherited from normalized BBO state and the established F06 full-curve lifecycle infrastructure. It is not an order-lifetime claim.

`30s` is an administrative right-censor boundary. The owner-supplied EC2 360h lifecycle audit reports fill p95 of 18.802s and cancel p95 of 25.126s, but that audit is not yet bound to a machine-readable artifact and SHA256. A formal model Spec must bind that evidence and report beyond-censor sensitivity.

The initial `0.5-120 USDC/BTC` distance range follows historical v4.1 support and maps to 5-1,200 BTCUSDC ticks at the current `0.1` tick size. A future model identity must freeze its actual support before scoring predictions.

## Cache Boundary

The reusable artifact is:

```text
decision origin
  x side
  x 100ms time-bin upper endpoint
  -> cumulative maximum reached distance in integer ticks
```

It may be reused across supported distance and horizon queries. It must not contain orders, queue position, fills, cancel races, cooldown, inventory, campaign state, markout, reward, or PnL. Those are action-path state and belong to lifecycle replay.

Cache identity must bind source files, decision-origin manifest, tick contract, grid contract, and label-kernel hash. Changing any one invalidates the cache.

## Implementation Evidence

The kernel is implemented in [`p3_reach_time_surface.py`](../audit/p3_reach_time_surface.py); the static graph identity is `p3_aggressive_reach_time_surface.v1`. The historical `p3_touch_volatility_conditioned.v4` graph hash remains unchanged.

A read-only smoke on 2026-06-13 used 8,640 canonical origins. It admitted 8,628 valid BBO origins per side, produced 300 time bins, and had zero cumulative-reach monotonicity violations for BUY and SELL. Input paths, hashes, and row counts are bound in the JSON design artifact. No economic outcome was read and no persistent market-data cache was generated.

## Next Freeze

Before fitting, a successor Spec must choose one decision denominator:

1. canonical 10-second prediction origins; or
2. exact baseline-eligible quote decisions.

A canonical-10s fit cannot be transported to 100ms quote decisions without a separate decision-cadence audit. The model must use chronological OOF folds, day-clustered inference, nondecreasing probability in time, nonincreasing probability in distance, side/source transport diagnostics, and integrated Brier/calibration over the supported time-distance surface.

The eventual order estimand remains:

\[
P(T_{\mathrm{fill}}<T_{\mathrm{terminal}}^{\pi}(a)\mid x,a).
\]

F02 supplies reach-time information only. Action-conditioned replay supplies activation, queue, partial fills, cancel ACK, continuation, and terminal value. The product `P(touch) x average markout` remains prohibited.

## Permissions

This implementation grants no trained-model, prediction, quote-mapping, operational-artifact replacement, action, shadow, or live authority. The current operational P3 v2 remains unchanged.

## Public References

See the [family README](../README.md), [machine-readable design artifact](p3_aggressive_reach_time_surface_v1_design_20260804.json), [frozen successor Spec](p3_aggressive_reach_time_conditioned_hazard_v1_spec_20260804.md), and [Development report](p3_aggressive_reach_time_conditioned_hazard_v1_development_20260804.md). Input paths and retained cache bytes referenced by the machine artifact belong to the private evidence store and are not distributed with the public repository.
