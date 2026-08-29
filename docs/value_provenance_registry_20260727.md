# NarrowGate Value Provenance Registry

Date: 2026-07-27

Last materially modified: 2026-08-29

Machine-readable registry: `docs/value_provenance_registry_20260727.json`

Status: historical provenance snapshot. It does not grant current live authority. Current operational authority is owner-private, `private_not_distributed`, and must fail closed when its bytes are unavailable or unverifiable.

## Why this exists

The project previously mixed several different kinds of statements:

1. mathematical identities and theoretical model forms;
2. quantities estimated from a frozen dataset or live environment;
3. engineering and research-governance choices;
4. exchange and account rules.

Those classes are not interchangeable. In particular, a theoretical optimum such as calibration intercept `0` does not imply an admissible finite-sample tolerance of `0.01`, and an empirically selected `85s` cooldown is not a Hawkes half-life implied by theory.

## Classification rules

| Class | Meaning | What can change it |
|---|---|---|
| Theory or identity | Formula, unit identity, or theoretical ideal | A different estimand or model family |
| Empirical direct estimate | Measured or fitted directly from frozen observations, without choosing a strategy action or operating value | Dataset, split, code, artifact, venue regime, or machine identity |
| Empirical policy selection | Chosen among candidate parameters, thresholds, artifacts, or policy values using observed outcomes | Policy family, search space, selection panel, code, data, or environment identity |
| Judgmental engineering | Safety, support, regularization, governance, or operating choice | A newly frozen engineering/research contract |
| Exchange/account rule | Tick, lot, filters, fee tier, order semantics | Venue/account metadata |
| Hybrid | A theoretical form populated by empirical inputs and bounded by engineering choices | Any component above |

The direct/selection split is operational. P3 slopes, latency distributions, OOF metrics, and observed calibration bias are direct estimates. `gamma`, a cooldown selected by replay, or a score threshold selected from candidate policies are policy selections. A selected value must not be described as if the market uniquely fitted it.

The maintained quote core is an **AS-shaped empirical quote controller**, not an exact or approximately optimal Avellaneda--Stoikov/GLFT implementation. Current interpretation separates four literature relationships: `exact derivation` for an explicitly reproduced mathematical object, `adapted proxy` when estimand or data semantics changed, `analogy` for design motivation only, and `archived research` for removed or closed routes. A citation never upgrades an empirical coefficient, threshold, proxy, or action into a theoretical result.

In particular, the P3 artifact estimates a fixed-horizon touch opportunity measured from the same-side BBO. Its legacy `effective_kappa` field is `-d log(P_touch)/d distance`; it excludes queue-ahead and touch-to-fill conversion and is not GLFT order-arrival intensity. Likewise, legacy prose names map as follows: `microprice` is a top-N-size weighted-mid proxy, wall-clock `vpin_*` is clock-volume imbalance, and `ber_*` is a trade-intensity-burst guard rather than a book-exhaustion estimator.

The `q_ref` / `eta_inventory` / `a_spread` unit split is admitted as a behavior-preserving B0 migration only when final bid/ask, the historical P3 pair-spread floor, post-only correction, and tick rounding are identical. A quantity-aware finite-order-size spread, a true per-side same-side-BBO floor, an H5/H10 risk horizon, or a variance-time cooldown changes the economic path and remains a candidate without economic, action, or live authority.

## Historical live-baseline snapshot

The table below records the state understood on 2026-07-27. Its values and `live_authority` flags are retained for provenance only and must not be used to resolve the current runtime. Later operational identities, host bindings, release state, and action enablement are owner-private and are not distributed here. Consumers must verify the private authority explicitly and fail closed when it is absent.

| Item | Historical value/status | Classification | Important limitation |
|---|---:|---|---|
| AS-shaped empirical quote controller | active | Hybrid | AS supplies a comparison shape; the implemented pair spread, P3/depth adapters, and regime multipliers are empirical and are not an AS/GLFT optimum |
| legacy `gamma` | `0.046` | Empirical policy selection | Retained/Sobol selection and compatibility input, not a universal CARA optimum; current unit contracts expose `q_ref`, `z`, `eta_inventory`, and `a_spread` separately |
| effective P3 `delta_star` | `13.9991 USDC/BTC` | Empirical direct estimate | About 140 ticks, not 13.999 ticks |
| effective P3 `kappa_eff` | `0.067356 (USDC/BTC)^-1` | Empirical direct estimate | Fixed-horizon, same-side-BBO local touch-probability slope; not arrival intensity, fill hazard, or queue conversion |
| legacy `kappa=0.073` | fallback only | Empirical policy selection | Not the current effective kappa while override is zero |
| quote horizon | `1s` | Judgmental engineering | Controller risk-integration horizon, not an observed order lifetime or the P3 touch horizon; value is not theory-derived |
| order size `z` | `0.001 BTC` | Judgmental risk budget | Must satisfy exchange filters; it is not present in the legacy spread logarithm and therefore prevents interpreting that expression as a complete finite-order-size GLFT derivation |
| maximum inventory | `0.026 BTC` | Hybrid | Independent base-asset hard fuse alongside separate USDC hard limits; not a unified scale-invariant risk coordinate |
| requote interval | `5s` | Hybrid | Historical operating choice and churn budget |
| add-side fill cooldown | `85s` | Empirical policy selection | No paper or theorem implies 85 seconds |
| total pair-spread threshold | `20 bps` | Hybrid | Historical inward-compression trigger, not a risk-safety guarantee |
| BUY fill-selection threshold | `0.44 score` | Empirical policy selection | A ranking score, not absolute fill probability |
| BUY active-order hazard | frozen adverse-value `q90` cancel/re-enter | Hybrid | Separate from the new placement-fill CIF |
| 13-head ML | disabled | Engineering runtime state | Empirical P3 remains active |
| tick / lot | `0.1 / 0.001` | Exchange rule | Refresh from symbol filters |
| maker / taker fee | `0 / 0.00036` | Account rule | Fee asset conversion remains part of accounting |

Later owner-side evidence may refer to BUY E3 or the SELL owner cooldown. Those are **owner-authorized live risk experiments**, not research-hard-gate passes or validated optima. Their exact enablement, parameters, release identity, and current state are private operational facts and cannot be inferred from this historical public registry.

Fixed base-asset quantity and fixed USDC notional, loss, and drawdown limits remain independent hard fuses, with the stricter applicable constraint binding. They do not jointly scale with equity, BTC price, volatility, fill frequency, or exposure time. An equity/volatility-aware replacement is a separate candidate and cannot silently inherit safety or live authority.

## Placement fill probability

The current full-curve estimand is:

\[
P(T_{fill}\le t,T_{fill}<T_{cancelACK}\mid do(a),x_0)
=
P(A_a\mid do(a),x_0)
P(T_{fill}\le t,T_{fill}<T_{cancelACK}\mid A_a,do(a),x_0).
\]

It is fitted as one complete side-specific discrete-time curve. The former

\[
BUY/SELL \times opener/add/reducing \times 1s/5s/10s.
\]

18-cell layout is retained only for historical `fixed_horizon_v1-v4` reports. It no longer defines model targets or promotion gates. Current report points are the Development active-lifetime p25/p50/p75: 5.010s, 5.816s, and 7.900s.

### What was theoretical

- Perfect logit calibration has intercept `0` and slope `1`.
- Brier score is a proper probability scoring rule.
- Increasing placement distance should not increase fill CIF when lifecycle, activation, latency, and market path are paired.
- Prediction probability alone is not action value.

### What was arbitrary in v1

- `abs(calibration_intercept) <= 0.01` was a judgmental tolerance.
- It had no mapping to allowable USDC action-value error.
- It was much tighter than the observed day-level base-rate drift.
- The frozen v1 result is retained; it is not rewritten after seeing outcomes.

### Historical fixed_horizon_v4 qualification contract

`fixed_horizon_v4` (immutable family ID `placement_fill_cif_v4`) separates two permissions:

1. **Prediction-transfer shadow gate**: support, day-cluster Brier improvement, calibration slope, daily rank direction, and empirical drift envelope.
2. **Action-value gate**: probability error translated to USDC and evaluated together with conditional fill value, incremental campaign cost, and explicit queue/reset/churn cost under known action propensity.

Passing the first gate permits a frozen shadow surface. It does **not** permit tighten, widen, re-center, cancel, size changes, or a live policy.

The current shallow tree also has limited one-tick resolution: in Development, only about 23%-33% of paired cohorts receive a nonzero predicted probability difference between `closer_1tick` and `farther_1tick`, and the median difference is zero. This does not invalidate state-conditioned fill-risk ranking, but it prevents treating `fixed_horizon_v4` as a one-tick action optimizer. A later action model should use an explicitly continuous monotone distance term or a wider pre-registered distance grid and then pass action-value tests.

`fixed_horizon_v4` subsequently passed 13 of 18 cells on its frozen ten-day Validation panel. All cells retained positive Brier improvement and daily rank direction, but five cells failed calibration-slope transfer; two also failed zero-centered probability/O-E intervals. Validation therefore closed the family before the sealed holdout. The result is recorded in `research_06_placement_fill_cif/docs/placement_fill_cif_v4_validation_20260727.md` and does not authorize live inference or an action arm.

### Full-curve competing family

The 100ms grid is an engineering discretization aligned with the normalized L2 state resolution. The model fits fill and cancel-ACK cause-specific hazards and combines them through one survival function. Its Development p99 exposure, 34.5s, defines the current maximum fitted support.

The p25/p50/p75 report points are empirical estimates. The legacy 1s/5s/10s cuts and the eight-interval sampling budget are engineering choices. None is a natural horizon supplied by theory.

All six BUY/SELL-by-role curves have positive day-clustered integrated Brier lower bounds over p25/p50/p75. Time and action-distance monotonicity have zero violations above the frozen `1e-5` numerical tolerance. This is Development diagnostic evidence only: the OOF table does not yet expose separate cancel-ACK CIF calibration, no curve-level qualification gate was frozen, and Validation and sealed holdout remain unread.

`full_curve_competing_v4` (immutable family ID `placement_fill_full_curve_competing_cif_v4`) subsequently exported the cancel-ACK OOF CIF and applied a pre-frozen curve-level gate. All identities, support checks and proper-score improvements passed, and all fill-CIF calibration intervals included zero. Cancel-ACK calibration failed for BUY/SELL opener and reducing: integrated bias was about `-0.75` to `-1.11` percentage points with intervals wholly below zero. `full_curve_competing_v4` therefore closed on Development; Validation and sealed holdout remain unread. KEEP, REPLACE, and campaign repair retain independent risk origins and models.

`full_curve_role_calibrated_v5` then retained the exact `full_curve_competing_v4` gate and used trailing train-only, role-specific fill/cancel offsets inside every outer fold. It improved reducing calibration and centered SELL add, but worsened both opener curves; the same four curves still failed. This is evidence that static role offsets do not transport the baseline cancel policy clock, not permission to relax the gate.

`full_curve_nested_role_v6` implemented the stricter nested design: inner-expanding OOF interval scores, role-by-cause weighted Platt maps inside every outer train, and full outer-train base-model refits. All probability identities, support, and proper-score lower bounds passed, but all six cancel-ACK curves were underpredicted by roughly 3.4-4.4 percentage points. The calibration map did not transport from smaller inner models to the full outer refit. `full_curve_nested_role_v6` therefore closed on Development with Validation and sealed holdout unread.

`policy_clock_race_v1` then replaced the learned stationary cancel head with the frozen baseline cancel-request timestamp, a separately fitted past-only request-to-ACK distribution, and an explicit pending-cancel fill race. Exact request parity passed, all 12 marginal ACK-latency curves calibrated, and all six side-by-role curves had positive joint and fill Brier lower bounds against a mechanism-matched exposure-only fill baseline. The complete race still did not qualify: only three of six absolute fill curves calibrated, while all six integrated cancel-ACK curves were underpredicted by roughly 0.96-1.37 percentage points. `policy_clock_race_v1` is therefore closed on Development; Validation and sealed holdout remain unread. The result rejects this marginal side/role ACK-race implementation, not the placement fill estimand.

The machine registry preserves historical artifact and family IDs. Its `family_display_names` mapping supplies the unambiguous names above without renaming frozen files or invalidating their hashes.

## How a qualified fill surface enters strategy research

At each placement opportunity a future qualified scorer may produce:

```text
side: BUY or SELL
role: opener, add, or reducing
action: current, one tick closer, or one tick farther
query time: any supported t up to the action-specific exposure boundary
P(activation), fill CIF(t), validity, model/artifact identity
```

The policy layer must then estimate, separately for each action:

\[
V(a\mid x)=
P_{fill}(a\mid x)
E[net\ fill\ value\mid fill,a,x]
-\Delta C_{campaign}(a,x)
-C_{queue/reset/churn}(a,x).
\]

Maker-signed markout already contains execution-price value relative to future mid, so spread must not be added again. KEEP and REPLACE are not placement actions: they require a separate active-order continuation estimand because KEEP retains queue while REPLACE resets it.

## Governance

- Every empirical value must carry dataset, split, code, artifact, and when relevant machine/environment identity.
- A SHA proves only byte identity. It does not prove data correctness, parameter reasonableness, leakage freedom, economic value, live-process health, order ownership, or exchange reconciliation; each requires a separate gate.
- Every judgmental threshold must say what loss or failure it budgets.
- A threshold changed after observing a panel creates a new version; it cannot rewrite the old experiment.
- Prediction qualification, action uplift, and live promotion remain separate decisions.
- `live_authority=true` means that an entry currently applies to live operation; it does not mean this registry configures the process. Deployment must still verify runtime config, artifact hashes, environment identity, and exchange metadata.
