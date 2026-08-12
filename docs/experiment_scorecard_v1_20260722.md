# NarrowGate Unified Experiment Scorecard v1

Last materially modified: 2026-08-12

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](public_private_documentation_contract.md).

## Purpose

`models.audit.experiment_scorecard` is the canonical scoring layer for new NarrowGate strategy research. It consumes paired daily evidence or causal action/OPE evidence after replay; it does not infer causality from raw PnL.

The scorecard answers two different questions in order:

1. **May this result be compared at all?** Identity, causal timing, overlap, support, tail, activity, and mechanism gates must pass.
2. **Among comparable survivors, which result is stronger?** A bounded, versioned weighted score ranks the survivors.

A large weighted score cannot compensate for a failed hard gate. Failed rows retain their component decomposition for diagnosis, but `ranking_score` is `null` and they cannot unlock the next evidence panel.

## Required Flow

```text
frozen experiment identity
        -> paired replay or randomized action panel
        -> chronological DR / paired-day evidence
        -> canonical score evidence
        -> validity and support gates
        -> value, tail, lifecycle, and mechanism gates
        -> weighted score for survivors only
        -> Validation
        -> sealed holdout
        -> shadow candidate, never automatic live promotion
```

Before any outcome is read, the family specification must include the exact output of:

```python
from models.audit.experiment_scorecard import score_profile_contract

family_spec["scorecard_profile"] = score_profile_contract("action_alpha_v1")
```

The contract contains a profile id and SHA256. Changing weights, scales, gates, or metric definitions requires a new profile id. Applying a profile after outcomes have been read is allowed only as retrospective diagnosis and never produces a rankable candidate.

## Constraint Layers

### Layer 0: identity and causality

Required:

- frozen family/action/evidence split before outcomes;
- code, config, data, model, P3, queue, latency, and baseline identities;
- feature-ready time no later than decision time;
- registered action propensity vector summing to one;
- one campaign-level reward attribution rather than repeated terminal PnL;
- no unregistered size, reducing-side, inventory-limit, or taker action.

### Layer 1: support and overlap

Action profiles require by default:

- at least 200 evaluated rows and 10 UTC days;
- policy ESS at least 100;
- minimum behavior propensity at least 0.05;
- unsupported candidate mass at most 5%;
- zero overlap violations.

Family-specific gates may be stricter, never looser after outcomes are read.

### Layer 2: non-compensable economic gates

Standard action profiles require:

- conditional net-value day-clustered lower bound greater than zero;
- reward positive on at least 55% of evaluated days;
- negative-terminal, q10-shortfall, and campaign-MAE lower bounds not negative;
- repair, repair-time, and censoring point estimates not negative;
- fills retention at least 85% (`action_execution_v1`: 90%);
- candidate action rate inside the frozen profile/family budget;
- all family-specific gates pass.

These gates deliberately reject a policy that appears safer only because it removes most activity.

Selective execution is the explicit exception. A policy may rationally trade less if the removed fills are disproportionately toxic. For this class, formal inference uses:

\[
S_T=(1-T_1/T_0)-(1-F_1/F_0)
\]

and:

\[
R_T=\log\frac{T_0/F_0}{T_1/F_1}.
\]

`action_execution_selective_v1` was the first conservative profile and kept an absolute 50% intervention-fill floor. The outcome-independent v2 contract removes that floor. `action_execution_selective_v2` allows any finite activity level only when reward, `S_T`, and `R_T` all have positive day-clustered lower bounds. A cancel-all policy has `S_T` approximately zero and still fails.

## Normalization

Every action metric is defined so that positive means improvement. For an economic metric (m), the score uses its day-clustered 2.5% lower bound:

\[
z_m = \operatorname{clip}\left(
\frac{LCB_{2.5\%}(m)}{s_m}, -1, 1
\right)
\]

The economic scale (s_m) is frozen in the profile, not estimated from the Validation or holdout result.

| Metric | v1 scale | Unit |
|---|---:|---|
| conditional net value | 0.02 | USDC / intervention campaign |
| negative-terminal protection | 0.02 | USDC / intervention campaign |
| development-q10 protection | 0.02 | USDC / intervention campaign |
| campaign MAE avoidance | 0.05 | USDC / intervention campaign |
| repair-event uplift | 0.01 | probability |
| repair-time avoidance | 300 | seconds |
| day-end censoring avoidance | 0.01 | probability |

Fills retention uses a bounded mechanism transform:

\[
z_{fills}=\operatorname{clip}
\left(\frac{retention-0.85}{1-0.85},-1,1\right)
\]

Thus 85% is zero score, unchanged activity is +1, and a severe fill collapse is -1. It remains a hard gate independently of this contribution.

The total is shrunk for a small number of independent days:

\[
S=\sqrt{\frac{n_{days}}{n_{days}+8}}
\sum_m w_m z_m
\]

Each metric is clipped, so no single outcome can buy the entire score.

## Built-in Profiles

| Profile | Conditional value | Tail | Lifecycle | Mechanism | Execution |
|---|---:|---:|---:|---:|---:|
| `action_alpha_v1` | 50% | 25% | 15% | 10% | 0% |
| `action_defense_v1` | 35% | 35% | 15% | 15% | 0% |
| `action_execution_v1` | 40% | 20% | 10% | 15% | 15% |

Selective execution profiles add a 25% selectivity component and reduce the other allocations accordingly:

| Profile | Value | Tail | Lifecycle | Mechanism | Execution | Selectivity |
|---|---:|---:|---:|---:|---:|---:|
| `action_execution_selective_v1` | 35% | 15% | 10% | 5% | 10% | 25% |
| `action_execution_selective_v2` | 35% | 15% | 10% | 5% | 10% | 25% |

The two versions have the same soft weights. They differ only in hard activity governance: v1 requires at least 50% retention for its selective override; v2 lets the two selectivity lower bounds and positive net value govern volume loss.

Within tail, the fixed split is 44% negative-terminal protection, 36% q10 protection, and 20% campaign MAE. Within lifecycle, repair, repair time, and censoring use a fixed 1/3, 40%, and 4/15 split. Execution families additionally must report queue-reset and latency-adjusted action value.

`paired_screen_v1` adapts the existing parameter racing selector to the same component output. It uses paired t-statistics and mechanism budgets, but is always `screening_rank_only`; it cannot promote an arm because screening rows do not by themselves provide action propensity or DR identification.

`paired_screen_v2` freezes a separate profile identity and is the sole ranking authority for new paired screens. Unlike v1, its `ranking_score` is populated after every support and mechanism gate passes. It remains screening-only and cannot unlock Validation, holdout, shadow or live. Panel transitions are owned by `panel_promotion_controller.py`; see `paired_screen_v2_architecture_20260727.md` for the migration contract.

## Weight Governance

Weights are not fitted to the best historical arm.

1. Calibrate a proposed profile only against already closed counterexamples, synthetic pass/fail fixtures, and explicit economic budgets.
2. Never use Validation or sealed holdout to change a profile.
3. Run a +/-20% sensitivity audit before creating a new profile version; a useful ranking should not reverse under small admissible changes.
4. Record the full component decomposition and profile hash in every result.
5. Do not include two metrics that count the same economics twice. In action OPE, terminal reward is the value contribution; campaign-cost evidence is a co-primary gate, not an additional weighted copy of terminal PnL.

## Probability Calibration Tolerance Governance

The scorecard's economic scales and a prediction family's probability calibration tolerances are separate contracts. For a logit calibration model,

\[
\operatorname{logit}(Y)=\alpha+\beta\operatorname{logit}(\hat p),
\]

perfect calibration has `alpha = 0` and `beta = 1`. Theory does not imply an admissible tolerance such as `abs(alpha) <= 0.01`. The `0.01` value frozen in `placement_fill_cif_v1_spec_20260726.json` and the earlier paired fill-surface spec is a judgmental, deliberately fail-closed engineering budget. It is not an empirically estimated standard and must not be described as one.

At low fill prevalence this budget is especially strict. Holding slope fixed, `alpha = 0.01` changes odds by only about 1%; at predicted probabilities of 0.3%, 3%, and 5%, the probability changes by approximately 0.003, 0.029, and 0.048 percentage points. Requiring every side-by-role-by-horizon cell to meet that point-estimate threshold therefore tests near-exact base-rate transfer, not merely useful ranking or lower Brier loss.

The frozen v1 threshold remains binding for v1. A failed v1 result may not be rescued by relaxing it after outcomes are read. A later profile must use a new versioned identity and derive cell-specific tolerances before its outcomes are opened from an explicit action-value error budget:

\[
|\Delta P_{\mathrm{fill},c}|\,
|E[\mathrm{net\ fill\ value}_c]|
\leq \epsilon_{\mathrm{action},c}.
\]

Any successor prediction contract must additionally freeze and report:

- probability-scale calibration-in-the-large and observed/expected fill ratio;
- day-clustered confidence intervals rather than intercept point estimates alone;
- separate side, inventory-role, and horizon tolerances when their economic values or support differ;
- any past-only rolling recalibration as a separately tested model component, fitted inside each chronological training fold; it is not mandatory when it worsens proper-score support or slope calibration;
- stability of the value ordering among `current`, `closer_1tick`, and `farther_1tick` under every probability perturbation allowed by the budget;
- minimum support and effective independent-day counts for every cell.

A ranking model can pass a ranking-only diagnostic while failing absolute probability use. It may then inform feature research, but it cannot supply the `P(fill)` term of quote EV or authorize an action. Conversely, passing a prediction calibration profile only permits registration of a subsequent known-propensity action experiment; it does not establish action uplift or live promotion.

The July 27 sequence now provides the concrete counterexample. v2's nested cell calibration retained all ranking/Brier evidence but only 8/18 cells met a zero-centered day-cluster level test. v3 added a three-day past-only rolling offset and reached 17/18, but made `SELL add x 1s` lose Brier and slope support; rolling calibration was therefore rejected rather than tuned again. v4 keeps the frozen nested model and uses the Development day-level drift envelope only for a **prediction-transfer shadow gate**. Zero-centered calibration intervals remain diagnostics. Absolute probability use still requires the USDC action error budget above, and v4 carries no action or live authority.

## Output Contract

Every scorecard emits:

```text
validity.passed / failures
support.passed / failures
hard_gates.passed / failures
metrics[] with source, LCB, scale, normalized score, and contribution
components.{value,tail,lifecycle,mechanism,execution}
total_score
ranking_score                 # null unless every gate passes
candidate_class
economic_classification
promotion_status
profile_sha256
input_identity_sha256
scorecard_sha256
```

The canonical economic classifications distinguish action uplift from risk reallocation. For example, a positive reward point estimate, positive downside diagnostics, an uncertain reward lower bound, and severe fill loss is `overbroad_risk_control`, not alpha.

## Current Counterexample

The closed `sell_campaign_add_permission_v1` Development result was scored retrospectively with `action_defense_v1`:

```text
total_score             -0.2681
ranking_score           null
economic_classification overbroad_risk_control
tail component          positive
mechanism component     -1.0 before component weighting
```

This is the intended behavior. Downside, q10, MAE, and repair-time evidence is preserved, while reward uncertainty, candidate-rate excess, and only 10.8% fills retention close the family.

Artifact:

`${NARROWGATE_RETIRED_DATA_ROOT}/reports/sell_campaign_add_permission_v1_20260722/development_cate_v1.scorecard.json`

The later `queue_value_net_hazard_keep_cancel_v2` result is a second counterexample. Aggregate strategy fills stayed at 99.37%, but the registered K1 action retained only 7.67% of eligible-order fills and 8.29% of toxic fills. Its toxic-reduction surplus and selectivity log ratio were negative, while randomized ITT reward was significantly negative. It fails both selective-v1 and the more volume-tolerant selective-v2 economic logic; see `research_07_active_order_continuation/docs/queue_value_net_hazard_keep_cancel_v2_20260722.md`.

## Commands

```bash
python3 -m models.audit.experiment_scorecard profiles

python3 -m models.audit.experiment_scorecard action-summary \
  --summary <family-result.summary.json> \
  --family-spec <family-spec.json> \
  --panel development \
  --profile action_alpha_v1 \
  --output <result.scorecard.json>
```

`--allow-retrofit-profile` exists only to classify evidence created before the scorecard contract. Such a result is never ranking-eligible solely because a profile was attached after the fact.
