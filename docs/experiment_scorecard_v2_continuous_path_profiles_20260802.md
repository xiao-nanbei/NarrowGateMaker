# Continuous-Path Action Scorecards v2

Last materially modified: 2026-08-02

The four successor profiles are:

- `action_alpha_v2`
- `action_defense_v2`
- `action_execution_v2`
- `action_execution_selective_v3`

They do not modify or reinterpret the frozen v1 profiles in `models/audit/experiment_scorecard.py`.

## Economic hierarchy

The primary value metric is closed-campaign value measured from assignment to campaign terminal. Conditional net value remains a supporting hard gate. Full panel continuous MTM is a secondary economic metric and accounting check.

For every UTC day:

\[
\mathrm{PnL}_d = \mathrm{realized}_d
+ q_{d,\mathrm{end}}m_{d,\mathrm{end}}
- q_{d,\mathrm{start}}m_{d,\mathrm{start}}.
\]

Cash, inventory, and campaign state must carry across midnight. UTC day is an inference cluster, not a liquidation, reset, or campaign-terminal boundary. The final panel inventory must be marked to market even when it is nonzero.

## Weight migration

The former day-end censoring weight moves to `conditional_net_value`:

| Profile | Old censoring weight | New conditional-value weight |
|---|---:|---:|
| `action_alpha_v2` | 4.00% | 4.00% |
| `action_defense_v2` | 4.00% | 4.00% |
| `action_execution_v2` | 2.67% | 2.67% |
| `action_execution_selective_v3` | 3.00% | 3.00% |

The historical primary value weight now belongs to `closed_campaign_value`. Day-end inventory, open-campaign MTM, and censoring retain zero ranking weight and no hard-gate authority.

Campaign q10/CVaR, terminal protection, MAE, maximum inventory, and inventory time remain noncompensable risk gates. A high value score cannot buy through a negative day-clustered lower bound for these path risks.

The machine-readable frozen registry is [`experiment_scorecard_v2_continuous_path_profiles_20260802.json`](experiment_scorecard_v2_continuous_path_profiles_20260802.json).
