# Live 120h Adverse-Selection Diagnostic v1 Errata

This errata narrows interpretation without modifying the frozen Spec, report, evaluator, or source artifacts.

## Loss concentration

The frozen `74.7383%` field is the multi-inventory campaign net PnL divided by the net PnL of all clean campaigns. It is not the share of gross losses from losing campaigns.

- Multi-inventory gross loss: `11.1298 / 24.1513 = 46.0836%`.
- SHORT campaign gross loss: `19.8407 / 24.1513 = 82.1517%`.

The supported conclusion is that SHORT campaigns dominate gross loss and multi-inventory campaigns concentrate the net negative aggregate. The frozen window does not show that multi-inventory campaigns caused 74.7% of all gross losses.

## Causal scope

Fees, restart, and cooldown cannot algebraically explain why the maker-signed price markout itself is negative. This does not prove that they are irrelevant to total account PnL, inventory paths, or campaign continuation. The diagnostic did not estimate a cooldown counterfactual, did not bind the restart source logs, and did not bind commission-asset conversion.

`SELL opener` in the frozen derived CSV is a fill-time inventory role. It does not establish that the order was submitted while flat. The follow-up native F10 trace therefore uses an exact submit -> activation -> fill -> campaign join and reports cross-boundary pending-cancel reopens separately.

## Robustness context

The 205 missing approximate-10-second markouts would need to average more than `+5.31898 bps` to reverse the aggregate sign. This is a post-run sensitivity calculation, not a frozen gate. The order-age evidence is likewise limited to the preregistered under-1-second and 4.5-to-5.5-second slices; it does not imply a globally monotone age-value curve.

The action, Validation, holdout, prediction, and live permissions remain false.
