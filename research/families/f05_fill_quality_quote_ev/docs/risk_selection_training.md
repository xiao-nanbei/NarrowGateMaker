# E/C paired-label training

Last materially modified: 2026-09-06

Last materially synchronized: 2026-09-06

[中文说明](risk_selection_training.zh-CN.md)

This entrypoint fits small action-value models from already-validated modeled counterfactual labels. It does not launch replay, fetch private data, claim live fill identity, or deploy a strategy. A successful fit is not evidence of profit.

## Sequence

1. Keep the current baseline, execution environment and Development dates fixed.
2. Use the existing F01 replay with `--save-risk-opportunities` to retain every eligible opportunity, including those that never fill. Select targets by a rule fixed before outcomes, not by eventual fills or losses.
3. Use `--risk-pair-baseline-arm` and single-opportunity arm overrides. Validate the common opportunity prefix, one changed action, complete future trajectory, common terminal mark, fees and funding. The current implementation reruns prefixes; it does not claim complete checkpoint or copy-on-write restoration.
4. Only then fit the model, separately for E/BUY, E/SELL, C/BUY and C/SELL.
5. Evaluate the resulting B/E/C/EC policies on complete out-of-sample paths before considering any economic or deployment conclusion. Random participation and flat controls remain necessary; one-step labels cannot replace this study.

E compares POST minus WAIT for a permitted flat opener. A trained E model requires strictly positive value to POST; at zero it waits. C compares KEEP minus CANCEL for a remaining exposure-increasing order and cancels only for negative value. Absent models or unavailable features preserve the existing baseline protections. Neither action supplies a new price, size, cooldown, or immediate replacement.

## Minimal offline command

The files below are owner-provided artifacts, not files distributed by this repo. Run from the repository with the research dependencies installed:

```bash
PYTHON="$NARROWGATE_ROOT/.venv/bin/python"
"$PYTHON" -c 'import sys; assert sys.version_info >= (3, 11)'
"$PYTHON" -m research.families.f05_fill_quality_quote_ev.risk_selection_training \
  --labels "$NARROWGATE_RESULTS_DIR/pairs.risk_paired_labels.jsonl" \
  --feature-units "$NARROWGATE_RESULTS_DIR/feature_units.json" \
  --validation-start-ns "$VALIDATION_START_NS" \
  --alpha 1 --min-train-rows 8 --policy-id development-ec-ridge \
  --output-dir "$NARROWGATE_RESULTS_DIR/training"
```

`feature_units.json` explicitly maps frozen feature names to their units. The model uses those fields as recorded; it does not guess units or replace missing values with zeros. Feature choice and Ridge alpha must be fixed before validation results. Eight rows is a configurable engineering minimum, not a statistical sample-size recommendation or economic threshold.

Training means and scales use training rows only. Labels whose outcome reaches the validation boundary are purged. The same order cannot occur in both sets. No random row split is provided: many opportunities can share one market path and terminal value. A one-window pilot can verify fitting but cannot supply an independent validation period.

`policy.json` is loadable by `strategy.risk_selection.RiskSelectionPolicy`. `training_report.json` records exclusions, per-surface support, prediction MSE against a past-only intercept, and whether a surface is training-only. Unsupported surfaces are absent from the policy, not silently pooled with another side. The output directory is new and private; existing artifacts are not overwritten.

## Complete learned-policy replay

After fitting, keep the existing frozen F01 baseline command and add
`--risk-selection-policy "$NARROWGATE_RESULTS_DIR/training/policy.json"`.
The command must use the Python diagnostic path, an explicit funding tape and
`--continuous`; do not split consecutive days into fresh-start arms. Select B/E/C/EC
through the existing arm-spec JSON, for example:

```json
[
  {"name": "E", "group": "risk_selection", "overrides": {"risk_selection_mode": "E"}},
  {"name": "C", "group": "risk_selection", "overrides": {"risk_selection_mode": "C"}},
  {"name": "EC", "group": "risk_selection", "overrides": {"risk_selection_mode": "EC"}}
]
```

Select these names alongside `baseline` using `--arms`. The policy file is read once
before market loading; all arms share its payload and the same immutable inputs but
own their orders, budgets, inventory and future actions. B remains the default and
does not score. E/C/EC score both sides from one visible snapshot before reserving
submission budget. WAIT adds no cooldown; C uses normal cancel/ACK/terminal handling.

This mode automatically retains the complete opportunity table with policy ID,
predicted USDC difference and reason. The economic rows report selected actions,
changes and fallback counts alongside net PnL and funding. A missing surface remains
baseline, not a hidden transfer from the opposite side. Do not combine this mode
with `--risk-pair-baseline-arm`: overlapping intervention labels are not full-policy
returns. C++ execution and live deployment are not enabled by this switch. Random
participation and Flat controls still need their own frozen comparison design.

## Remaining scope

The present trainer checks label structure and arithmetic, not the original raw market inputs. Real paired-trajectory validation must precede its use. Queue and latency remain modeled, funding uses the frozen tape, and a shortened pilot uses common terminal MTM rather than realized-only reward. Labels are USDC per action, not probabilities, additive portfolio returns, or evidence of an optimal size.

Current plumbing does not supply a live adapter, measured candidate-compute cost, complete checkpoint branching, or an economically validated deployed model. Do not infer these capabilities from the existence of a policy JSON.
