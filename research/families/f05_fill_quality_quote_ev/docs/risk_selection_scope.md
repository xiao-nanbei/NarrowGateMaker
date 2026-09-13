# E/C scope and re-entry

Updated: 2026-09-12. [中文](risk_selection_scope.zh-CN.md)

Last materially modified: 2026-09-12

Last materially synchronized: 2026-09-12

Two explicit research scopes are available. Old artifacts default to
`reachable_inventory`: a target must be exposure-increasing over all possible
fills of other pending orders. This excludes ordinary flat bilateral KEEP states
and can allow an entry previously rejected by E to re-enter outside E's scope.

New bilateral experiments use `risk_selection_scope=visible_inventory` in **all
arms, including B0**, and `selection_scope=visible_inventory` in the policy. The
role uses current policy-visible inventory. Other pending quantities remain in the
observation and features, rather than being mistaken for already filled inventory.

- E scores each otherwise eligible flat POST, including after WAIT and while an
  opposite order is pending. WAIT is not a timed cooldown or a permanent veto.
- C scores an eligible single OPEN KEEP on either side, including bilateral flat
  orders. CANCEL is a request; fills before exchange cancellation remain possible.
- Observed reducing and mixed cross-zero orders retain the baseline path. This
  does not guarantee that other in-flight fills cannot later change their role.
- Unknown ownership, same-side pending/coalescing, actual risk limits, and gateway
  FIFO are unchanged. C does **not** yet evaluate REPLACE as CONTINUE/WITHDRAW.
- After a fill, re-evaluate the role from the newly visible state; do not carry an
  old WAIT across a transition into genuine inventory reduction.

Rows, paired labels, training reports and models carry their selection scope.
Do not relabel an old fitted model as a newly trained bilateral policy. Generate
new paired trajectories first. B0 and candidates must use one source/runtime and
the same external market, funding, fee and latency inputs; an older B0 computed
by a different executor is historical context, not the new paired control.

## Diagnostics, not new trading gates

`risk_selection_route_counts` records each considered side/baseline action and
exactly one eligible or excluded destination. Keys separate scheduling, unsupported
baseline actions, ownership, role and budget exclusions. `risk_selection_score_counts`
separates finite predictions from no-model/missing-feature fallbacks per surface.
`risk_selection_execution_counts` counts actual lifecycle submit and cancel
requests across the account, **not exchange acceptance or selected-policy actions**.
Join the existing order/decision traces for request-to-terminal and fill attribution.
These three maps are also serialized in the runner's per-accounting-window CSV.

## Continuous data and training plan

The current BTCUSDC perpetual dataset is the full 401-day UTC calendar from 2025-08-01 through 2026-09-05. All of those dates belong to the current dataset. Current directories and research interfaces no longer retain supplier or historical-processing classifications, and dates are not divided into different datasets on those grounds. Actual observation, gap and reset semantics remain explicit. Use the [unified daily paths](../../../../docs/path_conventions.md#market-data-tree) and the same current reader throughout. This calendar statement does not claim that every file has completed reconstruction or validation: retain missing, aged and unverified inputs explicitly, and keep previous-use, Development, Validation and holdout permissions separate from data availability.

An observation gap stays in the calendar. Do not manufacture market updates or refresh the last real observation clock. When unchanged freshness rules require quoting to remain silent, account time, existing orders, inventory/campaign risk and funding exposure still continue; they are not reset or silently removed. Unknown fills or valuations remain uncertain, not zero. Data-layer book continuation alone does not prove complete account-state continuation.

The owner research plan requires at least three consecutive months of effectively supported training market data, followed by independently usable out-of-time (OOT) evidence. Check coverage and earlier research use before fixing the continuous interval. Report all four surfaces' input/excluded/train/validation rows, actual decision days, first/last timestamps, outcome windows and feature support. Calendar span alone is not effective training coverage. A few short windows remain engineering diagnostics, and an absent surface must not receive an invented model.

Sampling training labels is not permission to skip market days. A policy replay processes the complete declared interval and each arm independently carries its orders, inventory, campaigns and accounting across midnight. Selecting every third day's labels for one model fitted over the entire interval, then testing the intervening days, uses later training information to predict earlier dates; that is not deployment OOT. If an every-two-day update schedule is desired, each update must use only history and completed label outcomes available at that update, then evaluate the subsequent block. This is rolling-origin evaluation, not a globally interleaved train/test split. [Official TimeSeriesSplit documentation](https://scikit-learn.org/stable/modules/generated/sklearn.model_selection.TimeSeriesSplit.html) and [Forecasting: Principles and Practice](https://otexts.com/fpp3/tscv.html) describe past-only training and subsequent test blocks.

For model selection, prefer several past-only rolling or expanding folds inside compliant Development, then a predeclared independent final OOT segment. Fit transformations and select features/parameters using each fold's training information only. Purge labels whose actual outcome interval reaches the next validation boundary, including cross-day outcomes; an arbitrary two-day gap does not replace this check. Preserve shared-order and applicable campaign/outcome boundaries. Bidirectional purged blocked validation can answer a Development robustness question, but purging overlap does not make future-trained predictions of the past into deployment OOT. Do not use the final OOT segment to choose the schedule or rescue a candidate.

The [current trainer](../risk_selection_training.py) implements one `validation_start_ns` boundary, outcome-terminal purging, shared-order exclusion and training-only transformations. It does not implement a multi-fold rolling/expanding scheduler. A common terminal beyond the boundary can legitimately purge every earlier label; arrange causally matured label windows rather than deleting the purge. The plan above is not a claim that multi-fold training or any new fit has already run.

## Current and historical conclusions

The first 2026-09-07 Development pilot retains its original model, scope, source and economic conclusions. Its observation that C selected no cancellations is historical evidence, not proof that current bilateral C cannot act after the scope repair. Likewise, old labels or a differently executed B0 are not results under the newly unified market inputs. Revalidate the applicable paired trajectories before fitting or comparing new policies; do not rewrite old results as a new experiment.

The current repair is offline. It neither deploys E/C to live nor demonstrates
positive economic value or exact historical exchange queue recovery.

Synthetic persistence tests cover E/C/EC at five saved-runtime cut points. Full
resumed outputs, including selector counters and pending execution, match the
uninterrupted replay. This tests replay continuation, not historical live-state recovery.
