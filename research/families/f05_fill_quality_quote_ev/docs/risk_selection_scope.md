# E/C scope and re-entry

Updated: 2026-09-08. [中文](risk_selection_scope.zh-CN.md)

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

## Training plan

The owner research plan requires at least three consecutive months of training
market data, with subsequent out-of-time validation kept separate. Dates must be
chosen after checking input coverage and earlier research use. A few short windows
remain engineering diagnostics, not economic evidence. Report all four surfaces'
input/excluded/train/validation rows, actual decision days, first/last timestamps,
outcome windows and feature support. Calendar span alone does not provide three
months of effective samples. No model should be fabricated for an absent surface.

The current repair is offline. It neither deploys E/C to live nor demonstrates
positive economic value or exact historical exchange queue recovery.
