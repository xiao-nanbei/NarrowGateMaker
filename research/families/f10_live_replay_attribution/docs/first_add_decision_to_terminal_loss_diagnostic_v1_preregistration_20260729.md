# First-Add Decision-To-Terminal Loss Diagnostic v1

Status: preregistered; native producer required; Development outcomes unread. Validation and sealed holdout remain locked.

## Question

For each campaign, is the first order decision that later generates an exposure-increasing add fill followed by a stable negative decision-to-campaign-terminal value, and is that loss identifiable from state visible at the decision?

The primary observational estimand is:

\[
Y = E_{campaign\ terminal} - E_{before\ decision}
\]

in USDC per first-add decision. This is prognostic attribution, not an action counterfactual.

## Denominator

The primary panel contains the 24 Grade-A days already inside the frozen F09 Development identity. The 16 Grade-B days are a separately reported, preregistered gap-censored sensitivity panel. They cannot be pooled into the primary gate. Existing Validation and holdout dates are not borrowed.

Each day/campaign contributes at most one row: the first decision whose exact generated order later produces an exposure-increasing add fill. The producer must preserve `decision_id -> order_id -> fill -> campaign` identity and emit 100% of eligible rows. Nearest-time matching and repair of the old 14.84% matched subset are forbidden.

## Evidence Boundary

All model features must be ready by the decision timestamp. Individual-trade fields may reconcile outcomes but cannot masquerade as decision-visible taker flow; tradable flow uses the parent aggTrade visibility clock. BUY and SELL are fit and reported separately with expanding chronological folds.

Passing this diagnostic would only unlock a hash-bound F05 fit identity. It cannot register an F09 action, open later panels, or authorize live behavior.
