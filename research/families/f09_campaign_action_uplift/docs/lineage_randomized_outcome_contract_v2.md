# Lineage-Randomized Outcome Contract v2

Last materially modified: 2026-07-29

## Status

`foundation_active_variance_time_action_dormant`

This is a shared F09 infrastructure contract. It does not register an action, read an outcome panel, reopen the closed variance-time action, or grant Validation, holdout, action, shadow, or live authority.

## Outcome origin

Every future lineage-randomized action must freeze one primary outcome before path generation:

\[
Y_{post}
=V(T_{campaign})-V(t_{assignment})
=R_{lineage}+V_{continuation}.
\]

The native replay must also emit the exact decomposition:

\[
R_{lineage}
=V(T_{lineage})-V(t_{assignment}),
\]

\[
V_{continuation}
=V(T_{campaign})-V(T_{lineage}).
\]

Campaign PnL accrued before assignment is limited to a preregistered covariate or balance diagnostic. It cannot enter an action outcome. A future action may use no adjustment, or may preregister an estimator using only pre-assignment campaign PnL, inventory, and campaign age. That decision is frozen before any Development reward is read.

## Randomization

Future actions use independent PRF-Bernoulli assignment with exact conditional propensity 0.5. The PRF domain includes family identity, UTC day, side, and a pre-assignment lineage UID. This preserves day-by-side stratification without making a later, path-dependent lineage's action depend on an earlier action. Finite-sample imbalance is handled by preregistered stratified estimation and covariate adjustment, never by outcome-time rebalancing.

Because the primary outcome ends at campaign terminal, one campaign may contain at most one assignment. That assignment persists to campaign terminal; a later cooldown episode cannot be independently rerandomized into the same outcome. Sequential-regime estimators are outside v2 and require a separate contract.

The action-specific registration must separately freeze the assignment unit, actions, terminal event, random seed, primary estimand, and covariate-adjusted estimator choice. This foundation contract does not choose those semantics.

## Native trace

The authoritative randomized path must directly write every lineage field. Post-hoc matching to a baseline mechanics trace is forbidden. Each row carries:

- stable lineage and day-side stratum identity;
- assignment time, action, propensity, inventory, campaign age, and same-side fill units;
- final blocker and final action-change timestamp;
- lineage and campaign terminals;
- all four equity anchors and the exact post-assignment accounting bridge.

The core schema is action-neutral. Mechanism-specific state is carried through an explicitly registered extension. For example, `variance_time_v1` contains same-side fill units, variance budget/QV, ready timestamps, and clock direction; a future keep/cancel or skip action must define its own extension rather than inheriting variance-time fields.

The producer also emits an ordered event journal. Every assigned row must have exactly one `assignment`, one `lineage_terminal`, and one `campaign_terminal` event; same-side restarts appear as explicit episode close/start events, while blocker and final-action changes are path events rather than post-hoc joins. Assigned, lineage-finalized, campaign-terminalized, emitted, producer-validated, and event-terminal denominators must be identical. Open lineage state or pending campaign state at replay return is a hard failure.

Timestamp absence must be explicit. A variance clock censored before release uses `variance_ready_status=censored_before_ready`; a lineage with no final action change uses `eligible_no_change`, `no_eligible_decision`, or `censored_before_resolution` as appropriate. Missing producer coverage is a hard error, not a row filter.

## Research boundary

`volatility_time_add_rearm_randomized_replay_v1` remains closed on Development. Its Validation and sealed holdout remain unread. The next research sequence is F10 live/replay attribution, then F05 decision-visible net fill value evidence, then a genuinely different F09 action only if the negative mechanism is stable across days and available at decision time.
