# Cross-Venue Fair-Center Randomized Replay v1 Implementation Failure

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

## Status

`implementation_execution_failed_before_formal_development_evidence`

The frozen v1 action, panel, propensity, reward, scorecard, and gates remain unchanged. The initial two-worker execution was terminated because the runner returned complete per-day trace and event objects through the multiprocessing result pipe. After approximately 19 minutes it had emitted no day checkpoint, no canonical evidence, no scorecard, no report, and no readable economic result. Worker memory peaked at approximately 8.6 GiB while expanding native Arrow list columns.

## Frozen Identity

- Spec path: `research/families/f09_campaign_action_uplift/docs/cross_venue_fair_center_shift_randomized_replay_v1_spec_20260801.json`
- Spec file SHA256: `703a6c006c54430620f25710d662134a25394ecda183d0f838d3db764ca2fefb`
- Spec canonical identity: `10c1ef4755d0a360dc96c93a8c979b75357508dccfc610715c500c71aaf4f008`
- Executed evaluator SHA256: `42ab76a77abdfe5bf7150bcd48ece94be1b999b2d2361ecc1d00dd309ee47540`

## Correction Boundary

The v1.1 implementation may only change result transport and worker lifecycle:

- each worker writes one day's rows and event journal directly to atomic Parquet artifacts;
- each worker returns only a small hash-bound checkpoint descriptor;
- one worker process handles at most one day before recycling;
- resume verifies the spec and every persisted artifact hash.

It may not change the fair-price estimator, common-support mask, action, randomization seed or propensity, q90-off contract, replay mechanics, panels, reward, scorecard, bootstrap, LOO tests, gates, or permissions.

Validation and sealed holdout remain unread. No action or live authority was created by the failed execution.
