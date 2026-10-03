# Volatility-Time Add Rearm Full-Path Preflight v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

## Decision

Development mechanics support passes. The only newly opened work is native C++ BUY q90 parity. This result does not authorize a randomized action experiment, Validation, holdout, shadow, or live deployment.

The authoritative frozen report is [`report.json`](${NARROWGATE_RETIRED_DATA_ROOT}/reports/volatility_time_add_rearm_full_path_preflight_v1_20260729/development/report.json), with SHA256 `d4c47dfcc04dcc29db94e0117612f3af2950a7b18c157aded5aa198f6aa5252a`. The independent machine-readable audit is [`volatility_time_add_rearm_full_path_preflight_v1_postrun_audit_20260729.json`](volatility_time_add_rearm_full_path_preflight_v1_postrun_audit_20260729.json).

## What Was Tested

The explicit 85-second-per-fill-unit control and variance-time candidate were replayed independently over the same 40 Development days. Both arms used the frozen corrected config, empirical P3, q0.70 queue calibration, native snapshot/delta book scheduler, AWS Tokyo latency profile, BUY q90 action, path-dependent consecutive-loss cooldown, and deterministic sync stress tape.

The candidate regenerated quotes, cancels, ACKs, replacements, re-entry, queue position, fills, inventory, cooldown lineage, and downstream blockers. Baseline future fills were not reused. The evaluator read lifecycle mechanics only; it did not read PnL, reward, markout, Validation, or holdout.

## Support Result

| Side | Mechanical episodes | Unmasked episodes | Rate | Day-cluster 95% interval | Support days |
|---|---:|---:|---:|---:|---:|
| BUY | 5,490 | 3,994 | 72.75% | [65.28%, 79.26%] | 40/40 |
| SELL | 3,827 | 2,183 | 57.04% | [49.59%, 64.05%] | 40/40 |

The action-support authority is the candidate's final gate stack. Comparing control and candidate order states is only a regenerated-path diagnostic after the two paths first diverge.

The direction split is materially different:

| Side | Timing direction | Episodes | Unmasked | Rate |
|---|---|---:|---:|---:|
| BUY | earlier ready | 3,711 | 3,626 | 97.71% |
| BUY | later ready | 1,779 | 368 | 20.69% |
| SELL | earlier ready | 1,860 | 1,771 | 95.22% |
| SELL | later ready | 1,967 | 412 | 20.95% |

Later-ready action differences are frequently masked by markout/adverse gates. The first-blocker audit records 1,411 BUY and 1,555 SELL later-ready markout blocks. Candidate ready time was censored by an opposing fill/reset in 964 BUY and 1,335 SELL lineages; these cases are not represented as zero timing delta.

## Path Integrity

Every Development day had nonzero candidate-only and control-only order and fill outcomes. The minimum per-day candidate-only/control-only fill counts were 254/296. Across all days, control had 20,296 BUY and 20,298 SELL fills; the candidate had 20,631 BUY and 20,623 SELL fills. These are mechanics counts, not economic evidence.

BUY q90 produced 107/107/106 control cancel-request/ACK/re-entry events and 100/100/100 candidate events. Consecutive-loss cooldown triggered 939 times in control and 923 times in candidate. Sync stress produced 160 events per arm; it remains non-promotional stress evidence.

## Governance Errata

The frozen runner emitted the path-difference evidence but did not include the pre-registered path-regeneration requirement in its internal `_decision_from_support()` boolean. The independent post-run audit applies that hard gate and confirms it passes on all 40 days. The frozen Spec, runner, and report remain unchanged.

`control_reproduced=true` refers to the frozen explicit-wall/default contract test under the bound config identity. The formal run did not execute a third legacy-control arm for every day.

The predecessor's 69.8%/61.6% figures remain mechanical timing-change rates; they are not reinterpreted as action-effective rates.

## Next Boundary

Implement native C++ snapshot/delta BUY q90 scoring with event-level parity for cancel request, ACK-before-fill race, queue reset, score recovery, and re-entry. Only after Python/C++ full-path parity passes may F09 register a new randomized lineage-level action identity. AWS receive-time remains a separate final transport and live-authorization gate.
