# Time-Scale Evidence Classification v1

Last materially modified: 2026-08-04

Status: frozen shared semantics contract; mechanics only; no model, action, or live authority.

## Purpose

NarrowGate does not estimate every fixed duration from one lifecycle registry. The registry unifies clock semantics and provenance. The estimation method is selected by the role a duration plays.

| Class | Authoritative treatment |
|---|---|
| Estimand horizon | Full first-passage, hazard, or competing-risk CIF |
| Policy clock | Independent full-path action A/B |
| Transport limit | Latency tail, system SLA, and fail-closed cost |
| Feature basis | Preserve multiscale basis; test incremental value with chronological OOF |
| Governance threshold | Preregistered risk and statistical contract |

A feature lookback is not automatically a label horizon. A lifecycle percentile is not automatically a requote or cooldown policy. A provider timestamp is not automatically an AWS receive timestamp. A prediction improvement is not action or live authority.

## Baseline Epoch

The epoch identity binds runtime code, configuration, model bundle, P3, Feature DAG, execution ABI, action enablement, and initial runtime state. A new epoch begins when any of those identities changes, after an unrecoverable restart, or when the data-source or clock semantics change. UTC midnight is an accounting boundary only and does not create a new epoch.

Lifecycle evidence is first reported within each epoch. Calendar time and risk time are both retained, inactive or terminal intervals are excluded from the relevant risk set, and fill/cancel/ACK/reject/expiry/shutdown/partial/full events keep their distinct identities. Cross-epoch pooling is sensitivity only and follows epoch-specific drift reporting.

The machine-readable contract is [`time_scale_evidence_classification_v1_20260804.json`](time_scale_evidence_classification_v1_20260804.json).
