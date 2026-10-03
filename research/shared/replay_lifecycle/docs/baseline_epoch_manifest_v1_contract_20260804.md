# Baseline Epoch Manifest v1

Last materially modified: 2026-08-04

## Purpose

`baseline_epoch_manifest.v1` is the mandatory identity boundary for live order-lifecycle estimation. It contains no PnL, reward, or markout fields and grants no strategy authority.

An epoch identity binds at least:

\[
\begin{aligned}
E=\{&\text{runtime code},\text{ config},\text{ model bundle},\text{ P3},
\text{ Feature DAG},\\
&\text{execution ABI},\text{ action enablement},\text{ initial runtime state},\\
&\text{data-source identity},\text{ clock semantics}\}.
\end{aligned}
\]

UTC midnight is an accounting boundary only. It never starts a new epoch.

## Required Boundaries

A new epoch starts after any of the following:

- code or configuration deployment;
- model, P3, Feature DAG, execution ABI, source, or clock change;
- q90, selector, or other action permission change;
- process restart whose complete state was not restored;
- an explicitly state-restored restart, with the restored-state identity bound separately.

Epoch intervals cannot overlap. Every nanosecond in manifest scope must belong to one epoch or an explicit unbound interval. Missing evidence remains `null`; hashing the word `unknown` is forbidden.

## Authority

`lifecycle_estimation_authorized=true` requires all ten identity components and a complete restart audit. Continuous economic estimation additionally requires a complete initial economic state. Cross-epoch pooling is always false in v1: estimates are produced per epoch first, followed by an explicit transport comparison.

## Private Operational Evidence

The concrete epoch manifest, runtime boundaries, host/session bindings, and restart evidence are owner-private and `private_not_distributed`. Their absence grants no lifecycle, pooled-estimation, economic, action, or live authority. A consumer must resolve and verify the private bytes explicitly and fail closed when they are missing or mismatched.

## Implementation

- `models/replay/baseline_epoch_manifest.py`
- `scripts/build_baseline_epoch_manifest.py`
- `tests/test_baseline_epoch_manifest.py`
