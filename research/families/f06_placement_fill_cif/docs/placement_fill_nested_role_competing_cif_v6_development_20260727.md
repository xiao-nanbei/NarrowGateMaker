# Placement Fill CIF v6: Inner-OOF Role Calibration

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Date: 2026-07-27

Decision: closed on Development. Validation and sealed holdout were not read. No action or live deployment is authorized.

## Frozen Method

v6 is the strict nested successor to the single trailing calibration block in v5. For each side it first generated one reusable inner-expanding OOF interval score tape:

```text
inner train -> one-day embargo -> inner test
```

Inside each outer train, only inner-test rows whose dates belonged to that outer train were eligible. Separate weighted logit intercept/slope maps were then fitted for opener/add/reducing and fill/cancel-ACK. The base hazard was refitted on all outer-train days, calibrated, and applied only to the later outer test. Missing support, non-positive slope, or date leakage failed closed.

The v4 curve-level gate was inherited without modification. Frozen spec: `docs/placement_fill_nested_role_competing_cif_v6_spec_20260727.json`.

## Identity

- Development days: 50.
- Chronological OOF days: 24.
- Cohorts: 831,635.
- Action lifecycles: 2,494,905.
- OOF rows: 7,202,322.
- Report SHA256: `3fb88e705e3618509cd73ed3fc9b095e9d7b3a9f6ea49388a809149410d5aac3`.
- OOF SHA256: `7c12a0914b0f28f80dc1fa9eb2371176d881a21fb3eaa03b3f436933779330a1`.
- Evaluation SHA256: `c105cf4f9edd6cae88f013633afc8f8eaddc90a2f871316ece0e6f9a8e369ba3`.
- Model artifact SHA256: `d702ee14c14e58b09dc3e3158edd07cb2ce901ebfe412e15b9a13b231a2e25c7`.
- Validation read: false.
- Sealed holdout read: false.

## Development Result

Probability simplex, time monotonicity, distance monotonicity, support, and all joint/fill/cancel proper-score lower bounds passed. No side-role curve passed the unchanged calibration gate:

| Curve | Fill bias | Fill 95% interval | Cancel bias | Cancel 95% interval |
|---|---:|---:|---:|---:|
| BUY add | +0.00141 | [-0.00212, +0.00482] | -0.04156 | [-0.05317, -0.02929] |
| BUY opener | +0.00410 | [+0.00078, +0.00730] | -0.04385 | [-0.05311, -0.03445] |
| BUY reducing | +0.00242 | [-0.00085, +0.00545] | -0.03998 | [-0.04725, -0.03260] |
| SELL add | +0.00196 | [-0.00073, +0.00469] | -0.04159 | [-0.05078, -0.03192] |
| SELL opener | +0.00327 | [+0.00043, +0.00607] | -0.04253 | [-0.05055, -0.03418] |
| SELL reducing | +0.00260 | [-0.00013, +0.00517] | -0.03434 | [-0.04308, -0.02536] |

The maximum raw fill-distance discrepancy was `5.49e-6`, below the frozen `1e-5` numerical tolerance. Every integrated joint Brier-improvement lower bound remained positive.

## Interpretation

The role/cause Platt maps were well supported and genuinely out of fold, but their cancel slopes were only about 0.54-0.61. They corrected the raw hazards produced by smaller past-only inner models. Applying those maps to a base model refitted on the entire outer train caused a training-size transport error and systematically underpredicted the cancel-ACK CIF by roughly 3.4-4.4 percentage points.

This rejects the v6 inner-OOF interval-hazard calibration transport and its implementation. It does **not** reject the placement estimand

\[
P(T_{fill}\le t,T_{fill}<T_{cancelACK}\mid x,a),
\]

which remains the correct lifecycle probability. It also reinforces that baseline cancel request is largely a known policy stopping rule rather than an exchange event that should be learned as a stationary competing cause.

The next structurally distinct formulation, if pursued, should schedule cancel request deterministically from the frozen policy, estimate request-to-ACK latency conditionally, and explicitly replay fills while cancel is pending. It must use a new family identity. v6 Validation cannot be used to tune or rescue that formulation. That identity is frozen in `docs/placement_fill_policy_clock_race_v1_spec_20260727.json`.
