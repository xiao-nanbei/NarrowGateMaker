# Active Order Lifecycle CIF 100ms v1.6: 40-Day Completion

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../../docs/public_private_documentation_contract.md).

Status: 40-day mechanics, event lockstep, CIF training, and C++ inference parity passed. Live/AWS transport, economic evaluation, q90 action, and deployment remain unauthorized.

## Authoritative Result

- Development days: 40.
- Lifecycle events: 2,712,262.
- Journal rows: 686,224.
- Exact-native eligible lifecycle spells: 665,831.
- Native queue-censored spells: 20,393.
- Risk exposure: 5,172,921.02 seconds.
- Terminal causes: 636,186 cancel ACK; 29,645 full fill; zero unsupported terminal cause.
- CIF cells: 120 across 30 parent cells.
- Python/C++ event mismatches: zero.
- Post-terminal hazard/queue reuse: zero.
- Python/C++ CIF maximum absolute difference: `1.1102230246251565e-16`.
- Checkpoint-resume maximum absolute difference: zero.

## Artifact Binding

- Lockstep report: `${NARROWGATE_DATA_ROOT}/reports/f07_active_order_lifecycle_cif_v1_6_20260805/order_lifecycle_v2_40day_cpp_lockstep_v1_6.json`, SHA256 `ecd462c257a32fbb7d3f8fc31713ed97355872d2502bb81716c845cfff0f298d`.
- CIF artifact: `${NARROWGATE_DATA_ROOT}/reports/f07_active_order_lifecycle_cif_v1_6_20260805/active_order_lifecycle_cif_100ms_v1_6.json`, SHA256 `9ad809ab3e525069c8e9f2118cddb6e51951cd1620912a853c71f560f8dc71eb`.
- Training report: `${NARROWGATE_DATA_ROOT}/reports/f07_active_order_lifecycle_cif_v1_6_20260805/active_order_lifecycle_cif_100ms_v1_6_training_report.json`, SHA256 `a3bbd898922b102076a83125461b0e83c2293c45a3022f07ebe758e22f259aef`.
- C++ parity report: `${NARROWGATE_DATA_ROOT}/reports/f07_active_order_lifecycle_cif_v1_6_20260805/active_order_lifecycle_cif_cpp_inference_parity_v1_6.json`, SHA256 `7772ace4116247a2ce2d227ed0463274f39d5d051836b99254d428a63f7927f9`.

The run used the exact frozen `models/data_windows.py` SHA256 `91a690baf9636a5f9f6665d5f4a5385b114d4efebb00f82eb2a32455bf6b7223`. Cache access accounting was deliberately excluded from this historical execution identity and restored afterward as infrastructure-only behavior.

## Permission Boundary

This result validates the replay lifecycle/CIF implementation. It does not establish current-live score calibration or action value. A fully bound prospective baseline epoch and AWS receive-time lifecycle transport remain required before any q90 economic replay. q90 stays shadow-only and action-off.
