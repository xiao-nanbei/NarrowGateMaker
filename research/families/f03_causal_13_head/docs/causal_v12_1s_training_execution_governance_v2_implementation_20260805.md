# F03 Causal-v12 1s Training Execution Governance v2

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

Status: implemented and unit-verified; no model, prediction, economic, action, or live authority.

This implementation amendment leaves the frozen 1s training design unchanged. It closes five execution-governance gaps in the memory-bounded LightGBM trainer.

## Enforced contracts

1. The execution amendment accepts exactly the frozen missing-artifact set: `model_output_identity`, `one_second_feature_panel_manifest`, and `training_implementation_sha256`. Any additional or absent blocker fails before matrix access or model training. The first two executable preconditions are bound by the amendment; `model_output_identity` remains an atomic training postcondition.
2. The emitted training identity and bundle bind the amendment path, file SHA256, and canonical `execution_identity_sha256`.
3. Amendment freeze compares the ordered UTC day and resolved feature/label artifact directories in the day manifest with the supplied admitted daily artifacts.
4. A reusable matrix is accepted only with schema `causal_v12_1s_training_matrix_cache.v1`, `atomic_admission=true`, valid `_SUCCESS`, reproducible cache identity, matching payload hash, matrix hash, shape, and dtype.
5. The amendment freezes the actual LightGBM runtime ABI: LightGBM version, native library path and SHA256, NumPy version, and the float32-storage/float64-Sequence input contract. Validation rejects runtime drift before training.

## Bound implementation

- Trainer SHA256: `62115a585371c241a2c36dccdc027a2b30e988ac76758dc3a9c494be93902647`
- Test SHA256: `c6128b83088431bc54185c260eb130aab2fae0e7659b2eb25d8af44d39db6e1c`
- Frozen design SHA256: `98b57a3fe28263178d9df3df9aee9154bc0e9ae8281535cc26d5afcdb41d70d3`
- LightGBM: `4.6.0`
- NumPy: `2.4.6`
- LightGBM native library SHA256: `39cd8a13dc50d615b876b5dbdcacca722469617eec0f1b627697f09d72bdd8ef`

## Verification

Using `.venv/bin/python`:

- `35 passed` in `tests/test_causal_v12_1s_training.py`
- `ruff check` passed
- `ruff format --check` passed

No real predictions, PnL, Validation, or holdout outcomes were read.
