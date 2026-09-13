# BUY q90 Runtime Authority Contract v3

Last materially modified: 2026-08-02

## Scope

This is a baseline-integrity implementation identity. It closes startup and reload paths that could enable the unrepaired q90 action outside the deploy workflow. It does not change the q90 threshold, score, recovery estimand, or economic conclusion.

## Runtime Contract

- `live/config.py`, direct `live/main.py` startup, and deploy preflight use the same fail-closed q90 authority function.
- q90 action ON while prospective post-cancel recovery remains unsupported requires the explicit `NARROWGATE_ALLOW_UNREPAIRED_Q90_ACTION_DEPLOY=1` owner override.
- `live/run.sh start` runs preflight before `nohup`.
- `live/run.sh restart` runs preflight before stopping the healthy process.
- SIGHUP cannot change q90 action state; that transition requires restart.
- Startup atomically writes `logs/runtime_identity.json` and logs the complete policy identity. An effective owner override is emitted as a warning.
- `logs/maker.preflight.json` records the preflight identity used by the shell launcher.

The current private baseline passes with causal-v12 ON, q90 shadow ON, q90 action OFF, and BUY fill-selection ON.

## Version Boundary

The frozen v2 identity remains unchanged. Its mutable implementation paths no longer match current bytes after this runtime-authority repair, so v2 is now a historical implementation identity. The machine-readable current identity is [`buy_q90_runtime_authority_contract_v3_implementation_20260802.json`](buy_q90_runtime_authority_contract_v3_implementation_20260802.json).

## Verification

- Targeted runtime, lifecycle, F09 adapter, and registration checks: 72 passed.
- Full repository: 1353 passed, 4 skipped, 1 pre-existing joblib warning.
- `bash -n live/run.sh`: passed.
- Current private-config deploy preflight: passed.

No economics, Validation, or sealed holdout was read. The local repair has not been deployed. q90 action remains suspended, and prospective placement recovery, lockstep transport, and independent economic replay remain required.
