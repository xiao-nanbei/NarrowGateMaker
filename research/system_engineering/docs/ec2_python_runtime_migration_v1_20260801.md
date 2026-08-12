# EC2 Python Runtime Migration v1

Last materially modified: 2026-08-12

Evidence availability: SHA256 values below are identity metadata, not download links. Repository-relative links identify files available in this repository. Unless the surrounding text identifies a public repository source or release, a named artifact without a public link is owner-side evidence retained in the private evidence store and is not distributed with the public repository.

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../../../docs/public_private_documentation_contract.md).

Status: complete, system engineering only. No strategy action, parameter, model, P3, q90 policy, or live trading semantic was changed.

## Result

The Tokyo EC2 live process was moved from CPython 3.9.25 to CPython 3.12.13, matching the local project virtual environment at the Python patch-version level. Amazon Linux's official `python3.12-3.12.13-2.amzn2023.0.2` package was installed alongside the system Python. The system `python3` link was not replaced.

The target environment is `${NARROWGATE_REMOTE_ROOT}/.venv-py312`. The reversible launcher selector is `.venv-active -> .venv-py312`; the original `.venv` remains intact as the rollback environment.

The switch occurred only after a natural `FLAT` fill and while the bounded receive-time capture lock was available. PID `1371345` exited gracefully and PID `1683884` started with:

```text
${NARROWGATE_REMOTE_ROOT}/.venv-active/bin/python3
```

## Verification

- Exactly one maker process remained after the switch.
- The live process mapped `narrowgate_cpp.cpython-312-x86_64-linux-gnu.so` with SHA256 `6bb6d4dff15d1c43fcfa400fbd79f481fe50fc62c956928b0ae3238a3cacc152`.
- Config, P3, and q90 policy hashes were unchanged.
- The deploy preflight, q90 bundle/policy load, and Python/C++ quote-core cap/GTX smoke passed under 3.12.13.
- Native quote-core, signal-feature, global-flow, and live-routing paths remained enabled in strict mode.
- Recording remained disabled and quote decisions advanced after startup.
- No traceback, critical error, fatal error, or segmentation fault appeared in the startup window.

The post-restart global reference initially reported `basis_warmup`; this is the existing causal 30-sample basis contract resetting with process-local state, not a Python migration failure.

## Rollback

Stop the maker, remove `.venv-active`, and start `live/run.sh`. The launcher then falls back to the retained Python 3.9 `.venv`. The rollback environment was not modified or deleted.

## Engineering Finding

Creating a virtual environment from the repository root exposed that the top-level `signal.py` can shadow Python's standard-library `signal` module. The target venv was therefore created from `${NARROWGATE_REMOTE_HOME}`. This import-name collision is separate from the runtime migration and should be removed in a dedicated repository-governance change.
