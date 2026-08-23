# Developer Checks

Last materially modified: 2026-08-23

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../public_private_documentation_contract.md).

Create a Python 3.11-or-newer environment and install the complete contributor target before running the full suite:

```bash
python3.11 -m venv .venv  # only when .venv does not already exist
PYTHON=.venv/bin/python
$PYTHON -m pip install -e ".[all]"
$PYTHON -m pytest -q
$PYTHON -m ruff check narrowgate examples
$PYTHON -m py_compile narrowgate/cli.py
$PYTHON scripts/audit_public_documentation.py --repo-root .
git diff --check
```

The full test suite imports offline research modules and read-only public venue connectors, so `all` is the contributor contract even when no network request or live order is made. The separate `Base install smoke` CI job installs only the base package and runs the documented no-data CLI, replay demo, order-level example, and onboarding tests; it guards against accidental optional-dependency leakage.

The exact required GitHub check names and recommended `main` ruleset are frozen in [Branch Protection](branch_protection.md). The required names are `Base install smoke`, `Python tests and lint (3.11)`, `Python tests and lint (3.12)`, and `C++ extension build smoke`.

Historical reproduction tests whose exact predecessor bytes, temporary deployment trees, or owner-private execution configurations are not distributed are listed in `tests/fixtures/public_clone_historical_test_availability.json`. They are explicit skips in a public clone and grant no research, action, or live authority. After restoring every bound fixture, an owner may opt in with `NARROWGATE_RUN_HISTORICAL_REPRODUCTION_TESTS=1`; setting that variable without the exact evidence is expected to fail closed.

C++ extension smoke:

```bash
$PYTHON -m pip install -e cpp
$PYTHON -c "import narrowgate_cpp; print(narrowgate_cpp.__file__)"
```

Real-data golden replay tests are intentionally opt-in:

```bash
RUN_NARROWGATE_GOLDEN=1 $PYTHON -m pytest tests/test_cpp_tick_replay_golden_parity.py -q
```

The repository requires Python 3.11 or newer. The hosted GitHub Actions matrix covers Python 3.11 and the live-runtime minor 3.12. It runs synthetic/native parity tests but does not have the external `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` market-data identity required by the real-data golden windows. A skipped golden test in hosted CI is therefore an explicit data-availability boundary, not a recorded golden pass; release evidence must attach the separate `RUN_NARROWGATE_GOLDEN=1` result and its input hashes.
