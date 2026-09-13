# Developer Checks

Last materially modified: 2026-08-30

> Publication note: `${NARROWGATE_*}` values and deployment-epoch names are logical locators. Owner-side data and machine artifacts are in the private evidence store and are not distributed with this repository unless a repository-relative link is provided. See the [public/private documentation contract](../public_private_documentation_contract.md).

Create a Python 3.11-or-newer environment and install the complete contributor target before running the full suite:

```bash
python3.11 -m venv .venv  # only when .venv does not already exist
PYTHON=.venv/bin/python
$PYTHON -m pip install -e ".[all]"
$PYTHON -m pytest -q
git ls-files -z '*.py' | xargs -0 -r "$PYTHON" -m ruff check --
$PYTHON -m py_compile narrowgate/cli.py
$PYTHON scripts/audit_public_documentation.py --repo-root .
git diff --check
```

The full test suite imports offline research modules and read-only public venue connectors, so `all` is the contributor contract even when no network request or live order is made. The separate `Base install smoke` CI job installs only the base package and runs the documented no-data CLI, replay demo, order-level example, and onboarding tests; it guards against accidental optional-dependency leakage.

The exact required GitHub check and recommended `main` ruleset are frozen in [Branch Protection](branch_protection.md). Only the root `CI admission` result belongs in branch protection; path-specific worker jobs remain implementation details.

Historical reproduction tests whose exact predecessor bytes, temporary deployment trees, or owner-private execution configurations are not distributed are listed in `tests/fixtures/public_clone_historical_test_availability.json`. Public discovery ignores complete unavailable modules and deselects unavailable nodes from otherwise public modules; neither state grants research, action, or live authority. After restoring every bound fixture, an owner may opt in with `NARROWGATE_RUN_HISTORICAL_REPRODUCTION_TESTS=1`; setting that variable without the exact evidence is expected to fail closed.

C++ extension smoke:

```bash
$PYTHON -m pip install -e cpp
$PYTHON -c "import narrowgate_cpp; print(narrowgate_cpp.__file__)"
```

Real-data golden replay tests are intentionally opt-in:

```bash
RUN_NARROWGATE_GOLDEN=1 $PYTHON -m pytest tests/test_cpp_tick_replay_golden_parity.py -q
```

The repository requires Python 3.11 or newer. Ordinary hosted CI uses Python 3.11 for the base install/CLI compatibility boundary and Python 3.12 for the complete public suite with one native build. Scheduled and manually dispatched full runs additionally run the complete suite with a Python 3.11 native build. Hosted CI does not have the external `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` market-data identity required by real-data golden windows. A skipped golden test is therefore an explicit data-availability boundary, not a recorded golden pass; release evidence must attach the separate `RUN_NARROWGATE_GOLDEN=1` result and its input hashes.
