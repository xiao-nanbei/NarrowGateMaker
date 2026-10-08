# Developer Checks

[English](ci.md) | [简体中文](ci.zh-CN.md)

Last materially modified: 2026-09-21

Last materially synchronized: 2026-09-21

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

The recommended required check and `main` ruleset are described under [Branch protection](#branch-protection). Only the root `CI admission` result belongs in branch protection; path-specific worker jobs remain implementation details.

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

The repository requires Python 3.11 or newer. Ordinary hosted CI uses Python 3.11 for the base install/CLI compatibility boundary and Python 3.12 for the complete public suite with one native build. Manual runs default to the primary suite; select `compatibility` to add the Python 3.11 full suite. Nightly checks run both versions in NarrowGateMaker only. New push/PR runs cancel older runs of the same event type and ref; manual and nightly checks are separate.

NarrowGateMaker is the primary regression repository. On a NarrowGateMaker-private push, the workflow compares its tree with primary main through the GitHub API. Only an exact match delegates the duplicate full regression to primary; missing evidence, API failure or different trees require full regression locally. Mirror PRs and manual runs always retain full regression. Applicable public correctness/style/documentation, frontend and base packaging checks remain independent in both repositories. Mirror success alone does not prove the primary suite passed: check the matching primary run. No job failure is converted to success by admission.

Hosted CI does not have the external `${NARROWGATE_PRIVATE_EVIDENCE_ROOT}` market-data identity required by real-data golden windows. A skipped golden test is therefore an explicit data-availability boundary, not a recorded golden pass; release evidence must attach the separate `RUN_NARROWGATE_GOLDEN=1` result and its input hashes. Save source changes locally using the current alpha amend convention; push only when explicitly requested for that task.

## Branch protection

These are maintainer recommendations, not a claim that hosted settings are enabled. Verify actual GitHub rules separately; this consolidation changes no hosted setting.

### Required Checks

Protect `main` with one exact GitHub Actions job name from `.github/workflows/ci.yml`:

- `CI admission`

GitHub may render this check as `CI / CI admission` in the pull-request interface. Do not configure a step label or an internal path-specific job as a required context. `CI admission` runs after the classifier and every applicable boundary and fails when any non-skipped dependency fails or is cancelled.

For an ordinary code pull request, the base job installs only `-e .` under Python 3.11, the quality job runs repository-wide F/B correctness lint once, and the Python 3.12 job builds the C++ extension once before running the complete public suite including native parity. A documentation-only change runs the documentation audit without installing research/native dependencies. Primary nightly runs and manually selected `compatibility` runs add the Python 3.11 full suite with its own native build; ordinary manual runs do not. Exact-tree mirror pushes delegate full regression to the primary repository, while preserving their applicable packaging and public-boundary checks. Mirror admission alone is not primary regression evidence. See [Developer Checks](ci.md) for fallback behavior and cancellation policy.

### Recommended Ruleset

Require a pull request, at least one approval, resolution of review conversations, `CI admission`, and a branch that is up to date before merge. Block force pushes and branch deletion. Dismiss stale approvals when new commits materially change reviewed code, and apply the rules to administrators unless an emergency process is separately documented.

Required checks are a repository-host setting and cannot be created by the workflow file alone. During this migration, first let `CI admission` run successfully on the hosted repository, then add it to the ruleset and remove the retired `Base install smoke`, `Python tests and lint (3.11)`, `Python tests and lint (3.12)`, and `C++ extension build smoke` contexts. Never remove the old contexts before the new root check exists.
