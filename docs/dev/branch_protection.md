# Branch Protection

Last materially modified: 2026-08-30

Status: Current maintainer configuration for the public `main` branch.

## Required Checks

Protect `main` with one exact GitHub Actions job name from `.github/workflows/ci.yml`:

- `CI admission`

GitHub may render this check as `CI / CI admission` in the pull-request interface. Do not configure a step label or an internal path-specific job as a required context. `CI admission` runs after the classifier and every applicable boundary and fails when any non-skipped dependency fails or is cancelled.

For an ordinary code pull request, the base job installs only `-e .` under Python 3.11, the quality job runs repository-wide F/B correctness lint once, and the Python 3.12 job builds the C++ extension once before running the complete public suite including native parity. A documentation-only change runs the documentation audit without installing research/native dependencies. Scheduled and manually dispatched full runs add one complete Python 3.11 suite with its own ABI-compatible native build; they do not duplicate a standalone C++ job.

## Recommended Ruleset

Require a pull request, at least one approval, resolution of review conversations, `CI admission`, and a branch that is up to date before merge. Block force pushes and branch deletion. Dismiss stale approvals when new commits materially change reviewed code, and apply the rules to administrators unless an emergency process is separately documented.

Required checks are a repository-host setting and cannot be created by the workflow file alone. During this migration, first let `CI admission` run successfully on the hosted repository, then add it to the ruleset and remove the retired `Base install smoke`, `Python tests and lint (3.11)`, `Python tests and lint (3.12)`, and `C++ extension build smoke` contexts. Never remove the old contexts before the new root check exists.
