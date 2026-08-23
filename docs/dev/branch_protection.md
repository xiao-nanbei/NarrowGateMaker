# Branch Protection

Last materially modified: 2026-08-23

Status: Current maintainer configuration for the public `main` branch.

## Required Checks

Protect `main` with the following exact GitHub Actions job names from `.github/workflows/ci.yml`:

- `Base install smoke`
- `Python tests and lint (3.11)`
- `Python tests and lint (3.12)`
- `C++ extension build smoke`

GitHub may render a check as `CI / <job name>` in the pull-request interface; the stable required-check identity is the job name listed above. Do not configure a step label such as `Pytest` or the matrix template `Python tests and lint (${{ matrix.python-version }})` as a required context.

The base job installs only `-e .` in a clean Python 3.11 runner and proves that the documented demo workflow does not rely on research, live, provider, or runner-global dependencies. Both Python matrix jobs run blocking pytest, lint, and the public documentation audit with `.[all]`; the C++ job independently builds and exercises the extension.

## Recommended Ruleset

Require a pull request, at least one approval, resolution of review conversations, all four checks above, and a branch that is up to date before merge. Block force pushes and branch deletion. Dismiss stale approvals when new commits materially change reviewed code, and apply the rules to administrators unless an emergency process is separately documented.

Required checks are a repository-host setting and cannot be created by the workflow file alone. After changing a job `name`, update this page and the onboarding contract test in the same pull request, then change the hosted ruleset only after the renamed check has run once.
