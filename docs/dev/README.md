# Developer Notes

<p><a href="README.md">English</a> | <a href="README.zh-CN.md">简体中文</a></p>

Last materially modified: 2026-08-30

Last materially synchronized: 2026-08-30

Developer docs cover local installation, tests, linting, C++ extension builds, and contribution workflow.

- Maintained local checks: [ci.md](ci.md)
- Hosted branch protection uses one root `CI admission` check; worker jobs are path-specific implementation details.
- Package and optional dependency contract: repository `pyproject.toml`
