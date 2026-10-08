# F07 Lifecycle-v2 40-Day Replay Emitter v1.2

Last materially modified: 2026-08-05

Status: virtual-environment entrypoint repair implemented; formal 40-day replay not yet executed.

The v1.1 worker reached the repository import surface but stopped before replay because `Path.resolve()` dereferenced `.venv/bin/python` to the bundled base interpreter. That bypassed `pyvenv.cfg`, so the fresh process could not see the project's `pyarrow` installation. No journal row or economic outcome was admitted.

v1.2 makes the entrypoint absolute without dereferencing the symlink. The frozen denominator, source data, lifecycle semantics, journal schema, and economic firewall remain unchanged. A new execution plan is required because the runner implementation hash changed.
