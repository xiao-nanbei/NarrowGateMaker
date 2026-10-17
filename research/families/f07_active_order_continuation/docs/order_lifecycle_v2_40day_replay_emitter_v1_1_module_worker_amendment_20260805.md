# F07 Lifecycle-v2 40-Day Replay Emitter v1.1

Last materially modified: 2026-08-05

Status: worker import repair implemented; formal 40-day replay not yet executed.

The first execution attempt stopped before replaying `2026-04-17`. The parent used an absolute script path for its fresh-day child, so Python replaced the repository root on `sys.path` with the nested audit directory and could not import `models`. No journal row or economic outcome was admitted.

v1.1 invokes the same worker as a Python module from the repository root. It does not change the frozen 40-day denominator, source identity, state reset, journal schema, economic firewall, or downstream permissions. The failed v1 plan is preserved; v1.1 must prepare a new hash-bound plan before execution.
