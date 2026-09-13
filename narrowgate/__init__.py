"""Public package facade for NarrowGate.

The research code intentionally remains in the existing top-level modules
(`models`, `strategy`, `data`, `features`, and `live`) so older workflows keep
working.  This package provides a stable CLI/import surface for public users.
"""

__all__ = ["__version__"]

__version__ = "0.1.2.dev0"
