"""One import and ABI boundary for native strategy consumers.

Only an absent optional extension selects Python. Broken imports, incomplete
ABIs and calculation failures are errors, not backend-selection signals.
Deployment attests the loaded binary against its existing release receipt;
this module does not invent a second build identity or hash tracked files.
"""

from __future__ import annotations

import importlib
import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

NATIVE_REPLACE_CONTINUATION_METHODS = (
    "arm", "publish", "clear_exact", "clear_side", "clear_unready",
    "take_ready", "finalize_decision", "drop_in_flight", "clear_all", "telemetry",
)


def load_native_module(*, optional: bool = False) -> Any:
    """Load the installed extension, respecting an explicit deployment root.

    Python's import cache provides the single module object. The legacy token
    environment variable now denotes an absolute directory, never a substring
    guessed from a checkout or distribution name.
    """
    try:
        module = importlib.import_module("narrowgate_cpp")
    except ModuleNotFoundError as exc:
        if optional and exc.name == "narrowgate_cpp":
            return None
        raise
    expected_root = os.environ.get("NARROWGATE_CPP_EXPECT_MODULE_TOKEN", "").strip()
    if expected_root:
        root = Path(expected_root).expanduser()
        source = Path(str(getattr(module, "__file__", "")))
        if (
            not root.is_absolute()
            or not source.is_absolute()
            or not source.resolve().is_relative_to(root.resolve())
        ):
            raise RuntimeError("narrowgate_cpp module is outside the admitted runtime root")
    return module


def validate_native_capabilities(
    module: Any,
    *,
    symbols: Sequence[str] = (),
    methods: Mapping[str, Sequence[str]] | None = None,
    fields: Mapping[str, Sequence[str]] | None = None,
    abi_versions: Mapping[str, Any] | None = None,
) -> None:
    """Validate an entry point's required contract before constructing state."""
    missing = [name for name in symbols if not hasattr(module, name)]
    for class_name, names in (methods or {}).items():
        cls = getattr(module, class_name, None)
        missing.extend(
            f"{class_name}.{name}"
            for name in names
            if not callable(getattr(cls, name, None))
        )
    for class_name, names in (fields or {}).items():
        cls = getattr(module, class_name, None)
        instance = cls() if cls is not None else None
        missing.extend(
            f"{class_name}.{name}"
            for name in names
            if instance is None or not hasattr(instance, name)
        )
    if missing:
        raise RuntimeError("narrowgate_cpp ABI missing APIs/fields: " + ", ".join(missing))
    for name, expected in (abi_versions or {}).items():
        actual = getattr(module, name, None)
        if actual != expected:
            raise RuntimeError(
                f"narrowgate_cpp {name} ABI mismatch: expected={expected!r} actual={actual!r}"
            )


def validate_replace_continuation(module: Any) -> Any:
    """Return a checked continuation owner for both startup and direct engines."""
    validate_native_capabilities(
        module,
        symbols=("NativeReplaceContinuationState", "ReplaceContinuationEventKind", "Side"),
    )
    state = module.NativeReplaceContinuationState(True)
    missing = [
        name for name in NATIVE_REPLACE_CONTINUATION_METHODS
        if not callable(getattr(state, name, None))
    ]
    missing.extend(
        f"Side.{name}" for name in ("Buy", "Sell") if not hasattr(module.Side, name)
    )
    if missing:
        raise RuntimeError("narrowgate_cpp continuation ABI missing methods: " + ", ".join(missing))
    return state
