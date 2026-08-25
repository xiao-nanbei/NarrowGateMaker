import logging
import os
from types import SimpleNamespace
from pathlib import Path

import pytest

from live import main


def test_successor_cpp_module_token_is_derived_and_conflict_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected_venv = tmp_path / f"venv-{'a' * 40}"
    expected_venv.mkdir()
    expected_token = f"{expected_venv.resolve()}{os.sep}"

    monkeypatch.setenv(main.CPP_MODULE_TOKEN_ENV, "")
    assert main._bind_successor_cpp_module_token(expected_venv) == expected_token  # noqa: SLF001
    assert os.environ[main.CPP_MODULE_TOKEN_ENV] == expected_token

    assert main._bind_successor_cpp_module_token(expected_venv) == expected_token  # noqa: SLF001

    monkeypatch.setenv(main.CPP_MODULE_TOKEN_ENV, "/hostile/runtime/")
    with pytest.raises(RuntimeError, match="differs from successor authority"):
        main._bind_successor_cpp_module_token(expected_venv)  # noqa: SLF001


def _clear_flags(monkeypatch) -> None:
    for name in main.CPP_RUNTIME_FLAGS:
        monkeypatch.setenv(name, "0")


def test_python_profile_does_not_import_extension(monkeypatch) -> None:
    _clear_flags(monkeypatch)
    monkeypatch.setenv("NARROWGATE_LIVE_PROFILE_NAME", "python")
    monkeypatch.setattr(
        main.importlib,
        "import_module",
        lambda name: (_ for _ in ()).throw(AssertionError(name)),
    )

    result = main.audit_native_runtime(logging.getLogger("profile-test"))

    assert result["profile"] == "python"
    assert result["module"] == "disabled"


def test_strict_native_profile_fails_when_required_api_is_missing(monkeypatch) -> None:
    _clear_flags(monkeypatch)
    monkeypatch.setenv("NARROWGATE_CPP_LIVE_ROUTING", "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    monkeypatch.setenv("NARROWGATE_LIVE_PROFILE_NAME", "native")
    monkeypatch.setattr(
        main.importlib,
        "import_module",
        lambda name: SimpleNamespace(__file__="fake.so"),
    )

    with pytest.raises(RuntimeError, match="missing APIs"):
        main.audit_native_runtime(logging.getLogger("profile-test"))


def test_strict_native_profile_fails_before_market_start_on_old_quote_abi(monkeypatch) -> None:
    _clear_flags(monkeypatch)
    monkeypatch.setenv("NARROWGATE_CPP_QUOTE_CORE", "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")

    class OldQuoteFlags:
        delta_cap = False
        final_compressed = False

    class OldSideQuoteContext:
        pass

    monkeypatch.setattr(
        main.importlib,
        "import_module",
        lambda name: SimpleNamespace(
            __file__="old.so",
            compute_quote_core_live=lambda *args: None,
            QuoteFlags=OldQuoteFlags,
            SideQuoteContext=OldSideQuoteContext,
        ),
    )

    with pytest.raises(RuntimeError, match="ABI missing fields"):
        main.audit_native_runtime(logging.getLogger("profile-test"))


def test_strict_global_flow_profile_requires_batch_abi(monkeypatch) -> None:
    _clear_flags(monkeypatch)
    monkeypatch.setenv("NARROWGATE_CPP_GLOBAL_FLOW", "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")

    class OldTradeBarAggregator:
        def __init__(self, _track_runs):
            pass

    monkeypatch.setattr(
        main.importlib,
        "import_module",
        lambda name: SimpleNamespace(
            __file__="old.so",
            NativeGlobalFlowEngine=object,
            TradeBarAggregator=OldTradeBarAggregator,
        ),
    )

    with pytest.raises(RuntimeError, match="TradeBarAggregator.update_batch"):
        main.audit_native_runtime(logging.getLogger("profile-test"))
