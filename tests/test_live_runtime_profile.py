import logging
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from live import main
from strategy import maker_engine
from strategy.model_contract import REQUIRED_MODEL_HEADS


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
    with pytest.raises(RuntimeError, match="differs from deployment authority"):
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


def test_persisted_profiles_select_native_cooldown_explicitly() -> None:
    profile_root = Path(main.__file__).resolve().parent / "profiles"
    run_sh = (profile_root.parent / "run.sh").read_text(encoding="utf-8")

    native = (profile_root / "native.env").read_text(encoding="utf-8")
    python = (profile_root / "python.env").read_text(encoding="utf-8")

    assert 'export NARROWGATE_CPP_COOLDOWN="1"' in native
    assert 'export NARROWGATE_CPP_COOLDOWN="0"' in python
    assert "NARROWGATE_CPP_COOLDOWN=${NARROWGATE_CPP_COOLDOWN:-0}" in run_sh
    assert "cpp_cooldown=${NARROWGATE_CPP_COOLDOWN:-0}" in run_sh
    assert 'export NARROWGATE_CPP_ORDER_ACTION_PLAN="1"' in native
    assert 'export NARROWGATE_CPP_ORDER_ACTION_PLAN="0"' in python
    assert (
        "NARROWGATE_CPP_ORDER_ACTION_PLAN=${NARROWGATE_CPP_ORDER_ACTION_PLAN:-0}"
        in run_sh
    )
    assert (
        "cpp_order_action_plan=${NARROWGATE_CPP_ORDER_ACTION_PLAN:-0}"
        in run_sh
    )
    assert 'export NARROWGATE_CPP_FINAL_ORDER_PLAN="0"' in native
    assert 'export NARROWGATE_CPP_FINAL_ORDER_PLAN="0"' in python
    assert (
        "NARROWGATE_CPP_FINAL_ORDER_PLAN=${NARROWGATE_CPP_FINAL_ORDER_PLAN:-0}"
        in run_sh
    )
    assert (
        "cpp_final_order_plan=${NARROWGATE_CPP_FINAL_ORDER_PLAN:-0}"
        in run_sh
    )
    assert 'export NARROWGATE_CPP_QUOTE_POLICY_STAGE="1"' in native
    assert 'export NARROWGATE_CPP_QUOTE_POLICY_STAGE="0"' in python
    assert (
        "NARROWGATE_CPP_QUOTE_POLICY_STAGE=${NARROWGATE_CPP_QUOTE_POLICY_STAGE:-0}"
        in run_sh
    )
    assert (
        "cpp_quote_policy_stage=${NARROWGATE_CPP_QUOTE_POLICY_STAGE:-0}"
        in run_sh
    )
    assert 'export NARROWGATE_CPP_LIGHTGBM_INFERENCE="1"' in native
    assert 'export NARROWGATE_CPP_LIGHTGBM_INFERENCE="0"' in python
    assert (
        "NARROWGATE_CPP_LIGHTGBM_INFERENCE="
        "${NARROWGATE_CPP_LIGHTGBM_INFERENCE:-0}"
        in run_sh
    )
    assert (
        "cpp_lightgbm_inference=${NARROWGATE_CPP_LIGHTGBM_INFERENCE:-0}"
        in run_sh
    )


def test_native_lightgbm_inference_is_bound_independently(monkeypatch) -> None:
    _clear_flags(monkeypatch)
    monkeypatch.setenv("NARROWGATE_CPP_LIGHTGBM_INFERENCE", "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    fake_module = SimpleNamespace(
        __file__="native-lightgbm.so",
        NativeLightgbmBundle=object,
        LIGHTGBM_BUNDLE_HEAD_NAMES=REQUIRED_MODEL_HEADS,
        NATIVE_LIGHTGBM_BUNDLE_INFERENCE_AVAILABLE=True,
    )
    monkeypatch.setattr(main.importlib, "import_module", lambda name: fake_module)

    result = main.audit_native_runtime(logging.getLogger("profile-test"))

    assert result["NARROWGATE_CPP_LIGHTGBM_INFERENCE"] is True
    assert set(result["abi_contract"]["required_apis"]) == {
        "LIGHTGBM_BUNDLE_HEAD_NAMES",
        "NATIVE_LIGHTGBM_BUNDLE_INFERENCE_AVAILABLE",
        "NativeLightgbmBundle",
    }


def test_native_lightgbm_live_inference_requires_strict_mode(monkeypatch) -> None:
    _clear_flags(monkeypatch)
    monkeypatch.setenv("NARROWGATE_CPP_LIGHTGBM_INFERENCE", "1")

    with pytest.raises(RuntimeError, match="requires strict native mode"):
        main.audit_native_runtime(logging.getLogger("profile-test"))


def test_native_lightgbm_inference_rejects_head_order_drift(monkeypatch) -> None:
    _clear_flags(monkeypatch)
    monkeypatch.setenv("NARROWGATE_CPP_LIGHTGBM_INFERENCE", "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    fake_module = SimpleNamespace(
        __file__="native-lightgbm.so",
        NativeLightgbmBundle=object,
        LIGHTGBM_BUNDLE_HEAD_NAMES=tuple(reversed(REQUIRED_MODEL_HEADS)),
        NATIVE_LIGHTGBM_BUNDLE_INFERENCE_AVAILABLE=True,
    )
    monkeypatch.setattr(main.importlib, "import_module", lambda name: fake_module)

    with pytest.raises(RuntimeError, match="head order differs"):
        main.audit_native_runtime(logging.getLogger("profile-test"))


def test_native_quote_policy_stage_is_bound_into_runtime_identity(monkeypatch) -> None:
    _clear_flags(monkeypatch)
    monkeypatch.setenv("NARROWGATE_CPP_QUOTE_POLICY_STAGE", "1")
    fake_module = SimpleNamespace(
        __file__="native-quote-policy.so",
        NativeQuotePolicyStage=object,
        NativeQuotePolicyStageResult=object,
        NATIVE_QUOTE_POLICY_STAGE_AVAILABLE=True,
    )
    monkeypatch.setattr(main.importlib, "import_module", lambda name: fake_module)

    result = main.audit_native_runtime(logging.getLogger("profile-test"))

    assert result["NARROWGATE_CPP_QUOTE_POLICY_STAGE"] is True
    assert set(result["abi_contract"]["required_apis"]) == {
        "NativeQuotePolicyStage",
        "NativeQuotePolicyStageResult",
        "NATIVE_QUOTE_POLICY_STAGE_AVAILABLE",
    }


def test_native_quote_policy_stage_rejects_unsupported_live_policy(monkeypatch) -> None:
    _clear_flags(monkeypatch)
    monkeypatch.setenv("NARROWGATE_CPP_QUOTE_CORE", "1")
    monkeypatch.setenv("NARROWGATE_CPP_QUOTE_POLICY_STAGE", "1")
    cfg = SimpleNamespace(
        strategy=SimpleNamespace(
            ber_exposure_add_only=False,
            local_extreme_guard_enabled=True,
            fragile_order_ttl_s=0.0,
        )
    )
    with pytest.raises(RuntimeError, match="local-extreme"):
        main.audit_native_runtime(logging.getLogger("profile-test"), cfg=cfg)


def test_native_cooldown_is_bound_into_runtime_identity(monkeypatch) -> None:
    _clear_flags(monkeypatch)
    monkeypatch.setenv("NARROWGATE_CPP_COOLDOWN", "1")
    fake_module = SimpleNamespace(
        __file__="native-cooldown.so",
        NATIVE_LIVE_COOLDOWN_HOT_PATH_AVAILABLE=True,
        **{
            name: object
            for name in main.NATIVE_COOLDOWN_REQUIRED_APIS
            if name != "NATIVE_LIVE_COOLDOWN_HOT_PATH_AVAILABLE"
        },
    )
    monkeypatch.setattr(main.importlib, "import_module", lambda name: fake_module)

    result = main.audit_native_runtime(logging.getLogger("profile-test"))

    assert result["NARROWGATE_CPP_COOLDOWN"] is True
    assert set(result["abi_contract"]["required_apis"]) == set(
        main.NATIVE_COOLDOWN_REQUIRED_APIS
    )


def test_native_order_action_plan_is_bound_into_runtime_identity(
    monkeypatch,
) -> None:
    _clear_flags(monkeypatch)
    monkeypatch.setenv("NARROWGATE_CPP_ORDER_ACTION_PLAN", "1")
    fake_module = SimpleNamespace(
        __file__="native-order-action.so",
        NATIVE_LIVE_ORDER_ACTION_PLAN_AVAILABLE=True,
        **{
            name: object
            for name in main.NATIVE_ORDER_ACTION_REQUIRED_APIS
            if name != "NATIVE_LIVE_ORDER_ACTION_PLAN_AVAILABLE"
        },
    )
    monkeypatch.setattr(main.importlib, "import_module", lambda name: fake_module)

    result = main.audit_native_runtime(logging.getLogger("profile-test"))

    assert result["NARROWGATE_CPP_ORDER_ACTION_PLAN"] is True
    assert set(result["abi_contract"]["required_apis"]) == set(
        main.NATIVE_ORDER_ACTION_REQUIRED_APIS
    )


def test_native_final_order_plan_is_default_off_and_bound_independently(
    monkeypatch,
) -> None:
    _clear_flags(monkeypatch)
    monkeypatch.setenv("NARROWGATE_CPP_FINAL_ORDER_PLAN", "1")
    required = (
        main.NATIVE_ORDER_ACTION_REQUIRED_APIS
        | main.NATIVE_FINAL_ORDER_PLAN_REQUIRED_APIS
    )
    fake_module = SimpleNamespace(
        __file__="native-final-order.so",
        NATIVE_LIVE_ORDER_ACTION_PLAN_AVAILABLE=True,
        NATIVE_LIVE_FINAL_ORDER_PLAN_AVAILABLE=True,
        **{
            name: object
            for name in required
            if name
            not in {
                "NATIVE_LIVE_ORDER_ACTION_PLAN_AVAILABLE",
                "NATIVE_LIVE_FINAL_ORDER_PLAN_AVAILABLE",
            }
        },
    )
    monkeypatch.setattr(main.importlib, "import_module", lambda name: fake_module)

    result = main.audit_native_runtime(logging.getLogger("profile-test"))

    assert result["NARROWGATE_CPP_FINAL_ORDER_PLAN"] is True
    assert set(result["abi_contract"]["required_apis"]) == set(required)


@pytest.mark.parametrize("mode", ["inventory_shift", "flow_add_widen", "hybrid"])
def test_native_live_routing_rejects_non_noop_post_fill_before_import(
    monkeypatch,
    mode: str,
) -> None:
    _clear_flags(monkeypatch)
    monkeypatch.setenv("NARROWGATE_CPP_LIVE_ROUTING", "1")
    cfg = SimpleNamespace(
        strategy=SimpleNamespace(
            post_fill_quote_response_enabled=True,
            post_fill_quote_response_mode=mode,
        )
    )
    monkeypatch.setattr(
        main.importlib,
        "import_module",
        lambda name: (_ for _ in ()).throw(AssertionError(name)),
    )

    with pytest.raises(RuntimeError, match="cannot preserve non-noop post-fill"):
        main.audit_native_runtime(logging.getLogger("profile-test"), cfg=cfg)


def test_native_live_routing_allows_inactive_post_fill_mode(monkeypatch) -> None:
    _clear_flags(monkeypatch)
    monkeypatch.setenv("NARROWGATE_CPP_LIVE_ROUTING", "1")
    cfg = SimpleNamespace(
        strategy=SimpleNamespace(
            post_fill_quote_response_enabled=False,
            post_fill_quote_response_mode="inventory_shift",
        )
    )
    fake_module = SimpleNamespace(
        __file__="native-routing.so",
        compute_live_routing_decision=lambda *args: None,
    )
    monkeypatch.setattr(main.importlib, "import_module", lambda name: fake_module)

    result = main.audit_native_runtime(logging.getLogger("profile-test"), cfg=cfg)

    assert result["NARROWGATE_CPP_LIVE_ROUTING"] is True


def test_explicit_order_action_profile_rejects_disabled_capability(
    monkeypatch,
) -> None:
    _clear_flags(monkeypatch)
    monkeypatch.setenv("NARROWGATE_CPP_ORDER_ACTION_PLAN", "1")
    fake_module = SimpleNamespace(
        __file__="old.so",
        NATIVE_LIVE_ORDER_ACTION_PLAN_AVAILABLE=False,
        **{
            name: object
            for name in main.NATIVE_ORDER_ACTION_REQUIRED_APIS
            if name != "NATIVE_LIVE_ORDER_ACTION_PLAN_AVAILABLE"
        },
    )
    monkeypatch.setattr(main.importlib, "import_module", lambda name: fake_module)

    with pytest.raises(RuntimeError, match="order-action planner capability"):
        main.audit_native_runtime(logging.getLogger("profile-test"))


def test_live_routing_loader_preserves_strict_and_optional_fallback(
    monkeypatch,
) -> None:
    fake_module = SimpleNamespace(compute_live_routing_decision=lambda *args: None)
    monkeypatch.setenv("NARROWGATE_CPP_LIVE_ROUTING", "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "0")
    monkeypatch.setitem(sys.modules, "narrowgate_cpp", fake_module)
    monkeypatch.setattr(maker_engine, "_live_routing_cpp", None)
    monkeypatch.setattr(maker_engine, "_live_routing_cpp_failed", False)
    assert maker_engine._get_live_routing_cpp() is fake_module

    old_module = SimpleNamespace()
    monkeypatch.setitem(sys.modules, "narrowgate_cpp", old_module)
    monkeypatch.setattr(maker_engine, "_live_routing_cpp", None)
    monkeypatch.setattr(maker_engine, "_live_routing_cpp_failed", False)
    assert maker_engine._get_live_routing_cpp() is None
    assert maker_engine._live_routing_cpp_failed is True

    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    monkeypatch.setattr(maker_engine, "_live_routing_cpp_failed", False)
    with pytest.raises(RuntimeError, match="compute_live_routing_decision"):
        maker_engine._get_live_routing_cpp()


def test_explicit_order_action_loader_never_silently_falls_back(
    monkeypatch,
) -> None:
    monkeypatch.setenv("NARROWGATE_CPP_ORDER_ACTION_PLAN", "1")
    monkeypatch.setitem(sys.modules, "narrowgate_cpp", SimpleNamespace())
    monkeypatch.setattr(maker_engine, "_live_order_action_plan_cpp", None)

    with pytest.raises(RuntimeError, match="ABI is incomplete"):
        maker_engine._get_live_order_action_plan_cpp()


def test_explicit_order_action_loader_returns_complete_module(monkeypatch) -> None:
    fake_module = SimpleNamespace(
        NATIVE_LIVE_ORDER_ACTION_PLAN_AVAILABLE=True,
        **{
            name: object
            for name in main.NATIVE_ORDER_ACTION_REQUIRED_APIS
            if name != "NATIVE_LIVE_ORDER_ACTION_PLAN_AVAILABLE"
        },
    )
    monkeypatch.setenv("NARROWGATE_CPP_ORDER_ACTION_PLAN", "1")
    monkeypatch.setitem(sys.modules, "narrowgate_cpp", fake_module)
    monkeypatch.setattr(maker_engine, "_live_order_action_plan_cpp", None)

    assert maker_engine._get_live_order_action_plan_cpp() is fake_module


def test_native_runtime_reports_compiled_cpu_profile(monkeypatch) -> None:
    _clear_flags(monkeypatch)
    monkeypatch.setenv("NARROWGATE_CPP_LIVE_ROUTING", "1")
    monkeypatch.setattr(
        main.importlib,
        "import_module",
        lambda name: SimpleNamespace(
            __file__="fake.so",
            compute_live_routing_decision=lambda *args: None,
            NATIVE_LIVE_BUILD_PROFILE="ec2-cascadelake-avx2",
            NATIVE_LIVE_BUILD_COMPILE_OPTIONS=(
                "-O3 -march=haswell -mtune=cascadelake "
                "-mprefer-vector-width=256 -fno-fast-math "
                "-ffp-contract=off -fno-lto"
            ),
            NATIVE_LIVE_BUILD_IS_PRODUCTION=True,
            NATIVE_LIVE_BUILD_VECTOR_WIDTH_BITS=256,
        ),
    )

    result = main.audit_native_runtime(logging.getLogger("profile-test"))

    assert result["native_build"] == {
        "available": True,
        "profile": "ec2-cascadelake-avx2",
        "compile_options": (
            "-O3 -march=haswell -mtune=cascadelake "
            "-mprefer-vector-width=256 -fno-fast-math "
            "-ffp-contract=off -fno-lto"
        ),
        "production": True,
        "preferred_vector_width_bits": 256,
    }


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


def test_strict_native_profile_requires_replace_continuation_abi(monkeypatch) -> None:
    _clear_flags(monkeypatch)
    monkeypatch.setenv("NARROWGATE_CPP_REPLACE_CONTINUATION", "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    monkeypatch.setattr(
        main.importlib,
        "import_module",
        lambda name: SimpleNamespace(__file__="old.so"),
    )

    with pytest.raises(RuntimeError, match="missing APIs"):
        main.audit_native_runtime(logging.getLogger("profile-test"))


def test_strict_native_profile_requires_cooldown_abi(monkeypatch) -> None:
    _clear_flags(monkeypatch)
    monkeypatch.setenv("NARROWGATE_CPP_COOLDOWN", "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    monkeypatch.setattr(
        main.importlib,
        "import_module",
        lambda name: SimpleNamespace(__file__="old.so"),
    )

    with pytest.raises(RuntimeError, match="missing APIs"):
        main.audit_native_runtime(logging.getLogger("profile-test"))


def test_strict_native_profile_rejects_disabled_cooldown_capability(monkeypatch) -> None:
    _clear_flags(monkeypatch)
    monkeypatch.setenv("NARROWGATE_CPP_COOLDOWN", "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    api_names = {
        "F05BooleanClause",
        "F05BooleanLiteral",
        "F05BooleanPolicy",
        "F05BooleanRule",
        "F05PredicateDefinition",
        "F05PredicateMetric",
        "F05PredicatePair",
        "LiveCooldownDecisionStatus",
        "LiveCooldownProfile",
        "NativeLiveCooldownHotPath",
    }
    fake_module = SimpleNamespace(
        __file__="old.so",
        NATIVE_LIVE_COOLDOWN_HOT_PATH_AVAILABLE=False,
        **{name: object for name in api_names},
    )
    monkeypatch.setattr(main.importlib, "import_module", lambda name: fake_module)

    with pytest.raises(RuntimeError, match="cooldown hot-path capability"):
        main.audit_native_runtime(logging.getLogger("profile-test"))


def test_explicit_native_replace_continuation_never_silently_falls_back(
    monkeypatch,
) -> None:
    class OldContinuation:
        pass

    fake_module = SimpleNamespace(
        NativeReplaceContinuationState=lambda _enabled: OldContinuation(),
        ReplaceContinuationEventKind=object(),
        Side=SimpleNamespace(Buy=0, Sell=1),
    )
    monkeypatch.setenv("NARROWGATE_CPP_REPLACE_CONTINUATION", "1")
    monkeypatch.setitem(sys.modules, "narrowgate_cpp", fake_module)

    with pytest.raises(RuntimeError, match="ABI is incomplete"):
        maker_engine._build_native_replace_continuation_state()
