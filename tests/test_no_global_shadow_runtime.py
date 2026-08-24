from __future__ import annotations

import ast
import hashlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np
import pytest

from live import config as live_config
from live import main as live_main
from strategy import maker_engine as maker_engine_module
from strategy import signal as signal_module
from strategy.signal import SignalEngine

ROOT = Path(__file__).resolve().parents[1]


def _function_ast_sha256(relative: str, class_name: str, function_name: str) -> str:
    tree = ast.parse((ROOT / relative).read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for child in node.body:
                if (
                    isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and child.name == function_name
                ):
                    return hashlib.sha256(
                        ast.dump(child, include_attributes=False).encode("utf-8")
                    ).hexdigest()
    raise AssertionError(f"missing {class_name}.{function_name}")


def test_multi_market_shadow_flags_fail_closed_and_record_explicit_presence() -> None:
    implicit = live_config._parse({})  # noqa: SLF001
    assert implicit.multi_market.global_flow_shadow_enabled is False
    assert implicit.multi_market.global_reference_shadow_enabled is False
    assert implicit.multi_market._global_flow_shadow_enabled_explicit is False
    assert implicit.multi_market._global_reference_shadow_enabled_explicit is False

    explicit = live_config._parse(  # noqa: SLF001
        {
            "multi_market": {
                "global_flow_shadow_enabled": False,
                "global_reference_shadow_enabled": False,
            }
        }
    )
    assert explicit.multi_market._global_flow_shadow_enabled_explicit is True
    assert explicit.multi_market._global_reference_shadow_enabled_explicit is True


def test_config_loader_records_explicit_false(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(
        "multi_market:\n"
        "  global_flow_shadow_enabled: false\n"
        "  global_reference_shadow_enabled: false\n",
        encoding="ascii",
    )
    cfg = live_config.load_config(path)
    assert cfg.multi_market.global_flow_shadow_enabled is False
    assert cfg.multi_market.global_reference_shadow_enabled is False
    assert cfg.multi_market._global_flow_shadow_enabled_explicit is True
    assert cfg.multi_market._global_reference_shadow_enabled_explicit is True
    assert cfg._source_file_path == str(path.resolve())
    assert cfg._source_file_sha256 == hashlib.sha256(path.read_bytes()).hexdigest()
    assert live_config.revalidate_loaded_config_source(cfg, path) == (
        cfg._source_file_sha256
    )


def test_loaded_config_source_revalidation_rejects_post_load_replacement(
    tmp_path: Path,
) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(
        "multi_market:\n"
        "  global_flow_shadow_enabled: false\n"
        "  global_reference_shadow_enabled: false\n",
        encoding="ascii",
    )
    cfg = live_config.load_config(path)
    replacement = tmp_path / "replacement.yaml"
    replacement.write_text(
        "multi_market:\n"
        "  global_flow_shadow_enabled: true\n"
        "  global_reference_shadow_enabled: true\n",
        encoding="ascii",
    )
    replacement.replace(path)

    with pytest.raises(ValueError, match="source file identity drifted"):
        live_config.revalidate_loaded_config_source(cfg, path)


def test_maker_engine_passes_live_shadow_config_explicitly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StopAfterSignal(RuntimeError):
        pass

    captured = {}

    def signal_factory(**kwargs):
        captured.update(kwargs)
        raise StopAfterSignal

    cfg = live_config.Config()
    cfg.multi_market.global_flow_shadow_enabled = False
    cfg.multi_market.global_reference_shadow_enabled = False
    cfg.multi_market._global_flow_shadow_enabled_explicit = True
    cfg.multi_market._global_reference_shadow_enabled_explicit = True
    monkeypatch.setattr(maker_engine_module, "SignalEngine", signal_factory)
    monkeypatch.setattr(maker_engine_module, "_resolve_model_dir", lambda _cfg: ROOT / "models")
    with pytest.raises(StopAfterSignal):
        maker_engine_module.MakerEngine(cfg, None)
    assert captured["global_flow_shadow_enabled"] is False
    assert captured["global_reference_shadow_enabled"] is False


@pytest.mark.parametrize(
    "name",
    ["global_flow_shadow_enabled", "global_reference_shadow_enabled"],
)
def test_multi_market_shadow_flags_require_strict_boolean(name: str) -> None:
    cfg = live_config._parse({"multi_market": {name: "false"}})  # noqa: SLF001
    with pytest.raises(ValueError, match=rf"multi_market\.{name} must be a boolean"):
        live_config._validate_config(cfg)  # noqa: SLF001


def test_disabled_signal_shadow_has_zero_calls_and_preserves_active_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("NARROWGATE_CPP_GLOBAL_FLOW", raising=False)
    signal = SignalEngine(
        enable_ml=False,
        symbol="BTCUSDC",
        reference_symbol="BTCUSDT",
        global_flow_shadow_enabled=False,
        global_reference_shadow_enabled=False,
    )
    on_trade = Mock(wraps=signal._global_flow.on_trade)
    on_trade_batch = Mock(wraps=signal._global_flow.on_trade_batch)
    on_book = Mock(wraps=signal._global_flow.on_book)
    signal._global_flow.on_trade = on_trade
    signal._global_flow.on_trade_batch = on_trade_batch
    signal._global_flow.on_book = on_book
    depth_observer = Mock()
    signal.add_depth_observer(depth_observer)

    receive_ns = 1_800_000_000_000_000_000
    signal.on_agg_trade(
        {"s": "BTCUSDC", "T": 1_800_000_000_000, "p": "60000", "q": "0.01", "m": False},
        receive_ts_ns=receive_ns,
        sequence_number=7,
    )
    signal.on_cross_trade_arrays(
        "BTCUSDT",
        np.asarray([1_800_000_000_000], dtype=np.int64),
        np.asarray([60_001.0], dtype=np.float64),
        np.asarray([0.02], dtype=np.float64),
        np.asarray([1], dtype=np.uint8),
        receive_ts_ns=receive_ns + 1,
        sequence_numbers=np.asarray([8], dtype=np.int64),
    )
    signal.on_book_ticker(
        {"s": "BTCUSDC", "E": 1_800_000_000_000, "b": "59999", "B": "1", "a": "60001", "A": "1"},
        receive_ts_ns=receive_ns + 2,
        sequence_number=9,
    )
    signal.on_depth(
        {"T": 1_800_000_000_000, "b": [["59999", "1"]], "a": [["60001", "1"]]},
        receive_ts_ns=receive_ns + 3,
    )

    on_trade.assert_not_called()
    on_trade_batch.assert_not_called()
    on_book.assert_not_called()
    depth_observer.assert_called_once()
    assert signal._current_bar is not None
    assert "binance:perp:BTCUSDC" in signal._market_source_state
    assert "binance:perp:BTCUSDT" in signal._market_source_state
    assert signal._book_tickers["binance:perp:BTCUSDC"][:2] == [59999.0, 60001.0]
    assert len(signal._global_bridge_basis_history) == 0
    assert signal.global_flow_backend_snapshot() == {
        "native": 0,
        "market_count": 0,
        "trade_batches": 0,
        "trade_events_seen": 0,
        "trade_events_accepted": 0,
        "book_events_seen": 0,
        "book_events_accepted": 0,
        "out_of_order_events": 0,
        "stale_trade_events": 0,
        "trade_overflow_events": 0,
        "book_overflow_events": 0,
    }
    with pytest.raises(RuntimeError, match="global-flow shadow disabled"):
        signal.global_flow_state(now_ns=receive_ns + 4)
    with pytest.raises(RuntimeError, match="global-reference shadow disabled"):
        signal.global_reference_state(now_ms=1_800_000_000_001.0)


def test_explicit_true_preserves_python_fallback_shadow_calls() -> None:
    signal = SignalEngine(
        enable_ml=False,
        global_flow_shadow_enabled=True,
        global_reference_shadow_enabled=True,
    )
    on_trade = Mock(wraps=signal._global_flow.on_trade)
    on_book = Mock(wraps=signal._global_flow.on_book)
    signal._global_flow.on_trade = on_trade
    signal._global_flow.on_book = on_book
    receive_ns = 1_800_000_000_000_000_000
    signal.on_agg_trade(
        {"s": "BTCUSDC", "T": 1_800_000_000_000, "p": "60000", "q": "0.01", "m": False},
        receive_ts_ns=receive_ns,
    )
    signal.on_book_ticker(
        {"s": "BTCUSDC", "E": 1_800_000_000_000, "b": "59999", "B": "1", "a": "60001", "A": "1"},
        receive_ts_ns=receive_ns + 1,
    )
    on_trade.assert_called_once()
    on_book.assert_called_once()
    assert signal.global_flow_backend_snapshot()["market_count"] == 1


def test_cpp_global_flow_capability_does_not_construct_disabled_native_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeNative:
        constructed = 0

        def __init__(self, *_args) -> None:
            type(self).constructed += 1

    class FakeAggregator:
        def __init__(self, *_args) -> None:
            pass

        def update_batch(self, *_args):
            return []

    fake_module = SimpleNamespace(
        NativeGlobalFlowEngine=FakeNative,
        TradeBarAggregator=FakeAggregator,
        __file__="/private/fake/narrowgate_cpp.so",
    )
    monkeypatch.setenv("NARROWGATE_CPP_GLOBAL_FLOW", "1")
    monkeypatch.setattr(signal_module, "_load_cpp_signal_module", lambda: fake_module)

    signal = SignalEngine(
        enable_ml=False,
        global_flow_shadow_enabled=False,
        global_reference_shadow_enabled=False,
    )
    assert FakeNative.constructed == 0
    assert signal._cpp_global_flow_requested is True
    assert signal._cpp_global_flow_enabled is False
    assert signal._cpp_cross_batch_enabled is True
    assert signal.global_flow_backend_snapshot()["native"] == 0


def test_native_preflight_distinguishes_requested_capability_from_effective_shadow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeAggregator:
        def __init__(self, *_args) -> None:
            pass

        def update_batch(self, *_args):
            return []

    fake_module = SimpleNamespace(
        TradeBarAggregator=FakeAggregator,
        __file__="/private/fake/narrowgate_cpp.so",
    )
    monkeypatch.setenv("NARROWGATE_CPP_GLOBAL_FLOW", "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    monkeypatch.setattr(live_main.importlib, "import_module", lambda _name: fake_module)
    cfg = SimpleNamespace(multi_market=SimpleNamespace(global_flow_shadow_enabled=False))
    runtime = live_main.audit_native_runtime(Mock(), cfg=cfg)
    assert runtime["NARROWGATE_CPP_GLOBAL_FLOW_REQUESTED"] is True
    assert runtime["NARROWGATE_CPP_GLOBAL_FLOW_EFFECTIVE"] is False


def test_disabled_health_never_calls_evaluators_and_is_absolute_zero() -> None:
    backend = {
        name: 0
        for name in (
            "native",
            "market_count",
            "trade_batches",
            "trade_events_seen",
            "trade_events_accepted",
            "book_events_seen",
            "book_events_accepted",
            "out_of_order_events",
            "stale_trade_events",
            "trade_overflow_events",
            "book_overflow_events",
        )
    }
    signal = SimpleNamespace(
        global_reference_state=Mock(side_effect=AssertionError("must not evaluate")),
        global_flow_state=Mock(side_effect=AssertionError("must not evaluate")),
        global_flow_backend_snapshot=Mock(side_effect=AssertionError("must not evaluate")),
        shadow_runtime_snapshot=Mock(
            return_value={
                "global_flow_shadow_enabled": False,
                "global_reference_shadow_enabled": False,
                "global_flow_native_effective": False,
                "global_flow_backend": backend,
                "global_reference_bridge_basis_sample_count": 0,
            }
        ),
    )
    cfg = SimpleNamespace(
        tick_size=0.1,
        multi_market=SimpleNamespace(
            global_flow_shadow_enabled=False,
            global_reference_shadow_enabled=False,
        ),
    )
    ref, flow = live_main.collect_global_shadow_health(
        engine=SimpleNamespace(signal=signal),
        cfg=cfg,
        logger=Mock(),
    )
    signal.global_reference_state.assert_not_called()
    signal.global_flow_state.assert_not_called()
    signal.global_flow_backend_snapshot.assert_not_called()
    signal.shadow_runtime_snapshot.assert_called_once()
    assert ref["enabled"] == ref["state_error"] == ref["valid"] == 0
    assert ref["fresh_spot"] == ref["fresh_perp"] == ref["basis_samples"] == 0
    assert ref["reason"] == "disabled_by_config"
    assert flow["enabled"] == flow["state_error"] == flow["valid"] == 0
    assert flow["reason"] == "disabled_by_config"
    for name in (
        "native",
        "market_count",
        "trade_batches",
        "trade_events_seen",
        "trade_events_accepted",
        "book_events_seen",
        "book_events_accepted",
        "out_of_order_events",
        "stale_trade_events",
        "trade_overflow_events",
        "book_overflow_events",
    ):
        assert flow[name] == 0


def test_disabled_health_exposes_nonzero_backend_instead_of_fallback_zero() -> None:
    backend = {
        name: 0
        for name in (
            "native",
            "market_count",
            "trade_batches",
            "trade_events_seen",
            "trade_events_accepted",
            "book_events_seen",
            "book_events_accepted",
            "out_of_order_events",
            "stale_trade_events",
            "trade_overflow_events",
            "book_overflow_events",
        )
    }
    backend["book_events_accepted"] = 1
    signal = SimpleNamespace(
        global_reference_state=Mock(side_effect=AssertionError("must not evaluate")),
        global_flow_state=Mock(side_effect=AssertionError("must not evaluate")),
        shadow_runtime_snapshot=Mock(
            return_value={
                "global_flow_shadow_enabled": False,
                "global_reference_shadow_enabled": False,
                "global_flow_native_effective": False,
                "global_flow_backend": backend,
                "global_reference_bridge_basis_sample_count": 0,
            }
        ),
    )
    cfg = SimpleNamespace(
        tick_size=0.1,
        multi_market=SimpleNamespace(
            global_flow_shadow_enabled=False,
            global_reference_shadow_enabled=False,
        ),
    )
    _ref, flow = live_main.collect_global_shadow_health(
        engine=SimpleNamespace(signal=signal), cfg=cfg, logger=Mock()
    )
    assert flow["book_events_accepted"] == 1
    signal.global_flow_state.assert_not_called()


def test_enabled_health_exception_is_not_a_passing_zero_fallback() -> None:
    signal = SimpleNamespace(
        global_reference_state=Mock(side_effect=RuntimeError("ref")),
        global_flow_state=Mock(side_effect=RuntimeError("flow")),
        global_flow_backend_snapshot=Mock(),
    )
    cfg = SimpleNamespace(
        tick_size=0.1,
        multi_market=SimpleNamespace(
            global_flow_shadow_enabled=True,
            global_reference_shadow_enabled=True,
        ),
    )
    ref, flow = live_main.collect_global_shadow_health(
        engine=SimpleNamespace(signal=signal),
        cfg=cfg,
        logger=Mock(),
    )
    assert ref["state_error"] == 1
    assert flow["state_error"] == 1
    assert ref["reason"] == flow["reason"] == "error"


def test_shadow_flags_are_restart_only_before_engine_mutation() -> None:
    previous = live_config.Config()
    candidate = live_config.Config()
    candidate.multi_market.global_flow_shadow_enabled = True
    with pytest.raises(ValueError, match="global_flow_shadow_enabled is restart-only"):
        live_config.require_multi_market_shadow_restart(previous, candidate)


def test_buy_e3_and_sell_decision_asts_are_unchanged_from_exact_07ef() -> None:
    expected = {
        (
            "strategy/boolean_cooldown_buy_e3.py",
            "ReceiveTimeFullMidEmaWindows",
            "observe_depth",
        ): "e1734c7bcf2b87c78c64b7453051a140e0bb35560287e33d555c1341d0b6cac1",
        (
            "strategy/boolean_cooldown_buy_e3.py",
            "LiveBuyE3CooldownPolicy",
            "evaluate",
        ): "cde0b0893bdf2e2d60e4e130fce71c53f9d234fbb8e38cb35591accc6cda2e09",
        (
            "strategy/boolean_cooldown_live.py",
            "ReceiveTimeMidEmaWindows",
            "observe_depth",
        ): "ee332bb8992ca36181866d52a0406da13270102858e8a62c2013b4e130e7b647",
        (
            "strategy/boolean_cooldown_live.py",
            "LiveBooleanCooldownPolicy",
            "observe_depth",
        ): "655ad44af0c0c6041e65ad1965091364f296266447fa4066c3351d6dfb8e2e8c",
        (
            "strategy/boolean_cooldown_live.py",
            "LiveBooleanCooldownPolicy",
            "evaluate",
        ): "62d5b8ea0ecb5b9d67361f7ec5d161f4f1b989b5a4b270571463fbcb204ea012",
        (
            "strategy/maker_engine.py",
            "MakerEngine",
            "_select_boolean_cooldown_duration",
        ): "b5dca6c25cbbea4f194b645a7598f43eb4701e73fea71613bb92bcd2987a6227",
        (
            "strategy/maker_engine.py",
            "MakerEngine",
            "_select_buy_e3_cooldown_duration",
        ): "049454364176320e2ffaeec338827815a38c6ad463bca245068688f6921a4a1a",
        (
            "strategy/maker_engine.py",
            "MakerEngine",
            "_on_fill",
        ): "0e33055db2ccd57fb3e04968b5a5112bbd31d4017649d6d938c66e9954a61bed",
    }
    assert {key: _function_ast_sha256(*key) for key in expected} == expected
