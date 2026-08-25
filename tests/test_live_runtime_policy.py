from __future__ import annotations

import copy
import json
import stat
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from live.config import Config, _validate_config
from live.main import (
    EXECUTION_STATE_UNCERTAIN_EXIT_CODE,
    collect_runtime_safety_health,
    create_rest_client,
    record_startup_runtime_identity,
    resolve_live_shutdown_exit,
)
from live.runtime_policy import (
    F05_BOOLEAN_COOLDOWN_OWNER_OVERRIDE_ENV,
    F05_BUY_E3_OWNER_OVERRIDE_ENV,
    Q90_ACTION_OWNER_OVERRIDE_ENV,
    f05_boolean_cooldown_runtime_policy,
    f05_buy_e3_runtime_policy,
    q90_action_runtime_policy,
    require_f05_boolean_cooldown_restart,
    require_f05_buy_e3_restart,
    require_q90_action_restart,
    write_runtime_identity,
)
from live.ws_handler import WSHandler
from strategy.maker_engine import MakerEngine

ROOT = Path(__file__).resolve().parents[1]


def _q90_action_config() -> Config:
    cfg = Config()
    cfg.websocket.deep_book_enabled = True
    cfg.strategy.dynamic_fill_hazard_shadow_enabled = True
    cfg.strategy.dynamic_fill_hazard_shadow_model_path = "model.json"
    cfg.strategy.dynamic_fill_hazard_shadow_model_sha256 = "a" * 64
    cfg.strategy.dynamic_fill_hazard_shadow_sides = "BUY"
    cfg.strategy.dynamic_fill_hazard_action_enabled = True
    cfg.strategy.dynamic_fill_hazard_action_policy_path = "policy.json"
    cfg.strategy.dynamic_fill_hazard_action_policy_sha256 = "b" * 64
    return cfg


def test_q90_action_is_runtime_fail_closed_without_owner_override() -> None:
    with pytest.raises(ValueError, match="POST_CANCEL_RECOVERY"):
        q90_action_runtime_policy(True, environ={})


def test_direct_runtime_config_validation_uses_the_same_q90_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(Q90_ACTION_OWNER_OVERRIDE_ENV, raising=False)

    with pytest.raises(ValueError, match="POST_CANCEL_RECOVERY"):
        _validate_config(_q90_action_config())


def test_direct_runtime_config_records_an_explicit_owner_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(Q90_ACTION_OWNER_OVERRIDE_ENV, "1")

    _validate_config(_q90_action_config())


def test_q90_owner_override_is_explicit_in_runtime_identity(tmp_path: Path) -> None:
    policy = q90_action_runtime_policy(
        True,
        environ={Q90_ACTION_OWNER_OVERRIDE_ENV: "1"},
    )
    path = tmp_path / "runtime_identity.json"
    write_runtime_identity(path, policy)

    persisted = json.loads(path.read_text(encoding="utf-8"))
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert persisted["q90_action_runtime_authority"] == ("owner_risk_accepted_override")
    assert persisted["q90_owner_override_requested"] is True
    assert persisted["q90_owner_override_effective"] is True


def test_runtime_identity_rejects_symlink_destination(tmp_path: Path) -> None:
    target = tmp_path / "target.json"
    target.write_text("sentinel\n", encoding="utf-8")
    destination = tmp_path / "runtime_identity.json"
    destination.symlink_to(target)

    with pytest.raises(ValueError, match="must not be a symlink"):
        write_runtime_identity(destination, {"trusted": True})

    assert target.read_text(encoding="utf-8") == "sentinel\n"


def test_run_sh_preflights_before_background_launch() -> None:
    script = (ROOT / "live/run.sh").read_text(encoding="utf-8")
    start_body = script.split("start() {", 1)[1].split("\nstop() {", 1)[0]
    restart_body = script.split("restart() {", 1)[1].split("\nstatus() {", 1)[0]

    assert "_run_deploy_preflight" in start_body
    assert "scripts/preflight_live_deploy.py" in script
    assert start_body.index("_run_deploy_preflight") < start_body.index("nohup ")
    assert restart_body.index("_run_deploy_preflight") < restart_body.index("\n    stop\n")
    assert "stop ||" not in restart_body
    assert restart_body.index("\n    stop\n") < restart_body.index("\n    start\n")


def test_rest_client_applies_one_finite_timeout_to_every_sync_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import binance.um_futures

    captured = {}

    class FakeUMFutures:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def sign_request(self, method, path, params):
            captured["position_request"] = (method, path, params)
            return []

    monkeypatch.setattr(binance.um_futures, "UMFutures", FakeUMFutures)
    cfg = Config()
    cfg.api.timeout_s = 2.5

    client = create_rest_client(cfg)

    assert captured["timeout"] == pytest.approx(2.5)
    assert client.get_position_risk(symbol="BTCUSDC") == []
    assert captured["position_request"] == (
        "GET",
        "/fapi/v2/positionRisk",
        {"symbol": "BTCUSDC"},
    )


@pytest.mark.parametrize("timeout", (0.0, -1.0, float("nan"), float("inf"), True))
def test_rest_timeout_must_be_positive_finite_and_non_boolean(timeout) -> None:
    cfg = Config()
    cfg.api.timeout_s = timeout

    with pytest.raises(ValueError, match="api.timeout_s"):
        _validate_config(cfg)


def test_runtime_health_exposes_only_general_loop_and_stream_safety_facts() -> None:
    engine = SimpleNamespace(
        runtime_safety_snapshot=lambda **_kwargs: {
            "quote_loop_running": False,
            "ownership_conflict_latched": True,
            "fatal_runtime_latched": True,
            "reconciliation_required": True,
            "reconciliation_pending": True,
            "fatal_runtime_reason": "ORDER_MANAGER_FATAL",
            "last_tick_age_s": 3.0,
        }
    )
    ws = SimpleNamespace(
        user_event_safety_snapshot=lambda **_kwargs: {
            "last_user_event_age_s": 4.0,
            "user_event_count": 7,
        }
    )

    health = collect_runtime_safety_health(
        engine=engine,
        ws=ws,
        now_monotonic_s=10.0,
    )

    assert health["quoteLoopRunning"] is False
    assert health["ownershipConflictLatched"] is True
    assert health["lastTickAge"] == pytest.approx(3.0)
    assert health["lastUserEventAge"] == pytest.approx(4.0)
    assert health["reconciliationRequired"] is True
    assert health["reconciliationPending"] is True
    assert health["fatalReason"] == "ORDER_MANAGER_FATAL"


def test_normal_cleanup_with_final_reconciliation_pending_exits_78() -> None:
    engine = SimpleNamespace(
        runtime_safety_snapshot=lambda: {
            "ownership_conflict_latched": False,
            "reconciliation_required": False,
            "reconciliation_pending": True,
            "fatal_runtime_reason": "LATE_USER_CALLBACK",
        }
    )

    assert resolve_live_shutdown_exit(
        engine=engine,
        fatal_error=None,
        fatal_traceback=None,
        cleanup_errors=[],
    ) == EXECUTION_STATE_UNCERTAIN_EXIT_CODE


def test_ws_stop_waits_for_late_user_callback_and_listen_key_thread() -> None:
    close_requested = threading.Event()
    callback_latched = threading.Event()
    release_callback = threading.Event()
    callback_exited = threading.Event()
    listen_key_entered = threading.Event()
    release_listen_key = threading.Event()
    sync_calls: list[bool] = []
    thread_errors: dict[str, BaseException] = {}

    engine = object.__new__(MakerEngine)
    engine.cfg = Config()
    engine.rest = SimpleNamespace(cancel_open_orders=Mock(return_value={}))
    engine.signal = SimpleNamespace(stop=Mock())
    engine._persist_fill_cooldown_checkpoint = Mock()
    engine._runtime_fatal_lock = threading.Lock()
    engine._runtime_fatal_reason = ""
    engine._runtime_fatal_error = None
    engine._runtime_reconciliation_required = False
    engine._runtime_reconciliation_pending = False
    engine._runtime_reconciliation_inflight = False
    engine._runtime_reconciliation_generation = 0
    engine._running = True
    engine._order_submit_fail_closed = False
    engine._order_lifecycle_live_writer_v2 = None
    engine._exact_opportunity_tape_runtime = None

    def run(name: str, target) -> None:
        try:
            target()
        except BaseException as exc:  # pragma: no cover - asserted below
            thread_errors[name] = exc

    user_thread: threading.Thread

    engine.orders = SimpleNamespace(
        fatal_status=lambda: {
            "latched": False,
            "reason": "",
            "reconciliation_required": False,
        },
        in_callback_dispatch=lambda: threading.current_thread() is user_thread,
    )

    def sync_position(*, required: bool = False) -> bool:
        assert required is True
        assert not user_thread.is_alive()
        sync_calls.append(required)
        return True

    engine.sync_position = sync_position

    def late_user_callback() -> None:
        assert close_requested.wait(timeout=2.0)
        engine.latch_runtime_fatal(
            reason="LATE_USER_CALLBACK",
            error=RuntimeError("late user callback"),
            reconciliation_required=True,
        )
        callback_latched.set()
        assert release_callback.wait(timeout=2.0)
        callback_exited.set()

    def listen_key_worker() -> None:
        listen_key_entered.set()
        assert release_listen_key.wait(timeout=2.0)

    user_thread = threading.Thread(target=late_user_callback)
    listen_key_thread = threading.Thread(target=listen_key_worker)
    user_thread.start()
    listen_key_thread.start()
    assert listen_key_entered.wait(timeout=1.0)

    handler = WSHandler(engine, Config())
    handler._running = True
    handler._ws_user = SimpleNamespace(close=close_requested.set)
    handler._user_thread = user_thread
    handler._listen_key_thread = listen_key_thread

    stop_thread = threading.Thread(target=run, args=("ws_stop", handler.stop))
    stop_thread.start()
    assert callback_latched.wait(timeout=1.0)
    assert stop_thread.is_alive()
    assert sync_calls == []

    release_callback.set()
    assert callback_exited.wait(timeout=1.0)
    assert stop_thread.is_alive()
    release_listen_key.set()
    stop_thread.join(timeout=2.0)

    assert not stop_thread.is_alive()
    assert not user_thread.is_alive()
    assert not listen_key_thread.is_alive()
    assert thread_errors == {}
    assert engine._runtime_reconciliation_pending is True

    engine.stop()

    assert sync_calls == [True]
    assert engine._runtime_reconciliation_pending is False


def test_ws_stop_join_timeout_latches_execution_uncertainty_and_raises() -> None:
    release_user_thread = threading.Event()
    user_thread = threading.Thread(target=release_user_thread.wait)
    user_thread.start()
    engine = SimpleNamespace(latch_runtime_fatal=Mock())
    handler = WSHandler(engine, Config())
    handler._running = True
    handler._user_stream_shutdown_join_timeout_s = 0.01
    handler._ws_user = SimpleNamespace(close=Mock())
    handler._user_thread = user_thread

    try:
        with pytest.raises(RuntimeError, match="callback quiescence"):
            handler.stop()
    finally:
        release_user_thread.set()
        user_thread.join(timeout=1.0)

    assert not user_thread.is_alive()
    engine.latch_runtime_fatal.assert_called_once()
    call = engine.latch_runtime_fatal.call_args
    assert call.kwargs["reason"].startswith(
        "USER_STREAM_SHUTDOWN_NOT_QUIESCENT:user-data WebSocket thread"
    )
    assert call.kwargs["reconciliation_required"] is True
    assert call.kwargs["defer_reconciliation"] is True


def test_listen_key_expiry_restarts_outside_callback_without_self_join(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import websocket

    state_lock = threading.Lock()
    second_stream_started = threading.Event()
    active_streams = 0
    max_active_streams = 0
    created_apps = []

    class _FakeWebSocketApp:
        def __init__(self, _url, *, on_message, on_error, on_close):
            del on_error, on_close
            self.on_message = on_message
            self.closed = threading.Event()
            self.index = len(created_apps)
            created_apps.append(self)

        def run_forever(self, **_kwargs) -> None:
            nonlocal active_streams, max_active_streams
            with state_lock:
                active_streams += 1
                max_active_streams = max(max_active_streams, active_streams)
            try:
                if self.index == 0:
                    self.on_message(self, json.dumps({"e": "listenKeyExpired"}))
                else:
                    second_stream_started.set()
                assert self.closed.wait(timeout=2.0)
            finally:
                with state_lock:
                    active_streams -= 1

        def close(self) -> None:
            self.closed.set()

    monkeypatch.setattr(websocket, "WebSocketApp", _FakeWebSocketApp)
    rest = SimpleNamespace(
        new_listen_key=Mock(
            side_effect=(
                {"listenKey": "first-listen-key"},
                {"listenKey": "second-listen-key"},
            )
        )
    )
    engine = SimpleNamespace(latch_runtime_fatal=Mock())
    handler = WSHandler(engine, Config())
    handler._running = True
    handler._rest_client = rest

    handler._start_user_stream(rest)
    assert second_stream_started.wait(timeout=2.0)
    handler.stop()

    assert rest.new_listen_key.call_count == 2
    assert max_active_streams == 1
    assert active_streams == 0
    assert handler._user_thread is None
    assert handler._user_restart_thread is None
    engine.latch_runtime_fatal.assert_not_called()


def test_concurrent_restart_and_renewal_leave_at_most_one_user_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import websocket

    state_lock = threading.Lock()
    active_streams = 0
    max_active_streams = 0
    created_apps = []

    class _FakeWebSocketApp:
        def __init__(self, _url, **_callbacks):
            self.closed = threading.Event()
            created_apps.append(self)

        def run_forever(self, **_kwargs) -> None:
            nonlocal active_streams, max_active_streams
            with state_lock:
                active_streams += 1
                max_active_streams = max(max_active_streams, active_streams)
            try:
                assert self.closed.wait(timeout=2.0)
            finally:
                with state_lock:
                    active_streams -= 1

        def close(self) -> None:
            self.closed.set()

    monkeypatch.setattr(websocket, "WebSocketApp", _FakeWebSocketApp)
    next_key = 0
    key_lock = threading.Lock()

    def new_listen_key() -> dict[str, str]:
        nonlocal next_key
        with key_lock:
            next_key += 1
            return {"listenKey": f"listen-key-{next_key}"}

    rest = SimpleNamespace(new_listen_key=new_listen_key)
    engine = SimpleNamespace(latch_runtime_fatal=Mock())
    handler = WSHandler(engine, Config())
    handler._running = True
    handler._rest_client = rest
    handler._start_user_stream(rest)

    start_gate = threading.Event()
    workers = (
        threading.Thread(
            target=lambda: (start_gate.wait(), handler.restart_user_stream("manual")),
        ),
        threading.Thread(
            target=lambda: (start_gate.wait(), handler._start_user_stream(rest)),
        ),
    )
    for worker in workers:
        worker.start()
    start_gate.set()
    for worker in workers:
        worker.join(timeout=2.0)

    assert all(not worker.is_alive() for worker in workers)
    assert max_active_streams == 1
    assert active_streams == 1
    handler.stop()
    assert active_streams == 0
    assert handler._user_thread is None
    engine.latch_runtime_fatal.assert_not_called()


def test_verified_user_event_updates_monotonic_health_and_callback_failure_latches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import live.ws_handler as ws_handler_module

    engine = SimpleNamespace(
        orders=SimpleNamespace(
            on_order_update=Mock(side_effect=RuntimeError("inventory callback failed"))
        ),
        latch_runtime_fatal=Mock(),
    )
    handler = WSHandler(engine, Config())
    monkeypatch.setattr(
        ws_handler_module,
        "time",
        SimpleNamespace(monotonic=lambda: 10.0, time_ns=lambda: 123_000_000),
    )

    handler._on_user_message(
        None,
        json.dumps(
            {
                "e": "ORDER_TRADE_UPDATE",
                "o": {"c": "mm_B_1", "X": "NEW", "i": 41},
            }
        ),
    )

    assert handler.user_event_safety_snapshot(now_monotonic_s=14.0) == {
        "user_event_count": 1,
        "last_user_event_age_s": 4.0,
    }
    engine.latch_runtime_fatal.assert_called_once()
    assert engine.latch_runtime_fatal.call_args.kwargs["reconciliation_required"] is True


def test_supervisor_never_restarts_execution_state_uncertainty() -> None:
    script = (ROOT / "live/run.sh").read_text(encoding="utf-8")
    supervisor = script.split("supervise() {", 1)[1].split("\nstart() {", 1)[0]

    assert f"EXECUTION_STATE_UNCERTAIN_EXIT_CODE={EXECUTION_STATE_UNCERTAIN_EXIT_CODE}" in script
    assert "reconciliation_required_no_restart" in supervisor
    assert "fatal_exit_no_restart" in supervisor
    assert "((++restart_count))" not in supervisor
    assert 'NARROWGATE_SUPERVISOR_MAX_RESTARTS:-0' in script


def test_supervisor_does_not_restart_unclassified_exit_one() -> None:
    script = (ROOT / "live/run.sh").read_text(encoding="utf-8")
    supervisor = script.split("supervise() {", 1)[1].split("\nstart() {", 1)[0]

    assert 'if [[ $child_exit -eq 0 ]]' in supervisor
    assert 'if [[ $child_exit -eq $EXECUTION_STATE_UNCERTAIN_EXIT_CODE ]]' in supervisor
    assert '_record_supervisor_state "fatal_exit_no_restart"' in supervisor
    assert "restart_backoff" not in supervisor


def test_fatal_cancel_bypasses_latched_ledger_and_stop_preserves_ownership() -> None:
    engine = object.__new__(MakerEngine)
    engine.cfg = Config()
    engine.rest = SimpleNamespace(cancel_open_orders=Mock(return_value={}))
    engine.orders = SimpleNamespace(
        fatal_status=Mock(
            return_value={
                "latched": True,
                "reason": "callback_delivery_failed:on_fill",
                "reconciliation_required": True,
            }
        ),
        cancel_all_local=Mock(),
    )
    engine.signal = SimpleNamespace(stop=Mock())
    engine._persist_fill_cooldown_checkpoint = Mock()
    engine._running = True
    engine._order_submit_fail_closed = False
    engine.sync_position = Mock(side_effect=RuntimeError("ledger is fatal"))

    engine.latch_runtime_fatal(
        reason="ORDER_MANAGER_FATAL",
        error=RuntimeError("inventory callback delivery failed"),
        reconciliation_required=True,
    )
    engine.stop()

    assert engine.rest.cancel_open_orders.call_count == 2
    engine.orders.cancel_all_local.assert_not_called()
    assert engine.orders.fatal_status()["reconciliation_required"] is True


def test_normal_stop_does_not_invent_terminal_orders_after_cancel_all_ack() -> None:
    engine = object.__new__(MakerEngine)
    engine.cfg = Config()
    engine.orders = SimpleNamespace(
        fatal_status=Mock(
            return_value={"latched": False, "reconciliation_required": False}
        ),
        cancel_all_local=Mock(),
    )
    engine.signal = SimpleNamespace(stop=Mock())
    engine._persist_fill_cooldown_checkpoint = Mock()
    engine._cancel_all_orders = Mock(return_value=True)
    engine.sync_position = Mock(return_value=True)
    engine._running = True
    engine._order_submit_fail_closed = False
    engine._order_lifecycle_live_writer_v2 = None
    engine._exact_opportunity_tape_runtime = None

    engine.stop()

    engine._cancel_all_orders.assert_called_once_with()
    engine.sync_position.assert_called_once_with(required=True)
    engine.orders.cancel_all_local.assert_not_called()


def test_q90_action_state_cannot_change_via_sighup() -> None:
    require_q90_action_restart(False, False)
    require_q90_action_restart(True, True)
    with pytest.raises(ValueError, match="restart through live/run.sh"):
        require_q90_action_restart(False, True)


def test_startup_identity_preserves_its_own_schema(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("project_name: NarrowGate\n", encoding="utf-8")
    cfg = Config()
    cfg.logging.file = str(tmp_path / "maker.log")

    path, identity = record_startup_runtime_identity(
        cfg=cfg,
        config_path=config_path,
        native_runtime={"profile": "test"},
        dry_run=True,
    )

    assert path == tmp_path / "runtime_identity.json"
    assert identity["schema_version"] == "narrowgate_live_runtime_identity.v1"
    assert identity["q90_runtime_policy_schema_version"] == ("narrowgate_runtime_policy.v1")
    assert identity["q90_action_runtime_authority"] == ("action_suspended_shadow_only")


def test_f05_boolean_cooldown_requires_permanent_owner_label_and_override() -> None:
    with pytest.raises(ValueError, match="owner-authorized"):
        f05_boolean_cooldown_runtime_policy(
            True,
            evidence_route="owner_risk_accepted_promotion",
            environ={},
        )
    with pytest.raises(ValueError, match="permanent"):
        f05_boolean_cooldown_runtime_policy(
            True,
            evidence_route="research_supported_promotion",
            environ={F05_BOOLEAN_COOLDOWN_OWNER_OVERRIDE_ENV: "1"},
        )

    policy = f05_boolean_cooldown_runtime_policy(
        True,
        evidence_route="owner_risk_accepted_promotion",
        environ={F05_BOOLEAN_COOLDOWN_OWNER_OVERRIDE_ENV: "1"},
    )
    assert policy["f05_boolean_cooldown_hard_gates_passed"] is False
    assert policy["f05_boolean_cooldown_owner_override_effective"] is True
    assert policy["f05_boolean_cooldown_runtime_authority"] == ("owner_risk_accepted_active")


def test_f05_boolean_cooldown_identity_is_restart_only() -> None:
    base = {
        "boolean_cooldown_policy_enabled": False,
        "boolean_cooldown_policy_path": "",
        "boolean_cooldown_policy_sha256": "",
        "boolean_cooldown_predicate_bundle_path": "",
        "boolean_cooldown_predicate_bundle_sha256": "",
        "boolean_cooldown_ema_warmup_s": 2048.0,
        "boolean_cooldown_evidence_route": "owner_risk_accepted_promotion",
    }
    require_f05_boolean_cooldown_restart(base, dict(base))
    changed = {**base, "boolean_cooldown_policy_enabled": True}
    with pytest.raises(ValueError, match="restart-only"):
        require_f05_boolean_cooldown_restart(base, changed)


def test_f05_buy_e3_requires_separate_owner_override_and_label() -> None:
    with pytest.raises(ValueError, match="owner risk-accepted"):
        f05_buy_e3_runtime_policy(
            True,
            evidence_route="owner_risk_accepted_buy_e3_v1",
            environ={},
        )
    with pytest.raises(ValueError, match="permanent"):
        f05_buy_e3_runtime_policy(
            True,
            evidence_route="research_supported",
            environ={F05_BUY_E3_OWNER_OVERRIDE_ENV: "1"},
        )
    policy = f05_buy_e3_runtime_policy(
        True,
        evidence_route="owner_risk_accepted_buy_e3_v1",
        environ={F05_BUY_E3_OWNER_OVERRIDE_ENV: "1"},
    )
    assert policy["f05_buy_e3_research_supported"] is False
    assert policy["f05_buy_e3_hard_gates_passed"] is False
    assert policy["f05_buy_e3_owner_override_effective"] is True


def test_f05_buy_e3_identity_is_restart_only() -> None:
    base = {
        "buy_e3_cooldown_policy_enabled": False,
        "buy_e3_cooldown_artifact_manifest_path": "",
        "buy_e3_cooldown_artifact_manifest_sha256": "",
        "buy_e3_cooldown_artifact_sha256": "",
        "buy_e3_cooldown_policy_path": "",
        "buy_e3_cooldown_policy_sha256": "",
        "buy_e3_cooldown_predicate_bundle_path": "",
        "buy_e3_cooldown_predicate_bundle_sha256": "",
        "buy_e3_cooldown_ema_warmup_s": 2048.0,
        "buy_e3_cooldown_evidence_route": "owner_risk_accepted_buy_e3_v1",
    }
    require_f05_buy_e3_restart(base, dict(base))
    with pytest.raises(ValueError, match="restart-only"):
        require_f05_buy_e3_restart(
            base,
            {**base, "buy_e3_cooldown_policy_enabled": True},
        )


def _maker_engine_reload_fixture(
    tmp_path: Path,
    *,
    enabled: bool,
) -> tuple[MakerEngine, Config, object | None]:
    cfg = Config()
    checkpoint = tmp_path / "fill_cooldown_state.json"
    cfg.logging.fill_cooldown_checkpoint = str(checkpoint)
    strategy = cfg.strategy
    strategy.buy_e3_cooldown_policy_enabled = enabled
    strategy.buy_e3_cooldown_artifact_manifest_path = str(
        tmp_path / "artifact_manifest.json"
    )
    strategy.buy_e3_cooldown_artifact_manifest_sha256 = "1" * 64
    strategy.buy_e3_cooldown_artifact_sha256 = "2" * 64
    strategy.buy_e3_cooldown_policy_path = str(tmp_path / "policy.json")
    strategy.buy_e3_cooldown_policy_sha256 = "3" * 64
    strategy.buy_e3_cooldown_predicate_bundle_path = str(
        tmp_path / "predicates.json"
    )
    strategy.buy_e3_cooldown_predicate_bundle_sha256 = "4" * 64
    strategy.buy_e3_cooldown_ema_warmup_s = 2048.0
    strategy.buy_e3_cooldown_evidence_route = "owner_risk_accepted_buy_e3_v1"

    engine = object.__new__(MakerEngine)
    engine.cfg = cfg
    engine._fill_cooldown_checkpoint_path = checkpoint
    policy = object() if enabled else None
    engine._buy_e3_cooldown_policy = policy
    return engine, cfg, policy


@pytest.mark.parametrize(
    ("previous_enabled", "candidate_enabled"),
    ((True, False), (False, True)),
)
def test_maker_engine_reload_rejects_buy_e3_enablement_change_before_mutation(
    tmp_path: Path,
    previous_enabled: bool,
    candidate_enabled: bool,
) -> None:
    engine, previous, previous_policy = _maker_engine_reload_fixture(
        tmp_path,
        enabled=previous_enabled,
    )
    previous_strategy = vars(previous.strategy).copy()
    candidate = copy.deepcopy(previous)
    candidate.strategy.buy_e3_cooldown_policy_enabled = candidate_enabled

    with pytest.raises(ValueError, match="F05 BUY E3 cooldown policy is restart-only"):
        engine.on_config_reload(candidate)

    assert engine.cfg is previous
    assert vars(engine.cfg.strategy) == previous_strategy
    assert engine._buy_e3_cooldown_policy is previous_policy


@pytest.mark.parametrize(
    ("field", "replacement"),
    (
        ("buy_e3_cooldown_artifact_manifest_path", "/changed/manifest.json"),
        ("buy_e3_cooldown_artifact_manifest_sha256", "a" * 64),
        ("buy_e3_cooldown_artifact_sha256", "b" * 64),
        ("buy_e3_cooldown_policy_path", "/changed/policy.json"),
        ("buy_e3_cooldown_policy_sha256", "c" * 64),
        ("buy_e3_cooldown_predicate_bundle_path", "/changed/predicates.json"),
        ("buy_e3_cooldown_predicate_bundle_sha256", "d" * 64),
        ("buy_e3_cooldown_ema_warmup_s", 4096.0),
        ("buy_e3_cooldown_evidence_route", "changed_owner_route"),
    ),
)
def test_maker_engine_reload_rejects_buy_e3_binding_drift_before_mutation(
    tmp_path: Path,
    field: str,
    replacement: object,
) -> None:
    engine, previous, previous_policy = _maker_engine_reload_fixture(
        tmp_path,
        enabled=True,
    )
    previous_strategy = vars(previous.strategy).copy()
    candidate = copy.deepcopy(previous)
    setattr(candidate.strategy, field, replacement)

    with pytest.raises(ValueError, match=field):
        engine.on_config_reload(candidate)

    assert engine.cfg is previous
    assert vars(engine.cfg.strategy) == previous_strategy
    assert engine._buy_e3_cooldown_policy is previous_policy
