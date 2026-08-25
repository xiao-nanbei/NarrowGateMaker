import threading
from types import SimpleNamespace

import pytest

from execution.order_lifecycle import OrderLifecyclePhase
from live.config import Config
from live.main import ROOT, resolve_logging_paths
from strategy.dynamic_fill_hazard_model import (
    DynamicFillHazardShadowObservation,
    ProspectivePlacementRecoveryEvaluation,
)
from strategy.maker_engine import MakerEngine
from strategy.order_manager import OrderManager, OrderState, Side


class _Policy:
    policy_id = "buy_exposure_adverse_q90_cancel_reenter_v1"
    file_sha256 = "a" * 64
    model_file_sha256 = "b" * 64
    entry_threshold = 0.1

    @staticmethod
    def score(observation: DynamicFillHazardShadowObservation) -> float:
        return (
            observation.adverse_probability
            - observation.favorable_probability
        )

    def eligible(
        self,
        observation: DynamicFillHazardShadowObservation,
    ) -> bool:
        return bool(
            observation.valid
            and observation.side == "BUY"
            and observation.inventory_role in {"opener", "add"}
        )

    def cancel_required(
        self,
        observation: DynamicFillHazardShadowObservation,
    ) -> bool:
        return self.eligible(observation) and self.score(observation) >= 0.1

    def recovered(
        self,
        observation: DynamicFillHazardShadowObservation,
    ) -> bool:
        return self.eligible(observation) and self.score(observation) < 0.1


class _Rest:
    def __init__(self) -> None:
        self.canceled: list[str] = []

    def cancel_order(self, *, symbol: str, origClientOrderId: str) -> dict:
        assert symbol == "BTCUSDC"
        self.canceled.append(origClientOrderId)
        return {}


class _Ws:
    def __init__(self) -> None:
        self.terminal: list[str] = []
        self.active_depth_cursors: set[str] = set()

    def retain_active_order_depth_path(self, client_order_id: str) -> bool:
        raise AssertionError("terminal path retention must not be called")

    def release_active_order_depth_path(self, client_order_id: str) -> None:
        raise AssertionError("policy release must not mutate the depth path")

    def terminal_active_order_depth_path(self, client_order_id: str) -> None:
        self.terminal.append(client_order_id)
        self.active_depth_cursors.discard(client_order_id)

    @staticmethod
    def dynamic_fill_hazard_prospective_state(
        *,
        side: str,
        price: float,
        now_ns: int,
    ) -> tuple[dict, dict]:
        assert side == "BUY"
        return (
            {
                "valid": 1,
                "generation": 9,
                "best_bid": price,
                "best_bid_qty": 3.0,
                "best_ask": price + 0.2,
                "best_ask_qty": 2.0,
                "last_receive_ts_ns": now_ns - 1_000_000,
                "feature_ready_ts_ns": now_ns,
                "age_ms": 1.0,
            },
            {
                "valid": True,
                "covered": True,
                "price": price,
                "quantity": 3.0,
                "receive_ts_ns": now_ns - 1_000_000,
                "feature_ready_ts_ns": now_ns,
                "age_ms": 1.0,
            },
        )


class _ProspectiveRuntime:
    def __init__(self) -> None:
        self.dropped: list[str] = []
        self.inputs: list[dict] = []

    def drop_order(self, client_order_id: str) -> None:
        self.dropped.append(client_order_id)

    def evaluate_prospective_cancel_reentry(self, **kwargs):
        self.inputs.append(dict(kwargs))
        assert "path" not in kwargs
        assert kwargs["terminal_policy_route"] == "PROSPECTIVE_CANCEL_REENTRY"
        assert kwargs["terminal_reason"] == "cancel_ack"
        assert kwargs["remaining_quantity"] == pytest.approx(0.001)
        observation = _observation(
            kwargs["prospective_id"],
            favorable=0.2,
            adverse=0.25,
            now_ns=kwargs["now_ns"],
        )
        return ProspectivePlacementRecoveryEvaluation(
            terminal_policy_route="PROSPECTIVE_CANCEL_REENTRY",
            terminal_reason="cancel_ack",
            remaining_quantity=0.001,
            candidate_price=kwargs["candidate_price"],
            age_ms=0.0,
            fresh_queue_at_tail=3.0,
            gtx_eligible=True,
            activation_supported=True,
            old_path_reused=False,
            observation=observation,
        )


def _observation(
    client_order_id: str,
    *,
    role: str = "opener",
    valid: bool = True,
    favorable: float = 0.1,
    adverse: float = 0.3,
    now_ns: int = 1_000_000_000,
) -> DynamicFillHazardShadowObservation:
    return DynamicFillHazardShadowObservation(
        client_order_id=client_order_id,
        side="BUY",
        inventory_role=role,
        valid=valid,
        reason="ok" if valid else "deep_book_invalid",
        edge_ms=100,
        elapsed_ms=100.0,
        missed_edges=0,
        feature_source_ts_ns=now_ns - 1_000_000,
        feature_ready_ts_ns=now_ns,
        deep_generation=7,
        deep_age_ms=1.0,
        order_price=99.0,
        mid=100.0,
        microprice=100.0,
        queue_initial=2.0,
        queue_remaining=1.0,
        cancel_events=1,
        cancel_qty=0.5,
        refill_events=1,
        refill_qty=0.25,
        favorable_probability=favorable,
        adverse_probability=adverse,
        favorable_raw_probability=favorable,
        adverse_raw_probability=adverse,
        model_family_id="test",
    )


def _engine() -> tuple[MakerEngine, str, _Rest, _Ws]:
    engine = object.__new__(MakerEngine)
    cfg = Config()
    cfg.symbol = "BTCUSDC"
    cfg.lot_size = 0.001
    engine.cfg = cfg
    engine.rest = _Rest()
    engine._ws_handler = _Ws()
    engine._dynamic_fill_hazard_action_policy = _Policy()
    engine._dynamic_fill_hazard_shadow_runtime = None
    engine._dynamic_fill_hazard_action_lock = threading.RLock()
    engine._dynamic_fill_hazard_action_hold = None
    engine._dynamic_fill_hazard_action_cancel_count = 0
    engine._dynamic_fill_hazard_action_reentry_count = 0
    engine._dynamic_fill_hazard_action_keep_count = 0
    engine._dynamic_fill_hazard_action_invalid_hold_count = 0
    engine._dynamic_fill_hazard_action_last_score = float("nan")
    engine._last_requote_time = 123.0
    engine._order_ref_lock = threading.RLock()
    engine._bid_cid = None
    engine._ask_cid = None
    engine._log_dynamic_fill_hazard_action = lambda **_kwargs: None
    engine._log_order_outcome = lambda *_args, **_kwargs: None
    engine._pop_order_context = lambda *_args, **_kwargs: None
    engine.orders = OrderManager(
        on_cancel=engine._on_cancel,
        on_terminal=engine._on_order_terminal,
    )
    cid = engine.orders.create_order(
        "BTCUSDC",
        Side.BUY,
        price=99.0,
        quantity=0.001,
    )
    engine.orders.confirm_new(cid, 42)
    engine._ws_handler.active_depth_cursors.add(cid)
    return engine, cid, engine.rest, engine._ws_handler


def test_buy_exposure_cancel_ack_enters_independent_recovery_state() -> None:
    engine, cid, rest, ws = _engine()

    assert engine._apply_dynamic_fill_hazard_action(
        _observation(cid),
    ) == "cancel"
    assert rest.canceled == [cid]
    assert engine.orders.get_order(cid).state == OrderState.PENDING_CANCEL
    assert engine._dynamic_fill_hazard_buy_blocked(0.0)

    assert engine._apply_dynamic_fill_hazard_action(
        _observation(cid, valid=False, now_ns=1_100_000_000),
    ) == "hold_invalid"
    assert engine._dynamic_fill_hazard_action_invalid_hold_count == 1

    assert engine._apply_dynamic_fill_hazard_action(
        _observation(
            cid,
            favorable=0.2,
            adverse=0.25,
            now_ns=1_200_000_000,
        ),
    ) == "recovery_wait_cancel_ack"
    assert engine._dynamic_fill_hazard_action_hold is not None

    engine.orders.on_order_update(
        {
            "s": "BTCUSDC",
            "c": cid,
            "S": "BUY",
            "o": "LIMIT",
            "X": "CANCELED",
            "i": 42,
            "p": "99.0",
            "q": "0.001",
        }
    )
    hold = engine._dynamic_fill_hazard_action_hold
    assert hold is not None
    assert hold.phase == OrderLifecyclePhase.POST_CANCEL_RECOVERY
    assert ws.terminal == [cid]
    assert len(ws.active_depth_cursors) == 0
    assert engine._last_requote_time == 123.0
    assert engine._dynamic_fill_hazard_action_reentry_count == 0
    assert engine.orders.lifecycle_snapshot(cid)["fill_risk_active"] is False
    assert engine.orders.lifecycle_snapshot(cid)["phase"] == (
        OrderLifecyclePhase.POST_CANCEL_RECOVERY.value
    )
    assert engine._apply_dynamic_fill_hazard_action(
        _observation(cid, now_ns=1_300_000_000),
    ) == "post_cancel_recovery_requires_prospective_placement"


def test_fresh_prospective_recovery_rebuilds_age_and_queue_without_old_path() -> None:
    engine, cid, _rest, ws = _engine()
    runtime = _ProspectiveRuntime()
    engine._dynamic_fill_hazard_shadow_runtime = runtime
    assert engine._apply_dynamic_fill_hazard_action(_observation(cid)) == "cancel"
    engine.orders.on_order_update(
        {
            "s": "BTCUSDC",
            "c": cid,
            "S": "BUY",
            "o": "LIMIT",
            "X": "CANCELED",
            "i": 42,
            "p": "99.0",
            "q": "0.001",
        }
    )
    assert runtime.dropped == [cid]
    assert engine.orders.lifecycle_snapshot(cid)["fill_risk_active"] is False
    assert ws.terminal == [cid]
    assert len(ws.active_depth_cursors) == 0

    now_ns = int(
        engine.orders.lifecycle_snapshot(cid)["terminal_visibility_ts_ns"]
    ) + 1_000_000
    assert engine._evaluate_dynamic_fill_hazard_prospective_recovery(
        candidate_price=99.5,
        inventory=0.0,
        now_ns=now_ns,
    ) == "baseline_reenter"
    snapshot = engine.orders.lifecycle_snapshot(cid)
    assert snapshot["phase"] == OrderLifecyclePhase.REENTRY_ELIGIBLE.value
    assert snapshot["fill_risk_active"] is False
    assert engine._dynamic_fill_hazard_action_hold is None
    assert engine._dynamic_fill_hazard_action_reentry_count == 1
    assert len(runtime.inputs) == 1
    assert runtime.inputs[0]["candidate_price"] == pytest.approx(99.5)
    assert "path" not in runtime.inputs[0]


def test_unknown_terminal_reason_fails_fast_after_cursor_and_hazard_cleanup() -> None:
    engine, cid, _rest, ws = _engine()
    runtime = _ProspectiveRuntime()
    engine._dynamic_fill_hazard_shadow_runtime = runtime
    order = engine.orders.get_order(cid)
    with pytest.raises(RuntimeError, match="unsupported q90 terminal reason"):
        engine._on_dynamic_fill_hazard_order_terminal(
            order,
            terminal_reason="unknown",
        )
    assert runtime.dropped == [cid]
    assert ws.terminal == [cid]


def test_full_fill_releases_hold_without_post_cancel_reentry() -> None:
    engine, cid, _rest, ws = _engine()
    assert engine._apply_dynamic_fill_hazard_action(_observation(cid)) == "cancel"

    engine.orders.on_order_update(
        {
            "s": "BTCUSDC",
            "c": cid,
            "S": "BUY",
            "o": "LIMIT",
            "X": "FILLED",
            "i": 42,
            "p": "99.0",
            "q": "0.001",
            "z": "0.001",
            "l": "0.001",
            "L": "99.0",
            "ap": "99.0",
        }
    )

    snapshot = engine.orders.lifecycle_snapshot(cid)
    assert snapshot is not None
    assert snapshot["phase"] == OrderLifecyclePhase.EXCHANGE_TERMINAL.value
    assert snapshot["terminal_policy_route"] == "TERMINAL_COMPLETE"
    assert engine._dynamic_fill_hazard_action_hold is None
    assert engine._dynamic_fill_hazard_action_reentry_count == 0
    assert ws.terminal == [cid]


@pytest.mark.parametrize("status", ["REJECTED", "EXPIRED"])
def test_reject_or_expiry_returns_to_baseline_resubmit(status: str) -> None:
    engine, cid, _rest, ws = _engine()
    assert engine._apply_dynamic_fill_hazard_action(_observation(cid)) == "cancel"

    engine.orders.on_order_update(
        {
            "s": "BTCUSDC",
            "c": cid,
            "S": "BUY",
            "o": "LIMIT",
            "X": status,
            "i": 42,
            "p": "99.0",
            "q": "0.001",
        }
    )

    assert engine._dynamic_fill_hazard_action_hold is None
    assert engine._last_requote_time == 0.0
    assert engine._dynamic_fill_hazard_action_reentry_count == 0
    assert ws.terminal == [cid]


def test_shutdown_terminal_never_creates_reentry() -> None:
    engine, cid, _rest, ws = _engine()
    assert engine._apply_dynamic_fill_hazard_action(_observation(cid)) == "cancel"

    engine.orders.cancel_all_local()

    assert engine._dynamic_fill_hazard_action_hold is None
    assert engine._last_requote_time == 123.0
    assert engine._dynamic_fill_hazard_action_reentry_count == 0
    assert ws.terminal == [cid]


def test_invalid_and_reducing_buy_states_never_cancel() -> None:
    engine, cid, rest, ws = _engine()

    assert engine._apply_dynamic_fill_hazard_action(
        _observation(cid, valid=False),
    ) == "invalid_keep"
    assert engine._apply_dynamic_fill_hazard_action(
        _observation(cid, role="reducing"),
    ) == "baseline"
    assert rest.canceled == []
    assert ws.terminal == []

    engine._dynamic_fill_hazard_action_hold = SimpleNamespace()
    assert not engine._dynamic_fill_hazard_buy_blocked(-0.001)
    assert engine._dynamic_fill_hazard_buy_blocked(0.0)


def test_dynamic_hazard_logs_resolve_under_project_root() -> None:
    cfg = Config()
    cfg.logging.dynamic_fill_hazard_shadow_log = "logs/hazard_shadow.csv"
    cfg.logging.dynamic_fill_hazard_action_log = "logs/hazard_action.csv"

    resolve_logging_paths(cfg)

    assert cfg.logging.dynamic_fill_hazard_shadow_log == str(
        ROOT / "logs/hazard_shadow.csv"
    )
    assert cfg.logging.dynamic_fill_hazard_action_log == str(
        ROOT / "logs/hazard_action.csv"
    )
