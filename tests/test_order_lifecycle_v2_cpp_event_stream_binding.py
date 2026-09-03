from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from execution.order_lifecycle import (
    OrderLifecyclePhase,
    QuantityWeightedOrderLifecycle,
    TerminalPolicyRoute,
    terminal_policy_route,
)
from execution.order_lifecycle_quantity_contract import (
    ORDER_LIFECYCLE_QUANTITY_CONTRACT_ID,
    TERMINAL_REMAINDER_ABS_TOLERANCE_BTC,
)
from models.audit.order_lifecycle import OrderLifecycleRecorder
from models.replay.order_lifecycle_v2_replay_adapter import (
    OrderLifecycleV2ReplayAdapter,
)
from research.families.f07_active_order_continuation.audit.order_lifecycle_v2_cpp_event_stream_binding import (
    CPP_EVENT_STREAM_BINDING_SCHEMA_VERSION,
    CPP_EVENT_STREAM_MIRROR_ABI_VERSION,
    audit_cpp_event_stream_lockstep,
    build_cpp_event_stream_binding_artifact,
)

cpp = pytest.importorskip("narrowgate_cpp")


def _order(trace_id: int, submit_ms: int) -> dict[str, object]:
    return {
        "trace_id": trace_id,
        "side": "BUY" if trace_id % 2 else "SELL",
        "submit_ts": submit_ms,
        "quote_ts": submit_ms,
        "quantity": 0.001,
        "remaining": 0.001,
        "price": 100.0,
        "inventory_at_submit": 0.0,
        "inventory_role_at_submit": "opener",
        "campaign_id_at_submit": 0,
        "state": "PENDING_NEW",
        "fill_eligible": True,
    }


def _journal_rows(root: Path, session: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for manifest_path in sorted(
        (root / f"session-{session}" / "parts").glob("part-*.manifest.json")
    ):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        rows.extend(
            pq.read_table(manifest_path.parent / manifest["data_file"]).to_pylist()
        )
    return sorted(
        rows,
        key=lambda row: (str(row["lifecycle_id"]), int(row["lifecycle_sequence"])),
    )


def _two_cancel_reject_branches(tmp_path: Path) -> list[dict[str, object]]:
    session = "cpp-event-stream"
    adapter = OrderLifecycleV2ReplayAdapter(
        root=tmp_path,
        session_id=session,
        runtime_identity={
            "baseline_epoch_id": "synthetic-mechanics",
            "runtime_code_sha256": "a" * 64,
            "config_sha256": "b" * 64,
            "execution_abi": "order-lifecycle-v2",
        },
        symbol="BTCUSDC",
    )

    active = _order(1, 1_000)
    adapter.submit(active, 1_000)
    adapter.activate(active, visibility_ts_ms=1_020, exchange_ts_ms=1_010)
    adapter.request_cancel(active, 1_040)
    adapter.cancel_reject(active, visibility_ts_ms=1_060, exchange_ts_ms=1_050)
    adapter.request_cancel(active, 1_080)
    adapter.cancel_ack(active, visibility_ts_ms=1_100, exchange_ts_ms=1_090)

    partial = _order(2, 2_000)
    adapter.submit(partial, 2_000)
    adapter.activate(partial, visibility_ts_ms=2_020, exchange_ts_ms=2_010)
    adapter.fill(
        partial,
        remaining_after=0.0004,
        visibility_ts_ms=2_040,
        exchange_ts_ms=2_030,
        full_fill=False,
    )
    adapter.request_cancel(partial, 2_060)
    adapter.cancel_reject(partial, visibility_ts_ms=2_080, exchange_ts_ms=2_070)
    adapter.request_cancel(partial, 2_100)
    adapter.cancel_ack(partial, visibility_ts_ms=2_120, exchange_ts_ms=2_110)
    health = adapter.close()
    assert health["rows_dropped"] == 0
    assert health["error_count"] == 0
    return _journal_rows(tmp_path, session)


def test_cpp_mirror_matches_authoritative_python_event_stream(tmp_path: Path) -> None:
    rows = _two_cancel_reject_branches(tmp_path)
    report = audit_cpp_event_stream_lockstep(
        rows,
        require_cancel_reject_branches=True,
    )

    assert report["mechanics_lockstep_passed"] is True
    assert report["mismatch_counts"] == {}
    assert report["gates"] == {
        "journal_schema_lockstep": True,
        "event_sequence_and_terminal_route_lockstep": True,
        "cancel_reject_risk_set_continuation": True,
        "terminal_remainder_zero_contract": True,
    }
    assert report["counts"]["cancel_reject_to_ACTIVE"] == 1
    assert report["counts"]["cancel_reject_to_PARTIALLY_FILLED"] == 1
    assert report["counts"]["cpp_cancel_reject_to_ACTIVE"] == 1
    assert report["counts"]["cpp_cancel_reject_to_PARTIALLY_FILLED"] == 1


def test_cancel_reject_resumes_correct_risk_phase_in_python() -> None:
    active = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    active.activate(1_100_000_000, exchange_ts_ns=1_090_000_000)
    active.request_cancel(1_200_000_000)
    active.cancel_rejected(1_300_000_000, exchange_ts_ns=1_290_000_000)
    assert active.phase == OrderLifecyclePhase.ACTIVE
    assert active.fill_risk_active is True

    partial = QuantityWeightedOrderLifecycle(0.001, 2_000_000_000)
    partial.activate(2_100_000_000, exchange_ts_ns=2_090_000_000)
    partial.observe_fill(
        remaining_after=0.0004,
        visibility_ts_ns=2_200_000_000,
        exchange_ts_ns=2_190_000_000,
    )
    partial.request_cancel(2_300_000_000)
    partial.cancel_rejected(2_400_000_000, exchange_ts_ns=2_390_000_000)
    assert partial.phase == OrderLifecyclePhase.PARTIALLY_FILLED
    assert partial.remaining_quantity == pytest.approx(0.0004)
    assert partial.fill_risk_active is True


def test_positive_sub_lot_cannot_be_forced_to_full_fill() -> None:
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    lifecycle.activate(1_100_000_000, exchange_ts_ns=1_090_000_000)
    with pytest.raises(ValueError, match="full fill claim has positive remaining"):
        lifecycle.observe_fill(
            remaining_after=0.0004,
            visibility_ts_ns=1_200_000_000,
            exchange_ts_ns=1_190_000_000,
            full_fill=True,
        )
    assert lifecycle.phase == OrderLifecyclePhase.ACTIVE
    assert lifecycle.remaining_quantity == pytest.approx(0.001)
    assert terminal_policy_route("full_fill", 0.0004) == TerminalPolicyRoute.UNSUPPORTED


def test_numerical_zero_is_canonicalized_before_persistence() -> None:
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    lifecycle.activate(1_100_000_000, exchange_ts_ns=1_090_000_000)
    lifecycle.observe_fill(
        remaining_after=TERMINAL_REMAINDER_ABS_TOLERANCE_BTC / 2.0,
        visibility_ts_ns=1_200_000_000,
        exchange_ts_ns=1_190_000_000,
        full_fill=True,
    )
    assert lifecycle.phase == OrderLifecyclePhase.EXCHANGE_TERMINAL
    assert lifecycle.remaining_quantity == 0.0
    assert lifecycle.events()[-1]["remaining_qty_after"] == 0.0


def test_legacy_recorder_does_not_use_lot_size_as_terminal_tolerance() -> None:
    recorder = OrderLifecycleRecorder(
        symbol="BTCUSDC",
        lot_size=0.001,
        tick_size=0.1,
        price_jump_ticks=1.0,
        max_orders=10,
    )
    order = _order(3, 1_000)
    recorder.submit(order, 1_000)
    order["state"] = "OPEN"
    recorder.activate(order, 1_010, mid=100.0)
    recorder.fill(
        order,
        1_020,
        fill_qty=0.0006,
        remaining_before=0.001,
        remaining_after=0.0004,
        fill_price=100.0,
        inventory_before=0.0,
        inventory_after=0.0006,
        campaign_id=1,
    )
    event = recorder.events()[-1]
    assert event["event_type"] == "partial_fill"
    assert event["state_after"] == "open"
    assert event["remaining_qty"] == pytest.approx(0.0004)


def test_cpp_rejects_terminal_sublot_remainder(tmp_path: Path) -> None:
    rows = _two_cancel_reject_branches(tmp_path)
    lifecycle_id = str(rows[0]["lifecycle_id"])
    lifecycle_rows = [row for row in rows if row["lifecycle_id"] == lifecycle_id]
    terminal = lifecycle_rows[-1]
    terminal["lifecycle_event"] = "full_fill"
    terminal["event_reason"] = ""
    terminal["phase_before"] = "CANCEL_PENDING"
    terminal["phase_after"] = "EXCHANGE_TERMINAL"
    terminal["terminal_observation"] = "EXCHANGE_TERMINAL"
    terminal["exchange_terminal_reason"] = "full_fill"
    terminal["remaining_quantity_after"] = 0.0004
    terminal["fill_risk_active_after"] = False
    with pytest.raises(ValueError, match="exact zero"):
        cpp.mirror_order_lifecycle_journal_v2_event_stream(lifecycle_rows)


def test_binding_artifact_is_mechanics_only_and_hash_bound(tmp_path: Path) -> None:
    report = audit_cpp_event_stream_lockstep(
        _two_cancel_reject_branches(tmp_path),
        require_cancel_reject_branches=True,
    )
    root = Path(__file__).resolve().parents[1]
    artifact = build_cpp_event_stream_binding_artifact(
        lockstep_report=report,
        runtime_code_identity_sha256="c" * 64,
        implementation_paths=(
            root
            / "research/families/f07_active_order_continuation/cpp/"
            "order_lifecycle_journal_v2_mirror.cpp",
            root / "cpp/narrowgate_cpp/bindings_research.cpp",
        ),
    )
    assert artifact["schema_version"] == CPP_EVENT_STREAM_BINDING_SCHEMA_VERSION
    assert artifact["abi_version"] == CPP_EVENT_STREAM_MIRROR_ABI_VERSION
    assert artifact["quantity_contract_id"] == ORDER_LIFECYCLE_QUANTITY_CONTRACT_ID
    assert artifact["status"] == "bound"
    assert artifact["economic_outcomes_read"] is False
    assert artifact["formal_40day_lockstep_executed"] is False
