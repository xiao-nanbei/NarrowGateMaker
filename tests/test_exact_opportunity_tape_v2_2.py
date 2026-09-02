from __future__ import annotations

import json
import threading
import time
from dataclasses import asdict
from pathlib import Path

import pytest
import yaml

from execution.exact_opportunity_tape import (
    ExactQuoteOpportunityTapeRow,
    empty_exact_opportunity_row,
)
from execution.exact_opportunity_tape_runtime import (
    EXACT_OPPORTUNITY_RUNTIME_SCHEMA_VERSION,
    ExactOpportunityDailyWriter,
    build_exact_opportunity_runtime_identity,
    canonical_sha256,
    validate_exact_opportunity_runtime_config,
)
from live.config import Config, ExternalVenueSourceConfig, _parse
from research.families.f04_external_market_alpha.audit.exact_opener_opportunity_tape_v2_2 import (
    admit_ready_chunk,
    scan_staging,
    validate_ready_chunk,
)
from strategy.maker_engine import MakerEngine
from strategy.order_manager import OrderManager, OrderState, Side


def _identity() -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": EXACT_OPPORTUNITY_RUNTIME_SCHEMA_VERSION,
        "test_identity": "writer-contract",
    }
    payload["runtime_identity_sha256"] = canonical_sha256(payload)
    return payload


def _row(event_ts_ns: int, *, decision_id: str = "d:BUY") -> dict[str, object]:
    payload = empty_exact_opportunity_row(
        event_type="decision",
        event_ts_ns=event_ts_ns,
        symbol="BTCUSDC",
        side="BUY",
    )
    payload.update(
        decision_group_id="d",
        decision_id=decision_id,
        origin_decision_id=decision_id,
        decision_start_ts_ns=event_ts_ns - 2,
        feature_ready_ts_ns=event_ts_ns - 1,
        role="opener",
        signed_inventory_before=0.0,
        exposure_increasing=1,
        baseline_eligible=1,
        baseline_quote_price=99.9,
        candidate_quote_price=99.8,
        guard_valid=1,
        guard_reason="valid",
        guard_adverse_side="BUY",
        requested_outward_ticks=1,
        effective_outward_ticks=1,
        final_executed_action="place",
        order_quantity=0.001,
    )
    return asdict(ExactQuoteOpportunityTapeRow(**payload))


def _ready_manifest(root: Path) -> Path:
    matches = list(root.glob("session-*/utc_day=*/*.ready.manifest.json"))
    assert len(matches) == 1
    return matches[0]


def _enabled_config() -> Config:
    cfg = Config()
    cfg.logging.exact_opportunity_tape_enabled = True
    cfg.external_venues.enabled = True
    cfg.external_venues.shadow_only = True
    cfg.multi_market.enabled = True
    cfg.multi_market.stablecoin_anchor_symbol = "USDCUSDT"
    cfg.external_venues.sources = [
        ExternalVenueSourceConfig(venue="bitget", enabled=True),
        ExternalVenueSourceConfig(
            venue="bybit", enabled=True, product_type="linear"
        ),
        ExternalVenueSourceConfig(
            venue="okx",
            enabled=True,
            product_type="swap",
            instrument_id="BTC-USDT-SWAP",
        ),
    ]
    return cfg


def test_runtime_config_requires_anchor_three_venues_and_shadow_only() -> None:
    cfg = _enabled_config()
    assert validate_exact_opportunity_runtime_config(cfg)["valid"]

    cfg.external_venues.shadow_only = False
    with pytest.raises(ValueError, match="shadow_only"):
        validate_exact_opportunity_runtime_config(cfg)
    cfg.external_venues.shadow_only = True
    cfg.external_venues.sources.pop()
    with pytest.raises(ValueError, match="missing=.*okx"):
        validate_exact_opportunity_runtime_config(cfg)
    cfg.external_venues.sources.append(
        ExternalVenueSourceConfig(
            venue="okx",
            enabled=True,
            product_type="swap",
            instrument_id="BTC-USDT-SWAP",
        )
    )
    cfg.multi_market.stablecoin_anchor_symbol = ""
    with pytest.raises(ValueError, match="stablecoin anchor"):
        validate_exact_opportunity_runtime_config(cfg)


def test_runtime_identity_binds_actual_config_artifact(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    raw = yaml.safe_load((root / "live/config.yaml").read_text(encoding="utf-8"))
    raw["logging"]["exact_opportunity_tape_enabled"] = True
    raw["external_venues"]["enabled"] = True
    raw["external_venues"]["shadow_only"] = True
    raw["multi_market"]["enabled"] = True
    for source in raw["external_venues"]["sources"]:
        source["enabled"] = True
    config_path = tmp_path / "private.yaml"
    config_path.write_text(yaml.safe_dump(raw), encoding="utf-8")
    cfg = _parse(raw)
    cfg._config_source_path = str(config_path)

    identity = build_exact_opportunity_runtime_identity(cfg, repo_root=root)
    assert identity["config_artifact_path"] == str(config_path)
    assert identity["config_artifact_sha256"]
    assert identity["config_payload_sha256"]

    cfg.tick_size = 0.2
    with pytest.raises(ValueError, match="does not match its artifact"):
        build_exact_opportunity_runtime_identity(cfg, repo_root=root)


def test_writer_quarantines_hot_start_and_emits_health_and_ready_chunk(
    tmp_path: Path,
) -> None:
    writer = ExactOpportunityDailyWriter(
        tmp_path,
        runtime_identity=_identity(),
        initial_active_order_ids={"old-order"},
        session_id="session-a",
        flush_rows=1,
        flush_interval_s=0.01,
        heartbeat_interval_s=0.01,
    )
    now_ns = time.time_ns()
    assert writer.append(_row(now_ns)) is False
    writer.observe_order_terminal("old-order")
    assert writer.append(_row(now_ns + 1)) is True
    health = writer.close()

    assert health["state"] == "closed"
    assert health["rows_quarantined"] == 1
    assert health["rows_written"] == 1
    assert health["rows_dropped"] == 0
    assert health["error_count"] == 0
    assert health["last_flush_ts_ns"] > 0
    assert {
        "last_heartbeat_ts_ns",
        "queue_depth",
        "rows_dropped",
        "error_count",
        "last_error",
        "last_flush_ts_ns",
    }.issubset(health)
    validation = validate_ready_chunk(_ready_manifest(tmp_path))
    assert validation["valid"] is True
    assert validation["row_count"] == 1
    assert validation["economic_outcomes_read"] is False
    assert validation["operational_lifecycle_outcomes_read"] is True


def test_direct_commit_bypasses_secondary_queue_and_finalizes_on_close(
    tmp_path: Path,
) -> None:
    writer = ExactOpportunityDailyWriter(
        tmp_path,
        runtime_identity=_identity(),
        session_id="direct-session",
        queue_size=1,
        flush_rows=1,
    )
    assert writer.commit_frozen(_row(time.time_ns())) is True
    assert writer._queue.qsize() == 0

    health = writer.close()
    assert health["rows_enqueued"] == 1
    assert health["rows_written"] == 1
    assert health["rows_dropped"] == 0
    assert validate_ready_chunk(_ready_manifest(tmp_path))["valid"] is True


def test_exact_writer_submission_owner_cannot_mix_direct_and_queue(
    tmp_path: Path,
) -> None:
    writer = ExactOpportunityDailyWriter(
        tmp_path,
        runtime_identity=_identity(),
        session_id="owner-latch",
    )
    assert writer.commit_frozen(_row(time.time_ns())) is True
    with pytest.raises(RuntimeError, match="submission owner changed"):
        writer.append(_row(time.time_ns() + 1))
    health = writer.close()
    assert health["rows_written"] == 1


def test_exact_direct_commit_failure_is_sticky(tmp_path: Path) -> None:
    writer = ExactOpportunityDailyWriter(
        tmp_path,
        runtime_identity=_identity(),
        session_id="direct-error",
    )
    writer._commit_direct_row = lambda _row: (_ for _ in ()).throw(
        OSError("synthetic direct write failure")
    )
    with pytest.raises(OSError, match="synthetic direct write failure"):
        writer.commit_frozen(_row(time.time_ns()))

    health = writer.close()
    assert health["error_count"] == 1
    assert health["formal_collection_valid"] is False
    assert "direct_commit:OSError" in health["last_error"]


def test_exact_writer_close_is_bounded_when_worker_has_stopped(tmp_path: Path) -> None:
    class _StoppedWorkerWriter(ExactOpportunityDailyWriter):
        def _run(self) -> None:
            return

    writer = _StoppedWorkerWriter(
        tmp_path,
        runtime_identity=_identity(),
        session_id="stopped-worker",
        queue_size=1,
    )
    writer._worker.join(timeout=1.0)
    assert writer.append(_row(time.time_ns())) is True

    started = time.monotonic()
    health = writer.close(timeout_s=0.05)

    assert time.monotonic() - started < 0.5
    assert health["state"] == "error"
    assert health["closed"] is True
    assert health["error_count"] == 1
    assert health["last_error"] == "writer_close_queue_full"
    assert health["formal_collection_valid"] is False


def test_writer_error_invalidates_chunk_and_blocks_ready_admission(
    tmp_path: Path,
) -> None:
    writer = ExactOpportunityDailyWriter(
        tmp_path,
        runtime_identity=_identity(),
        session_id="error-session",
        flush_rows=1,
    )
    writer.append(_row(time.time_ns()))
    writer.report_error("synthetic producer failure")
    health = writer.close()

    assert health["error_count"] == 1
    assert health["last_error"] == "synthetic producer failure"
    scan = scan_staging(tmp_path)
    assert scan["ready_manifests"] == []
    assert len(scan["invalid_manifests"]) == 1


def test_runtime_staging_rejects_removable_volume() -> None:
    removable_root = Path("/", "Volumes", "NARROWGATE_TEST_REMOVABLE")
    with pytest.raises(ValueError, match="local temporary storage"):
        ExactOpportunityDailyWriter(
            removable_root / "exact-staging",
            runtime_identity=_identity(),
        )


def test_admission_is_atomic_idempotent_and_rejects_overlap(
    tmp_path: Path,
) -> None:
    now_ns = time.time_ns()
    staging_a = tmp_path / "staging-a"
    writer_a = ExactOpportunityDailyWriter(
        staging_a,
        runtime_identity=_identity(),
        session_id="a",
        flush_rows=1,
    )
    writer_a.append(_row(now_ns))
    writer_a.close()
    manifest_a = _ready_manifest(staging_a)
    destination = tmp_path / "admitted"
    first = admit_ready_chunk(manifest_a, destination, require_orico=False)
    second = admit_ready_chunk(manifest_a, destination, require_orico=False)
    assert first["idempotent_replay"] is False
    assert second["idempotent_replay"] is True
    registry = json.loads(
        (destination / "admission_manifest.json").read_text(encoding="utf-8")
    )
    assert registry["row_count"] == 1
    assert registry["rows"][0]["schema_sha256"]
    assert registry["rows"][0]["file_sha256"]
    assert registry["rows"][0]["row_sha256"]

    staging_b = tmp_path / "staging-b"
    writer_b = ExactOpportunityDailyWriter(
        staging_b,
        runtime_identity=_identity(),
        session_id="b",
        flush_rows=1,
    )
    writer_b.append(_row(now_ns, decision_id="other:BUY"))
    writer_b.close()
    with pytest.raises(ValueError, match="splicing is forbidden"):
        admit_ready_chunk(
            _ready_manifest(staging_b),
            destination,
            require_orico=False,
        )


def test_crash_partial_never_becomes_admissible(tmp_path: Path) -> None:
    partial = (
        tmp_path
        / "session-crashed"
        / "utc_day=2026-08-03"
        / "exact-opportunity.csv.partial"
    )
    partial.parent.mkdir(parents=True)
    partial.write_text("incomplete", encoding="utf-8")

    scan = scan_staging(tmp_path)
    assert scan["ready_manifests"] == []
    assert scan["orphan_partials"] == [str(partial)]
    with pytest.raises(ValueError, match="only a ready"):
        admit_ready_chunk(partial, tmp_path / "dest", require_orico=False)


def test_cancel_reject_restores_active_lifecycle_and_journals_event() -> None:
    events: list[tuple[str, str]] = []
    manager = OrderManager(
        on_lifecycle_event=lambda order, event, _payload: events.append(
            (order.client_order_id, event)
        )
    )
    cid = manager.create_order("BTCUSDC", Side.BUY, 100.0, 0.001)
    manager.confirm_new(cid, 123, exchange_ts_ns=time.time_ns() - 1_000)
    manager.mark_pending_cancel(cid)

    assert manager.cancel_rejected(cid, "exchange busy") is True
    order = manager.get_order(cid)
    assert order is not None
    assert order.state == OrderState.OPEN
    assert order.is_fill_risk_active
    assert events[-1] == (cid, "cancel_rejected")


def test_maker_cancel_rest_failure_emits_cancel_rejected() -> None:
    class RejectingRest:
        @staticmethod
        def cancel_order(**_kwargs):
            raise RuntimeError("cancel rejected")

    engine = object.__new__(MakerEngine)
    engine.cfg = Config()
    engine.rest = RejectingRest()
    engine._order_context_lock = threading.RLock()
    engine._order_policy_context = {}
    engine._exact_opportunity_tape_path = ""
    engine._exact_opportunity_tape_runtime = None
    engine._record_perf_rest_latency = lambda *_args, **_kwargs: None
    journal: list[str] = []
    engine.orders = OrderManager(
        on_lifecycle_event=lambda _order, event, _payload: journal.append(event)
    )
    cid = engine.orders.create_order("BTCUSDC", Side.BUY, 100.0, 0.001)
    engine.orders.confirm_new(cid, 1, exchange_ts_ns=time.time_ns() - 1_000)

    assert engine._cancel_order(cid) is False
    order = engine.orders.get_order(cid)
    assert order is not None
    assert order.state == OrderState.OPEN
    assert order.is_fill_risk_active
    assert journal[-1] == "cancel_rejected"
