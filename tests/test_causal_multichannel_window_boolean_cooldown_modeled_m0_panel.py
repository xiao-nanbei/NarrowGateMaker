from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_modeled_m0_panel as panel,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    M0_REQUIRED_FIELDS,
)

DAY = "2026-04-17"
TS_MS = 1_776_384_508_503
TS_NS = TS_MS * 1_000_000


def _m0() -> dict[str, object]:
    return {
        "assignment_ts_ns": TS_NS,
        "fill_visible_ts_ns": TS_NS,
        "side": "BUY",
        "role_at_fill": "opener",
        "inventory_before_fill_btc": 0.0,
        "inventory_after_fill_btc": 0.001,
        "fill_qty_btc": 0.001,
        "order_qty_btc": 0.002,
        "cumulative_filled_qty_before_btc": 0.0,
        "cumulative_filled_qty_after_btc": 0.001,
        "remaining_order_qty_after_btc": 0.001,
        "partial_fill_ordinal": 1,
        "fill_is_partial": True,
        "order_age_s": 1.25,
        "queue_ahead_before_fill_btc": None,
        "queue_state_before_fill": "unknown",
        "target_price_tick": 640_000,
        "target_price_displayed_qty_btc": None,
        "target_price_displayed_qty_status": "unknown",
        "target_price_displayed_qty_known": False,
        "target_price_displayed_qty_is_queue_ahead": False,
        "consecutive_units_after": 1.0,
        "baseline_duration_ms": 85_000.0,
        "campaign_age_s": 0.0,
        "campaign_add_count": 0,
        "campaign_mae_to_date_usdc": 0.0,
        "campaign_inventory_time_to_date_btc_s": 0.0,
        "last_same_side_fill_age_s": None,
        "last_opposite_side_fill_age_s": None,
        "cooldown_remaining_ms": 0.0,
        "cooldown_blocker_active": False,
        "cooldown_lineage_revision_before": 0,
        "cooldown_deadline_owner": "none",
    }


def _trace() -> dict[str, object]:
    return {
        "schema_version": "multiscale_ema_boolean_cooldown_duration_opportunity.v1",
        "exposure_fill_ordinal": 1,
        "fill_visible_ts_ms": TS_MS,
        "fill_exchange_ts_ms": TS_MS,
        "fill_clock_semantics": panel.FILL_CLOCK_SEMANTICS,
        "live_receive_time_authority": False,
        "side": "BUY",
        "role_at_fill": "opener",
        "order_id": 118,
        "campaign_id": 1,
        "inventory_before_fill_btc": 0.0,
        "inventory_after_fill_btc": 0.001,
        "fill_qty_btc": 0.001,
        "consecutive_units_after": 1.0,
        "baseline_duration_ms": 85_000.0,
    }


def _source_part(tmp_path: Path) -> panel.SourcePart:
    census_path = tmp_path / "source.parquet"
    manifest_path = tmp_path / "source.json"
    census_path.write_bytes(b"source")
    manifest_path.write_text("{}\n", encoding="utf-8")
    return panel.SourcePart(
        utc_day=DAY,
        opportunity_count=1,
        census_data_path=census_path,
        census_data_sha256=panel._sha256_file(census_path),
        census_manifest_path=manifest_path,
        census_manifest_sha256=panel._sha256_file(manifest_path),
    )


def _capture(
    emitter: panel.ModeledM0CaptureEmitter,
) -> tuple[panel.M0CaptureReceipt, dict[str, object]]:
    receipt = emitter.capture_exposure_fill(
        assignment_id="cooldown-v2:BTCUSDC:1:BUY:118:1",
        fill_event_id="fill:118:1:1:1",
        client_order_id="replay-order-118",
        lineage_id="cooldown-buy-lineage",
        lineage_revision=1,
        partial_fill_ordinal=1,
        partial_fill_qty_btc=0.001,
        fill_exchange_ts_ns=TS_NS,
        fill_visible_ts_ns=TS_NS,
        m0_context=_m0(),
    )
    replay_receipt = {
        "snapshot_id": receipt.snapshot_id,
        "assignment_id": receipt.assignment_id,
        "side": "BUY",
        "role_at_fill": "opener",
        "campaign_id": 1,
        "exposure_fill_ordinal": 1,
        "partial_fill_ordinal": 1,
        "policy_input_valid": True,
        "fallback_policy_id": None,
        "fallback_reason": None,
        "source_bundle_sha256": receipt.source_bundle_sha256,
    }
    return receipt, replay_receipt


def _census() -> pd.DataFrame:
    trace = _trace()
    trace["opportunity_id"] = panel._v1_opportunity_id(DAY, trace)
    trace["exact_queue_policy_eligible"] = False
    trace["queue_path_semantics"] = panel.QUEUE_PATH_SEMANTICS
    return pd.DataFrame([trace])


def test_lightweight_emitter_preserves_unknown_queue_without_labels() -> None:
    emitter = panel.ModeledM0CaptureEmitter()
    receipt, _ = _capture(emitter)

    assert receipt.policy_input_valid is True
    assert len(emitter.records) == 1
    assert emitter.records[0]["m0_context"]["queue_state_before_fill"] == "unknown"
    assert emitter.records[0]["m0_context"]["queue_ahead_before_fill_btc"] is None
    assert emitter.audit()["economic_outcomes_read"] is False
    assert emitter.audit()["arm_outcomes_read"] is False


def test_lightweight_emitter_rejects_fabricated_exact_queue() -> None:
    emitter = panel.ModeledM0CaptureEmitter()
    m0 = _m0()
    m0["queue_state_before_fill"] = "exact"
    m0["queue_ahead_before_fill_btc"] = 0.25

    with pytest.raises(panel.ModeledM0PanelError, match="exact queue"):
        emitter.capture_exposure_fill(
            assignment_id="assignment",
            fill_event_id="fill",
            client_order_id="order",
            lineage_id="lineage",
            lineage_revision=1,
            partial_fill_ordinal=1,
            partial_fill_qty_btc=0.001,
            fill_exchange_ts_ns=TS_NS,
            fill_visible_ts_ns=TS_NS,
            m0_context=m0,
        )


def test_panel_matches_v1_census_and_contains_complete_m0(tmp_path: Path) -> None:
    emitter = panel.ModeledM0CaptureEmitter()
    _, replay_receipt = _capture(emitter)
    result = {
        "_cooldown_duration_opportunity_trace": [_trace()],
        "_cooldown_v2_snapshot_receipts": [replay_receipt],
        "_cooldown_v2_snapshot_emitter_audit": emitter.audit(),
    }

    frame = panel.build_m0_panel(
        day=DAY,
        census=_census(),
        result=result,
        emitter=emitter,
        source_part=_source_part(tmp_path),
    )

    assert len(frame) == 1
    assert set(M0_REQUIRED_FIELDS) <= set(frame.columns)
    assert frame.loc[0, "opportunity_id"] == _census().loc[0, "opportunity_id"]
    assert bool(frame.loc[0, "exact_queue_policy_eligible"]) is False
    assert frame.loc[0, "queue_state_before_fill"] == "unknown"
    assert pd.isna(frame.loc[0, "queue_ahead_before_fill_btc"])
    assert panel.FORBIDDEN_OUTPUT_COLUMNS.isdisjoint(frame.columns)


def test_panel_fails_closed_on_order_identity_drift(tmp_path: Path) -> None:
    emitter = panel.ModeledM0CaptureEmitter()
    _, replay_receipt = _capture(emitter)
    trace = _trace()
    trace["order_id"] = 119
    result = {
        "_cooldown_duration_opportunity_trace": [trace],
        "_cooldown_v2_snapshot_receipts": [replay_receipt],
        "_cooldown_v2_snapshot_emitter_audit": emitter.audit(),
    }

    with pytest.raises(panel.ModeledM0PanelError, match="order_id"):
        panel.build_m0_panel(
            day=DAY,
            census=_census(),
            result=result,
            emitter=emitter,
            source_part=_source_part(tmp_path),
        )


def test_day_admission_is_atomic_and_hash_validated(tmp_path: Path) -> None:
    emitter = panel.ModeledM0CaptureEmitter()
    _, replay_receipt = _capture(emitter)
    source_part = _source_part(tmp_path)
    frame = panel.build_m0_panel(
        day=DAY,
        census=_census(),
        result={
            "_cooldown_duration_opportunity_trace": [_trace()],
            "_cooldown_v2_snapshot_receipts": [replay_receipt],
            "_cooldown_v2_snapshot_emitter_audit": emitter.audit(),
        },
        emitter=emitter,
        source_part=source_part,
    )
    identity = {"execution_identity_sha256": "a" * 64}

    manifest = panel._admit_day(
        output=tmp_path / "out",
        day=DAY,
        frame=frame,
        execution_identity=identity,
        source_part=source_part,
        replay_audit=emitter.audit(),
    )

    day_root = tmp_path / "out" / "days" / DAY
    assert {path.name for path in day_root.iterdir()} == {
        "_SUCCESS",
        "m0_context.parquet",
        "manifest.json",
    }
    assert manifest["row_count"] == 1
    assert manifest["economic_outcomes_read"] is False
    assert json.loads((day_root / "_SUCCESS").read_text())["manifest_sha256"]

    with (day_root / "m0_context.parquet").open("ab") as handle:
        handle.write(b"drift")
    with pytest.raises(panel.ModeledM0PanelError, match="hash drifted"):
        panel._validate_day(
            output=tmp_path / "out",
            day=DAY,
            execution_identity_sha256="a" * 64,
            source_part=source_part,
        )


def test_worker_bound_rejects_parallel_full_day_replays() -> None:
    assert panel._validate_workers(1) == 1
    with pytest.raises(panel.ModeledM0PanelError, match="between 1 and 1"):
        panel._validate_workers(2)
