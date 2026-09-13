from __future__ import annotations

from dataclasses import asdict

import pytest

from execution.order_lifecycle import QuantityWeightedOrderLifecycle
from execution.order_lifecycle_journal import order_lifecycle_journal_payload
from models.replay.baseline_epoch_manifest import (
    REQUIRED_IDENTITY_FIELDS,
    canonical_sha256,
    epoch_identity_sha256,
    finalize_manifest,
)
from models.replay.baseline_epoch_manifest import (
    SCHEMA_VERSION as EPOCH_SCHEMA_VERSION,
)
from models.replay.order_lifecycle_clock_registry import (
    aalen_johansen_table,
    build_clock_registry,
    build_order_lifecycle_episodes,
)


def _manifest(start: int = 1, end: int = 20_000_000_000) -> dict[str, object]:
    identity = {
        name: canonical_sha256(["epoch", name]) for name in REQUIRED_IDENTITY_FIELDS
    }
    return finalize_manifest(
        {
            "schema_version": EPOCH_SCHEMA_VERSION,
            "manifest_id": "registry-test",
            "source_clock": "utc_ns",
            "scope_start_ts_ns": start,
            "scope_end_ts_ns": end,
            "utc_midnight_splits_epoch": False,
            "pooled_estimation_authorized": False,
            "required_identity_fields": list(REQUIRED_IDENTITY_FIELDS),
            "epochs": [
                {
                    "epoch_id": "E1",
                    "start_ts_ns": start,
                    "end_ts_ns": end,
                    "start_reason": "scope_start",
                    "boundary_status": "first_decision_bound",
                    "identity": identity,
                    "identity_sha256": epoch_identity_sha256(identity),
                    "binding_status": "fully_bound",
                    "initial_economic_state_complete": True,
                    "lifecycle_estimation_authorized": True,
                    "continuous_economic_estimation_authorized": True,
                    "pooling_authorized": False,
                }
            ],
            "unbound_intervals": [],
        }
    )


def _row(lifecycle, event_type: str, order_id: str = "OID-1") -> dict[str, object]:
    return order_lifecycle_journal_payload(
        lifecycle=lifecycle,
        runtime_source="python_replay",
        source_event_type=event_type,
        client_order_id=order_id,
        exchange_order_id=1,
        symbol="BTCUSDC",
        side="BUY",
        order_state=lifecycle.phase.value,
    )


def _partial_then_cancel_rows() -> list[dict[str, object]]:
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    rows = [_row(lifecycle, "submit")]
    lifecycle.activate(1_100_000_000, exchange_ts_ns=1_050_000_000)
    rows.append(_row(lifecycle, "activate"))
    lifecycle.observe_fill(
        remaining_after=0.0004,
        visibility_ts_ns=2_100_000_000,
        exchange_ts_ns=2_000_000_000,
    )
    rows.append(_row(lifecycle, "partial_fill"))
    lifecycle.request_cancel(2_500_000_000)
    rows.append(_row(lifecycle, "cancel_request"))
    lifecycle.exchange_terminal(
        3_100_000_000,
        reason="cancel_ack",
        exchange_ts_ns=3_000_000_000,
    )
    rows.append(_row(lifecycle, "cancel_ack"))
    return rows


def test_registry_preserves_partial_fill_and_cancel_ack_identity() -> None:
    episodes = build_order_lifecycle_episodes(_partial_then_cancel_rows(), _manifest())
    assert len(episodes) == 1
    episode = episodes[0]
    assert episode.partial_fill_count == 1
    assert episode.cancel_request_count == 1
    assert episode.terminal_competing_risk == "cancel_ack"
    assert episode.first_fill_risk_time_s == pytest.approx(1.0)
    assert episode.terminal_risk_time_s == pytest.approx(2.0)
    assert episode.quantity_time_exposure_visible_btc_s == pytest.approx(0.0014)


def test_first_fill_and_terminal_are_distinct_estimands() -> None:
    episodes = build_order_lifecycle_episodes(_partial_then_cancel_rows(), _manifest())
    first = aalen_johansen_table(
        episodes, estimand="first_fill", clock="risk_visible", max_time_s=3.0
    )
    terminal = aalen_johansen_table(
        episodes,
        estimand="exchange_terminal",
        clock="risk_visible",
        max_time_s=3.0,
    )
    assert first[9]["events_first_fill"] == 1
    assert terminal[19]["events_cancel_ack"] == 1


def test_registry_outputs_calendar_and_risk_time_per_epoch() -> None:
    report = build_clock_registry(
        _partial_then_cancel_rows(), _manifest(), max_time_s=3.0
    )
    assert report["economic_outcomes_read"] is False
    assert report["pooled_estimation_authorized"] is False
    assert report["episode_count"] == 1
    epoch = report["epochs"][0]
    assert epoch["terminal_risk_counts"] == {"cancel_ack": 1}
    assert epoch["first_fill_calendar_visible"]
    assert epoch["first_fill_risk_visible"]


def test_registry_rejects_non_contract_outcome_column() -> None:
    rows = _partial_then_cancel_rows()
    rows[0]["reward_usdc"] = 1.0
    with pytest.raises(ValueError, match="non-contract columns"):
        build_order_lifecycle_episodes(rows, _manifest())


def test_registry_rejects_order_leaving_manifest_scope() -> None:
    manifest = _manifest(end=2_900_000_000)
    with pytest.raises(ValueError, match="leaves manifest scope"):
        build_order_lifecycle_episodes(_partial_then_cancel_rows(), manifest)


def test_episode_serialization_contains_no_economic_outcome() -> None:
    episode = build_order_lifecycle_episodes(_partial_then_cancel_rows(), _manifest())[0]
    keys = set(asdict(episode))
    assert not {"pnl", "reward", "markout"}.intersection(keys)


def test_reject_before_activation_is_preserved_but_not_in_risk_estimate() -> None:
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    rows = [_row(lifecycle, "submit", "OID-R")]
    lifecycle.exchange_terminal(
        1_100_000_000,
        reason="rejected",
        exchange_ts_ns=1_050_000_000,
    )
    rows.append(_row(lifecycle, "reject", "OID-R"))
    episodes = build_order_lifecycle_episodes(rows, _manifest())
    assert episodes[0].entered_fill_risk_set is False
    report = build_clock_registry(rows, _manifest(), max_time_s=1.0)
    assert report["epochs"][0]["owned_order_count"] == 1
    assert report["epochs"][0]["order_count"] == 0
    assert report["epochs"][0]["never_activated_count"] == 1


def test_local_shutdown_is_right_censor_not_exchange_terminal_cause() -> None:
    lifecycle = QuantityWeightedOrderLifecycle(0.001, 1_000_000_000)
    rows = [_row(lifecycle, "submit", "OID-S")]
    lifecycle.activate(1_100_000_000, exchange_ts_ns=1_050_000_000)
    rows.append(_row(lifecycle, "activate", "OID-S"))
    lifecycle.exchange_terminal(
        2_100_000_000,
        reason="local_shutdown_cancel",
        exchange_ts_ns=0,
    )
    rows.append(_row(lifecycle, "shutdown", "OID-S"))
    episode = build_order_lifecycle_episodes(rows, _manifest())[0]
    assert episode.terminal_competing_risk is None
    assert episode.censor_type == "local_shutdown_without_exchange_terminal_confirmation"
    table = aalen_johansen_table(
        [episode],
        estimand="exchange_terminal",
        clock="risk_visible",
        max_time_s=2.0,
    )
    assert all(row["all_events"] == 0 for row in table)
