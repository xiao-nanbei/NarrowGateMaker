from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_mechanics_v1 as mechanics,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_v1 as offline,
)


@dataclass(frozen=True)
class Fixture:
    layout: offline.OfflineSourceLayout
    source_manifest: Path
    owner_artifacts: dict[str, Path]
    selected_days: tuple[str, ...]


def _write_csv(path: Path, header: tuple[str, ...], row: tuple[object, ...]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerow(row)


def _write_normalized(root: Path, day: str) -> None:
    start = int(offline.np.datetime64(day, "ms").astype(offline.np.int64))
    timestamps = [start + 100, start + 200]
    bbo = {
        "timestamp": timestamps,
        "best_bid": [100.0, 100.1],
        "best_bid_qty": [1.0, 1.0],
        "best_ask": [100.2, 100.3],
        "best_ask_qty": [1.0, 1.0],
    }
    l2: dict[str, list[float] | list[int]] = {"timestamp": timestamps}
    for level in range(1, 21):
        l2[f"bid_px_{level}"] = [100.0 - level / 10, 100.1 - level / 10]
        l2[f"bid_qty_{level}"] = [1.0, 1.0]
        l2[f"ask_px_{level}"] = [100.2 + level / 10, 100.3 + level / 10]
        l2[f"ask_qty_{level}"] = [1.0, 1.0]
    for kind, values in (("bbo", bbo), ("l2", l2)):
        path = root / kind / f"BTCUSDC-{kind}-{day}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table(values), path)


def _source_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Fixture:
    selected = ("2026-01-02", "2026-01-03")
    monkeypatch.setattr(offline, "PRIMARY_TARGET_DAYS", selected)
    monkeypatch.setattr(offline, "BACKUP_TARGET_DAYS", ())
    monkeypatch.setattr(offline, "CANDIDATE_TARGET_DAYS", selected)
    monkeypatch.setattr(offline, "CONSUMED_TARGET_DAYS", ("2026-01-01",))
    monkeypatch.setattr(offline, "REQUIRED_DAYS", 2)

    project = tmp_path / "project-data"
    market = tmp_path / "market-data"
    owner_root = project / "owner"
    owner_root.mkdir(parents=True)
    owner_artifacts = {
        "policy": owner_root / "policy.json",
        "predicate_bundle": owner_root / "predicates.json",
        "private_config": owner_root / "config.yaml",
    }
    for role, path in owner_artifacts.items():
        path.write_bytes(f"frozen-{role}\n".encode("ascii"))
    monkeypatch.setattr(
        offline,
        "ACTIVE_OWNER_POLICY_SHA256",
        mechanics.file_sha256(owner_artifacts["policy"]),
    )
    monkeypatch.setattr(
        offline,
        "ACTIVE_PREDICATE_BUNDLE_SHA256",
        mechanics.file_sha256(owner_artifacts["predicate_bundle"]),
    )
    monkeypatch.setattr(
        offline,
        "ACTIVE_PRIVATE_CONFIG_SHA256",
        mechanics.file_sha256(owner_artifacts["private_config"]),
    )

    raw = market / "cryptohftdata/binance_futures"
    normalized = project / "normalized"
    agg = project / "raw"
    individual = project / "raw_trades/BTCUSDC"
    sequence_rows: dict[str, dict[str, object]] = {}
    for day_number in range(1, 5):
        day = f"2026-01-{day_number:02d}"
        for hour in offline.RAW_HOURS:
            path = raw / day / hour / "BTCUSDC_orderbook.parquet.zst"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(f"{day}:{hour}".encode("ascii"))
        _write_normalized(normalized, day)
        timestamp = int(offline.np.datetime64(day, "ms").astype(offline.np.int64)) + 1
        _write_csv(
            agg / f"BTCUSDC-aggTrades-{day}.csv",
            offline._AGG_COLUMNS,
            (1, 100.0, 1.0, 10, 10, timestamp, False),
        )
        _write_csv(
            individual / f"BTCUSDC-trades-{day}.csv",
            offline._TRADE_COLUMNS,
            (10, 100.0, 1.0, 100.0, timestamp, False),
        )
        sequence_rows[day] = {
            "eligible": True,
            "target_initialized_at_start": True,
            "target_initialization_source_at_start": "snapshot",
            "target_accepted_updates": 1,
            "target_sequence_gaps": 0,
            "target_invalid_sequence_messages": 0,
            "target_message_time_reversals": 0,
            "target_duplicate_messages": 0,
            "target_stale_updates": 0,
        }
    sequence = project / "sequence.json"
    sequence.write_text(json.dumps({"day_audits": sequence_rows}), encoding="utf-8")
    layout = offline.OfflineSourceLayout(
        project_data_root=project,
        marketdata_root=market,
        raw_orderbook_root=raw,
        normalized_roots=(normalized,),
        aggtrades_root=agg,
        individual_trades_root=individual,
        sequence_audit_paths=(sequence,),
    )
    output = project / "reports/offline-source"
    offline.audit_historical_sources(layout=layout, output_dir=output, workers=2)
    return Fixture(
        layout=layout,
        source_manifest=output / "canonical_source_manifest.json",
        owner_artifacts=owner_artifacts,
        selected_days=selected,
    )


def _write_panel_files(
    root: Path,
    days: tuple[str, ...],
    *,
    economic_column: bool = False,
    economic_read_true: bool = False,
    owner_missing: bool = False,
    day_order_drift: bool = False,
) -> dict[str, Path]:
    root.mkdir(parents=True, exist_ok=True)
    ordered_days = [days[0], days[0], days[1], days[1]]
    opportunity_ids = ["o1", "o2", "o3", "o4"]
    metadata = pa.table(
        {
            "opportunity_id": opportunity_ids,
            "utc_day": ordered_days,
            "side": ["BUY", "SELL", "BUY", "SELL"],
            "role": ["opener", "add", "add", "opener"],
        }
    )
    boolean_columns: dict[str, object] = {
        "opportunity_id": opportunity_ids,
        "utc_day": ordered_days,
        "predicate::ema_pair_h1s_h8s:ordering_favorable": [True, False, True, True],
    }
    if economic_column:
        boolean_columns["terminal_pnl_usdc"] = [0.0, 0.0, 0.0, 0.0]
    boolean = pa.table(boolean_columns)
    continuous_days = list(reversed(ordered_days)) if day_order_drift else ordered_days
    continuous = pa.table(
        {
            "opportunity_id": opportunity_ids,
            "utc_day": continuous_days,
            "continuous::ema_distance_bps": [0.1, -0.2, 0.3, -0.4],
        }
    )
    owner_actions: list[str | None] = [
        "CONTROL_85N",
        "FIXED_166S",
        "CONTROL_85N",
        "FIXED_1748S",
    ]
    if owner_missing:
        owner_actions[2] = None
    owners = pa.table(
        {
            "opportunity_id": opportunity_ids,
            "utc_day": ordered_days,
            "exact_owner_action": owner_actions,
        }
    )
    replay = pa.table(
        {
            "opportunity_id": opportunity_ids,
            "utc_day": ordered_days,
            "replay_input_id": [f"input-{index}" for index in range(4)],
            "candidate_actions_generated": [False] * 4,
            "continuation_creates_target_assignments": [False] * 4,
            "economic_outcomes_read": [economic_read_true, False, False, False],
            "labels_read": [False] * 4,
        }
    )
    tables = {
        "metadata": metadata,
        "boolean_features": boolean,
        "continuous_features": continuous,
        "exact_owner_actions": owners,
        "replay_inputs": replay,
    }
    paths: dict[str, Path] = {}
    for role, table in tables.items():
        path = root / f"{role}.parquet"
        pq.write_table(table, path)
        paths[role] = path
    return paths


def _build_view(fixture: Fixture) -> Path:
    view = fixture.layout.project_data_root / "f05-offline-book-view"
    mechanics.build_book_view(
        fixture.source_manifest,
        view,
        layout=fixture.layout,
    )
    return view


def test_build_and_validate_book_view_uses_only_hardlinks_and_portable_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _source_fixture(tmp_path, monkeypatch)
    view = _build_view(fixture)
    manifest = mechanics.validate_book_view(view, layout=fixture.layout)

    assert manifest["source_authority"] == "native_normalized_modeled_queue"
    assert manifest["exact_queue_policy_eligible"] is False
    assert manifest["context_days"] == [
        "2026-01-01",
        "2026-01-02",
        "2026-01-03",
        "2026-01-04",
    ]
    source = fixture.layout.normalized_roots[0] / "bbo/BTCUSDC-bbo-2026-01-02.parquet"
    linked = view / "bbo/BTCUSDC-bbo-2026-01-02.parquet"
    assert os.path.samefile(source, linked)
    serialized = json.dumps(manifest)
    assert str(tmp_path) not in serialized
    assert "${NARROWGATE_DATA_ROOT}" in serialized
    with (view / "daily_quality.csv").open(newline="", encoding="ascii") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 4
    assert all(row["exact_queue_policy_eligible"] == "False" for row in rows)


def test_build_book_view_fails_before_output_when_source_is_cross_device(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _source_fixture(tmp_path, monkeypatch)
    view = fixture.layout.project_data_root / "cross-device-view"
    original = mechanics._device_id

    def fake_device(path: Path) -> int:
        if path.resolve() == view.parent.resolve():
            return 1
        return original(path) + 10_000

    monkeypatch.setattr(mechanics, "_device_id", fake_device)
    with pytest.raises(mechanics.OfflineMechanicsError, match="same-filesystem"):
        mechanics.build_book_view(
            fixture.source_manifest,
            view,
            layout=fixture.layout,
        )
    assert not view.exists()
    assert not list(view.parent.glob(f".{view.name}.staging-*"))


def test_validate_book_view_rejects_non_hardlink_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _source_fixture(tmp_path, monkeypatch)
    view = _build_view(fixture)
    target = view / "l2/BTCUSDC-l2-2026-01-03.parquet"
    payload = target.read_bytes()
    target.unlink()
    target.write_bytes(payload)
    with pytest.raises(mechanics.OfflineMechanicsError, match="not a hardlink"):
        mechanics.validate_book_view(view, layout=fixture.layout)


def test_book_view_post_publish_validation_failure_removes_admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _source_fixture(tmp_path, monkeypatch)
    view = fixture.layout.project_data_root / "post-validation-failure"

    def fail_validation(*_args: object, **_kwargs: object) -> dict[str, object]:
        raise mechanics.OfflineMechanicsError("forced post-publish failure")

    monkeypatch.setattr(mechanics, "validate_book_view", fail_validation)
    with pytest.raises(mechanics.OfflineMechanicsError, match="forced post-publish"):
        mechanics.build_book_view(
            fixture.source_manifest,
            view,
            layout=fixture.layout,
        )
    assert not view.exists()


def test_admit_and_validate_outcome_blind_mechanics_panel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _source_fixture(tmp_path, monkeypatch)
    view = _build_view(fixture)
    panel_files = _write_panel_files(
        fixture.layout.project_data_root / "panel-files",
        fixture.selected_days,
    )
    panel_manifest = fixture.layout.project_data_root / "reports/mechanics-panel.json"
    manifest = mechanics.admit_panel(
        fixture.source_manifest,
        view,
        panel_manifest,
        panel_files=panel_files,
        owner_artifacts=fixture.owner_artifacts,
        layout=fixture.layout,
    )

    assert set(manifest["files"]) == set(mechanics.PANEL_FILE_ROLES)
    assert manifest["economic_outcomes_present"] is False
    assert manifest["one_shot_training_labels_precomputed"] is False
    assert manifest["outer_train_label_generation_required"] is True
    assert manifest["repeated_sequential_policy_required"] is True
    assert manifest["files"]["metadata"]["rows"] == 4
    assert manifest["files"]["replay_inputs"]["rows"] == 4
    assert str(tmp_path) not in panel_manifest.read_text(encoding="ascii")
    assert mechanics.validate_panel(panel_manifest, layout=fixture.layout) == manifest


def test_formal_mechanics_admission_binds_sequential_builder_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _source_fixture(tmp_path, monkeypatch)
    view = _build_view(fixture)
    panel_files = _write_panel_files(
        fixture.layout.project_data_root / "panel-files",
        fixture.selected_days,
    )
    builder_manifest = fixture.layout.project_data_root / "builder/manifest.json"
    builder_manifest.parent.mkdir(parents=True)
    builder_manifest.write_text("{}\n", encoding="ascii")
    roots = mechanics.PortableRoots.from_layout(fixture.layout)
    builder_binding = mechanics._binding(builder_manifest, roots=roots)
    sequential_receipt = {
        "identity": "synthetic-sequential-panel-v2",
        "status": "outcome_blind_b0_mechanics_days_admitted",
        "selected_days": list(fixture.selected_days),
        "selected_day_count": len(fixture.selected_days),
        "input_binding_sha256": "a" * 64,
        "sequential_replay_input_identity": "synthetic-replay-input-v2",
        "manifest": builder_binding,
        "merged_panel_manifest": builder_binding,
        "portable_replay_binding": builder_binding,
        "day_manifest_sha256": {day: "b" * 64 for day in fixture.selected_days},
        "permissions": {
            "economic_outcomes_read": False,
            "labels_read": False,
            "candidate_actions_generated": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    monkeypatch.setattr(
        mechanics,
        "_validate_sequential_panel_builder",
        lambda *_args, **_kwargs: sequential_receipt,
    )
    panel_manifest = fixture.layout.project_data_root / "reports/formal-mechanics-panel.json"
    manifest = mechanics.admit_panel(
        fixture.source_manifest,
        view,
        panel_manifest,
        panel_files=panel_files,
        owner_artifacts=fixture.owner_artifacts,
        sequential_panel_builder_manifest_path=builder_manifest,
        layout=fixture.layout,
    )

    assert manifest["schema_version"] == mechanics.PANEL_SCHEMA_VERSION
    assert manifest["formal_execution_eligible"] is True
    assert manifest["sequential_panel_builder"] == sequential_receipt


@pytest.mark.parametrize(
    ("fixture_option", "message"),
    (
        ({"economic_column": True}, "forbidden economic columns"),
        ({"economic_read_true": True}, "economic_outcomes_read must be false"),
        ({"owner_missing": True}, "NaN or missing"),
        ({"day_order_drift": True}, "day order drifted"),
    ),
)
def test_panel_admission_rejects_economics_missing_owner_and_day_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fixture_option: dict[str, bool],
    message: str,
) -> None:
    fixture = _source_fixture(tmp_path, monkeypatch)
    view = _build_view(fixture)
    panel_files = _write_panel_files(
        fixture.layout.project_data_root / "panel-files",
        fixture.selected_days,
        **fixture_option,
    )
    panel_manifest = fixture.layout.project_data_root / "reports/rejected-panel.json"
    with pytest.raises(mechanics.OfflineMechanicsError, match=message):
        mechanics.admit_panel(
            fixture.source_manifest,
            view,
            panel_manifest,
            panel_files=panel_files,
            owner_artifacts=fixture.owner_artifacts,
            layout=fixture.layout,
        )
    assert not panel_manifest.exists()


def test_panel_validation_rejects_file_byte_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _source_fixture(tmp_path, monkeypatch)
    view = _build_view(fixture)
    panel_files = _write_panel_files(
        fixture.layout.project_data_root / "panel-files",
        fixture.selected_days,
    )
    panel_manifest = fixture.layout.project_data_root / "reports/mechanics-panel.json"
    mechanics.admit_panel(
        fixture.source_manifest,
        view,
        panel_manifest,
        panel_files=panel_files,
        owner_artifacts=fixture.owner_artifacts,
        layout=fixture.layout,
    )
    replacement = pa.table(
        {
            "opportunity_id": ["o1", "o2", "o3", "o4"],
            "utc_day": [fixture.selected_days[0]] * 2 + [fixture.selected_days[1]] * 2,
            "continuous::ema_distance_bps": [9.0, 9.0, 9.0, 9.0],
        }
    )
    pq.write_table(replacement, panel_files["continuous_features"])
    with pytest.raises(mechanics.OfflineMechanicsError, match="identity drifted"):
        mechanics.validate_panel(panel_manifest, layout=fixture.layout)


def test_panel_admission_rejects_replay_input_row_identity_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _source_fixture(tmp_path, monkeypatch)
    view = _build_view(fixture)
    panel_files = _write_panel_files(
        fixture.layout.project_data_root / "panel-files",
        fixture.selected_days,
    )
    replay = pq.read_table(panel_files["replay_inputs"]).to_pydict()
    replay["opportunity_id"][1], replay["opportunity_id"][2] = (
        replay["opportunity_id"][2],
        replay["opportunity_id"][1],
    )
    pq.write_table(pa.table(replay), panel_files["replay_inputs"])

    with pytest.raises(
        mechanics.OfflineMechanicsError,
        match="replay_inputs row identity drifted from metadata",
    ):
        mechanics.admit_panel(
            fixture.source_manifest,
            view,
            fixture.layout.project_data_root / "reports/rejected-replay-order.json",
            panel_files=panel_files,
            owner_artifacts=fixture.owner_artifacts,
            layout=fixture.layout,
        )


def test_owner_hash_drift_and_file_role_injection_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _source_fixture(tmp_path, monkeypatch)
    view = _build_view(fixture)
    panel_files = _write_panel_files(
        fixture.layout.project_data_root / "panel-files",
        fixture.selected_days,
    )
    panel_files["action_outcomes"] = panel_files["metadata"]
    with pytest.raises(mechanics.OfflineMechanicsError, match="file roles"):
        mechanics.admit_panel(
            fixture.source_manifest,
            view,
            fixture.layout.project_data_root / "reports/injected.json",
            panel_files=panel_files,
            owner_artifacts=fixture.owner_artifacts,
            layout=fixture.layout,
        )
    panel_files.pop("action_outcomes")
    fixture.owner_artifacts["policy"].write_bytes(b"drift\n")
    with pytest.raises(mechanics.OfflineMechanicsError, match="owner policy hash drifted"):
        mechanics.admit_panel(
            fixture.source_manifest,
            view,
            fixture.layout.project_data_root / "reports/drifted-owner.json",
            panel_files=panel_files,
            owner_artifacts=fixture.owner_artifacts,
            layout=fixture.layout,
        )


def test_cli_exposes_only_contract_commands() -> None:
    assert mechanics.parse_args(
        ["build-book-view", "source.json", "view"]
    ).command == "build-book-view"
    assert mechanics.parse_args(["validate-book-view", "view"]).command == (
        "validate-book-view"
    )
    assert mechanics.parse_args(["validate-panel", "panel.json"]).command == (
        "validate-panel"
    )
    with pytest.raises(SystemExit):
        mechanics.parse_args(["run-economics", "anything"])
