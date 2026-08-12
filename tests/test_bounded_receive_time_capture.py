from __future__ import annotations

import gzip
import hashlib
import json
import os
import shutil
from pathlib import Path

from scripts.bounded_receive_time_capture import (
    CURRENT_LEDGER_FILENAME,
    LEGACY_AWS_SOURCE_KEY,
    _capture_destination_name,
    _full_window_completed_today,
    _is_full_window,
    _ledger_rows,
    _legacy_safe_ledger_source_key,
    _pending_capture_ids,
    capture_source_identity,
    collect_capture_cycle,
    discover_remote_captures,
    finalize_capture,
    main,
    validate_local_capture,
)


def _vultr_source() -> dict[str, str]:
    example_ipv4 = "198.51.100.20"
    return capture_source_identity(
        provider="vultr",
        region="nrt",
        city="tokyo",
        public_ipv4=example_ipv4,
        ssh_target=f"ec2-user@{example_ipv4}",
    )


def _write_tape(
    path: Path,
    *,
    market_id: str,
    receive_ts_ns: int,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "schema_version": "market_tape.v2",
        "market_id": market_id,
        "event_type": "book",
        "exchange_event_ts_ns": receive_ts_ns - 1_000_000,
        "local_receive_ts_ns": receive_ts_ns,
        "feature_ready_ts_ns": receive_ts_ns + 100_000,
        "sequence_number": receive_ts_ns,
        "bid": 100.0,
        "ask": 100.1,
    }
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write(json.dumps(row) + "\n")


def _capture_fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    root = tmp_path / "remote"
    capture_id = "20260724T000000Z"
    marker = root / "logs" / "receive_time_capture" / capture_id
    marker.mkdir(parents=True)
    sentinel = marker / "capture.started"
    sentinel.touch()
    config = root / "live" / "config.yaml"
    config.parent.mkdir(parents=True)
    config.write_text(
        """
external_venues:
  sources:
    - venue: bitget
      instrument_type: perp
      record_enabled: false
logging:
  market_tape_enabled: false
""".lstrip(),
        encoding="utf-8",
    )
    baseline_config_sha256 = hashlib.sha256(config.read_bytes()).hexdigest()
    enabled_config_sha256 = "capture-enabled-config-sha256"
    enable = {
        "timestamp_utc": "2026-07-24T00:00:00+00:00",
        "strategy_hash": "same-strategy",
        "config_sha256_before": baseline_config_sha256,
        "config_sha256_after": enabled_config_sha256,
    }
    disable = {
        "timestamp_utc": "2026-07-24T00:01:00+00:00",
        "strategy_hash": "same-strategy",
        "config_sha256_before": enabled_config_sha256,
        "config_sha256_after": baseline_config_sha256,
    }
    (marker / "enable.json").write_text(json.dumps(enable), encoding="utf-8")
    (marker / "disable.json").write_text(json.dumps(disable), encoding="utf-8")

    tapes = [
        (
            root
            / "logs"
            / "market_tape"
            / "binance_receive_tape_2026-07-24_test.jsonl.gz",
            "binance:perp:BTCUSDC",
        ),
        *[
            (
                root
                / "logs"
                / "external_venues"
                / f"{venue}_{instrument}_btcusdt_test.jsonl.gz",
                f"{venue}:{instrument}:BTCUSDT",
            )
            for venue in ("bitget", "bybit", "okx")
            for instrument in ("perp", "spot")
        ],
    ]
    for index, (path, market_id) in enumerate(tapes):
        _write_tape(
            path,
            market_id=market_id,
            receive_ts_ns=1_784_851_200_000_000_000 + index * 1_000_000,
        )
        os.utime(path, (sentinel.stat().st_mtime + 1, sentinel.stat().st_mtime + 1))

    (root / "logs" / "maker.log").write_text(
        "2026-07-24 00:00:30 [main] INFO HEALTH "
        "marketTapeDropped=0 marketTapeInvalid=0 marketTapeQueueHwm=17 "
        "marketTapeMaxQueueAgeMs=2.5 externalRecordDropped=0 "
        "externalRecordHwm=5 externalRecordMaxAgeMs=1.5\n",
        encoding="utf-8",
    )
    (root / "logs" / "trades.csv").write_text(
        "timestamp,side,trade_type,qty,price\n"
        "1784851230.0,BUY,OPEN,0.001,100.0\n",
        encoding="utf-8",
    )
    return root, marker, sentinel


def test_finalize_and_local_validate_capture_are_idempotent(tmp_path: Path) -> None:
    root, marker, sentinel = _capture_fixture(tmp_path)
    summary = finalize_capture(
        root=root,
        config_path=root / "live" / "config.yaml",
        marker_dir=marker,
        sentinel=sentinel,
        duration_s=60,
        source_identity=_vultr_source(),
    )

    assert summary["valid"]
    assert summary["file_count"] == 7
    assert summary["unique_file_count"] == 7
    assert summary["total_events"] == 7
    assert summary["health_rows"] == 1
    assert summary["maker_fills"] == 1
    assert summary["queue"]["market_tape_hwm_max"] == 17.0
    assert summary["source_identity"] == _vultr_source()
    assert summary["config_enable_disable_chain_valid"]
    assert summary["config_restored_to_baseline"]

    local = tmp_path / "local" / _capture_destination_name(
        _vultr_source(), "20260724T000000Z"
    )
    shutil.copytree(marker, local / "marker")
    for item in summary["files"]:
        source = root / item["path"]
        destination = local / item["path"]
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    shutil.copy2(marker / "summary.json", local / "summary.json")
    ledger = tmp_path / "local" / CURRENT_LEDGER_FILENAME

    first = validate_local_capture(capture_dir=local, ledger_path=ledger)
    second = validate_local_capture(capture_dir=local, ledger_path=ledger)

    assert first["valid"]
    assert first["requested_duration_s"] == 60
    assert first["ledger_appended"]
    assert second["valid"]
    assert not second["ledger_appended"]
    assert len(ledger.read_text(encoding="utf-8").splitlines()) == 1
    assert first["total_events"] == 7
    assert first["source_identity"] == _vultr_source()
    assert first["config_restored_to_baseline"]
    assert json.loads(ledger.read_text(encoding="utf-8"))[
        "source_identity"
    ] == _vultr_source()
    assert all(row["sha256_match"] for row in first["files"])
    assert all(row["event_count_match"] for row in first["files"])
    assert not _is_full_window(first)


def test_discovery_only_returns_remote_captures_eligible_for_sync(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "scripts.bounded_receive_time_capture.remote_status",
        lambda **_: {
            "captures": [
                {
                    "capture_id": "20260722T000000Z",
                    "eligible_for_sync": True,
                },
                {
                    "capture_id": "20260722T010000Z",
                    "eligible_for_sync": False,
                },
                {
                    "capture_id": "not-a-capture",
                    "eligible_for_sync": True,
                },
            ]
        },
    )

    assert discover_remote_captures(remote="host", remote_root="/repo") == [
        "20260722T000000Z"
    ]


def test_collection_state_selects_pending_and_blocks_daily_duplicate() -> None:
    status = {
        "utc_day": "2026-07-24",
        "captures": [
            {
                "capture_id": "20260724T000000Z",
                "requested_duration_s": 3600,
                "valid": True,
                "all_files_valid": True,
                "capture_disabled_after_window": True,
                "eligible_for_sync": True,
            },
            {
                "capture_id": "20260723T000000Z",
                "requested_duration_s": 3600,
                "valid": True,
                "all_files_valid": True,
                "capture_disabled_after_window": True,
                "eligible_for_sync": True,
            },
        ],
    }

    assert _pending_capture_ids(status, {"20260723T000000Z"}) == [
        "20260724T000000Z"
    ]
    assert _full_window_completed_today(status, duration_s=3600)


def test_collect_cycle_follows_active_capture_and_syncs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    statuses = iter(
        [
            {
                "capture_active": True,
                "utc_day": "2026-07-24",
                "captures": [],
            },
            {
                "capture_active": False,
                "utc_day": "2026-07-24",
                "captures": [
                    {
                        "capture_id": "20260724T000000Z",
                        "eligible_for_sync": True,
                    }
                ],
            },
        ]
    )
    monkeypatch.setattr(
        "scripts.bounded_receive_time_capture.remote_status",
        lambda **_: next(statuses),
    )
    monkeypatch.setattr(
        "scripts.bounded_receive_time_capture._sync_one_with_lock",
        lambda **kwargs: {
            "capture_id": kwargs["capture_id"],
            "valid": True,
        },
    )

    result = collect_capture_cycle(
        remote="host",
        remote_root="/repo",
        local_root=tmp_path,
        ledger_path=tmp_path / CURRENT_LEDGER_FILENAME,
        legacy_ledger_path=tmp_path / "capture_ledger.v1.jsonl",
        duration_s=3600,
        poll_interval_s=0,
        timeout_s=1,
        delete_remote=True,
        source_identity=_vultr_source(),
    )

    assert result["started_capture"] is False
    assert result["sync"] == {
        "capture_id": "20260724T000000Z",
        "valid": True,
    }


def test_vultr_capture_directory_never_uses_aws_prefix() -> None:
    name = _capture_destination_name(
        _vultr_source(), "20260811T120000Z"
    )
    assert name == "vultr_tokyo_198_51_100_20_20260811T120000Z"
    assert not name.startswith("aws_tokyo_")


def test_legacy_aws_ledger_is_read_without_rewriting(tmp_path: Path) -> None:
    ledger = tmp_path / "capture_ledger.v1.jsonl"
    legacy = {
        "schema_version": "bounded_receive_time_capture_ledger.v1",
        "capture_id": "20260724T000000Z",
        "utc_day": "2026-07-24",
        "requested_duration_s": 3600,
        "valid": True,
    }
    original = json.dumps(legacy, sort_keys=True) + "\n"
    ledger.write_text(original, encoding="utf-8")

    rows = _ledger_rows(ledger)

    assert rows == [legacy]
    assert _legacy_safe_ledger_source_key(rows[0]) == LEGACY_AWS_SOURCE_KEY
    assert ledger.read_text(encoding="utf-8") == original


def test_collect_reads_legacy_v1_for_daily_dedup_without_writing_it(
    tmp_path: Path,
    monkeypatch,
) -> None:
    legacy_ledger = tmp_path / "capture_ledger.v1.jsonl"
    legacy = {
        "schema_version": "bounded_receive_time_capture_ledger.v1",
        "capture_id": "20260724T000000Z",
        "utc_day": "2026-07-24",
        "requested_duration_s": 3600,
        "valid": True,
    }
    original = json.dumps(legacy, sort_keys=True) + "\n"
    legacy_ledger.write_text(original, encoding="utf-8")
    monkeypatch.setattr(
        "scripts.bounded_receive_time_capture.remote_status",
        lambda **_: {
            "capture_active": False,
            "captures": [],
            "utc_day": "2026-07-24",
        },
    )

    result = collect_capture_cycle(
        remote="host",
        remote_root="/repo",
        local_root=tmp_path,
        ledger_path=tmp_path / CURRENT_LEDGER_FILENAME,
        legacy_ledger_path=legacy_ledger,
        duration_s=3600,
        poll_interval_s=0,
        timeout_s=0,
        delete_remote=True,
        source_identity=_vultr_source(),
    )

    assert result == {
        "skipped": True,
        "reason": "full_window_already_admitted_today",
    }
    assert legacy_ledger.read_text(encoding="utf-8") == original
    assert not (tmp_path / CURRENT_LEDGER_FILENAME).exists()


def test_status_merges_frozen_v1_and_current_v2_by_source(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    legacy_path = tmp_path / "capture_ledger.v1.jsonl"
    current_path = tmp_path / CURRENT_LEDGER_FILENAME
    legacy = {
        "schema_version": "bounded_receive_time_capture_ledger.v1",
        "capture_id": "20260724T000000Z",
        "utc_day": "2026-07-24",
        "requested_duration_s": 3600,
        "valid": True,
    }
    current = {
        "schema_version": "bounded_receive_time_capture_ledger.v2",
        "capture_id": "20260811T120000Z",
        "utc_day": "2026-08-11",
        "requested_duration_s": 3600,
        "valid": True,
        "source_identity": _vultr_source(),
    }
    legacy_bytes = json.dumps(legacy, sort_keys=True) + "\n"
    legacy_path.write_text(legacy_bytes, encoding="utf-8")
    current_path.write_text(
        json.dumps(current, sort_keys=True) + "\n", encoding="utf-8"
    )
    monkeypatch.setattr(
        "scripts.bounded_receive_time_capture.remote_status",
        lambda **_: {
            "capture_active": False,
            "captures": [],
            "utc_day": "2026-08-11",
        },
    )

    source = _vultr_source()
    assert (
        main(
            [
                "status",
                "--remote",
                source["ssh_target"],
                "--local-root",
                str(tmp_path),
                "--source-provider",
                source["provider"],
                "--source-region",
                source["region"],
                "--source-city",
                source["city"],
                "--source-public-ipv4",
                source["public_ipv4"],
                "--source-ssh-target",
                source["ssh_target"],
            ]
        )
        == 0
    )
    status = json.loads(capsys.readouterr().out)

    assert status["valid_full_window_capture_count"] == 2
    assert status["valid_full_window_utc_days"] == [
        "2026-07-24",
        "2026-08-11",
    ]
    assert status["valid_full_window_utc_days_by_source"] == {
        LEGACY_AWS_SOURCE_KEY: ["2026-07-24"],
        _vultr_source()["source_key"]: ["2026-08-11"],
    }
    assert status["legacy_ledger_capture_ids"] == ["20260724T000000Z"]
    assert status["current_ledger_capture_ids"] == ["20260811T120000Z"]
    assert status["current_ledger_path"].endswith(CURRENT_LEDGER_FILENAME)
    assert legacy_path.read_text(encoding="utf-8") == legacy_bytes


def test_finalize_fails_when_config_is_not_restored(tmp_path: Path) -> None:
    root, marker, sentinel = _capture_fixture(tmp_path)
    disable_path = marker / "disable.json"
    disable = json.loads(disable_path.read_text(encoding="utf-8"))
    disable["config_sha256_after"] = "not-the-baseline"
    disable_path.write_text(json.dumps(disable), encoding="utf-8")

    summary = finalize_capture(
        root=root,
        config_path=root / "live" / "config.yaml",
        marker_dir=marker,
        sentinel=sentinel,
        duration_s=60,
        source_identity=_vultr_source(),
    )

    assert not summary["valid"]
    assert not summary["config_restored_to_baseline"]
