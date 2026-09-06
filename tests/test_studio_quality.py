import json
from pathlib import Path

import pytest

from narrowgate.studio_quality import (
    import_quality,
    quality_catalog,
    quality_days,
    quality_export,
)


def setup_catalog(tmp_path: Path, **overrides):
    data = tmp_path / "raw"
    data.mkdir()
    (data / "BTCUSDC-2026-08-02.csv").write_text("known input")
    audit = tmp_path / "quality.csv"
    audit.write_text(
        "day,symbol,raw_ok,rows,coverage,max_gap_s,reason,strict\n"
        f"2026-08-02,BTCUSDC,True,2,0.5,120,old {tmp_path}/example.csv,False\n"
    )
    source = {
        "id": "trades-v1",
        "source": "Binance",
        "exchange": "Binance",
        "market": "perpetual",
        "symbol": "BTCUSDC",
        "data_type": "trades",
        "version": "v1",
        "label": "Official individual trades",
        "audit": {
            "path": str(audit),
            "check_column": "raw_ok",
            "symbol_column": "symbol",
            "scope": "Recorded raw ID/direction checks, not queue admission",
            "label": "raw audit v1",
            "checked_at": "2026-08-03T00:00:00Z",
            "records_column": "rows",
            "coverage_column": "coverage",
            "max_gap_seconds_column": "max_gap_s",
            "reason_columns": ["reason"],
            "task_columns": {"candles": "raw_ok", "strict_replay": "strict"},
        },
        "inventories": [
            {
                "node": "local",
                "directory": str(data),
                "pattern": "{symbol}-{day}.csv",
                "canonical": True,
            }
        ],
        **overrides,
    }
    manifest = {
        "start_day": "2026-08-01",
        "end_day": "2026-08-04",
        "nodes": [
            {"id": "local", "status": "online", "last_seen": None},
            {"id": "cloud", "status": "offline", "last_seen": "2026-08-01"},
        ],
        "datasets": [source],
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    root = tmp_path / "state"
    import_quality(root, path)
    return root, path


def test_full_calendar_includes_missing_head_tail_and_unknown_audits(tmp_path):
    root, _ = setup_catalog(tmp_path)
    report = quality_days(root, "2026-08-01", "2026-08-04")
    assert [r["day"] for r in report["items"]] == [f"2026-08-0{i}" for i in range(1, 5)]
    first, second, _, last = [r["sources"][0] for r in report["items"]]
    assert first["availability"] == last["availability"] == "missing"
    assert first["check_status"] == "unchecked"
    assert second["availability"] == "present"
    assert second["check_status"] == "passed"
    assert second["replica"]["status"] == "present_unverified"
    assert second["task_usability"]["strict_replay"] == "failed"
    assert second["task_usability"]["candles"] == "passed"
    assert second["task_usability"]["funding_pnl"] == "unknown"
    assert second["max_gap_ms"] == 120_000
    # Neither coverage nor gaps in trade timestamps are new interval findings.
    assert second["intervals"] == []
    assert "/Users/" not in json.dumps(report)
    assert str(tmp_path) not in json.dumps(quality_catalog(root))


def test_offline_node_does_not_erase_canonical_quality(tmp_path):
    root, _ = setup_catalog(tmp_path)
    row = quality_days(root, "2026-08-02", "2026-08-02", node="cloud")["items"][0]["sources"][0]
    assert row["availability"] == "present"
    assert row["check_status"] == "passed"
    assert row["replica"] == {
        "status": "unknown",
        "last_checked_at": None,
        "node_status": "offline",
    }


def test_unmounted_inventory_is_unknown_not_missing(tmp_path):
    root, _ = setup_catalog(
        tmp_path,
        inventories=[
            {
                "node": "local",
                "directory": str(tmp_path / "unmounted"),
                "pattern": "{day}",
                "canonical": True,
            }
        ],
    )
    row = quality_days(root, "2026-08-01", "2026-08-01")["items"][0]["sources"][0]
    assert row["availability"] == row["replica"]["status"] == "unknown"


def test_empty_catalog_still_has_requested_calendar(tmp_path):
    result = quality_days(tmp_path, "2026-08-01", "2026-08-03")
    assert len(result["items"]) == 3
    assert all(row["problem"] and not row["sources"] for row in result["items"])


def test_explicit_invalid_intervals_remain_distinct_from_source_gap(tmp_path):
    start = 1785628800000
    intervals = [
        {
            "dataset_id": "trades-v1",
            "version": "v1",
            "day": "2026-08-02",
            "start_ms": start,
            "end_ms": start + 5000,
            "status": "gap",
            "kind": "source_missing",
            "reason": "confirmed source gap",
        },
        {
            "dataset_id": "trades-v1",
            "version": "v1",
            "day": "2026-08-02",
            "start_ms": start + 5000,
            "end_ms": start + 60000,
            "status": "invalid",
            "kind": "reconstruction",
            "reason": "wait for qualified snapshot",
        },
    ]
    root, _ = setup_catalog(tmp_path, intervals=intervals)
    rows = quality_export(root, "2026-08-02", "2026-08-02")["items"]
    assert [r["reason"] for r in rows] == ["confirmed source gap", "wait for qualified snapshot"]
    assert rows[1]["end_ms"] == start + 60000


def test_wrong_version_interval_rejected(tmp_path):
    with pytest.raises(ValueError, match="different source/version"):
        setup_catalog(tmp_path, intervals=[{"dataset_id": "trades-v1", "version": "v2"}])


def test_duplicate_audit_days_rejected(tmp_path):
    root, path = setup_catalog(tmp_path)
    manifest = json.loads(path.read_text())
    audit = Path(manifest["datasets"][0]["audit"]["path"])
    rows = audit.read_text().splitlines()
    audit.write_text("\n".join([*rows, rows[-1]]) + "\n")
    with pytest.raises(ValueError, match="Duplicate date"):
        import_quality(root, path)


@pytest.mark.parametrize("start,end", [("2026-08-02", "2026-08-01"), ("2020-01-01", "2026-01-01")])
def test_bounded_ranges(tmp_path, start, end):
    with pytest.raises(ValueError):
        quality_days(tmp_path, start, end)


def test_export_is_actionable_but_never_executes_download(tmp_path):
    root, _ = setup_catalog(tmp_path)
    report = quality_export(root, "2026-08-01", "2026-08-04")
    assert report["execution"] == "export_only_no_download_started"
    assert "download/resume" in report["items"][0]["recommended_action"]
    assert len(report["items"]) == 4


def test_current_and_future_days_are_incomplete_not_historical_failures(tmp_path):
    report = quality_days(tmp_path, "2099-01-01", "2099-01-02")
    assert all(row["ongoing"] for row in report["items"])


def test_verified_replica_does_not_hide_audited_invalid_interval(tmp_path):
    root, path = setup_catalog(tmp_path)
    manifest = json.loads(path.read_text())
    source = manifest["datasets"][0]
    source["inventories"] = [
        {
            "node": "local",
            "days": {
                "2026-08-02": {"status": "verified", "last_checked_at": "2026-08-03"},
            },
        }
    ]
    source["intervals"] = [
        {
            "dataset_id": source["id"],
            "version": "v1",
            "day": "2026-08-02",
            "start_ms": 1785628800000,
            "end_ms": 1785628860000,
            "status": "invalid",
            "kind": "rebuild",
            "reason": "needs opening snapshot",
        }
    ]
    path.write_text(json.dumps(manifest))
    import_quality(root, path)
    assert quality_days(root, "2026-08-02", "2026-08-02")["items"][0]["problem"]
    assert quality_export(root, "2026-08-02", "2026-08-02")["items"][0]["end_ms"] == 1785628860000


def test_private_node_fields_not_projected(tmp_path):
    root, path = setup_catalog(tmp_path)
    manifest = json.loads(path.read_text())
    manifest["nodes"][0]["ssh_private_key_path"] = str(tmp_path / "owner-key")
    path.write_text(json.dumps(manifest))
    import_quality(root, path)
    assert "owner-key" not in json.dumps(quality_catalog(root))


def test_missing_local_copy_with_verified_remote_uses_sync(tmp_path):
    root, path = setup_catalog(tmp_path)
    manifest = json.loads(path.read_text())
    manifest["datasets"][0]["inventories"].append(
        {
            "node": "cloud",
            "days": {
                "2026-08-01": {"status": "verified", "last_checked_at": "2026-08-01"},
            },
        }
    )
    path.write_text(json.dumps(manifest))
    import_quality(root, path)
    row = quality_days(root, "2026-08-01", "2026-08-01")["items"][0]["sources"][0]
    assert row["availability"] == "present"
    assert row["replica"]["status"] == "missing"
    assert (
        "Synchronize"
        in quality_export(root, "2026-08-01", "2026-08-01")["items"][0]["recommended_action"]
    )
