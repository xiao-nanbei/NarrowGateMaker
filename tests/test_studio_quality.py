import json
from pathlib import Path

import pytest

from narrowgate import studio_quality as quality
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
        "observation_reason": "remote_replica_not_observed",
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


def test_reimport_replaces_selected_sources_without_deleting_raw_data(tmp_path):
    root, path = setup_catalog(tmp_path)
    manifest = json.loads(path.read_text())
    execution = manifest["datasets"][0]
    reference_data = tmp_path / "raw" / "BTCUSDT-2026-08-02.csv"
    reference_data.write_text("retained optional reference book")
    reference_audit = tmp_path / "reference-quality.csv"
    reference_audit.write_text("day,symbol,raw_ok\n2026-08-02,BTCUSDT,False\n")
    manifest["datasets"].append(
        {
            **execution,
            "id": "reference-book-v1",
            "symbol": "BTCUSDT",
            "data_type": "reference_bbo_l2_100ms",
            "label": "Optional reference book, separately selected version",
            "audit": {**execution["audit"], "path": str(reference_audit)},
        }
    )
    path.write_text(json.dumps(manifest))
    import_quality(root, path)
    assert {row["id"] for row in quality_catalog(root)["datasets"]} == {
        "trades-v1",
        "reference-book-v1",
    }

    # Deselection replaces the projection, without revisiting the old audit.
    manifest["datasets"] = [execution]
    path.write_text(json.dumps(manifest))
    reference_audit.unlink()
    import_quality(root, path)
    assert [row["id"] for row in quality_catalog(root)["datasets"]] == ["trades-v1"]
    days = quality_days(root, "2026-08-01", "2026-08-04")["items"]
    assert all([row["dataset_id"] for row in day["sources"]] == ["trades-v1"] for day in days)
    exported = quality_export(root, "2026-08-01", "2026-08-04")["items"]
    assert len(exported) == 4
    assert {row["dataset_id"] for row in exported} == {"trades-v1"}
    assert reference_data.read_text() == "retained optional reference book"


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


@pytest.fixture
def registered(tmp_path):
    data = tmp_path / "processed"
    data.mkdir()
    output = data / "book.parquet"
    output.write_bytes(b"recorded processed output")
    audit = tmp_path / "quality.csv"
    audit.write_text("day,ok,feature,modeled,strict\n2026-08-02,true,true,true,false\n")
    source = {
        "id": "provider-book",
        "source": "recorded-provider",
        "exchange": "exchange",
        "market": "perpetual",
        "symbol": "BTCUSDC",
        "data_type": "normalized_book",
        "version": "selected-v1",
        "label": "Provider-normalized candidate",
        "stage": "processed",
        "audit": {
            "path": str(audit),
            "check_column": "ok",
            "checked_at": "2026-08-03T00:00:00Z",
            "scope": "Provider-normalized candidate, not native queue or current B0 inputs",
            "label": "Existing processed content audit",
            "task_columns": {
                "feature_input": "feature",
                "modeled_replay": "modeled",
                "strict_replay": "strict",
            },
            "task_reasons": {"strict_replay": "Native U/u/pu sequence was not recorded"},
            "not_applicable_tasks": ["funding_pnl"],
        },
        "inventories": [
            {
                "node": "local",
                "directory": str(data),
                "canonical": True,
                "audit_version": "selected-v1",
                "files_by_day": {
                    "2026-08-02": [{"path": str(output), "size_bytes": output.stat().st_size}]
                },
            }
        ],
    }
    manifest = tmp_path / "owner.json"
    manifest.write_text(
        json.dumps(
            {
                "start_day": "2026-08-01",
                "end_day": "2026-08-04",
                "nodes": [{"id": "local", "status": "online"}, {"id": "lan", "status": "offline"}],
                "datasets": [source],
            }
        )
    )
    state = tmp_path / "state"
    quality.import_quality(state, manifest)
    return state, manifest, output


def request(node="local", **kwargs):
    return {
        "start_day": "2026-08-02",
        "end_day": "2026-08-02",
        "dataset_id": "provider-book",
        "node": node,
        **kwargs,
    }


def row(state, node="local"):
    return quality.quality_days(state, **request(node))["items"][0]["sources"][0]


def test_existing_processed_audit_size_association_is_not_a_new_sha_or_strict_pass(registered):
    state, _, _ = registered
    source = row(state)
    assert source["audit_applicability"]["status"] == "recorded_content_audit_current_size_matched"
    assert source["current_task_usability"]["feature_input"] == "passed"
    assert source["current_task_usability"]["modeled_replay"] == "passed"
    assert source["current_task_usability"]["strict_replay"] == "failed"
    assert source["task_reasons"]["strict_replay"] == "Native U/u/pu sequence was not recorded"
    assert source["replica"]["status"] == "present_unverified"
    assert source["stage"] == "processed" and source["observed_at"]


def test_selected_remote_replica_does_not_erase_confirmed_canonical_quality(registered):
    state, _, _ = registered
    local, remote = row(state), row(state, "lan")
    assert remote["current_task_usability"] == local["current_task_usability"]
    assert remote["observed_at"] == local["observed_at"]
    assert remote["replica"]["status"] == "unknown"
    assert remote["replica"]["last_checked_at"] is None
    result = quality.refresh_quality(state, request("lan"))
    assert result["refresh"]["reason"] == "remote_replica_not_observed"
    assert row(state, "lan")["replica"]["last_checked_at"] is None
    assert row(state)["current_task_usability"]["feature_input"] == "passed"


def test_refresh_only_stats_registered_files_never_reads_contents_or_audit(registered, monkeypatch):
    state, manifest, output = registered
    audit = Path(json.loads(manifest.read_text())["datasets"][0]["audit"]["path"])
    original = Path.open

    def guarded(self, *args, **kwargs):
        if self in {manifest, output, audit}:
            pytest.fail("refresh read source/audit instead of frozen metadata registration")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded)
    result = quality.refresh_quality(state, request())
    assert result["refresh"]["status"] == "refreshed"
    assert result["items"][0]["sources"][0]["current_task_usability"]["feature_input"] == "passed"
    assert str(output.parent) not in json.dumps(result)
    assert (state / quality.SOURCE_NAME).stat().st_mode & 0o077 == 0


def test_changed_processed_file_retains_history_but_revokes_current_applicability(registered):
    state, _, output = registered
    output.write_bytes(b"new replacement with a different size")
    quality.refresh_quality(state, request())
    source = row(state)
    assert source["check_status"] == source["task_usability"]["modeled_replay"] == "passed"
    assert source["audit_applicability"]["status"] == "changed_since_observation"
    assert source["current_task_usability"]["modeled_replay"] == "unknown"
    assert source["task_reasons"]["modeled_replay"] == "local_files_changed_since_observation"
    # A second button click cannot adopt the new file as the old audit's baseline.
    quality.refresh_quality(state, request())
    assert row(state)["current_task_usability"]["modeled_replay"] == "unknown"


def test_same_size_replacement_is_detected_by_stat_not_rehashed(registered):
    state, _, output = registered
    replacement = output.with_suffix(".replacement")
    replacement.write_bytes(b"x" * output.stat().st_size)
    replacement.replace(output)
    quality.refresh_quality(state, request())
    assert row(state)["audit_applicability"]["status"] == "changed_since_observation"


def test_missing_and_unmounted_are_distinct_and_do_not_reuse_old_pass(registered):
    state, _, output = registered
    output.unlink()
    quality.refresh_quality(state, request())
    assert row(state)["replica"]["status"] == "missing"
    output.parent.rmdir()
    quality.refresh_quality(state, request())
    assert row(state)["replica"]["status"] == "unknown"
    assert row(state)["replica"]["observation_reason"] == "local_inventory_root_unavailable"
    assert row(state)["current_task_usability"]["modeled_replay"] == "unknown"


@pytest.mark.parametrize("defect", ["size", "version", "no_binding"])
def test_explicit_binding_mismatch_or_absence_stays_historical(registered, defect):
    state, manifest, _ = registered
    data = json.loads(manifest.read_text())
    inventory = data["datasets"][0]["inventories"][0]
    if defect == "size":
        inventory["files_by_day"]["2026-08-02"][0]["size_bytes"] += 1
    elif defect == "version":
        inventory["audit_version"] = "other-version"
    else:
        inventory.pop("audit_version")
    manifest.write_text(json.dumps(data))
    quality.import_quality(state, manifest)
    assert row(state)["check_status"] == "passed"
    assert row(state)["current_task_usability"]["modeled_replay"] == "unknown"


def test_not_applicable_scope_remains_na_without_an_audit_row(registered):
    state, _, _ = registered
    source = quality.quality_days(state, "2026-08-01", "2026-08-01")["items"][0]["sources"][0]
    assert source["current_task_usability"]["funding_pnl"] == "not_applicable"
    assert source["current_task_usability"]["feature_input"] == "unknown"
    assert source["audit_applicability"]["status"] == "no_audit"


def test_full_research_calendar_is_visible_without_splitting_years(registered):
    state, manifest, _ = registered
    data = json.loads(manifest.read_text())
    data.update(start_day="2025-08-01", end_day="2026-09-05")
    manifest.write_text(json.dumps(data))
    quality.import_quality(state, manifest)
    assert len(json.loads((state / quality.NAME).read_text())["records"]["provider-book"]) == 401
    assert len(quality.quality_days(state, "2025-08-01", "2026-09-05")["items"]) == 401
    assert quality.quality_catalog(state)["calendar"] == {
        "start_day": "2025-08-01", "end_day": "2026-09-05",
    }
    with pytest.raises(ValueError, match="730 days"):
        quality.quality_days(state, "2024-08-01", "2026-09-05")


def test_raw_hourly_files_keep_partial_counts_without_claiming_content_quality(tmp_path):
    paths = [tmp_path / f"hour-{hour:02d}.zst" for hour in range(24)]
    for path in paths[:23]:
        path.write_bytes(b"source")
    result = quality._observe_inventory(
        {"node": "local", "directory": str(tmp_path),
         "files_by_day": {"2026-08-01": [str(p) for p in paths]}},
        "2026-08-01", "BTCUSDC", "provider-original",
    )
    assert result["status"] == "missing"
    assert result["file_counts"] == {"expected": 24, "present": 23, "missing": 1}
    paths[-1].write_bytes(b"source")
    complete = quality._observe_inventory(
        {"node": "local", "directory": str(tmp_path),
         "files_by_day": {"2026-08-01": [str(p) for p in paths]}},
        "2026-08-01", "BTCUSDC", "provider-original",
    )
    assert complete["status"] == "present_unverified"
    assert complete["file_counts"] == {"expected": 24, "present": 24, "missing": 0}


def test_refresh_endpoint_rejects_paths_commands_and_unknown_ids(registered):
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from narrowgate.studio import create_app

    state, _, _ = registered
    client = TestClient(create_app(state))
    assert client.post("/api/data-quality/refresh", json=request()).status_code == 200
    for extra in ("path", "command", "manifest", "argv"):
        assert (
            client.post(
                "/api/data-quality/refresh", json={**request(), extra: "/private/path"}
            ).status_code
            == 400
        )
    assert (
        client.post("/api/data-quality/refresh", json=request(dataset_id="unknown")).status_code
        == 404
    )
    assert client.post("/api/data-quality/refresh", json=request("unknown")).status_code == 404
    assert (
        client.post("/api/data-quality/refresh", json=request(start_day="2024-01-01")).status_code
        == 400
    )


def test_unregistered_refresh_is_explicit_without_mutating_old_catalog(tmp_path):
    result = quality.refresh_quality(tmp_path, request(dataset_id=""))
    assert result["refresh"]["status"] == "not_registered"
    assert not (tmp_path / quality.NAME).exists()
