"""Synthetic source fixtures only; never ship licensed raw market records."""

import json
from decimal import Decimal as D

import pytest

from data.__main__ import main
from data.facts import calendar_plan, inventory_calendar, materialize, read_facts, validate_bundle
from data.tardis_input import BookMessage, TradeExecution


def plan(tmp_path):
    book = tmp_path / "book.csv"
    book.write_text("exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount\n"
                    "binance-futures,BTCUSDC,1000000,2000000,true,bid,100,1\n"
                    "binance-futures,BTCUSDC,1000000,2000000,true,ask,102,2\n"
                    "binance-futures,BTCUSDC,2000000,3000000,false,bid,100,4\n")
    header = "exchange,symbol,timestamp,local_timestamp,id,side,price,amount\n"
    first, second = tmp_path / "first.csv", tmp_path / "second.csv"
    first.write_text(header + "binance-futures,BTCUSDC,1000000,3000000,1,buy,100.00000000000000001,0.01\n")
    second.write_text(header + "binance-futures,BTCUSDC,1000000,4000000,1,buy,100.00000000000000001,0.010\n"
                      "binance-futures,BTCUSDC,1000000,4000000,2,sell,101,1\n")
    path = tmp_path / "plan.json"
    path.write_text(json.dumps({"source_profile": "tardis_only", "files": [
        {"path": str(book), "channel": "incremental_book_L2", "symbol": "BTCUSDC"},
        {"path": str(first), "channel": "trades", "symbol": "BTCUSDC"},
        {"path": str(second), "channel": "trades", "symbol": "BTCUSDC"}]}))
    return path


@pytest.mark.parametrize("size", [1, 2, 8192])
def test_fact_roundtrip_and_cross_file_disk_dedup(tmp_path, size):
    path = plan(tmp_path)
    result = materialize(path, tmp_path / "facts", row_group_size=size)
    rows = list(read_facts(tmp_path / "facts"))
    assert len(rows) == 4
    assert isinstance(rows[0], BookMessage) and len(rows[0].levels) == 2
    assert isinstance(rows[2], TradeExecution)
    assert rows[2].price == D("100.00000000000000001")
    assert [x.trade_id for x in rows if isinstance(x, TradeExecution)] == [1, 2]
    assert [x.source_ordinal for x in rows if isinstance(x, TradeExecution)] == [0, 1]
    assert result["files"][2]["quality"]["duplicate_trades"] == 1
    assert not (tmp_path / "facts" / "identities.sqlite").exists()
    assert all(x.exchange_ts_ns is None for x in rows)
    check = validate_bundle(tmp_path / "facts")
    assert check["files"][0]["invalid_book_states"] == 0
    assert check["files"][0]["snapshots"] == 1


def test_failed_bundle_never_published_and_existing_bundle_not_overwritten(tmp_path):
    path = plan(tmp_path)
    (tmp_path / "second.csv").write_text((tmp_path / "second.csv").read_text().replace(",0.010", ",0.011"))
    with pytest.raises(ValueError, match="conflicting"):
        materialize(path, tmp_path / "facts")
    assert not (tmp_path / "facts").exists()
    assert not list(tmp_path.glob("facts.*.part"))
    (tmp_path / "facts").mkdir()
    with pytest.raises(FileExistsError):
        materialize(path, tmp_path / "facts")


def test_bound_reader_rejects_corruption(tmp_path):
    result = materialize(plan(tmp_path), tmp_path / "facts")
    file = tmp_path / "facts" / result["files"][0]["file"]
    with file.open("ab") as handle:
        handle.write(b"bad")
    with pytest.raises(ValueError, match="identity"):
        list(read_facts(tmp_path / "facts"))


def test_bound_reader_rejects_manifest_source_profile_change(tmp_path):
    result = materialize(plan(tmp_path), tmp_path / "facts")
    result["source_profile"] = "retired_mixed_sources"
    (tmp_path / "facts" / "manifest.json").write_text(json.dumps(result))
    with pytest.raises(ValueError, match="incompatible"):
        list(read_facts(tmp_path / "facts"))


def test_inventory_keeps_all_407_dates_and_unknowns(tmp_path):
    result = inventory_calendar(tmp_path)
    assert result["summary"]["dates"] == 407
    assert result["summary"]["channels"] == {"missing": 1628}
    assert result["days"][0]["calendar_date"] == "2025-08-01"
    assert result["days"][-1]["calendar_date"] == "2026-09-11"
    assert len({x["calendar_date"] for x in result["days"]}) == 407
    assert all(c["future_fill_violations"] is None for r in result["days"] for c in r["channels"])


def test_inventory_duplicate_selection_not_silently_ranked(tmp_path):
    root = tmp_path / "binance-futures/trades/2025/08/01"
    root.mkdir(parents=True)
    for suffix in ("xz", "zst"):
        (root / ("BTCUSDC.csv." + suffix)).touch()
    result = inventory_calendar(tmp_path)
    assert result["summary"]["channels"]["duplicate_source_selection"] == 1
    with pytest.raises(ValueError, match="duplicate_source_selection"):
        calendar_plan(tmp_path, start="2025-08-01", end="2025-08-01", symbols=["BTCUSDC"], channels=["trades"])


def test_calendar_selection_never_drops_missing_middle_day(tmp_path):
    root = tmp_path / "binance-futures/trades/2025/08/01"
    root.mkdir(parents=True)
    (root / "BTCUSDC.csv.xz").touch()
    with pytest.raises(ValueError, match="2025-08-02"):
        calendar_plan(tmp_path, start="2025-08-01", end="2025-08-03", symbols=["BTCUSDC"], channels=["trades"])


def test_neutral_download_always_archive_only(monkeypatch, tmp_path):
    import data.downloaders.tardis_archive as adapter
    calls = []
    monkeypatch.setattr(adapter, "main", lambda argv: calls.append(argv) or 0)
    assert main(["download", "--config", str(tmp_path / "private.json")]) == 0
    assert calls == [["--delivery-config", str(tmp_path / "private.json"), "--archive-only"]]


def test_retired_pipeline_dispatcher_does_not_exist():
    from pathlib import Path

    assert not (Path(__file__).resolve().parents[1] / "pipeline.py").exists()


def test_installed_data_entry_help_is_same_neutral_interface(capsys):
    from narrowgate.cli import main as installed_main
    with pytest.raises(SystemExit) as result:
        installed_main(["data", "--help"])
    assert result.value.code == 0
    output = capsys.readouterr().out
    assert "normalize" in output and "validate" in output and "Tardis" not in output


def _calendar_raw(tmp_path, day, *, trade_id=1, snapshot=True):
    import lzma
    from datetime import datetime, timezone
    ts = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()) * 1_000_000
    base = tmp_path / "raw/binance-futures"
    records = {
        "incremental_book_L2": "exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount\n"
            + f"binance-futures,BTCUSDC,{ts},{ts+100},{'true' if snapshot else 'false'},bid,100,1\n"
            + f"binance-futures,BTCUSDC,{ts},{ts+100},{'true' if snapshot else 'false'},ask,102,2\n",
        "trades": "exchange,symbol,timestamp,local_timestamp,id,side,price,amount\n"
            + f"binance-futures,BTCUSDC,{ts},{ts+100},{trade_id},buy,101,1\n"}
    for channel, content in records.items():
        p = base / channel / day.replace("-", "/") / "BTCUSDC.csv.xz"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(lzma.compress(content.encode()))
    return tmp_path / "raw"


def test_full_calendar_content_resume_and_missing_denominator(tmp_path):
    from data.facts import materialize_calendar
    root = _calendar_raw(tmp_path, "2025-08-01")
    _calendar_raw(tmp_path, "2025-08-02", trade_id=2)
    kwargs = dict(start="2025-08-01", end="2025-08-03", symbols=["BTCUSDC"], workers=2)
    result = materialize_calendar(root, tmp_path / "out", **kwargs)
    assert result["status"] == "incomplete"
    assert result["summary"] == {"content_scanned": 2, "blocked": 1}
    assert result["calendar_dates"] == 3
    q = result["days"][1]["files"][0]
    assert q["quality"]["invalid_book_states"] == 0
    assert q["boundary"]["interval_us"] == 86_400_000_000
    original = (tmp_path / "out/2025-08-01/manifest.json").read_bytes()
    _calendar_raw(tmp_path, "2025-08-03", trade_id=3)
    result = materialize_calendar(root, tmp_path / "out", **kwargs)
    assert result["status"] == "full_content_scanned"
    assert (tmp_path / "out/2025-08-01/manifest.json").read_bytes() == original
    assert result["boundary_checks"]["identity_conflicts"] == 0
    from data.facts import validate_calendar
    accepted = validate_calendar(tmp_path / "out")
    assert accepted["dates"] == 3 and accepted["economic_admission"] is False
    assert accepted["totals"]["BTCUSDC/trades"]["rows"] == 3
    shard = tmp_path / "out/2025-08-01/00001-BTCUSDC-trades.parquet"
    shard.write_bytes(shard.read_bytes() + b"corrupt")
    with pytest.raises(ValueError, match="bytes changed"):
        validate_calendar(tmp_path / "out")


def test_calendar_cross_file_id_conflict_not_timestamp_dedup(tmp_path):
    from data.facts import materialize_calendar
    root = _calendar_raw(tmp_path, "2025-08-01", trade_id=1)
    _calendar_raw(tmp_path, "2025-08-02", trade_id=1)
    result = materialize_calendar(root, tmp_path / "out", start="2025-08-01", end="2025-08-02", symbols=["BTCUSDC"], workers=1)
    assert result["status"] == "boundary_failed"
    assert "conflicting" in result["boundary_error"]


def test_calendar_resume_rejects_modified_original(tmp_path):
    from data.facts import materialize_calendar
    root = _calendar_raw(tmp_path, "2025-08-01")
    kwargs = dict(start="2025-08-01", end="2025-08-01", symbols=["BTCUSDC"], workers=1)
    materialize_calendar(root, tmp_path / "out", **kwargs)
    _calendar_raw(tmp_path, "2025-08-01", trade_id=9)
    result = materialize_calendar(root, tmp_path / "out", **kwargs)
    assert result["summary"] == {"failed": 1}


def test_explicit_source_location_reuse_preserves_frozen_bundle(tmp_path):
    from data.facts import _calendar_day_task, calendar_plan, materialize_calendar, read_facts, validate_calendar
    root = _calendar_raw(tmp_path, "2025-08-01")
    output = tmp_path / "out"
    result = materialize_calendar(root, output, start="2025-08-01", end="2025-08-01", symbols=["BTCUSDC"], workers=1)
    daily = output / "2025-08-01"
    original = (daily / "manifest.json").read_bytes()
    calendar_bytes = (output / "manifest.json").read_bytes()
    events = list(read_facts(daily))
    new = tmp_path / "retained"
    root.rename(new)
    plan = calendar_plan(new, start="2025-08-01", end="2025-08-01", symbols=["BTCUSDC"], channels=["incremental_book_L2", "trades"])
    plan["build_identity"] = "f" * 64
    with pytest.raises(ValueError, match="source plan/parser"):
        _calendar_day_task(plan, daily, "f" * 64)
    reused = _calendar_day_task(plan, daily, "f" * 64, result["build_identity"])
    assert reused["source_build_identity"] == result["build_identity"]
    assert validate_calendar(output, raw_root=new)["dates"] == 1
    assert list(read_facts(daily)) == events
    assert (daily / "manifest.json").read_bytes() == original
    assert (output / "manifest.json").read_bytes() == calendar_bytes
    assert not root.exists()


def test_explicit_source_location_rejects_other_content_identity_and_missing(tmp_path):
    import copy
    import os
    import shutil
    from pathlib import Path
    from data.facts import _calendar_day_task, calendar_plan, materialize_calendar
    root = _calendar_raw(tmp_path, "2025-08-01")
    result = materialize_calendar(root, tmp_path / "out", start="2025-08-01", end="2025-08-01", symbols=["BTCUSDC"], workers=1)
    new = tmp_path / "retained"
    shutil.copytree(root, new)
    plan = calendar_plan(new, start="2025-08-01", end="2025-08-01", symbols=["BTCUSDC"], channels=["incremental_book_L2", "trades"])
    plan["build_identity"] = result["build_identity"]
    for mutation in ("symbol", "file_date", "order", "mapper"):
        wrong = copy.deepcopy(plan)
        if mutation == "order":
            wrong["files"].reverse()
        elif mutation == "mapper":
            wrong["mapper_evidence"] = "changed"
        else:
            wrong["files"][0][mutation] = "changed"
        with pytest.raises(ValueError, match="identity|source plan/parser"):
            _calendar_day_task(wrong, tmp_path / "out/2025-08-01", result["build_identity"])
    source = Path(plan["files"][0]["path"])
    stat = source.stat()
    contents = source.read_bytes()
    source.write_bytes(bytes([contents[0] ^ 1]) + contents[1:])
    os.utime(source, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    with pytest.raises(ValueError, match="content changed"):
        _calendar_day_task(plan, tmp_path / "out/2025-08-01", result["build_identity"])
    source.unlink()
    with pytest.raises(FileNotFoundError):
        _calendar_day_task(plan, tmp_path / "out/2025-08-01", result["build_identity"])
    assert root.exists()  # An available historical source is deliberately not used.


def test_calendar_explicit_historical_reuse_does_not_relabel_new_parser(tmp_path, monkeypatch):
    from data import facts
    root = _calendar_raw(tmp_path, "2025-08-01")
    original = facts.materialize_calendar(root, tmp_path / "out", start="2025-08-01", end="2025-08-01",
                                         symbols=["BTCUSDC"], workers=1)
    before = (tmp_path / "out/2025-08-01/manifest.json").read_bytes()
    monkeypatch.setattr(facts, "_build_identity", lambda: "f" * 64)
    _calendar_raw(tmp_path, "2025-08-02", trade_id=2)
    result = facts.materialize_calendar(root, tmp_path / "out", start="2025-08-01", end="2025-08-02",
                                       symbols=["BTCUSDC"], workers=1,
                                       reuse_build_identity=original["build_identity"])
    assert result["status"] == "full_content_scanned"
    assert [d["source_build_identity"] for d in result["days"]] == [original["build_identity"], "f" * 64]
    assert (tmp_path / "out/2025-08-01/manifest.json").read_bytes() == before
    resumed = facts.materialize_calendar(root, tmp_path / "out", start="2025-08-01", end="2025-08-02",
                                        symbols=["BTCUSDC"], workers=1,
                                        reuse_build_identity=original["build_identity"])
    assert resumed["status"] == "full_content_scanned"
