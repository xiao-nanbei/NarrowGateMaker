import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from data.daily_raw import UNION_BOOK_SCHEMA, book_stream_priority
from data.daily_schema_cutover import identity, publish_transaction, usage_digest
from data.unify_book_calendar import prepare_catalogs, prepare_day, verify_publication


@pytest.mark.parametrize("tamper", [None, "date", "clock", "book", "journal", "plan", "unverified"])
def test_new_batch_inherits_only_unchanged_adjacent_publication(tmp_path, tamper):
    from data.unify_book_calendar import _batch_predecessor
    state, book = tmp_path / "state", tmp_path / "book"
    day = "2025-09-17"
    (state / day).mkdir(parents=True)
    book.mkdir()
    target = book / "bbo.parquet"
    pq.write_table(pa.table({"value": [1]}), target)
    files = [{"after": identity(target)}]
    plan = {"symbol": "BTCUSDC", "capture_mode": "tardis", "logical_stream_id": "paid-book"}
    source_plan = tmp_path / "plan.json"
    source_plan.write_text(json.dumps(plan))
    end = int(datetime(2025, 9, 18, tzinfo=timezone.utc).timestamp()) * 1_000_000
    journal = state / day / "publication.json"
    journal.write_text(json.dumps({"day": day, "status": "FILES_PUBLISHED", "catalog_prepared": True,
        "files": files, "deep_final_state": {"observed": end-200_000},
        "observation_grid": {"last_output_us": end-100_000, "last_observed_us": end-200_000}}))
    previous = {"start": "2025-08-01", "end": "2026-09-05",
        "supplement_manifest": identity(source_plan), "days": {day: {
            "status": "PUBLISHED_VERIFIED", "files": files, "stream_id": "s3",
            "top_final_state": {"observed": end-200_000}, "deep_final_state_ref": identity(journal)}}}
    path = state / "current.json"
    first = "2025-09-18"
    if tamper == "date":
        first = "2025-09-19"
    elif tamper == "clock":
        plan = {**plan, "capture_mode": "binance_futures_native"}
    elif tamper == "book":
        pq.write_table(pa.table({"value": [2]}), target)
    elif tamper == "journal":
        journal.write_text("{}")
    elif tamper == "plan":
        source_plan.write_text("{}")
    elif tamper == "unverified":
        previous["days"][day]["status"] = "FAILED"
    path.write_text(json.dumps(previous))
    before = path.read_bytes()
    args = (path, first, "2025-08-01", "2026-09-05", plan, tmp_path, book)
    if tamper:
        with pytest.raises(ValueError):
            _batch_predecessor(*args)
    else:
        top, deep, slot, grid = _batch_predecessor(*args)
        assert top == deep == {"observed": end-200_000}
        assert slot == "s3"
        assert grid == (end-100_000, end-200_000)
    assert path.read_bytes() == before  # a new batch never edits its predecessor


def test_supplement_plan_requires_explicit_full_suffix_mapping():
    from data.unify_book_calendar import supplement_stream_plan
    days = ["2025-11-30", "2025-12-01", "2025-12-02"]
    assert supplement_stream_plan({"schema": "daily_book_supplements.v1", "stream_id": "s3"}, days) == (
        dict.fromkeys(days, "s3"), None)
    slots = dict(zip(days, ["s3", "s1", "s1"], strict=True))
    plan = {"schema": "daily_book_supplements.v2", "logical_stream_id": "capture-1",
            "stream_ids_by_day": slots}
    assert supplement_stream_plan(plan, days) == (slots, "capture-1")
    for changed in (
        {**plan, "stream_ids_by_day": {days[0]: "s3"}},
        {**plan, "stream_ids_by_day": {**slots, "2025-12-03": "s1"}},
        {**plan, "stream_ids_by_day": {**slots, days[1]: "tardis"}},
        {**plan, "logical_stream_id": None},
        {**plan, "stream_id": "s3"},
        {**plan, "schema": "daily_book_supplements.v1"},
    ):
        with pytest.raises(ValueError):
            supplement_stream_plan(changed, days)


@pytest.mark.parametrize("capture_mode", ["tardis", "binance_futures_native"])
@pytest.mark.parametrize("legacy_inline_state", [False, True])
@pytest.mark.parametrize("write_workers", [1, 2])
def test_calendar_seam_mapping_and_resume_use_previous_successful_slot(tmp_path, monkeypatch, capture_mode,
                                                                      legacy_inline_state, write_workers):
    import data.unify_book_calendar as module
    catalog, state = tmp_path / "catalog", tmp_path / "state"
    catalog.mkdir()
    days = ["2025-11-30", "2025-12-01", "2025-12-02"]
    (catalog / "owner-manifest.json").write_text(json.dumps({"start_day": days[0], "end_day": days[-1]}))
    source = tmp_path / "capture"
    source.write_text("synthetic captured input")
    plan = tmp_path / "plan.json"
    plan.write_text(json.dumps({"schema": "daily_book_supplements.v2", "symbol": "BTCUSDC", "capture_mode": capture_mode,
        "logical_stream_id": "capture-1", "stream_ids_by_day": dict(zip(days, ["s3", "s1", "s1"], strict=True)),
        "days": {days[0]: {"path": str(source), "sha256": identity(source)["sha256"]}}}))
    prepared, calls = {}, []
    def prepare(data, book, stage, day, previous_top, **kwargs):
        calls.append((day, kwargs))
        end = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp())*1_000_000 + 86_400_000_000
        item = {"raw_rows": 1, "top_rows": 0, "top_metrics": {}, "files": [],
                "top_final_state": {"day": day}, "deep_final_state": {"day": day},
                "day": day, "status": "FILES_PUBLISHED", "catalog_prepared": True,
                "observation_grid": {"last_output_us": end-100_000, "last_observed_us": end-120_000},
                "write_pipeline": {"workers": kwargs["write_workers"], "max_pending_bytes": kwargs["write_max_pending_bytes"]},
                "seam_transition": {"previous_slot": kwargs["previous_stream_id"]}}
        stage.mkdir(parents=True)
        (stage / "publication.json").write_text(json.dumps(item))
        prepared[stage / "publication.json"] = item
        return item
    monkeypatch.setattr(module, "prepare_day", prepare)
    monkeypatch.setattr(module, "prepare_catalogs", lambda journal, **kwargs: prepared[journal])
    monkeypatch.setattr(module, "publish_transaction", lambda journal: prepared[journal])
    monkeypatch.setattr(module, "verify_publication", lambda *args, **kwargs: None)
    args = dict(data_root=tmp_path, book_root=tmp_path, catalog_root=catalog, state_root=state,
                start=days[0], end=days[-1], supplement_manifest=plan,
                write_workers=write_workers, write_max_pending_mib=3)
    assert module.run(**args, limit=1)["status"] == "PARTIAL"
    # An old successful operation has no cached grid tuple; recover it only
    # from that operation's already-published journal, not a book message.
    prior = json.loads((state / "current.json").read_text())
    assert "deep_final_state" not in prior["days"][days[0]]
    assert prior["days"][days[0]]["deep_final_state_ref"]["sha256"] == identity(state / days[0] / "publication.json")["sha256"]
    if legacy_inline_state:
        prior["days"][days[0]].pop("deep_final_state_ref")
        prior["days"][days[0]]["deep_final_state"] = {"day": days[0]}
    del prior["days"][days[0]]["observation_grid_tail"]
    (state / "current.json").write_text(json.dumps(prior))
    assert module.run(**args)["status"] == "COMPLETED"
    finished = json.loads((state / "current.json").read_text())
    assert all("deep_final_state" not in row and "deep_final_state_ref" in row for row in finished["days"].values())
    assert [item[0] for item in calls] == days
    assert all(item[1]["write_workers"] == write_workers and item[1]["write_max_pending_bytes"] == 3 * 1024**2
               for item in calls)
    assert finished["write_pipeline_requested"] == {"workers": write_workers, "max_pending_bytes": 3 * 1024**2}
    assert all(item["write_pipeline"] == finished["write_pipeline_requested"] for item in finished["days"].values())
    assert [item[1]["stream_id"] for item in calls] == ["s3", "s1", "s1"]
    assert [item[1]["previous_stream_id"] for item in calls] == [None, "s3", "s1"]
    assert calls[1][1]["previous_deep_state"] == {"day": days[0]}
    assert all(item[1]["logical_stream_id"] == "capture-1" for item in calls)
    assert all(item[1].get("capture_mode", "tardis") == capture_mode for item in calls)
    assert calls[0][1]["previous_grid"] is None
    for index in (1, 2):
        grid = prepared[state / days[index-1] / "publication.json"]["observation_grid"]
        assert calls[index][1]["previous_grid"] == (grid["last_output_us"], grid["last_observed_us"])
    if capture_mode == "tardis":
        assert all("capture_mode" not in item[1] for item in calls)


@pytest.mark.parametrize("tamper", ["sha", "day", "files", "inline", "status"])
def test_continuation_reference_rejects_unbound_or_mutated_journal(tmp_path, tamper):
    from data.unify_book_calendar import _resume_deep_state
    day = "2025-08-01"
    path = tmp_path / day / "publication.json"
    path.parent.mkdir()
    journal = {"day": day, "status": "FILES_PUBLISHED", "catalog_prepared": True,
               "files": [], "deep_final_state": {"levels": [1, 2, 3]}}
    if tamper in {"day", "status"}:
        journal[tamper] = "WRONG"
    path.write_text(json.dumps(journal))
    ref = {key: identity(path)[key] for key in ("path", "sha256", "size_bytes")}
    entry = {"files": [], "deep_final_state_ref": ref}
    if tamper == "sha":
        ref["sha256"] = "0"*64
    elif tamper == "files":
        entry["files"] = [{"unexpected": True}]
    elif tamper == "inline":
        entry["deep_final_state"] = {"other": 1}
    with pytest.raises(ValueError, match="continuation"):
        _resume_deep_state(tmp_path, day, entry)


def test_hybrid_continuation_keeps_only_verified_reference(tmp_path):
    from data.unify_book_calendar import _resume_deep_state
    day = "2025-08-01"
    path = tmp_path / day / "publication.json"
    path.parent.mkdir()
    state = {"levels": [1, 2, 3]}
    path.write_text(json.dumps({"day": day, "status": "FILES_PUBLISHED", "catalog_prepared": True,
                               "files": [], "deep_final_state": state}))
    ref = {key: identity(path)[key] for key in ("path", "sha256", "size_bytes")}
    entry = {"files": [], "deep_final_state_ref": ref, "deep_final_state": state}
    assert _resume_deep_state(tmp_path, day, entry) == state
    assert "deep_final_state" not in entry and entry["deep_final_state_ref"] == ref


def test_native_manifest_rejects_unknown_or_per_day_clock_mode():
    from data.unify_book_calendar import supplement_stream_plan
    plan = {"schema": "daily_book_supplements.v1", "stream_id": "s3",
            "capture_mode": "binance_futures_native"}
    assert supplement_stream_plan(plan, ["2025-08-01"])[0] == {"2025-08-01": "s3"}
    with pytest.raises(ValueError, match="unsupported supplement capture mode"):
        supplement_stream_plan({**plan, "capture_mode": "auto"}, ["2025-08-01"])
    with pytest.raises(ValueError, match="whole stream"):
        supplement_stream_plan({**plan, "days": {"2025-08-01": {"capture_mode": "tardis"}}}, ["2025-08-01"])
    with pytest.raises(ValueError, match="capture clock"):
        supplement_stream_plan({**plan, "capture_clock": "received"}, ["2025-08-01"])


def test_prepared_native_journal_cannot_be_reused_as_tardis(tmp_path):
    stage = tmp_path / "stage"
    stage.mkdir()
    (stage / "publication.json").write_text(json.dumps({
        "capture_contract": {"capture_mode": "binance_futures_native"}}))
    with pytest.raises(ValueError, match="prepared supplement capture mode"):
        prepare_day(tmp_path, tmp_path, stage, "2025-08-01", None, stream_id="s1")


@pytest.mark.parametrize("with_previous", [False, True])
@pytest.mark.parametrize("local_scratch", [False, True])
def test_one_day_real_projection_publication_and_catalog_resume(tmp_path, monkeypatch, with_previous, local_scratch):
    from pathlib import Path
    import data.unify_book_calendar as module
    monkeypatch.setattr(module.shutil, "disk_usage", lambda _: SimpleNamespace(free=10**14))
    day, start = "2025-08-01", 1754006400000000
    previous_grid = (start-100_000, start-150_000) if with_previous else None
    data, book, catalog, stage = [tmp_path / name for name in ("data", "book", "catalog", "stage")]
    raw = data / "raw/binance_futures/BTCUSDC" / day / "incremental_book_L2.parquet"
    raw.parent.mkdir(parents=True)
    for name in ("bbo", "l2", "clock", "quality"):
        (book / name).mkdir(parents=True)
    catalog.mkdir()
    ts = [start//1000+100, start//1000+200]
    bbo = pa.table({"timestamp": ts, "best_bid": [100., 100.], "best_bid_qty": [1., 1.],
                    "best_ask": [101., 101.], "best_ask_qty": [1., 1.]})
    l2 = pa.table({"timestamp": ts, "bid_px_1": [100., 100.], "bid_qty_1": [1., 1.],
                   "ask_px_1": [101., 101.], "ask_qty_1": [1., 1.]})
    clock = pa.table({"timestamp": ts, "exchange_cut_timestamp_us": [start+1]*2,
                      "last_provider_local_timestamp_us": [start+2]*2,
                      "exchange_resample_age_us": [99999, 199999],
                      "last_observation_timestamp_us": [start+1]*2,
                      "observation_age_us": [99999, 199999],
                      "observation_kind": ["source_observed", "carried_forward"],
                      "update_coverage": ["source_message_present", "unknown"]})
    outputs = {}
    for kind, table in (("bbo", bbo), ("l2", l2), ("clock", clock)):
        path = book / kind / f"BTCUSDC-{kind}-{day}.parquet"
        pq.write_table(table, path)
        outputs[kind] = identity(path)
    rows = [{"exchange": "binance-futures", "symbol": "BTCUSDC", "timestamp": start+1,
             "local_timestamp": start+2, "is_snapshot": True, "side": side,
             "price": price, "amount": "1", "source_id": "tardis", "source_native_sequence": False,
             "source_timestamp_us": start+1, "source_observed_timestamp_us": start+1}
            for side, price in (("bid", "100"), ("ask", "101"))]
    receipt = {"schema": "observed_union.v1", "day": day, "symbol": "BTCUSDC", "output_rows": 2,
               "stream_priority": book_stream_priority(), "stats": {"normalized": outputs}}
    table = pa.Table.from_pylist(rows, schema=UNION_BOOK_SCHEMA).replace_schema_metadata({
        b"narrowgate.book_fusion": b"observed_union.v1", b"narrowgate.fusion_receipt": json.dumps(receipt).encode()})
    pq.write_table(table, raw)
    raw_before = identity(raw)
    quality = book / "quality" / f"BTCUSDC-{day}.json"
    quality.write_text(json.dumps({"day": day, "symbol": "BTCUSDC", "raw_source": raw_before,
                                  **{f"{k}_output": v for k, v in outputs.items()}}))
    quality_before = identity(quality)
    raw_index = data / "raw/daily-index.json"
    raw_index.write_text(json.dumps({"records": [{**raw_before, "day": day, "symbol": "BTCUSDC",
        "channel": "incremental_book_L2", "included_sources": ["private name"]}]}))
    datasets = []
    channels = []
    for name, selected in ((module.RAW_ID, [raw_before]), (module.BOOK_ID, [outputs["bbo"], outputs["l2"]])):
        audit = catalog / f"{name}.csv"
        audit.write_text(f"day,source_quality_sha256,eligible\n{day},{quality_before['sha256']},false\n")
        datasets.append({"id": name, "audit": {"path": str(audit)},
                         "inventories": [{"node": "local", "files_by_day": {day: selected}}]})
        channels.append({"source_id": name, "files": selected, "source_content_validation": {"sha256": raw_before["sha256"]}})
    owner = {"start_day": day, "end_day": day, "datasets": datasets}
    (catalog / "owner-manifest.json").write_text(json.dumps(owner))
    readable = {"records": [{"calendar_date": day, "channels": channels,
        "research_use": {"validation": "locked", "known_previous_use": [{"sha256": raw_before["sha256"]}]}}]}
    (catalog / "readability.json").write_text(json.dumps(readable))
    rights = usage_digest(readable)
    (book / "daily_quality.csv").write_text(f"day,source_quality_sha256\n{day},{quality_before['sha256']}\n")
    (book / "manifest.json").write_text(json.dumps({"sources": [{"day": day,
        "raw_sha256": raw_before["sha256"], "quality_sha256": quality_before["sha256"], "included_sources": ["private name"]}]}))
    plan = prepare_day(data, book, stage, day, None, previous_grid=previous_grid)
    assert plan["observation_grid"]["cross_day_inherited"] is with_previous
    assert plan["observation_grid_predecessor"]["previous"] == (list(previous_grid) if with_previous else None)
    assert identity(raw) == raw_before
    assert plan["top_verification"]["future_fill_violations"] == 0
    prepare_catalogs(stage / "publication.json", data_root=data, book_root=book, catalog_root=catalog)
    published = publish_transaction(stage / "publication.json")
    verify_publication(published, catalog_root=catalog)
    prepare_catalogs(stage / "publication.json", data_root=data, book_root=book, catalog_root=catalog)
    verify_publication(published, catalog_root=catalog)
    assert usage_digest(json.loads((catalog / "readability.json").read_text())) == rights
    assert json.loads(raw_index.read_text())["records"][0]["sha256"] == identity(raw)["sha256"]
    assert "included_sources" not in json.loads(raw_index.read_text())["records"][0]
    selected = json.loads((book / "manifest.json").read_text())["sources"][0]
    assert selected["quality_sha256"] == identity(quality)["sha256"]

    # Publication wiring, independent of the fusion kernel's own state tests:
    # a supplement rebuild must replace L2 as well as BBO/clock/raw/metadata.
    import data.daily_raw as daily
    import shutil
    old_l2_sha = identity(book / "l2" / f"BTCUSDC-l2-{day}.parquet")["sha256"]
    def supplemented(current, supplement, output, actual_day, *, stream_id, previous_state, previous_top_state, normalized_root):
        assert actual_day == day and stream_id == "s3" and previous_state is None
        assert supplement is None  # a state-propagation day is not a missing research day
        if local_scratch:
            assert current.is_relative_to(tmp_path / "ssd")
            assert output.is_relative_to(tmp_path / "ssd")
            assert identity(current)["sha256"] == identity(raw)["sha256"]
        shutil.copyfile(current, output)
        generated = {}
        for kind in ("bbo", "l2", "clock"):
            table = pq.read_table(book / kind / f"BTCUSDC-{kind}-{day}.parquet")
            name = {"bbo": "best_bid_qty", "l2": "bid_qty_1"}.get(kind)
            if name:
                table = table.set_column(table.schema.get_field_index(name), name, pa.array([2., 2.]))
            target = normalized_root / kind / f"BTCUSDC-{kind}-{day}.parquet"
            target.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, target)
            generated[kind] = identity(target)
        return {**identity(output), "stats": {"normalized": generated},
                "initial_state": None, "final_state": {"test_only_state": 1},
                "top_final_state": None, "top_metrics": {}, "consumed_inputs": []}
    monkeypatch.setattr(daily, "supplement_daily_book_day", supplemented)
    if local_scratch:
        monkeypatch.setenv("NARROWGATE_BOOK_WORK_ROOT", str(tmp_path / "ssd"))
    repair_stage = tmp_path / "repair"
    repair = prepare_day(data, book, repair_stage, day, None, stream_id="s3", previous_grid=previous_grid)
    if local_scratch:
        assert not list((tmp_path / "ssd").iterdir())
        assert all(Path(item["staged"]).is_relative_to(repair_stage) for item in repair["files"])
    assert repair["observation_grid"]["cross_day_inherited"] is with_previous
    assert repair["observation_grid"]["unknown_state_rows"] == (0 if with_previous else 1)
    assert repair["deep_final_state"] == {"test_only_state": 1}
    assert identity(book / "l2" / f"BTCUSDC-l2-{day}.parquet")["sha256"] == old_l2_sha
    prepare_catalogs(repair_stage / "publication.json", data_root=data, book_root=book, catalog_root=catalog)
    repaired = publish_transaction(repair_stage / "publication.json")
    verify_publication(repaired, catalog_root=catalog)
    assert identity(book / "l2" / f"BTCUSDC-l2-{day}.parquet")["sha256"] != old_l2_sha
    assert usage_digest(json.loads((catalog / "readability.json").read_text())) == rights
    import csv
    with (catalog / f"{module.BOOK_ID}.csv").open() as handle:
        audit = next(csv.DictReader(handle))
    assert audit["continuous_read_verified"] == "false"
    assert audit["source_quality_path"] == str(quality)
    assert audit["source_quality_sha256"] == identity(quality)["sha256"]
    assert float(audit["max_stale_age_s"]) == repaired["observation_grid"]["max_stale_age_us"] / 1_000_000
    changed = json.loads((catalog / "readability.json").read_text())["records"][0]
    selected = next(c for c in changed["channels"] if c["source_id"] == module.BOOK_ID)
    assert selected["observation_grid"] == repaired["observation_grid"]
    assert selected["cross_day_data_state"] == "FULL_CALENDAR_RECHECK_PENDING"


@pytest.mark.parametrize("failure_at", ["prepare", "publish", "verify"])
def test_failed_day_does_not_advance_grid_and_book_last_message_is_not_a_grid(tmp_path, monkeypatch, failure_at):
    import data.unify_book_calendar as module
    catalog, state = tmp_path / "catalog", tmp_path / "state"
    catalog.mkdir()
    days = ["2025-08-01", "2025-08-02", "2025-08-03"]
    (catalog / "owner-manifest.json").write_text(json.dumps({"start_day": days[0], "end_day": days[-1]}))
    calls, prepared, failed = [], {}, False
    tails = {}
    for day in days:
        end = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp())*1_000_000 + 86_400_000_000
        tails[day] = (end-100_000, end-130_000)
    def prepare(data, book, stage, day, previous_top, **kwargs):
        nonlocal failed
        calls.append((day, kwargs["previous_grid"]))
        if day == days[1] and not failed and failure_at == "prepare":
            failed = True
            raise OSError("synthetic asynchronous writer failure")
        tail = tails[day]
        item = {"day": day, "raw_rows": 1, "top_rows": 0, "top_metrics": {}, "files": [],
            "top_final_state": {"last_grid_us": tail[0]},
            # This message is later than the last sampled grid. It must NOT
            # become the next day's grid predecessor.
            "deep_final_state": {"last_message_us": tail[0]+90_000, "observed_us": tail[0]+80_000},
            "observation_grid": {"last_output_us": tail[0], "last_observed_us": tail[1]}}
        prepared[stage / "publication.json"] = item
        return item
    def publish(journal):
        nonlocal failed
        if journal.parent.name == days[1] and not failed and failure_at == "publish":
            failed = True
            raise RuntimeError("synthetic publication failure")
        item = {**prepared[journal], "status": "FILES_PUBLISHED", "catalog_prepared": True}
        journal.parent.mkdir(parents=True, exist_ok=True)
        journal.write_text(json.dumps(item))
        return item
    def verify(item, **kwargs):
        nonlocal failed
        if item["day"] == days[1] and not failed and failure_at == "verify":
            failed = True
            raise RuntimeError("synthetic verification failure")
    monkeypatch.setattr(module, "prepare_day", prepare)
    monkeypatch.setattr(module, "prepare_catalogs", lambda journal, **kwargs: prepared[journal])
    monkeypatch.setattr(module, "publish_transaction", publish)
    monkeypatch.setattr(module, "verify_publication", verify)
    kwargs = dict(data_root=tmp_path, book_root=tmp_path, catalog_root=catalog, state_root=state,
                  start=days[0], end=days[-1])
    with pytest.raises((RuntimeError, OSError), match="synthetic"):
        module.run(**kwargs)
    stopped = json.loads((state / "current.json").read_text())
    assert set(stopped["days"]) == {days[0]}
    assert stopped["days"][days[0]]["observation_grid_tail"] == list(tails[days[0]])
    assert module.run(**kwargs)["status"] == "COMPLETED"
    assert calls == [(days[0], None), (days[1], tails[days[0]]),
                     (days[1], tails[days[0]]), (days[2], tails[days[1]])]


def test_previous_grid_recovery_refuses_unverified_or_other_operation_journal(tmp_path):
    import data.unify_book_calendar as module
    day = "2025-08-01"
    stage = tmp_path / day
    stage.mkdir()
    entry = {"files": [{"after": {"sha256": "frozen"}}], "status": "PUBLISHED_VERIFIED"}
    for journal in ({"day": day, "status": "PREPARED", "catalog_prepared": True, "files": entry["files"]},
                    {"day": day, "status": "FILES_PUBLISHED", "catalog_prepared": True, "files": []}):
        (stage / "publication.json").write_text(json.dumps(journal))
        with pytest.raises(ValueError, match="same verified operation"):
            module._resume_grid_tail(tmp_path, day, entry)


def test_prepared_grid_cannot_silently_change_predecessor(tmp_path):
    import data.unify_book_calendar as module
    day, start = "2025-08-01", 1754006400000000
    stage = tmp_path / "stage"
    stage.mkdir()
    (stage / "publication.json").write_text(json.dumps({
        "observation_grid_predecessor": module._grid_predecessor(day, None)}))
    with pytest.raises(ValueError, match="predecessor contract differs"):
        prepare_day(tmp_path, tmp_path, stage, day, None, previous_grid=(start-100_000, start-200_000))


def test_resume_cannot_skip_a_missing_predecessor_day(tmp_path):
    import data.unify_book_calendar as module
    catalog, state = tmp_path / "catalog", tmp_path / "state"
    catalog.mkdir()
    state.mkdir()
    start, end = "2025-08-01", "2025-08-02"
    (catalog / "owner-manifest.json").write_text(json.dumps({"start_day": start, "end_day": end}))
    (state / "current.json").write_text(json.dumps({"start": start, "end": end,
        "supplement_manifest": None, "days": {end: {"status": "PUBLISHED_VERIFIED"}}}))
    with pytest.raises(ValueError, match="continuous operation prefix"):
        module.run(data_root=tmp_path, book_root=tmp_path, catalog_root=catalog, state_root=state,
                   start=start, end=end)
