from __future__ import annotations

import threading

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from data.normalize_tardis_orderbook import _ParquetPairWriter


def payload(offset=0, rows=2):
    times = 1_767_225_600_100 + offset + np.arange(rows, dtype=np.int64) * 100
    return {"normalized_timestamp": times,
            "normalized_levels": np.tile(np.array([100, 1, 102, 1, 99, 1, 103, 1], dtype=np.int64) * 100_000_000,
                                         (rows, 1)).ravel(),
            "normalized_observed": times * 1000 - 1000,
            "normalized_local": times * 1000 + 100,
            "normalized_carried": np.ones(rows, dtype=bool)}


def writer(root, **kwargs):
    return _ParquetPairWriter(root, "BTCUSDC", "2026-01-01", 2, **kwargs)


def test_parallel_writer_is_byte_equal_and_drained_before_publication(tmp_path, monkeypatch):
    import data.normalize_tardis_orderbook as module
    serial, parallel = writer(tmp_path / "serial"), writer(tmp_path / "parallel", write_workers=2)
    observed_threads = []
    original = parallel._write_tables
    def observed(tables):
        observed_threads.append(threading.get_ident())
        original(tables)
    monkeypatch.setattr(parallel, "_write_tables", observed)
    original_publish = module._publish_files
    def publish(pairs):
        if pairs[0][1] == parallel.bbo_final:
            assert parallel.closed and parallel._pending is None and parallel._executor is None
        original_publish(pairs)
    monkeypatch.setattr(module, "_publish_files", publish)
    for value in (serial, parallel):
        # Exercise the list-buffer flush and native matrix paths together.
        value.append(1_767_225_600_000, [(100., 1.), (99., 1.)], [(102., 1.), (103., 1.)],
                     exchange_cut_us=1_767_225_599_999_000, last_provider_local_us=1_767_226_000_000_000)
        value.append_fusion_batch(payload())
        value.append_fusion_batch(payload(200))
        value.close(publish=True)
    for kind in ("bbo", "l2", "clock"):
        left, right = getattr(serial, f"{kind}_final"), getattr(parallel, f"{kind}_final")
        assert left.read_bytes() == right.read_bytes()
        assert pq.read_table(left).equals(pq.read_table(right))
    assert observed_threads and set(observed_threads) != {threading.get_ident()}
    assert parallel.runtime()["async_batches"] == 3
    assert parallel.runtime()["max_in_flight_batches"] == 1
    assert parallel.runtime()["pending_batches"] == 0
    assert parallel.runtime()["peak_pending_bytes"] <= parallel.max_pending_bytes


def test_parallel_writer_has_only_one_pending_batch(tmp_path, monkeypatch):
    value = writer(tmp_path, write_workers=2)
    entered, release, returned = threading.Event(), threading.Event(), threading.Event()
    original = value._write_tables
    calls, errors = [], []
    def blocked(tables):
        calls.append(len(tables[0]))
        if len(calls) == 1:
            entered.set()
            assert release.wait(5)
        original(tables)
    monkeypatch.setattr(value, "_write_tables", blocked)
    value.append_fusion_batch(payload())
    assert entered.wait(5)
    def next_batch():
        try:
            value.append_fusion_batch(payload(200))
        except BaseException as exc:
            errors.append(exc)
        finally:
            returned.set()
    thread = threading.Thread(target=next_batch)
    thread.start()
    try:
        assert not returned.wait(.05)
        assert calls == [2]
        assert value.runtime()["pending_batches"] == 1
        assert value.runtime()["async_batches"] == 1
    finally:
        release.set()
        thread.join(5)
        value.close(publish=False)
    assert not thread.is_alive() and not errors
    assert value.runtime()["async_batches"] == 2
    assert not list(tmp_path.rglob("*.tmp"))


def test_oversize_batch_uses_synchronous_fallback_without_rowgroup_changes(tmp_path, monkeypatch):
    value = writer(tmp_path, write_workers=2, write_max_pending_bytes=1)
    ids = []
    original = value._write_tables
    def observed(tables):
        ids.append(threading.get_ident())
        original(tables)
    monkeypatch.setattr(value, "_write_tables", observed)
    value.append_fusion_batch(payload())
    value.close(publish=True)
    assert ids == [threading.get_ident()]
    assert value.runtime()["async_batches"] == 0
    assert value.runtime()["oversize_synchronous_batches"] == 1
    assert value.runtime()["peak_pending_bytes"] == 0
    assert pq.ParquetFile(value.l2_final).metadata.num_rows == 2


@pytest.mark.parametrize("where", ["finish", "append", "close"])
def test_background_failure_propagates_and_never_publishes(tmp_path, monkeypatch, where):
    value = writer(tmp_path, write_workers=2)
    failed = threading.Event()
    def fail(tables):
        value.bbo_writer.write_table(tables[0])
        failed.set()
        raise OSError("synthetic background disk failure")
    monkeypatch.setattr(value, "_write_tables", fail)
    value.append_fusion_batch(payload())
    assert failed.wait(5)
    with pytest.raises(OSError, match="background disk"):
        if where == "finish":
            value.finish()
        elif where == "append":
            value.append_fusion_batch(payload(200))
        else:
            value.close(publish=True)
    value.close(publish=False)
    assert value.closed and value._executor is None and value._pending is None
    assert value.runtime()["failure_observed"]
    assert not list(tmp_path.rglob("*.parquet")) and not list(tmp_path.rglob("*.tmp"))


def test_abort_joins_writer_before_removing_temporary_files(tmp_path, monkeypatch):
    value = writer(tmp_path, write_workers=2)
    entered, release, stopped = threading.Event(), threading.Event(), threading.Event()
    original = value._write_tables
    def blocked(tables):
        entered.set()
        assert release.wait(5)
        assert value.bbo_tmp.exists()
        original(tables)
    monkeypatch.setattr(value, "_write_tables", blocked)
    value.append_fusion_batch(payload())
    assert entered.wait(5)
    def abort():
        value.close(publish=False)
        stopped.set()
    thread = threading.Thread(target=abort)
    thread.start()
    try:
        assert not stopped.wait(.05)
        assert value.bbo_tmp.exists()
    finally:
        release.set()
        thread.join(5)
    assert not thread.is_alive() and stopped.is_set()
    assert value.closed and value._executor is None
    assert not list(tmp_path.rglob("*.tmp"))


@pytest.mark.parametrize("kwargs", [{"write_workers": 0}, {"write_workers": 3}, {"write_workers": True},
                                    {"write_max_pending_bytes": 0}, {"write_max_pending_bytes": -1}])
def test_invalid_pipeline_resources_are_rejected_before_outputs(tmp_path, kwargs):
    with pytest.raises(ValueError):
        writer(tmp_path / "new", **kwargs)
    assert not (tmp_path / "new").exists()


def test_empty_writer_does_not_create_a_background_task(tmp_path):
    value = writer(tmp_path, write_workers=2)
    value.append_fusion_batch(payload(rows=0))
    value.close(publish=True)
    assert value.runtime()["async_batches"] == 0
    assert value._executor is None and not list(tmp_path.rglob("*.tmp"))


@pytest.mark.parametrize("workers,cap", [(1, 32 * 1024**2), (2, 1)])
def test_synchronous_write_failure_is_visible_in_failure_receipt(tmp_path, monkeypatch, workers, cap):
    value = writer(tmp_path, write_workers=workers, write_max_pending_bytes=cap)
    def fail(tables):
        raise OSError("synthetic synchronous write failure")
    monkeypatch.setattr(value, "_write_tables", fail)
    with pytest.raises(OSError, match="synchronous write"):
        value.append_fusion_batch(payload())
    value.close(publish=False)
    assert value.runtime()["failure_observed"]
    assert value.closed and value._executor is None
    assert not list(tmp_path.rglob("*.tmp"))


def test_generator_close_drains_and_preserves_existing_final_files(tmp_path, monkeypatch):
    import data.normalize_tardis_orderbook as module
    base = module._day_start_us("2026-01-01")
    source = tmp_path / "source.parquet"
    records = []
    for offset, snapshot, qty in ((100_000, True, "1.0"), (300_000, False, "2.0"), (500_000, False, "3.0")):
        for side, price in (("bid", "100"), ("bid", "99"), ("ask", "102"), ("ask", "103")):
            records.append({"exchange": "binance-futures", "symbol": "BTCUSDC", "timestamp": base+offset,
                            "local_timestamp": base+offset+50, "is_snapshot": snapshot,
                            "side": side, "price": price, "amount": qty})
    pq.write_table(pa.Table.from_pylist(records), source)
    root = tmp_path / "views"
    finals = [root / kind / f"BTCUSDC-{kind}-2026-01-01.parquet" for kind in ("bbo", "l2", "clock")]
    for path in finals:
        path.parent.mkdir(parents=True)
        path.write_bytes(b"existing final must remain untouched")
    entered, release = threading.Event(), threading.Event()
    original = module._ParquetPairWriter._write_tables
    def blocked(self, tables):
        entered.set()
        assert release.wait(5)
        original(self, tables)
    monkeypatch.setattr(module._ParquetPairWriter, "_write_tables", blocked)
    monkeypatch.setattr(module, "FUSION_PUSH_ROWS", 4)
    batches, stats = module.iter_fused_book_batches({"tardis": source}, "2026-01-01", minimum_levels=2,
        observed_union=True, normalized_root=root, output_end_us=base+1_000_000, write_workers=2)
    next(batches)  # Same-clock messages stay pending until the next clock.
    next(batches)
    next(batches)
    assert entered.wait(5)
    release.set()
    batches.close()
    assert stats["status"] == "FAILED"
    assert stats["write_pipeline"]["closed"] and stats["write_pipeline"]["pending_batches"] == 0
    assert all(path.read_bytes() == b"existing final must remain untouched" for path in finals)
    assert not list(root.rglob("*.tmp"))
