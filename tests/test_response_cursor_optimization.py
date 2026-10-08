"""Synthetic equivalence of observation materialization, not strategy speed."""
import pickle
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from data.response_cursor import _Rows, _COLUMNS, ResponseRowGroupCache, using_row_group_cache
from data.tardis_input import BookView
from features.trade_book_response import ResponseState


def test_suppressed_frames_preserve_full_state_and_queries():
    left = ResponseState(trade_coverage='observed', fast_response_state=True)
    right = ResponseState(trade_coverage='observed', fast_response_state=True)
    for i in range(140):
        view = BookView(i, ((Decimal(100), Decimal(1+i % 7)), (Decimal(98), Decimal(2))),
                        ((Decimal(102), Decimal(2)),), None, None, 'synthetic', i % 19 != 0)
        now = i*100_000_000
        left.observe_book(now, now, view, reset=i == 75)
        assert right.observe_book(now, now, view, reset=i == 75, emit_frames=False) == ()
        assert pickle.dumps(left) == pickle.dumps(right)
        for side in ('bid', 'ask'):
            assert left.frame(side) == right.frame(side)
            for price in (97, 98, 99, 100, 101, 102, 103):
                assert left.order_frame(side, price) == right.order_frame(side, price)
    for price in ('NaN', 'Infinity', -1, 0):
        with pytest.raises(ValueError):
            right.order_frame('bid', price)


class Bundle:
    def __init__(self, path):
        self.path = path
        self.root = path.parent
        self.manifest = {'files': {'depth': {'sha256': 'synthetic-admitted-identity'}}}
        self.verified = {}

    def _verified_parquet(self, name):
        stat = self.path.stat()
        self.verified[name] = (stat.st_ino, stat.st_size, stat.st_mtime_ns)
        return pq.ParquetFile(self.path)


def test_projected_rowgroups_and_restore(tmp_path):
    rows = []
    for i in range(11):
        row = {k: i for k in _COLUMNS['depth']}
        row.update(valid=True, bid_px_exact=['100', '99'], bid_qty_exact=['1', '2'],
                   ask_px_exact=['101'], ask_qty_exact=['3'], unused_float=[999.5])
        rows.append(row)
    path = tmp_path/'synthetic.parquet'
    pq.write_table(pa.Table.from_pylist(rows), path, row_group_size=3)
    stream = _Rows(Bundle(path), 'depth')
    for i in range(11):
        row = stream.peek()
        assert row is stream.peek()
        assert set(stream._rows) == set(_COLUMNS['depth'])
        assert {k: row[k] for k in _COLUMNS['depth']} == {
            k: rows[i][k] for k in _COLUMNS['depth']}
        if i in (2, 3, 7):
            restored = pickle.loads(pickle.dumps(stream))
            assert restored._reader is restored._rows is restored._head is None
            assert restored.peek()['observation_sequence'] == i
            restored.pop()
            assert stream.peek()['observation_sequence'] == i
        stream.pop()
    assert stream.peek() is None
    assert stream.opened_groups == 4
    assert stream.decoded_rows == 11


def test_bounded_cache_preserves_restore_counts_and_immutable_columns(tmp_path):
    rows = []
    for i in range(8):
        row = {k: i for k in _COLUMNS['depth']}
        row.update(valid=True, bid_px_exact=['100'], bid_qty_exact=['1'],
                   ask_px_exact=['101'], ask_qty_exact=['2'])
        rows.append(row)
    path = tmp_path/'cache.parquet'
    pq.write_table(pa.Table.from_pylist(rows), path, row_group_size=4)
    cache = ResponseRowGroupCache(100_000)
    stream = _Rows(Bundle(path), 'depth')
    with using_row_group_cache(cache):
        assert stream.pop()['observation_sequence'] == 0
        for _ in range(3):
            restored = pickle.loads(pickle.dumps(stream))
            assert restored.peek()['observation_sequence'] == 1
            assert restored.opened_groups == 2 and restored.decoded_rows == 8
            assert stream.offset == 1
            for column in restored._rows.values():
                assert all(not b.is_mutable for b in column.buffers() if b is not None)
        assert cache.physical_decode == 1 and cache.cache_hit == 3
        while stream.peek() is not None:
            stream.pop()
        assert cache.physical_decode == 2
        assert cache.bytes <= cache.max_bytes
    # No process-global retained rows: outside the owner scope the reference
    # reader remains unchanged, and only positions cross a checkpoint.
    clone = pickle.loads(pickle.dumps(stream))
    assert clone._rows is None


def test_cache_identity_and_limit():
    table = pa.table({'x': [1, 2, 3]})
    cache = ResponseRowGroupCache(1)
    cache.read(('first',), lambda: table)
    cache.read(('first',), lambda: table)
    assert cache.physical_decode == 2 and not cache.entries
    cache = ResponseRowGroupCache()
    cache.read(('first',), lambda: table)
    columns, _ = cache.read(('changed',), lambda: pa.table({'x': [4, 5, 6]}))
    assert columns['x'].to_pylist() == [4, 5, 6]
    assert cache.physical_decode == 2
    columns, count = cache.read(('empty',), lambda: pa.table({'x': pa.array([], type=pa.int64())}))
    assert count == 0 and columns['x'].to_pylist() == []
