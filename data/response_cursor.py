"""Causal cursor over explicitly retained shared-producer observations.

No parser, latency sampler, strategy, or synthetic delivery clocks live here.
Checkpoint state contains row-group positions, not file handles or iterators.
"""
from decimal import Decimal
from collections import OrderedDict
from contextlib import contextmanager
from contextvars import ContextVar
from types import MappingProxyType

from data.observation import TradeContribution
from data.runtime import ConsumerBundle, ObservationProfile
from data.tardis_input import BookView
from features.trade_book_response import ResponseState


_CLOCK_COLUMNS = ('observation_sequence', 'source_asof_ns', 'publish_ns', 'receive_ns', 'ready_ns')
_COLUMNS = {
    'depth': _CLOCK_COLUMNS + ('book_version', 'valid', 'bid_px_exact', 'bid_qty_exact',
                              'ask_px_exact', 'ask_qty_exact'),
    'trade_observations': _CLOCK_COLUMNS + ('event_id', 'exchange_ts_ns', 'price', 'quantity',
        'side', 'individual_count', 'native_packet_count', 'count_origin', 'source_ordinal'),
}

_ROW_CACHE = ContextVar('prepared_response_row_cache', default=None)


class ResponseRowGroupCache:
    """Bounded prepared-owned immutable columns, never checkpoint state."""
    def __init__(self, max_bytes=64*1024*1024):
        self.max_bytes = max_bytes
        self.bytes = self.physical_decode = self.cache_hit = 0
        self.entries = OrderedDict()

    def read(self, key, decode):
        if key in self.entries:
            self.cache_hit += 1
            self.entries.move_to_end(key)
            columns, rows, _ = self.entries[key]
            return columns, rows
        table = decode()
        self.physical_decode += 1
        if table.num_rows == 0 or table.nbytes > self.max_bytes:
            return {name: table[name].combine_chunks() for name in table.column_names}, table.num_rows
        # Arrow's public arrays are immutable, but allocator-owned buffers can
        # expose a mutable buffer. IPC on Python bytes makes ownership explicit.
        import pyarrow as pa
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table.combine_chunks(), max_chunksize=max(1, table.num_rows))
        payload = sink.getvalue().to_pybytes()
        frozen = pa.ipc.open_stream(pa.BufferReader(payload)).read_all()
        columns = MappingProxyType({name: frozen[name].chunk(0) for name in frozen.column_names})
        size = len(payload)
        if size <= self.max_bytes:
            while self.entries and self.bytes + size > self.max_bytes:
                _, (_, _, removed) = self.entries.popitem(last=False)
                self.bytes -= removed
            self.entries[key] = columns, table.num_rows, size
            self.bytes += size
        return columns, table.num_rows


@contextmanager
def using_row_group_cache(cache):
    token = _ROW_CACHE.set(cache)
    try:
        yield
    finally:
        _ROW_CACHE.reset(token)


class _ColumnRow:
    """Transient view; no per-row dictionary or unused depth float arrays."""
    def __init__(self, columns, offset):
        self.columns, self.offset = columns, offset

    def __getitem__(self, name):
        return self.columns[name][self.offset].as_py()


class _Rows:
    def __init__(self, bundle, name):
        self.bundle = bundle
        self.name = name
        self.group = self.offset = 0
        self._reader = self._rows = None
        self._head = None
        self.decoded_rows = self.opened_groups = 0

    def __getstate__(self):
        return {**self.__dict__, '_reader': None, '_rows': None, '_head': None}

    def peek(self):
        if self._head is not None:
            return self._head
        if self._reader is None:
            self._reader = self.bundle._verified_parquet(self.name)
        while self.group < self._reader.num_row_groups:
            if self._rows is None:
                def decode():
                    return self._reader.read_row_group(self.group, columns=_COLUMNS[self.name], use_threads=False)
                cache = _ROW_CACHE.get()
                if cache is None:
                    table = decode()
                    self._rows = {name: table[name].combine_chunks() for name in table.column_names}
                    rows = table.num_rows
                else:
                    key = (str(self.bundle.root), self.bundle.manifest['files'][self.name]['sha256'],
                           self.bundle.verified[self.name], self.name, self.group, _COLUMNS[self.name])
                    self._rows, rows = cache.read(key, decode)
                self.decoded_rows += rows
                self.opened_groups += 1
            if self.offset < len(self._rows['observation_sequence']):
                self._head = _ColumnRow(self._rows, self.offset)
                return self._head
            self.group += 1
            self.offset = 0
            self._rows = None
        return None

    def pop(self):
        row = self.peek()
        if row is None:
            raise EOFError(self.name)
        self.offset += 1
        self._head = None
        return row


class ResponseObservationCursor:
    """One path's visible history, independently restorable at any read point.

    The selected bundle must explicitly preserve both sides of the original
    observation sequence. Sparse market research panels cannot stand in for it.
    """
    def __init__(self, bundle):
        self.bundle = bundle if isinstance(bundle, ConsumerBundle) else ConsumerBundle(bundle)
        plan = self.bundle.manifest['plan']
        if (plan.get('include_response_observations') is not True or
                'trade_observations' not in self.bundle.manifest['files']):
            raise ValueError('bundle has no bound ready trade observation stream')
        profile = ObservationProfile(**plan['observation_profile'])
        self.state = ResponseState(max_book_age_ns=profile.max_book_age_ns,
                                   trade_coverage=profile.trade_coverage,
                                   fast_response_state=True, range_block_size=64)
        self.streams = (_Rows(self.bundle, 'depth'), _Rows(self.bundle, 'trade_observations'))
        self.sequence = 0
        self.last_ready_ns = -1
        self.read_ns = -1

    def advance(self, read_ns):
        if type(read_ns) is not int or read_ns < self.read_ns:
            raise ValueError('response read clock regressed or is not integer nanoseconds')
        while True:
            left, right = self.streams
            a, b = left.peek(), right.peek()
            if a is None and b is None:
                break
            if b is None or (a is not None and a['observation_sequence'] <= b['observation_sequence']):
                row, stream = a, left
            else:
                row, stream = b, right
            if row['observation_sequence'] != self.sequence + 1:
                raise ValueError('missing or duplicate observation sequence')
            ready = row['ready_ns']
            if not (0 <= row['source_asof_ns'] <= row['publish_ns'] <= row['receive_ns'] <= ready
                    and ready >= self.last_ready_ns):
                raise ValueError('invalid observation clocks; do not sort to repair')
            if ready > read_ns:
                break
            if stream.name == 'trade_observations':
                self.state.observe_trade(ready, TradeContribution(
                    row['event_id'], row['exchange_ts_ns'], Decimal(row['price']), Decimal(row['quantity']),
                    row['side'], row['individual_count'], row['native_packet_count'],
                    row['count_origin'], row['source_ordinal']))
            else:
                def levels(side, row=row):
                    return tuple((Decimal(p), Decimal(q)) for p, q in zip(
                        row[side + '_px_exact'], row[side + '_qty_exact'], strict=True))
                # Source clock provenance remains on the bound input profile;
                # this is not a claim of native exchange timestamps.
                view = BookView(row['book_version'], levels('bid'), levels('ask'),
                                None, None, 'retained_shared_observation', row['valid'])
                self.state.observe_book(ready, row['source_asof_ns'], view, emit_frames=False)
            stream.pop()
            self.sequence = row['observation_sequence']
            self.last_ready_ns = ready
        self.state.advance(read_ns)
        self.read_ns = read_ns

    def frame(self, side, *, order_price):
        return self.state.order_frame(side, order_price)
