"""Normalized delivered history, stored once instead of per-panel duplication."""

import json

from data.observation import TradeContribution


class VisibleHistoryWriter:
    def __init__(self, root):
        import pyarrow as pa
        import pyarrow.parquet as pq
        self.pa = pa
        self.schema = pa.schema([
            ("ready_ns", pa.int64()), ("event_id", pa.string()),
            ("kind", pa.string()), ("side", pa.string()),
            ("price", pa.string()), ("quantity", pa.string()),
            ("metadata", pa.string())])
        self.writer = pq.ParquetWriter(root / "visible-history.parquet", self.schema, compression="zstd")
        self.rows, self.previous, self.count = [], None, 0

    def add(self, observation, kind, side=None, price=None, quantity=None, metadata=None):
        self.rows.append(dict(ready_ns=observation.ready_ns, event_id=observation.event_id,
                              kind=kind, side=side, price=None if price is None else str(price),
                              quantity=None if quantity is None else str(quantity),
                              metadata=json.dumps(metadata or {}, allow_nan=False)))
        self.count += 1
        if len(self.rows) >= 2048:
            self.flush()

    def depth(self, observation):
        new, old = observation.snapshot, self.previous
        bounds = dict(new.bounds)
        reset = old is None or new.snapshot_version != old.snapshot_version or new.book.version < old.book.version
        self.add(observation.original, "depth_reset" if reset else "depth_publication", metadata=dict(
            version=new.book.version, snapshot_version=new.snapshot_version, valid=new.book.valid,
            source_asof_ns=observation.original.source_asof_ns,
            bid_bounds=[str(x) for x in bounds['bid']] if bounds.get('bid') else None,
            ask_bounds=[str(x) for x in bounds['ask']] if bounds.get('ask') else None))
        for index, right in enumerate(new.buckets):
            left = {} if reset else old.buckets[index]
            if left is right:
                continue
            for side, price in left.keys() | right.keys():
                before, after = left.get((side, price), 0), right.get((side, price), 0)
                if reset or before != after:
                    self.add(observation.original, "visible_absolute_quantity", side, price, after)
        self.previous = new

    def trade(self, observation):
        if isinstance(observation.payload, TradeContribution):
            trade = observation.payload
            self.add(observation, "visible_trade", trade.side, trade.price, trade.quantity,
                     dict(coverage=observation.coverage, source_asof_ns=observation.source_asof_ns,
                          individual_count=trade.individual_count, native_packet_count=trade.native_packet_count,
                          source_ordinal=trade.source_ordinal))

    def flush(self):
        if self.rows:
            self.writer.write_table(self.pa.Table.from_pylist(self.rows, schema=self.schema))
            self.rows.clear()

    def close(self):
        self.flush()
        self.writer.close()
