"""Single-owner continuous journal: audit new rows, retain immutable prefixes.

The ordinary branching journal is unchanged. This opt-in writer may continue
the exact prefix it just produced in memory; a fresh process copies and fully
validates immutable parts without re-encoding their rows before appending.
Economic state is never owned by this output sink.
"""
from collections import Counter
from copy import deepcopy
import json
import os

import pyarrow.parquet as pq

from execution.chunked_parquet_journal import _ARROW_SCHEMA, _sha256_file
from models.replay.l2_journal import ReplayL2Journal, ReplayL2Production


class ContinuousReplayL2Journal(ReplayL2Journal):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._audited = ReplayL2Production()
        self._audited_parts = 0
        self._last_prefix = None

    def _audit_new_parts(self):
        """Independently read back only newly sealed, checksummed parts."""
        for part in self.writer._parts[self._audited_parts:]:
            path = self.writer.output_dir/part['path']
            if path.stat().st_size != part['bytes'] or _sha256_file(path) != part['sha256']:
                raise ValueError('continuous journal part changed')
            rows = pq.read_table(path, schema=_ARROW_SCHEMA).to_pylist()
            if len(rows) != part['rows']:
                raise ValueError('continuous journal row count differs')
            for row in rows:
                payload = json.loads(row['payload_json'])
                expected = self._audited.observe(row['event_type'], row['event_ts_ns']//1_000_000, row['side'])
                if row['sequence'] != expected['sequence'] or payload.get('production_event') != expected:
                    raise ValueError('continuous journal production sequence differs')
            self._audited_parts += 1
        if self._audited.receipt() != self.delivery.receipt():
            raise ValueError('continuous journal persisted coverage differs')

    def checkpoint_prefix(self, production):
        if self.production_mode is False or production != self.delivery.receipt():
            raise ValueError('continuous journal requires audited production')
        self.writer.flush()
        self._audit_new_parts()
        if not self.writer.manifest_path.exists():
            self.writer._write_manifest(closed=False)
        # Keep the writer open, but expose a separate immutable closed snapshot.
        # Later appends never mutate the manifest referenced by a disk checkpoint.
        manifest = self.writer.output_dir/f'prefix-{self.writer.row_count:012d}.json'
        payload = json.loads(self.writer.manifest_path.read_text())
        payload['closed'] = True
        encoded = json.dumps(payload, sort_keys=True, indent=2)+'\n'
        if manifest.exists():
            if manifest.read_text() != encoded:
                raise ValueError('conflicting continuous prefix')
        else:
            with manifest.open('x') as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
        prefix = dict(identity=self.identity, manifest=str(manifest), production=production,
                      manifest_sha256=_sha256_file(manifest))
        self._last_prefix = deepcopy(prefix)
        return prefix

    def restore_prefix(self, prefix):
        if self._last_prefix is not None:
            if (prefix != self._last_prefix or self.writer.closed
                    or prefix['production'] != self.delivery.receipt()):
                raise ValueError('continuous writer may only resume its exact current prefix')
            return
        if self.writer.row_count or self.writer.closed or self.identity != prefix['identity']:
            raise ValueError('L2 resume requires a fresh writer with the same identity')
        # This is full recovery validation, not the in-memory fast path. The
        # writer owns raw copy, hashes, sequence and manifest publication; this
        # sink still independently observes every persisted producer token.
        recovered = ReplayL2Production()
        counts = Counter()

        def validate_rows(rows):
            for row in rows:
                payload = json.loads(row['payload_json'])
                expected = recovered.observe(
                    row['event_type'], row['event_ts_ns']//1_000_000, row['side']
                )
                if payload.get('production_event') != expected:
                    raise ValueError('continuous journal production sequence differs')
                # Preserve the actual L2 envelope, not a differently typed or
                # accidentally mixed journal with matching producer counters.
                record = payload.get('record')
                if (not isinstance(record, dict)
                        or row['event_ts_ns'] % 1_000_000
                        or row['decision_id'] != str(record.get('decision_id', ''))
                        or payload.get('clock_provenance') != 'modeled_replay_millisecond_clock'
                        or payload.get('native_exchange_clock_observed') is not False):
                    raise ValueError('continuous journal L2 row envelope differs')
                counts[row['event_type']] += 1

        def validate_complete():
            if recovered.receipt() != prefix['production']:
                raise ValueError('L2 checkpoint prefix coverage differs')

        self.writer.copy_verified_prefix(
            prefix['manifest'],
            expected_manifest_sha256=prefix.get('manifest_sha256'),
            validate_rows=validate_rows,
            validate_complete=validate_complete,
        )
        self.delivery = recovered
        self.counts = counts
        self.production_mode = True if recovered.total else None
        self._audited = deepcopy(recovered)
        self._audited_parts = self.writer.part_count
        self._last_prefix = deepcopy(prefix)
