"""Single-owner continuous journal: audit new rows, retain immutable prefixes.

The ordinary branching journal is unchanged. This opt-in writer may continue
the exact prefix it just produced in memory; a fresh process still performs
the ordinary full read/restore before appending. Economic state is never owned
by this output sink.
"""
from copy import deepcopy
import json
import os
from pathlib import Path

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
        # Recovery/branching is never treated as trusted in-memory continuation.
        # The inherited path validates every producer token and copies the
        # immutable prefix into this new writer once, not every market window.
        manifest = Path(prefix['manifest'])
        if _sha256_file(manifest) != prefix.get('manifest_sha256'):
            raise ValueError('continuous recovery manifest changed')
        payload = json.loads(manifest.read_text())
        for part in payload['parts']:
            if _sha256_file(manifest.parent/part['path']) != part['sha256']:
                raise ValueError('continuous recovery prefix changed')
        super().restore_prefix(prefix)
        self.writer.flush()
        self._audit_new_parts()
        self._last_prefix = deepcopy(prefix)
