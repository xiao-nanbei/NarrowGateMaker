"""Output-only replay journal using the existing bounded atomic writer."""
from collections import Counter
import hashlib
import json
import math
from pathlib import Path

from execution.chunked_parquet_journal import ChunkedParquetJournalWriter
from execution.chunked_parquet_journal import iter_chunked_parquet_journal


class ReplayL2Production:
    """Producer-owned logical event ledger, independent of optional delivery.

    Call observe at the production site, before branching on the output sink.
    One logical event requires one journal row; delivery retries reuse its ID.
    This proves the instrumented scope, not uninstrumented business paths.
    """
    def __init__(self):
        self.counts = Counter()
        self.total = 0
        self.digest = b''

    def observe(self, kind, timestamp_ms, side=''):
        self.total += 1
        self.counts[f'{kind}:{side}'] += 1
        token = dict(sequence=self.total, kind=kind, timestamp_ms=int(timestamp_ms), side=side)
        self.digest = hashlib.sha256(self.digest + json.dumps(token, sort_keys=True).encode()).digest()
        return token

    def receipt(self):
        return dict(total=self.total, counts=dict(self.counts), sha256=self.digest.hex(),
                    scope='instrumented_replay_callbacks_after_original_warmup_filters',
                    records_per_logical_event=1)


def audit_l2_delivery(manifest, production):
    """Verify producer -> submitted -> persisted/read-back IDs and counts."""
    observed = ReplayL2Production()
    for row in iter_chunked_parquet_journal(manifest):
        token = row.get('production_event')
        expected = observed.observe(row['event_type'], row['event_ts_ns'] // 1_000_000, row['side'])
        if token != expected:
            raise ValueError('L2 missing, duplicate or mismatched production event')
    if observed.receipt() != production:
        raise ValueError('L2 production and persisted event coverage differ')
    return observed.receipt()


def _plain(value):
    if hasattr(value, 'item'):
        value = value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {str(k): _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    return value


class ReplayL2Journal:
    """No policy/RNG ownership; a failed write fails the replay, never drops rows."""
    def __init__(self, output, *, identity, chunk_rows=4096):
        self.identity = dict(identity)
        self.writer = ChunkedParquetJournalWriter(output, journal_id='replay_l2.v1', chunk_rows=chunk_rows)
        self.counts = Counter()
        self.delivery = ReplayL2Production()
        self.production_mode = None

    def checkpoint_prefix(self, production):
        """Seal output only, not the simulated account; branches use new writers."""
        if self.production_mode and production != self.delivery.receipt():
            raise ValueError('L2 checkpoint production and delivery differ')
        self.writer.close()
        audit_l2_delivery(self.writer.manifest_path, production)
        return dict(identity=self.identity, manifest=str(self.writer.manifest_path),
                    manifest_sha256=hashlib.sha256(self.writer.manifest_path.read_bytes()).hexdigest(),
                    production=production)

    def restore_prefix(self, prefix):
        """Reproduce a verified immutable prefix in a fresh branch output."""
        if self.writer.row_count or self.writer.closed or self.identity != prefix['identity']:
            raise ValueError('L2 resume requires a fresh writer with the same identity')
        manifest = Path(prefix['manifest'])
        if manifest.resolve() == self.writer.manifest_path.resolve():
            raise ValueError('L2 branch cannot overwrite its checkpoint prefix')
        if hashlib.sha256(manifest.read_bytes()).hexdigest() != prefix['manifest_sha256']:
            raise ValueError('L2 checkpoint prefix changed')
        for row in iter_chunked_parquet_journal(manifest):
            self.emit(row['event_type'], row['event_ts_ns'] // 1_000_000,
                      side=row['side'], payload=row['record'], production_event=row['production_event'])
        if self.delivery.receipt() != prefix['production']:
            raise ValueError('L2 checkpoint prefix coverage differs')

    def emit(self, kind, timestamp_ms, *, side='', payload=None, production_event=None):
        mode = production_event is not None
        if self.production_mode is not None and mode != self.production_mode:
            raise ValueError('cannot mix audited and unaudited journal rows')
        self.production_mode = mode
        if mode:
            expected = self.delivery.observe(kind, timestamp_ms, side)
            if production_event != expected:
                raise ValueError('L2 missing, duplicate or mismatched production event')
        row = _plain(dict(payload or {}))
        sequence = self.writer.row_count + 1
        self.writer.append(dict(sequence=sequence, event_type=kind,
                                event_ts_ns=int(timestamp_ms)*1_000_000,
                                side=side, decision_id=str(row.get('decision_id', '')),
                                clock_provenance='modeled_replay_millisecond_clock',
                                native_exchange_clock_observed=False,
                                production_event=production_event,
                                record=row))
        self.counts[kind] += 1

    def close(self, *, production=None):
        if self.production_mode:
            if production != self.delivery.receipt():
                raise ValueError('L2 production and submitted event coverage differ')
            # Persist producer evidence before closing. A later write failure
            # leaves an unclosed journal, never an invented successful receipt.
            path = self.writer.output_dir / 'production.json'
            encoded = json.dumps(production, sort_keys=True, indent=2) + '\n'
            if path.exists():
                if path.read_text() != encoded:
                    raise ValueError('conflicting immutable production receipt')
            else:
                with path.open('x') as handle:
                    handle.write(encoded)
        manifest = self.writer.close()
        if self.production_mode:
            audit_l2_delivery(self.writer.manifest_path, production)
        return dict(identity=self.identity, counts=dict(self.counts),
                    rows=manifest['row_count'], parts=manifest['part_count'],
                    manifest=str(self.writer.manifest_path), dropped=0 if production is not None else None,
                    production=production, independent_delivery_verified=production is not None,
                    opportunity_scope='all_side_decision_callback_invocations_after_warmup',
                    native_parity=False)
