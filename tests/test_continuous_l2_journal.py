"""Continuous output retains exact rows without replaying each old prefix."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from execution.chunked_parquet_journal import _ARROW_SCHEMA, iter_chunked_parquet_journal
from models.backtest_tick import simulate_tick
from models.replay.continuous_l2_journal import ContinuousReplayL2Journal
from models.replay.l2_journal import ReplayL2Journal, ReplayL2Production
from models.replay.runtime_checkpoint_io import save_runtime_checkpoint, load_trusted_runtime_checkpoint
from tests.test_tick_runtime_checkpoint import assert_same, scenario


@pytest.mark.parametrize('mode', ['ordinary', 'async', 'compute', 'timeout', 'emergency', 'close_replace'])
def test_in_memory_windows_and_disk_recovery_match_ordinary_journal(tmp_path, mode):
    args, kwargs = scenario(mode)
    expected = simulate_tick(*args[:3], {**args[3], '_l2_journal': ReplayL2Journal(tmp_path/'whole', identity={})}, **kwargs)
    expected_receipt = expected.pop('_l2_journal')
    writer = ContinuousReplayL2Journal(tmp_path/'continuous', identity={}, chunk_rows=3)
    cp = None
    for cut in [0, 500, 1100]:
        cp = simulate_tick(*args[:3], {**args[3], '_l2_journal': writer}, **kwargs,
                           checkpoint_at_ts_ms=cut, resume_checkpoint=cp,
                           consume_resume_checkpoint=cp is not None)['_replay_checkpoint']
    path = tmp_path/'saved.checkpoint'
    save_runtime_checkpoint(path, cp)
    prefix = Path(cp['l2_prefix']['manifest'])
    frozen = prefix.read_bytes()
    actual = simulate_tick(*args[:3], {**args[3], '_l2_journal': writer}, **kwargs,
                           resume_checkpoint=cp, consume_resume_checkpoint=True)
    receipt = actual.pop('_l2_journal')
    assert prefix.read_bytes() == frozen
    assert_same(actual, expected)
    expected_rows = list(iter_chunked_parquet_journal(expected_receipt['manifest']))
    assert list(iter_chunked_parquet_journal(receipt['manifest'])) == expected_rows
    fresh = ContinuousReplayL2Journal(tmp_path/'recovered', identity={}, chunk_rows=3)
    recovered = simulate_tick(*args[:3], {**args[3], '_l2_journal': fresh}, **kwargs,
                              resume_checkpoint=load_trusted_runtime_checkpoint(path))
    recovered_receipt = recovered.pop('_l2_journal')
    assert_same(recovered, expected)
    assert list(iter_chunked_parquet_journal(recovered_receipt['manifest'])) == expected_rows


def test_each_part_read_once_in_same_process_and_stale_prefix_refused(tmp_path, monkeypatch):
    import models.replay.continuous_l2_journal as module
    read = module.pq.read_table
    seen = []
    def counted(path, **kwargs):
        seen.append(str(path))
        return read(path, **kwargs)
    monkeypatch.setattr(module.pq, 'read_table', counted)
    writer = ContinuousReplayL2Journal(tmp_path/'out', identity={}, chunk_rows=2)
    producer = ReplayL2Production()
    prefixes = []
    for i in range(6):
        writer.emit('decision', i, production_event=producer.observe('decision', i))
        prefix = writer.checkpoint_prefix(producer.receipt())
        prefixes.append(prefix)
        writer.restore_prefix(prefix)
    assert len(seen) == 6 and len(set(seen)) == 6
    with pytest.raises(ValueError, match='exact current prefix'):
        writer.restore_prefix(prefixes[0])


def test_recovery_detects_payload_corruption(tmp_path):
    writer = ContinuousReplayL2Journal(tmp_path/'out', identity={})
    producer = ReplayL2Production()
    writer.emit('fill', 1, production_event=producer.observe('fill', 1))
    prefix = writer.checkpoint_prefix(producer.receipt())
    part = next((tmp_path/'out').glob('*.parquet'))
    part.write_bytes(part.read_bytes()+b'corrupt')
    fresh = ContinuousReplayL2Journal(tmp_path/'new', identity={})
    with pytest.raises(ValueError, match='prefix changed'):
        fresh.restore_prefix(prefix)


def test_recovery_rejects_rewritten_manifest(tmp_path):
    writer = ContinuousReplayL2Journal(tmp_path/'out', identity={})
    producer = ReplayL2Production()
    prefix = writer.checkpoint_prefix(producer.receipt())
    path = Path(prefix['manifest'])
    path.write_text(path.read_text()+' ')
    fresh = ContinuousReplayL2Journal(tmp_path/'new', identity={})
    with pytest.raises(ValueError, match='manifest changed'):
        fresh.restore_prefix(prefix)


def _sample_prefix(tmp_path):
    source = ContinuousReplayL2Journal(
        tmp_path/'source', identity={'account': 'original'}, chunk_rows=2
    )
    producer = ReplayL2Production()
    for i in range(7):
        kind = 'decision' if i % 2 else 'fill'
        source.emit(
            kind, i, side='BUY', payload={'decision_id': str(i), 'wide': [i, {'x': i}]},
            production_event=producer.observe(kind, i, 'BUY'),
        )
    return source, producer, source.checkpoint_prefix(producer.receipt())


def test_fresh_recovery_copies_exact_parts_once_and_branches_append_independently(tmp_path, monkeypatch):
    import models.replay.l2_journal as l2_module

    source, producer, prefix = _sample_prefix(tmp_path)
    source_bytes = {p.name: p.read_bytes() for p in source.writer.output_dir.glob('*.parquet')}
    original_manifest = Path(prefix['manifest']).read_bytes()
    expected_rows = list(iter_chunked_parquet_journal(prefix['manifest']))
    branches = [
        ContinuousReplayL2Journal(tmp_path/name, identity=source.identity, chunk_rows=3)
        for name in ('left', 'right')
    ]

    def no_encode(*args, **kwargs):
        raise AssertionError('prefix restoration must not re-encode any row')

    with monkeypatch.context() as patch:
        patch.setattr(pq, 'write_table', no_encode)
        patch.setattr(l2_module, '_plain', no_encode)
        for branch in branches:
            writes = []
            original_write = branch.writer._write_manifest

            def counted_write(*, closed, writes=writes, original_write=original_write):
                writes.append(closed)
                return original_write(closed=closed)

            patch.setattr(branch.writer, '_write_manifest', counted_write)
            branch.restore_prefix(prefix)
            assert writes == [False]
            assert branch.delivery.receipt() == producer.receipt()
            assert branch.counts == source.counts
            assert branch.writer.row_count == 7
            assert branch.writer.part_count == 4
            assert not branch.writer.closed
            for name, raw in source_bytes.items():
                copied = branch.writer.output_dir/name
                assert copied.read_bytes() == raw
                assert not copied.is_symlink()
                assert copied.stat().st_ino != (source.writer.output_dir/name).stat().st_ino
            assert list(iter_chunked_parquet_journal(
                branch.writer.manifest_path, require_closed=False
            )) == expected_rows

    for i, branch in enumerate(branches):
        branch_producer = deepcopy(producer)
        branch.emit('fill', 100+i, side='SELL', payload={'branch': i},
                    production_event=branch_producer.observe('fill', 100+i, 'SELL'))
        receipt = branch.close(production=branch_producer.receipt())
        rows = list(iter_chunked_parquet_journal(receipt['manifest']))
        assert rows[:7] == expected_rows
        assert rows[-1]['sequence'] == 8 and rows[-1]['record'] == {'branch': i}
        assert receipt['counts'] == {'fill': 5, 'decision': 3}
        assert receipt['production'] == branch_producer.receipt()
        assert receipt['dropped'] == 0 and receipt['independent_delivery_verified']
    assert Path(prefix['manifest']).read_bytes() == original_manifest
    assert {p.name: p.read_bytes() for p in source.writer.output_dir.glob('*.parquet')} == source_bytes


def _rewrite_signed_prefix(prefix, mutate_manifest=None, mutate_rows=None):
    """Update hashes deliberately so structural/token validation is exercised."""
    manifest = Path(prefix['manifest'])
    payload = json.loads(manifest.read_text())
    if mutate_rows:
        part = payload['parts'][1]
        path = manifest.parent/part['path']
        rows = pq.read_table(path, schema=_ARROW_SCHEMA).to_pylist()
        mutate_rows(rows)
        pq.write_table(pa.Table.from_pylist(rows, schema=_ARROW_SCHEMA), path, compression='zstd')
        part['sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
        part['bytes'] = path.stat().st_size
    if mutate_manifest:
        mutate_manifest(payload)
    manifest.write_text(json.dumps(payload))
    prefix['manifest_sha256'] = hashlib.sha256(manifest.read_bytes()).hexdigest()


@pytest.mark.parametrize('mutation, message', [
    ('sequence', 'sequence gap'),
    ('token', 'production sequence'),
    ('rows', 'row count mismatch'),
    ('bounds', 'sequence bounds'),
    ('bytes', 'prefix changed'),
    ('total', 'total row count'),
    ('last', 'last sequence'),
    ('parts', 'part count'),
    ('journal_id', 'identity differs'),
    ('open', 'not closed'),
    ('schema', 'unexpected chunked'),
    ('identity', 'same identity'),
    ('receipt', 'coverage differs'),
    ('envelope', 'row envelope'),
    ('path', 'relative filename'),
])
def test_copy_recovery_rejects_invalid_prefix_without_publishing(tmp_path, mutation, message):
    source, _, prefix = _sample_prefix(tmp_path)
    if mutation == 'identity':
        prefix['identity'] = {'account': 'other'}
    elif mutation == 'receipt':
        prefix['production']['total'] += 1
    elif mutation in ('sequence', 'token', 'envelope'):
        def mutate(rows):
            if mutation == 'sequence':
                rows[0]['sequence'] += 1
            else:
                payload = json.loads(rows[0]['payload_json'])
                if mutation == 'token':
                    payload['production_event']['sequence'] += 1
                else:
                    payload['record'] = []
                rows[0]['payload_json'] = json.dumps(payload)
        _rewrite_signed_prefix(prefix, mutate_rows=mutate)
    else:
        def mutate(payload):
            if mutation == 'rows':
                payload['parts'][1]['rows'] += 1
            elif mutation == 'bounds':
                payload['parts'][1]['last_sequence'] += 1
            elif mutation == 'bytes':
                payload['parts'][1]['bytes'] += 1
            elif mutation == 'total':
                payload['row_count'] += 1
            elif mutation == 'last':
                payload['last_sequence'] += 1
            elif mutation == 'parts':
                payload['part_count'] += 1
            elif mutation == 'journal_id':
                payload['journal_id'] = 'different'
            elif mutation == 'open':
                payload['closed'] = False
            elif mutation == 'schema':
                payload['schema_version'] = 'different'
            elif mutation == 'path':
                payload['parts'][1]['path'] = '../elsewhere.parquet'
        _rewrite_signed_prefix(prefix, mutate_manifest=mutate)
    branch = ContinuousReplayL2Journal(tmp_path/'branch', identity=source.identity)
    with pytest.raises(ValueError, match=message):
        branch.restore_prefix(prefix)
    assert not branch.writer.manifest_path.exists()
    assert not list(branch.writer.output_dir.glob('part-*'))
    assert branch.writer.row_count == 0 and branch.writer.part_count == 0
    assert branch.delivery.total == 0 and branch.counts == {}
    assert branch._last_prefix is None


def test_copy_recovery_verifies_target_read_back(tmp_path, monkeypatch):
    import execution.chunked_parquet_journal as module

    source, _, prefix = _sample_prefix(tmp_path)
    branch = ContinuousReplayL2Journal(tmp_path/'branch', identity=source.identity)
    real_hash = module._sha256_file

    def corrupt_read_back(path):
        if path.parent == branch.writer.output_dir:
            return '0'*64
        return real_hash(path)

    monkeypatch.setattr(module, '_sha256_file', corrupt_read_back)
    with pytest.raises(ValueError, match='hash mismatch'):
        branch.restore_prefix(prefix)
    assert not branch.writer.manifest_path.exists()
    assert not list(branch.writer.output_dir.glob('part-*'))


def test_empty_prefix_recovery_then_append(tmp_path):
    source = ContinuousReplayL2Journal(tmp_path/'source', identity={})
    producer = ReplayL2Production()
    prefix = source.checkpoint_prefix(producer.receipt())
    branch = ContinuousReplayL2Journal(tmp_path/'branch', identity={})
    branch.restore_prefix(prefix)
    branch.emit('fill', 1, production_event=producer.observe('fill', 1))
    receipt = branch.close(production=producer.receipt())
    assert receipt['rows'] == 1
    assert list(iter_chunked_parquet_journal(receipt['manifest']))[0]['sequence'] == 1


def test_recovered_prefix_is_not_reaudited_at_next_window(tmp_path, monkeypatch):
    source, producer, prefix = _sample_prefix(tmp_path)
    branch = ContinuousReplayL2Journal(tmp_path/'branch', identity=source.identity)
    branch.restore_prefix(prefix)
    read = pq.read_table
    seen = []

    def counted(path, **kwargs):
        seen.append(Path(path).name)
        return read(path, **kwargs)

    monkeypatch.setattr(pq, 'read_table', counted)
    branch.restore_prefix(prefix)
    branch.emit('fill', 8, production_event=producer.observe('fill', 8))
    branch.checkpoint_prefix(producer.receipt())
    assert seen == ['part-000004.parquet']


def test_failed_publication_removes_only_unaccepted_new_branch(tmp_path, monkeypatch):
    source, _, prefix = _sample_prefix(tmp_path)
    branch = ContinuousReplayL2Journal(tmp_path/'branch', identity=source.identity)
    original = branch.writer._write_manifest

    def failed_write(*, closed):
        original(closed=closed)
        raise OSError('simulated publish failure')

    monkeypatch.setattr(branch.writer, '_write_manifest', failed_write)
    with pytest.raises(OSError, match='publish failure'):
        branch.restore_prefix(prefix)
    assert not branch.writer.manifest_path.exists()
    assert not list(branch.writer.output_dir.glob('part-*'))
    assert branch.writer.row_count == 0
    assert len(list(iter_chunked_parquet_journal(prefix['manifest']))) == 7
