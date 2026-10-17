"""Continuous output retains exact rows without replaying each old prefix."""
from pathlib import Path

import pytest

from execution.chunked_parquet_journal import iter_chunked_parquet_journal
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
