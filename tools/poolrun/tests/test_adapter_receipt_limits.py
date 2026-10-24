import json
from types import SimpleNamespace

import pytest

from poolrun.runner import adapter


def receipt_output(monkeypatch, value, *, ensure_ascii=True):
    output = json.dumps(value, ensure_ascii=ensure_ascii)
    monkeypatch.setattr(
        'poolrun.runner.subprocess.run',
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=output),
    )
    return output


def test_large_result_manifest_is_retained_in_full(tmp_path, monkeypatch):
    receipt = {
        'durable': True,
        'uri': '/durable/original-attempt',
        'files': [
            {'file': f'daily/2026-09-11/diagnostics/orders-{i:05d}.json',
             'sha256': 'a' * 64, 'size': 123456, 'contract': 'continuous-result-v1'}
            for i in range(7385)
        ],
    }
    output = receipt_output(monkeypatch, receipt)
    assert 2**20 < len(output.encode()) < 8 * 2**20
    assert adapter(['save'], {'action': 'save_result'}, tmp_path) == receipt


@pytest.mark.parametrize('action', ['validate_result', 'validate_checkpoint', None])
def test_other_adapters_keep_one_mib_limit(tmp_path, monkeypatch, action):
    receipt_output(monkeypatch, {'payload': 'x' * 2**20})
    request = {} if action is None else {'action': action}
    with pytest.raises(ValueError, match='oversized adapter receipt'):
        adapter(['validate'], request, tmp_path)


def test_result_receipt_still_has_a_finite_limit(tmp_path, monkeypatch):
    receipt_output(monkeypatch, {'payload': 'x' * (8 * 2**20)})
    with pytest.raises(ValueError, match='oversized adapter receipt'):
        adapter(['save'], {'action': 'save_result'}, tmp_path)


def test_receipt_limit_counts_utf8_bytes(tmp_path, monkeypatch):
    output = receipt_output(monkeypatch, {'payload': '中' * 400000}, ensure_ascii=False)
    assert len(output) < 2**20 < len(output.encode())
    with pytest.raises(ValueError, match='oversized adapter receipt'):
        adapter(['validate'], {}, tmp_path)
