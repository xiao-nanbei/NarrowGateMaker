"""A settlement failure must preserve, not repair, the producer's evidence."""

import json

import numpy as np
import pytest

from models import backtest_tick
from models.replay import public_accounting
from models.replay.benchmark import measure


@pytest.mark.parametrize("existing", [False, True])
def test_accounting_failure_preserves_order_and_original_exception(tmp_path, monkeypatch, existing):
    result = {"_fill_trace": [
        {"fill_sequence": 0, "fill_ts": 1010, "unknown": np.nan},
        {"fill_sequence": 1, "fill_ts": 1000, "unknown": None},
    ], "values": np.array([2, 1])}
    error = ValueError("economic fill clock regressed")
    monkeypatch.setattr(backtest_tick, "simulate_public_inputs", lambda *a, **kw: result)

    def reject(*args, **kwargs):
        raise error

    monkeypatch.setattr(public_accounting, "settle_public_replay", reject)
    output = tmp_path / "result.json"
    failed = tmp_path / "result.json.accounting-failed.json"
    if existing:
        failed.write_text("original evidence")
    with pytest.raises(ValueError) as raised:
        measure(None, {}, result_path=output)
    assert raised.value is error
    assert not output.exists()
    assert [r["fill_ts"] for r in result["_fill_trace"]] == [1010, 1000]
    if existing:
        assert failed.read_text() == "original evidence"
        assert "Failed to preserve" in error.__notes__[0]
    else:
        saved = json.loads(failed.read_text())
        assert saved["status"] == "accounting_failed"
        assert "accounting" not in saved
        assert [r["fill_ts"] for r in saved["replay"]["_fill_trace"]] == [1010, 1000]
        assert np.isnan(saved["replay"]["_fill_trace"][0]["unknown"])
        assert saved["replay"]["_fill_trace"][1]["unknown"] is None
        assert saved["replay"]["values"] == [2, 1]
