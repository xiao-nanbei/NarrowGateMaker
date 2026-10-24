import json
import sqlite3

import pyarrow.parquet as pq
import pytest

from models.replay.l2_journal import ReplayL2Journal, ReplayL2Production
from research.families.f08_side_taker_lifecycle import depth_panel as panel
from test_depth_observation import stream


def journal(root, count=100):
    sink, producer = ReplayL2Journal(root, identity={"test": True}), ReplayL2Production()
    for index in range(count):
        row = dict(read_ns=100_000_000, side="BUY", old_price=None,
                   target_price=990, sequence=index+1, book_version=2,
                   features={"original_dynamic": 7}, role="opener")
        sink.emit("optional_update_observation", 100, side="BUY", payload=row,
                  production_event=producer.observe("optional_update_observation", 100, "BUY"))
    sink.emit("order_outcome", 300, side="BUY", payload={"order_id": "1", "future_fill": True},
              production_event=producer.observe("order_outcome", 300, "BUY"))
    return sink.close(production=producer.receipt())


def test_all_opportunities_and_later_outcomes_are_separate(tmp_path, monkeypatch):
    receipt = journal(tmp_path / "journal")
    monkeypatch.setattr(panel, "streams", lambda config, tap, verified: stream(tap))
    config = dict(start_ns=0, end_ns=400_000_000, windows_ns=[100_000_000],
                  sample_period_ns=100_000_000, tick_size="1", neighbor_ticks=1,
                  identity=dict(start_ns=0, end_ns=400_000_000))
    output = tmp_path / "panel"
    result = panel.run(config, output, journal=receipt["manifest"], production=receipt["production"], compare=True)
    assert result["order_journal"]["opportunity_rows"] == 100
    assert result["counters"]["opportunities_seen"] == 100
    assert result["counters"]["strategy_region_rows"] == 200
    assert result["observation_parity"] == "PASS"
    rows = pq.read_table(output / "depth-panel.parquet").to_pylist()
    strategy = [r for r in rows if r["entrance"] == "strategy"]
    assert all("future_fill" not in r["original"] for r in strategy)
    assert all(json.loads(r["original"])["features"]["original_dynamic"] == 7 for r in strategy)
    assert all(json.loads(r["depth"])["quantity"] is not None for r in strategy)
    with sqlite3.connect(output / "order-events.sqlite") as db:
        assert db.execute("SELECT count(*) FROM events").fetchone()[0] == 101
        assert db.execute("SELECT count(*) FROM opportunities").fetchone()[0] == 100
    accepted = panel.validate_output(output, config['identity'])
    assert accepted['valid'] and not accepted['summary']['economic_complete']
    with sqlite3.connect(output / "order-events.sqlite") as db:
        db.execute('DELETE FROM opportunities WHERE sequence=1')
    with pytest.raises(ValueError, match='dropped'):
        panel.validate_output(output, config['identity'])


def test_bad_producer_receipt_never_admits_sample(tmp_path):
    receipt = journal(tmp_path / "journal")
    with pytest.raises(ValueError, match="coverage differ"):
        panel.ingest_journal(receipt["manifest"], tmp_path / "bad.sqlite",
                             dict(receipt["production"], total=1))
