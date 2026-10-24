"""Count-only diagnostics preserve decisions and replay state, not detail output."""
import copy
import hashlib
import json
import pickle

import pytest

from models.backtest_tick import simulate_tick
from models.replay.risk_selection import ReplayRiskSelection, require_opportunity_details
from tests.test_state_event_quote_consumer import scenario
from tests.test_tick_runtime_checkpoint import assert_same


def row(i):
    return dict(opportunity_id=str(i), kind="EC"[i % 2],
                baseline_action="POST" if i % 2 == 0 else "KEEP",
                features={"value": [i, None]}, pending_orders=[{"id": str(i)}])


def business(result):
    return {k: v for k, v in result.items() if k not in {
        "_risk_selection_opportunities", "risk_selection_opportunity_retention",
        "risk_selection_opportunities_recorded"}}


def test_collector_counts_actions_restore_and_legacy():
    full = ReplayRiskSelection()
    counts = ReplayRiskSelection(retention="counts_only")
    for i in range(100):
        assert full.observe(row(i)) == counts.observe(row(i))
    assert counts.rows == []
    assert counts.counts == {"E": 50, "C": 50}
    assert business(full.finish()) == business(counts.finish())
    restored = pickle.loads(pickle.dumps(counts))
    clone = copy.deepcopy(restored)
    restored.observe(row(100))
    assert clone.counts == counts.counts
    assert restored.rows == [] and restored.retention == "counts_only"
    del full.retention  # Old pickled collector has the historical full contract.
    full.observe(row(100))
    assert len(full.rows) == 101
    assert full.finish()["risk_selection_opportunity_retention"] == "full"
    with pytest.raises(ValueError, match="not per-opportunity"):
        require_opportunity_details(counts.finish())


@pytest.mark.parametrize("kwargs", [dict(mode="E"), dict(control="random"),
    dict(intervention={}), dict(sink=lambda r: None), dict(retention="invalid")])
def test_unsafe_retention_rejected(kwargs):
    with pytest.raises(ValueError):
        ReplayRiskSelection(**{**dict(retention="counts_only"), **kwargs})


@pytest.mark.parametrize("retention", ["full", "counts_only"])
def test_limit_not_a_silent_truncation(retention):
    c = ReplayRiskSelection(retention=retention, max_rows=1)
    c.observe(row(0))
    with pytest.raises(RuntimeError, match="exceeded max_rows"):
        c.observe(row(1))


def test_full_sink_failure_propagates():
    def fail(r):
        raise OSError("write failed")
    with pytest.raises(OSError, match="write failed"):
        ReplayRiskSelection(sink=fail).observe(row(0))


@pytest.mark.parametrize("cut", [10001, 10005, 10010, 10040, 11001, 12001])
@pytest.mark.parametrize("consume", [False, True])
def test_u1_full_counts_business_and_checkpoint(cut, consume, monkeypatch):
    signatures = []
    original = ReplayRiskSelection._observe
    def observe(self, row, policy_action):
        action = original(self, row, policy_action)
        signatures.append(hashlib.sha256(json.dumps([row, action], sort_keys=True).encode()).digest())
        return action
    monkeypatch.setattr(ReplayRiskSelection, "_observe", observe)
    args, kw = scenario()
    args[3].update(risk_selection_collect_opportunities=True,
                   _requote_tail_work_samples_ms=[5.], target_observe_only=True)
    full = simulate_tick(*args, **kw)
    full_signatures = signatures[:]
    signatures.clear()
    assert sum(full["risk_selection_opportunity_counts"].values()) > 0
    args[3]["risk_selection_opportunity_retention"] = "counts_only"
    counts = simulate_tick(*args, **kw)
    assert signatures == full_signatures
    assert_same(business(counts), business(full))
    cp = simulate_tick(*args, **kw, checkpoint_at_ts_ms=cut)["_replay_checkpoint"]
    cp = pickle.loads(pickle.dumps(cp))
    assert cp["runtime"].risk_selection.rows == []
    result = simulate_tick(*args, **kw, resume_checkpoint=cp,
                           consume_resume_checkpoint=consume)
    assert_same(result, counts)
    assert result["risk_selection_policy_decision_count"] == 0
    assert result["risk_selection_intervention_count"] == 0


def test_cannot_change_old_checkpoint_retention():
    args, kw = scenario()
    args[3]["risk_selection_collect_opportunities"] = True
    cp = simulate_tick(*args, **kw, checkpoint_at_ts_ms=10005)["_replay_checkpoint"]
    args[3]["risk_selection_opportunity_retention"] = "counts_only"
    with pytest.raises(ValueError, match="retention differs"):
        simulate_tick(*args, **kw, resume_checkpoint=cp)


def test_prefill_and_disabled_collection_rejected():
    args, kw = scenario()
    args[3]["risk_selection_opportunity_retention"] = "counts_only"
    with pytest.raises(ValueError, match="requires collection"):
        simulate_tick(*args, **kw)


def test_counts_rows_do_not_hold_nested_objects():
    import weakref
    class Value:
        pass
    value = Value()
    ref = weakref.ref(value)
    c = ReplayRiskSelection(retention="counts_only")
    c.observe({**row(0), "nested": {"value": value}})
    del value
    assert ref() is None


def test_prefill_parent_rejects_counts_before_loading_inputs():
    from research.families.f05_fill_quality_quote_ev.public_input import iter_prefill_parent_segments
    with pytest.raises(ValueError, match="full opportunity details"):
        next(iter_prefill_parent_segments(None,
             {"risk_selection_opportunity_retention": "counts_only"}, cut_times_ms=[]))
