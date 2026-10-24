"""Decision-link scratch stays bounded without losing any emitted evidence."""

import pytest

from execution.chunked_parquet_journal import iter_chunked_parquet_journal
from models.backtest_tick import simulate_tick
from models.replay.l2_journal import ReplayL2Journal
from models.replay.runtime_checkpoint_io import (
    load_trusted_runtime_checkpoint,
    save_runtime_checkpoint,
)
from tests.test_tick_runtime_checkpoint import assert_same, scenario


class HistoricalDecisionLinks(dict):
    """Test reference for the former never-cleared dictionary."""

    def clear(self):
        pass


def inputs(mode):
    args, kwargs = scenario("ordinary" if mode == "serial" else mode)
    if mode == "serial":
        args[3].update(
            replay_purpose="diagnostic",
            rest_gateway_timing_mode="sampled_serial",
            replay_main_loop_sleep_ms=100,
            _decision_to_gateway_latency_samples_ms=[30.0],
            _serial_rest_return_samples_by_operation={
                "new": [[40.0, 100.0, 150.0]],
                "cancel": [[20.0, 60.0, 200.0]],
            },
            _serial_rest_return_sample_semantics="synthetic_test_only",
        )
    return args, kwargs


def replay(args, kwargs, root, **options):
    journal = ReplayL2Journal(root, identity={"case": "decision-link-retention"})
    return simulate_tick(
        *args[:3], {**args[3], "_l2_journal": journal}, **kwargs, **options
    )


def evidence(result):
    receipt = result.pop("_l2_journal")
    return (
        result,
        list(iter_chunked_parquet_journal(receipt["manifest"])),
        receipt["production"],
    )


@pytest.mark.parametrize(
    "mode", ["ordinary", "async", "compute", "timeout", "emergency", "close_replace", "serial"]
)
def test_bounded_links_preserve_all_results_and_journal_rows(tmp_path, mode):
    args, kwargs = inputs(mode)
    checkpoint = replay(args, kwargs, tmp_path / "prefix", checkpoint_at_ts_ms=0)[
        "_replay_checkpoint"
    ]
    saved = tmp_path / "initial.checkpoint"
    save_runtime_checkpoint(saved, checkpoint)
    outcomes = []
    maps = []
    for label, mapping in (("historical", HistoricalDecisionLinks), ("bounded", dict)):
        restored = load_trusted_runtime_checkpoint(saved)
        # A legacy checkpoint can contain a long, no-longer-consumed history.
        links = mapping({(-1, index, "BUY"): -1 for index in range(10_000)})
        restored["runtime"].l2_decision_links = links
        maps.append(links)
        outcomes.append(
            evidence(
                replay(
                    args, kwargs, tmp_path / label,
                    resume_checkpoint=restored, consume_resume_checkpoint=True,
                )
            )
        )
    assert_same(outcomes[1], outcomes[0])
    assert len(maps[0]) >= 10_000
    assert len(maps[1]) <= 2
    if mode in {"ordinary", "async", "compute", "serial"}:
        assert len(maps[0]) > 10_002
        assert len(maps[1]) == 2


@pytest.mark.parametrize("cut", [1, 31, 100, 201, 1100])
@pytest.mark.parametrize("consume", [False, True])
def test_pending_serial_plan_keeps_exact_creation_links_after_disk_restore(
    tmp_path, cut, consume
):
    args, kwargs = inputs("serial")
    expected = evidence(replay(args, kwargs, tmp_path / "whole"))
    checkpoint = replay(args, kwargs, tmp_path / "prefix", checkpoint_at_ts_ms=cut)[
        "_replay_checkpoint"
    ]
    plan = checkpoint["runtime"].serial_rest_decision
    assert plan is not None and all(plan["creation_opportunities"].values())
    expected_creation_links = dict(plan["creation_opportunities"])
    checkpoint["runtime"].l2_decision_links.update(
        {(-1, index, "BUY"): -1 for index in range(10_000)}
    )
    saved = tmp_path / "pending.checkpoint"
    save_runtime_checkpoint(saved, checkpoint)
    restored = load_trusted_runtime_checkpoint(saved)
    assert restored["runtime"].serial_rest_decision["creation_opportunities"] == expected_creation_links
    actual = evidence(
        replay(
            args, kwargs, tmp_path / "resumed",
            resume_checkpoint=restored, consume_resume_checkpoint=consume,
        )
    )
    assert_same(actual, expected)
    decisions = {
        row["production_event"]["sequence"]
        for row in actual[1]
        if row["event_type"] == "decision"
    }
    created = [row["record"] for row in actual[1] if row["event_type"] == "order_created"]
    assert created
    assert all(row["creation_opportunity_event_id"] in decisions for row in created)
