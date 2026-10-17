"""Native book sampling contracts independent of observed-union raw storage."""

import copy

import numpy as np
import pytest


native = pytest.importorskip("narrowgate_cpp")


def message(source, timestamp, levels, *, snapshot=False, native_ids=None, local=None):
    first, final, previous, last = native_ids or (-1, -1, -1, -1)
    return [(source, [timestamp, timestamp, local or timestamp + 10, int(snapshot),
                      int(native_ids is not None), first, final, previous, last,
                      timestamp // 1000, side, price, quantity, ordinal])
            for ordinal, (side, price, quantity) in enumerate(levels)]


def snapshot(source, timestamp, *, deep=False, native_ids=None):
    levels = [(0, 100, 1), (0, 99, 1), (1, 101, 1), (1, 102, 1)]
    if deep:
        levels.append((0, 90, 1))
    return message(source, timestamp, levels, snapshot=True, native_ids=native_ids)


def kernel(*, diff=False, allow_legacy=False, start=1_000_000, end=5_000_000):
    result = native.BookFusion(2, preferred_source=1, minimum_levels=2)
    result.configure_raw_diff_output(diff, allow_legacy_continuation=allow_legacy)
    result.configure_sampling(start, end, 100_000)
    return result


def run(result, rows, *, chunk=100_000):
    output = []
    for offset in range(0, len(rows), chunk):
        part = rows[offset:offset + chunk]
        output.append(result.push_rows(np.array([r[0] for r in part], dtype=np.int64),
                                       np.array([r[1] for r in part], dtype=np.int64)))
    output.append(result.finish())
    return {name: np.concatenate([item[name] for item in output]) for name in output[0]}


def test_no_diff_output_preserves_samples_without_synthetic_depth_toggles():
    rows = (snapshot(0, 1_000_000, deep=True) + snapshot(1, 2_000_000)
            + message(0, 3_000_000, [(0, 100, 1)])
            + message(1, 4_000_000, [(0, 100, 1)]))
    assert all(row[12] > 0 for _, row in rows)
    current, legacy = kernel(), kernel(diff=True)
    new, old = run(current, rows), run(legacy, rows)
    assert old["amount"][old["price"] == 90].tolist() == [1, 0, 1, 0]
    assert len(new["timestamp"]) == current.stats()["output_rows"] == 0
    for field in new:
        if field.startswith("normalized_"):
            np.testing.assert_array_equal(new[field], old[field])
    assert current.stats()["source_switches"] == 3
    assert current.stats()["aged_fallback_captures"] == 0
    assert current.stats()["equal_clock_conflicting_states"] is None
    assert current.continuation()["schema"] == "book_fusion.continuation.v2"


@pytest.mark.parametrize("failure", ["crossed_delta", "insufficient_snapshot", "negative", "native_gap"])
def test_invalid_selected_source_preserves_last_verified_book_and_clock(failure):
    native_ids = (-1, 100, -1, 100) if failure == "native_gap" else None
    rows = snapshot(1, 500_000) + snapshot(0, 1_000_000, native_ids=native_ids)
    if failure == "crossed_delta":
        rows += message(0, 2_000_000, [(0, 105, 1)])
    elif failure == "insufficient_snapshot":
        rows += message(0, 2_000_000, [(0, 100, 1), (1, 101, 1)], snapshot=True)
    elif failure == "negative":
        rows += message(0, 2_000_000, [(0, 100, -1)])
    else:
        rows += message(0, 2_000_000, [(0, 100, 7)], native_ids=(103, 103, 102, -1))
    result = kernel(end=3_000_000)
    output = run(result, rows)
    state = result.continuation()
    assert state["view_source"] == -1
    assert state["global_observed_us"] == 1_000_000
    assert result.stats()["aged_fallback_captures"] == 1
    assert result.stats()["older_fallback_suppressed"] == 1
    assert output["normalized_observed"][-1] == 1_000_000
    assert result.stats()["max_observation_age_us"] == 1_900_000
    levels = output["normalized_levels"].reshape(-1, 8)
    assert (levels == np.array([100, 1, 101, 1, 99, 1, 102, 1])).all()


def test_same_clock_update_then_reset_rolls_fallback_back_before_entire_batch():
    rows = snapshot(0, 1_000_000)
    rows += message(0, 2_000_000, [(0, 100, 7)], local=2_000_010)
    rows += message(0, 2_000_000, [(0, 100, -1)], local=2_000_020)
    result = kernel(end=3_000_000)
    output = run(result, rows, chunk=1)
    state = result.continuation()
    assert state["view_source"] == -1
    assert state["global_observed_us"] == 1_000_000
    assert [row[2] for row in state["global_levels"] if row[:2] == [0, 100]] == [1]
    assert output["normalized_levels"].reshape(-1, 8)[-1, 1] == 1


def test_no_diff_batch_splits_preserve_message_atomicity_and_checkpoint():
    rows = snapshot(0, 1_000_000, deep=True) + snapshot(1, 1_000_000)
    rows += message(0, 2_000_000, [(0, 100, 4), (1, 101, 3)])
    whole, single = kernel(), kernel()
    a, b = run(whole, rows), run(single, rows, chunk=1)
    for field in a:
        np.testing.assert_array_equal(a[field], b[field])
    assert whole.continuation() == single.continuation()


@pytest.mark.parametrize("fallback", [False, True])
def test_no_diff_checkpoint_restore_preserves_current_or_aged_view(fallback):
    initial = snapshot(0, 1_000_000)
    if fallback:
        initial += message(0, 1_500_000, [(0, 105, 1)])
    first = kernel(end=2_000_000)
    run(first, initial)
    checkpoint = first.continuation()
    resumed = kernel(start=2_000_000, end=4_000_000)
    resumed.restore(checkpoint)
    tail = snapshot(1, 3_000_000)
    actual = run(resumed, tail)
    complete = kernel(end=4_000_000)
    expected = run(complete, initial + tail)
    selected = expected["normalized_timestamp"] >= 2000
    for field in actual:
        if field == "normalized_levels":
            np.testing.assert_array_equal(actual[field].reshape(-1, 8),
                                          expected[field].reshape(-1, 8)[selected])
        elif field.startswith("normalized_"):
            np.testing.assert_array_equal(actual[field], expected[field][selected])
    assert actual["normalized_observed"][0] == 1_000_000
    assert resumed.continuation()["global_observed_us"] == 3_000_000


def test_legacy_checkpoint_requires_explicit_seed_opt_in_and_stays_labelled():
    previous = kernel(diff=True)
    run(previous, snapshot(0, 1_000_000))
    old = previous.continuation()
    with pytest.raises(ValueError, match="continuation configuration mismatch"):
        kernel().restore(old)
    result = kernel(allow_legacy=True)
    result.restore(old)
    assert result.stats()["legacy_continuation_seed"] is True
    current = result.continuation()
    assert current["schema"] == "book_fusion.continuation.v2"
    with pytest.raises(ValueError, match="continuation configuration mismatch"):
        kernel(diff=True).restore(current)


def test_bad_checkpoint_selected_view_is_rejected_atomically():
    first = kernel()
    run(first, snapshot(0, 1_000_000))
    damaged = copy.deepcopy(first.continuation())
    damaged["global_levels"][0][2] = 999
    target = kernel()
    before = target.continuation()
    with pytest.raises(ValueError, match="checkpoint view mismatch"):
        target.restore(damaged)
    assert target.continuation() == before
