"""Shared admission is pure; publication still belongs to each storage mode."""
from copy import deepcopy

import pytest

from execution.order_lifecycle_journal_writer_v2 import (
    DEFAULT_SCHEMA_CONTRACT, _canonical_sha256, _plan_batch_admission,
)


def _plan(rows, **changes):
    state = dict(payloads=tuple(rows), checkpoint=DEFAULT_SCHEMA_CONTRACT.cursor_type(
        lifecycle_id="life", client_order_id="client", last_emitted_sequence=len(rows),
        last_event_id=rows[-1]["event_id"],
    ).checkpoint(), identity={"writer": "test"}, known_batches=set(), known_events=set(),
        cursors={}, censored=set(), contract=DEFAULT_SCHEMA_CONTRACT)
    state.update(changes)
    return _plan_batch_admission(**state)


def _rows():
    return [dict(lifecycle_id="life", client_order_id="client", event_id=f"e{i}",
                 source_callback_id="cb", lifecycle_sequence=i,
                 terminal_observation="NONE") for i in (1, 2)]


def test_shared_plan_is_pure_and_preserves_batch_identity():
    rows = _rows()
    original = deepcopy(rows)
    plan = _plan(rows)
    assert rows == original
    assert plan.next_cursor.last_emitted_sequence == 2
    assert plan.cursor_before.last_emitted_sequence == 0
    assert plan.batch_id == _canonical_sha256(dict(
        writer="test", lifecycle_id="life", event_ids=["e1", "e2"],
        source_callback_id="cb", checkpoint_after=plan.next_cursor.checkpoint()))
    assert _plan(rows, known_batches={plan.batch_id}).duplicate_reason == "batch"
    assert _plan(rows, known_events={"e1", "e2"}).duplicate_reason == "events"


@pytest.mark.parametrize("case,message", [
    ("partial", "partially overlaps"), ("gap", "does not continue"),
    ("lifecycle", "multiple lifecycle"), ("client", "multiple client"),
    ("censor", "local_shutdown_censor"), ("checkpoint", "checkpoint schema"),
    ("cursor_client", "client order id mismatch"),
])
def test_shared_plan_rejects_invalid_admission(case, message):
    rows, changes = _rows(), {}
    if case == "partial":
        changes["known_events"] = {"e1"}
    elif case == "gap":
        rows[0]["lifecycle_sequence"] = 3
    elif case in {"lifecycle", "client"}:
        rows[1]["lifecycle_id" if case == "lifecycle" else "client_order_id"] = "other"
    elif case == "censor":
        changes["censored"] = {"life"}
    elif case == "checkpoint":
        changes["checkpoint"] = {}
    else:
        changes["cursors"] = {"life": DEFAULT_SCHEMA_CONTRACT.cursor_type(
            lifecycle_id="life", client_order_id="other")}
    with pytest.raises(ValueError, match=message):
        _plan(rows, **changes)
