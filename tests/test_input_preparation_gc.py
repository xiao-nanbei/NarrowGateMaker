"""Input-only collection deferral must not discard or freeze account state."""
import gc
import threading
import weakref

import pytest

from models.replay.public_input_window import deferred_input_gc


def test_nested_deferral_restores_enabled_state_and_does_not_freeze():
    before = gc.isenabled()
    frozen = gc.get_freeze_count()
    gc.enable()
    try:
        with deferred_input_gc():
            assert not gc.isenabled()
            with deferred_input_gc():
                assert not gc.isenabled()
            assert not gc.isenabled()
        assert gc.isenabled()
        assert gc.get_freeze_count() == frozen
    finally:
        if not before:
            gc.disable()


@pytest.mark.parametrize('enabled', [True, False])
def test_exception_restores_original_setting(enabled):
    before = gc.isenabled()
    (gc.enable if enabled else gc.disable)()
    try:
        with pytest.raises(ValueError), deferred_input_gc():
            assert not gc.isenabled()
            raise ValueError('input failure')
        assert gc.isenabled() is enabled
    finally:
        (gc.enable if before else gc.disable)()


def test_explicit_collection_still_reclaims_cycles_and_keeps_live_state():
    class Node:
        pass
    carried = Node()
    carried.self = carried
    with deferred_input_gc():
        temporary = Node()
        temporary.self = temporary
        reference = weakref.ref(temporary)
        del temporary
        gc.collect()
        assert reference() is None
        assert carried.self is carried


def test_background_caller_does_not_change_process_collection():
    before = gc.isenabled()
    values = []
    def run():
        with deferred_input_gc():
            values.append(gc.isenabled())
    worker = threading.Thread(target=run)
    worker.start()
    worker.join()
    assert values == [before]
    assert gc.isenabled() is before
