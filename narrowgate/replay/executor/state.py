"""Owned replay state and non-persistent execution context.

The v1 checkpoint graph remains a SimpleNamespace-compatible value graph.
Process dependencies and executor methods never belong to that persisted graph.
"""

from types import SimpleNamespace


class ReplayRuntimeState(SimpleNamespace):
    """Account/order/RNG/cursors and their checkpoint ownership transitions.

    Fields remain a namespace because original v1 checkpoints and input-window
    rotation operate on ``vars(runtime)``. A restored original SimpleNamespace
    is deliberately not rewrapped: changing its root could break graph aliases
    and the transfer-once contract. These owner methods accept both roots.
    """

    @staticmethod
    def capture_checkpoint(runtime, *, cut_ts_ms: int) -> dict:
        """Capture the selected, not-yet-executed event and complete writer prefixes.

        Sampling storage is compacted by its existing input-window owner. The
        economic graph stays shallow until the existing checkpoint pickler
        clones/persists it, preserving order/FIFO/policy aliases in one graph.
        Process callbacks and writers never enter the persisted state.
        """
        from models.replay.runtime_input_window import compact_runtime_checkpoint

        captured = compact_runtime_checkpoint(runtime)
        captured.replay_progress_callback = None
        prefix = None
        if runtime.l2_journal is not None:
            prefix = runtime.l2_journal.checkpoint_prefix(runtime.l2_production.receipt())
        captured.l2_journal = None
        target_prefix = None
        if runtime.target_journal is not None:
            target_prefix = runtime.target_journal.checkpoint_prefix(runtime.target_production.receipt())
        captured.target_journal = None
        return {
            "schema": "tick_replay_runtime.v1",
            "scope": "same_loaded_window_runtime_resume",
            "cut_ts_ms": int(cut_ts_ms),
            "loaded_window": (int(runtime.trade_ts[0]), int(runtime.trade_ts[-1]), int(runtime.n_trades)),
            "runtime": captured,
            "next_event": runtime.replay_event,
            "l2_prefix": prefix,
            "target_prefix": target_prefix,
        }

    @staticmethod
    def restore_checkpoint_runtime(checkpoint, *, consume: bool, immutable_inputs=()):
        """Transfer one admitted graph, or independently clone a branch.

        Schema/input/policy/writer admission remains ahead of this boundary in
        ReplayInitialization. After a consuming transfer the old checkpoint is
        explicitly consumed even if writer restoration or later replay fails.
        Original namespace graphs retain their root and internal identities.
        """
        from models.replay.runtime_checkpoint_io import clone_runtime_state
        from models.replay.runtime_input_window import (
            compact_runtime_checkpoint,
            release_runtime_input_scratch,
        )

        if consume:
            runtime = checkpoint.pop("runtime")
            checkpoint["consumed"] = True
            release_runtime_input_scratch(runtime)
            return runtime
        return clone_runtime_state(
            compact_runtime_checkpoint(checkpoint["runtime"]),
            immutable_inputs=immutable_inputs,
        )


class ReplayContext(SimpleNamespace):
    """Call-scoped inputs and assembly/finalization locals, never checkpointed."""
