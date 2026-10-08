"""Atomic persistence for trusted, local replay-runtime checkpoints.

Pickle preserves shared order ownership, RNG and NumPy objects. These files are
implementation checkpoints, not portable model artifacts: only load files from
your own replay process, using a compatible state schema/runtime. Never accept an uploaded
pickle through Studio or deserialize one supplied by an untrusted party.
"""

from __future__ import annotations

import os
import io
import hashlib
import json
import pickle
import tempfile
import threading
from functools import lru_cache
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

import numpy as np

from models.exchange_book_replay import HistoricalExchangeBookScheduler


@dataclass(frozen=True)
class PrefillFork:
    """Only one action and an earlier endpoint may differ from the parent."""
    opportunity_id: str
    decision_ns: int
    decision_sequence: int
    order_id: str
    kind: str
    action: str
    execution_end_ns: int

    def validate(self, checkpoint, *, account_end_ns):
        if (not self.opportunity_id or self.kind not in {'E', 'C'}
                or self.action not in ({'POST', 'WAIT'} if self.kind == 'E' else {'KEEP', 'CANCEL'})
                or type(self.decision_ns) is not int or self.decision_ns % 1_000_000
                or type(self.execution_end_ns) is not int
                or self.execution_end_ns-self.decision_ns != 30_000_000_000
                or self.execution_end_ns > account_end_ns
                or checkpoint['cut_ts_ms']*1_000_000 > self.decision_ns
                or not checkpoint.get('public_binding')):
            raise ValueError('prefill fork requires one identified action and a supported 30000ms outcome')
        collector = checkpoint['runtime'].risk_selection
        if (collector is None or collector.mode != 'B' or collector.control != 'learned'
                or collector.selection_scope != 'visible_inventory' or collector.target
                or collector.intervention_count):
            raise ValueError('prefill fork requires an unchanged visible-inventory B0 checkpoint')


def public_checkpoint_binding(input_manifest_id, params, predictions, *, prediction_owner=None):
    """Bind the actual prepared replay, excluding only its output sink.

    This is computed only for requested checkpoint operations, never per event.
    Compatibility follows state schema and effective inputs, not source bytes.
    """
    import sys
    import numpy as np
    from models.exchange_book_replay import ReceiveTimeCooldownReplayAdapter

    cooldown_bindings = {}

    def bind_cooldown(adapter):
        # Bind immutable inputs, not the cursor/EMA state that the checkpoint
        # itself owns. The evaluator and snapshot emitter may be one object.
        if id(adapter) in cooldown_bindings:
            return cooldown_bindings[id(adapter)]
        arrays = hashlib.sha256()
        for value in (adapter._depth.ts_ms, adapter._depth.bid_px,
                      adapter._depth.ask_px, adapter._depth.bid_qty,
                      adapter._depth.ask_qty, adapter._receive, adapter._ready):
            value = np.ascontiguousarray(value)
            arrays.update(str((value.shape, value.dtype.str)).encode())
            arrays.update(memoryview(value).cast('B'))
        policies = {}
        for side, policy in sorted(adapter._policies.items()):
            policies[side] = {
                'contract': adapter.checkpoint_policy_contract[side],
                'window_schema': 'receive_time_mid_ema.v1',
                'native': (_native_cooldown_binding(policy._native_cpp)
                           if policy._native_hot_path is not None else None),
            }
        binding = {'cooldown_depth_sha256': arrays.hexdigest(), 'policies': policies}
        cooldown_bindings[id(adapter)] = binding
        return binding

    def normalize(value):
        if isinstance(value, ReceiveTimeCooldownReplayAdapter):
            return bind_cooldown(value)
        if isinstance(value, dict):
            return {key: normalize(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [normalize(item) for item in value]
        if isinstance(value, np.ndarray):
            return normalize(value.tolist())
        if isinstance(value, np.generic):
            return value.item()
        if isinstance(value, Path):
            return str(value)
        return value

    payload = {key: normalize(value) for key, value in params.items()
               if key not in {'_l2_journal', '_replay_progress_callback',
                              '_config_source_sha256', '_replay_locator_projection'}}
    parameter_bytes = json.dumps(payload, sort_keys=True, allow_nan=False).encode()
    digest = hashlib.sha256(parameter_bytes)
    from models.replay.public_input import PreparedPublicPredictions
    if prediction_owner is not None:
        if not isinstance(prediction_owner, PreparedPublicPredictions):
            raise ValueError('unknown prediction owner')
        owned = prediction_owner.for_inputs(prediction_owner.prepared)
        if (predictions is None or len(predictions) != len(owned)
                or any(a is not b for a, b in zip(predictions[:-1], owned[:-1], strict=True))
                or predictions[-1].keys() != owned[-1].keys()
                or any(predictions[-1][k] is not owned[-1][k] for k in owned[-1])):
            raise ValueError('prediction owner mismatch')
    cached = (prediction_owner._bindings.get(parameter_bytes)
              if prediction_owner is not None else None)
    def bind_prediction(values):
        if isinstance(values, dict):
            for name in sorted(values):
                digest.update(name.encode())
                bind_prediction(values[name])
            return
        array = np.ascontiguousarray(values)
        if array.dtype.hasobject:
            raise ValueError('checkpoint predictions require numeric arrays')
        digest.update(str((array.shape, array.dtype.str)).encode())
        digest.update(memoryview(array).cast('B'))

    if cached is None and predictions is not None:
        for values in predictions:
            bind_prediction(values)
    effective_digest = digest.hexdigest() if cached is None else cached
    if prediction_owner is not None:
        prediction_owner._bindings[parameter_bytes] = effective_digest
    return dict(input_manifest_id=input_manifest_id, effective_inputs_sha256=effective_digest,
                python_version=list(sys.version_info[:2]),
                checkpoint_schema='tick_replay_runtime.v1')


def _native_cooldown_binding(cpp):
    # The native restore_state implementation checks its own state version.
    return {"interface": cpp.APPLICATION_INTERFACE_VERSION,
            "required_capabilities": ["export_state", "restore_state"]}


def validate_native_cooldown_checkpoint(evaluator):
    """Cold preflight for the actual replay adapter, before any replay events."""
    if evaluator is None:
        return
    from models.exchange_book_replay import ReceiveTimeCooldownReplayAdapter
    policies = (evaluator._policies.values() if isinstance(evaluator, ReceiveTimeCooldownReplayAdapter)
                else (evaluator,))
    for policy in policies:
        native = getattr(policy, "_native_hot_path", None)
        if native is not None and not all(callable(getattr(native, method, None))
                                         for method in ("export_state", "restore_state")):
            raise TypeError("native cooldown backend has no complete checkpoint capability")


def _restore_policy_object(cls, state, reentrant, native_state=None, native_binding=None):
    restored = cls.__new__(cls)
    restored.__dict__.update(state)
    restored._lock = threading.RLock() if reentrant else threading.Lock()
    if native_state is not None:
        from strategy.native_cooldown import build_hot_path
        from strategy.boolean_cooldown_buy_e3 import LiveBuyE3CooldownPolicy
        cpp, native = build_hot_path(
            restored, profile="BUY" if cls is LiveBuyE3CooldownPolicy else "SELL",
            warmup_s=restored.windows.warmup_s,
            max_feature_age_s=restored.windows.max_feature_age_s, requested=True,
        )
        if native is None or not callable(getattr(native, "restore_state", None)):
            raise RuntimeError("native cooldown checkpoint requires state-capable native backend")
        if _native_cooldown_binding(cpp) != native_binding:
            raise ValueError("native cooldown checkpoint interface mismatch")
        native.restore_state(native_state)
        restored._native_cpp, restored._native_hot_path = cpp, native
    return restored


def _restore_mapping_proxy(values):
    return MappingProxyType(values)


@lru_cache(maxsize=1)
def _policy_state_types():
    # These are the actual live policy/window classes reused by offline B0.
    # Synchronization primitives belong to the process, not the saved economics.
    from strategy.boolean_cooldown_live import (
        LiveBooleanCooldownPolicy, ReceiveTimeMidEmaWindows, RuntimeCooldownPolicyEvaluator,
    )
    from strategy.boolean_cooldown_buy_e3 import LiveBuyE3CooldownPolicy, ReceiveTimeFullMidEmaWindows
    windows = (ReceiveTimeMidEmaWindows, ReceiveTimeFullMidEmaWindows)
    classes = (*windows, LiveBooleanCooldownPolicy, LiveBuyE3CooldownPolicy,
               RuntimeCooldownPolicyEvaluator)
    return frozenset(classes), frozenset((*windows, LiveBooleanCooldownPolicy, LiveBuyE3CooldownPolicy))


def _policy_state_reducer(obj):
    classes, windows = _policy_state_types()
    if type(obj) not in classes:
        return NotImplemented
    native = getattr(obj, "_native_hot_path", None)
    reentrant = type(obj) in windows
    if ((reentrant and obj._lock._is_owned()) or not obj._lock.acquire(blocking=False)):
        raise RuntimeError("cannot checkpoint a policy while a callback owns its lock")
    try:
        state = {name: value for name, value in vars(obj).items()
                 if name not in {"_lock", "_native_cpp", "_native_hot_path"}}
        if native is not None:
            if not callable(getattr(native, "export_state", None)):
                raise TypeError("native cooldown backend has no complete state export")
            native_state = native.export_state()
            native_binding = _native_cooldown_binding(obj._native_cpp)
        else:
            native_state = None
            native_binding = None
            if "_native_hot_path" in vars(obj):
                state.update(_native_cpp=None, _native_hot_path=None)
    finally:
        obj._lock.release()
    return _restore_policy_object, (type(obj), state, reentrant, native_state, native_binding)


def _restore_book_scheduler(state):
    scheduler = HistoricalExchangeBookScheduler.__new__(HistoricalExchangeBookScheduler)
    scheduler.__dict__.update(state)
    # simulate_tick binds the unread source tail before executing another event.
    # None deliberately cannot pretend to be an exhausted, valid source.
    scheduler._iterator = None
    return scheduler


def _reduce_book_scheduler(scheduler):
    # Do not deepcopy independently: the enclosing Pickler's memo preserves
    # aliases between sequence/book and any other runtime references.
    return _restore_book_scheduler, ({
        name: value for name, value in vars(scheduler).items() if name != "_iterator"
    },)


class _RuntimePickler(pickle.Pickler):
    def reducer_override(self, obj):
        if isinstance(obj, HistoricalExchangeBookScheduler):
            return _reduce_book_scheduler(obj)
        if isinstance(obj, MappingProxyType):
            return _restore_mapping_proxy, (dict(obj),)
        return _policy_state_reducer(obj)


def _immutable_array_storage(array):
    """Only byte-owned arrays have no writable backing alias to re-enable."""
    if not isinstance(array, np.ndarray) or array.dtype.hasobject:
        return None
    base = array
    while isinstance(base, np.ndarray):
        if base.flags.writeable:
            return None
        base = base.base
    return base if isinstance(base, bytes) else None


class _InputSharingPickler(_RuntimePickler):
    """Local-only references; the disk writer never instantiates this class."""
    def __init__(self, stream, inputs):
        super().__init__(stream, protocol=pickle.HIGHEST_PROTOCOL)
        self.storage = {}
        for array in inputs:
            storage = _immutable_array_storage(array)
            if storage is None:
                raise ValueError('shared replay input requires immutable byte ownership')
            self.storage[id(storage)] = storage
        self.references = []
        self.reference_ids = {}

    def persistent_id(self, obj):
        storage = _immutable_array_storage(obj)
        if storage is None or self.storage.get(id(storage)) is not storage:
            return None
        # Return the exact original object, including any byte-owned view.
        # Repeated references and related bases/views keep their aliases; no
        # writable array or foreign storage can enter this local reference set.
        key = id(obj)
        if key not in self.reference_ids:
            self.reference_ids[key] = len(self.references)
            self.references.append(obj)
        return ('prepared-immutable-input', self.reference_ids[key])


class _InputSharingUnpickler(pickle.Unpickler):
    def __init__(self, stream, references):
        super().__init__(stream)
        self.references = references

    def persistent_load(self, identity):
        if (not isinstance(identity, tuple) or len(identity) != 2
                or identity[0] != 'prepared-immutable-input'
                or type(identity[1]) is not int
                or not 0 <= identity[1] < len(self.references)):
            raise pickle.UnpicklingError('invalid local prepared input reference')
        return self.references[identity[1]]


def clone_runtime_state(runtime, *, immutable_inputs=()):
    """Clone mutable state; optionally share proven current-owner input bytes.

    The whitelist is supplied by the admitted prepared/prediction owners. Old
    loaded arrays with other storage retain the ordinary independent clone.
    Disk persistence below continues to use the self-contained default writer.
    """
    stream = io.BytesIO()
    if immutable_inputs:
        writer = _InputSharingPickler(stream, immutable_inputs)
        writer.dump(runtime)
        stream.seek(0)
        return _InputSharingUnpickler(stream, writer.references).load()
    _RuntimePickler(stream, protocol=pickle.HIGHEST_PROTOCOL).dump(runtime)
    stream.seek(0)
    return pickle.load(stream)


def save_runtime_checkpoint(path: str | Path, checkpoint: dict) -> None:
    """Replace the previous checkpoint only after the complete new file is durable."""
    if checkpoint.get("schema") != "tick_replay_runtime.v1":
        raise ValueError("unsupported tick runtime checkpoint")
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            _RuntimePickler(stream, protocol=pickle.HIGHEST_PROTOCOL).dump(checkpoint)
            stream.flush()
            os.fsync(stream.fileno())
            metadata = {
                "schema": "tick_replay_checkpoint_metadata.v1",
                "checkpoint_bytes": stream.tell(),
                "cut_ts_ms": checkpoint.get("cut_ts_ms"),
            }
        runtime = checkpoint.get("runtime")
        if runtime is not None and hasattr(runtime, "trade_ts") and metadata["cut_ts_ms"] is not None:
            from models.replay.runtime_input_window import runtime_input_context_start
            metadata["context_ms"] = 300_000
            metadata["required_context_start_ms"] = runtime_input_context_start(
                runtime, int(metadata["cut_ts_ms"]) - 300_000, 300_000,
            )
        os.replace(temporary, path)
        # The data file is durable first. A crash between the two replacements
        # leaves a missing/mismatched sidecar, never an apparently valid one.
        _save_checkpoint_metadata(path, metadata)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _save_checkpoint_metadata(path: Path, metadata: dict) -> None:
    target = path.with_name(path.name + ".metadata.json")
    fd, temporary = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(metadata, stream, sort_keys=True, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def load_runtime_checkpoint_metadata(path: str | Path) -> dict:
    """Inspect an owned checkpoint without allocating its runtime graph.

    Legacy files lacking a sidecar raise FileNotFoundError. Callers explicitly
    choose whether to migrate a trusted legacy checkpoint; a mismatched sidecar
    must never be treated as permission to use a stale cutoff.
    """
    path = Path(path)
    metadata = json.loads(path.with_name(path.name + ".metadata.json").read_text())
    if metadata.get("schema") != "tick_replay_checkpoint_metadata.v1":
        raise ValueError("unsupported checkpoint metadata")
    with path.open("rb") as stream:
        before = os.fstat(stream.fileno())
        if before.st_size != metadata["checkpoint_bytes"]:
            raise ValueError("checkpoint metadata size mismatch")
    return metadata


def load_trusted_runtime_checkpoint(path: str | Path) -> dict:
    """Load only a trusted local checkpoint; pickle is not a safe exchange format."""
    with Path(path).open("rb") as stream:
        checkpoint = pickle.load(stream)
    if not isinstance(checkpoint, dict) or checkpoint.get("schema") != "tick_replay_runtime.v1":
        raise ValueError("unsupported tick runtime checkpoint")
    metadata = load_runtime_checkpoint_metadata(path)
    if checkpoint.get("cut_ts_ms") != metadata.get("cut_ts_ms"):
        raise ValueError("checkpoint metadata cutoff mismatch")
    return checkpoint
