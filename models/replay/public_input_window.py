"""Bounded market windows over one unchanged global ConsumerBundle.

Prediction state is advanced exactly once over the global feature stream.
Windows copy only their numerical slices into the ordinary immutable owners;
they never reset the economic account or the response-observation cursor.
"""
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, replace
import gc
import hashlib
from itertools import islice
import json
from pathlib import Path
import threading

import numpy as np

from data.runtime import ConsumerBundle
from .public_input import (PreparedReplayInputs, PreparedPublicPredictions,
                           _immutable_array, load_public_replay_inputs, public_predictions)


@contextmanager
def deferred_input_gc():
    """Do not repeatedly traverse the carried account during bounded input work.

    Reference counting stays enabled. Only automatic cyclic collection is
    deferred; explicit collections (including the engine's end-of-window
    cleanup) still run. Restore the caller's setting even on failure. This is
    for the single main-thread replay owner, not concurrent preprocessing.
    No object is frozen, removed, or excluded from subsequent collection.
    """
    enabled = gc.isenabled() and threading.current_thread() is threading.main_thread()
    if enabled:
        gc.disable()
    try:
        yield
    finally:
        if enabled:
            gc.enable()


def _digest(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as source:
        for block in iter(lambda: source.read(8*1024*1024), b''):
            digest.update(block)
    return digest.hexdigest()


def build_global_timelines(bundle, destination, *, signal_engine, cross_columns,
                           model_identity, max_bar_rows=12_000_000, batch_size=4096):
    """Finite preparation; an unfinished directory has no admitted manifest.

    The two-column variance prepass is explicitly capped (~192 MB numeric for
    12M rows), independent of the vastly larger trade/depth payload. Prediction
    expansion is bounded by batch_size; all output arrays are disk-backed.
    """
    from models.backtest_tick import build_rolling_variance
    bundle = bundle if isinstance(bundle, ConsumerBundle) else ConsumerBundle(bundle)
    root = Path(destination)
    if not model_identity or type(batch_size) is not int or batch_size <= 0:
        raise ValueError('frozen model identity and bounded batch size required')
    root.mkdir(parents=True, exist_ok=False)
    count = bundle.manifest['files']['features']['rows']
    bar_count = bundle.manifest['files']['bars']['rows']
    if bar_count > max_bar_rows:
        raise ValueError('global variance preparation exceeds declared row budget')
    bars = bundle._verified_parquet('bars').read(columns=['close', 'ready_ns']).to_pandas()
    bars['close'] = bars['close'].astype(float)
    bars.index = (bars.pop('ready_ns').to_numpy()+999_999)//1_000_000-1000
    bars.attrs['availability_clock'] = 'ready_base_ms_plus_1000'
    clocks, variance = build_rolling_variance(bars)
    np.save(root/'variance-clock.npy', clocks, allow_pickle=False)
    np.save(root/'variance.npy', variance, allow_pickle=False)
    del bars, clocks, variance
    frames = iter(bundle.frames())
    arrays = None
    offset = 0
    files = []
    while part := list(islice(frames, batch_size)):
        values = public_predictions(part, signal_engine, cross_columns)
        names = [f'prediction-{i}.npy' for i in range(len(values)-1)]
        names += [f'feature-{name}.npy' for name in values[-1]]
        flat = [*values[:-1], *values[-1].values()]
        if arrays is None:
            arrays = [np.lib.format.open_memmap(root/name, mode='w+', dtype=value.dtype,
                                               shape=(count,)) for name, value in zip(names, flat, strict=True)]
            files = names
            feature_names = list(values[-1])
            head_count = len(values)-1
        if names != files or offset+len(part) > count:
            raise ValueError('prediction schema/count changed')
        for array, value in zip(arrays, flat, strict=True):
            array[offset:offset+len(part)] = value
        offset += len(part)
    if offset != count or arrays is None:
        raise ValueError('global feature coverage incomplete')
    for array in arrays:
        array.flush()
    manifest = dict(schema='public_global_timelines.v1', input_manifest_id=bundle.input_manifest_id,
        model_identity=model_identity, rows=count, feature_names=feature_names,
        head_count=head_count, prediction_files=files,
        files={name: _digest(root/name) for name in [*files, 'variance-clock.npy', 'variance.npy']})
    (root/'manifest.json').write_text(json.dumps(manifest, sort_keys=True))
    return GlobalPublicTimelines(bundle, root)


@dataclass(frozen=True)
class PublicWindowBinding:
    global_identity: str
    begin_ns: int
    end_ns: int
    predictions: object

    def admit(self, prepared, predictions):
        if prepared.window_binding is not self or predictions is not self.predictions:
            raise ValueError('window prediction ownership mismatch')
        if prepared.inputs['loaded_window_ns'] != (self.begin_ns, self.end_ns):
            raise ValueError('prepared window bounds changed')


class RollingTradeWindow:
    """One original scheduled stream, retaining only the requested overlap.

    Keep the scheduler's clock-regression high-water mark, source ordinals and
    cross-file ID checks intact. A rewind (including a fresh checkpoint owner)
    replays the prefix once; forward windows never repeatedly decode it.
    This is input caching, not economic or observation state.
    """

    def __init__(self, bundle):
        self.bundle = bundle
        self._stream = None
        self._rows = deque()
        self._pending = None
        self._begin = None
        self._end = None

    def events(self, begin, end):
        plan = self.bundle.manifest['plan']
        if not plan['start_ns'] <= begin < end <= plan['end_ns']:
            raise ValueError('trade window outside global input')
        if self._stream is None or begin < self._begin or end < self._end:
            if self._stream is not None:
                self._stream.close()
            self._stream = iter(self.bundle.stream()._channel('trades'))
            self._rows.clear()
            self._pending = None
        # Recheck immutable source manifests even while using buffered overlap.
        self.bundle.source_paths()
        while self._rows and self._rows[0][0] < begin:
            self._rows.popleft()
        while True:
            if self._pending is None:
                self._pending = next(self._stream, None)
            if self._pending is None or self._pending[0] >= end:
                break
            if self._pending[0] >= begin:
                self._rows.append(self._pending)
            self._pending = None
        self._begin, self._end = begin, end
        return iter(self._rows)


class GlobalPublicTimelines:
    def __init__(self, bundle, root):
        self.bundle = bundle if isinstance(bundle, ConsumerBundle) else ConsumerBundle(bundle)
        self.root = Path(root)
        payload = (self.root/'manifest.json').read_bytes()
        self.manifest = json.loads(payload)
        if (self.manifest['schema'] != 'public_global_timelines.v1'
                or self.manifest['input_manifest_id'] != self.bundle.input_manifest_id):
            raise ValueError('global timeline input binding differs')
        for name, expected in self.manifest['files'].items():
            if Path(name).name != name or _digest(self.root/name) != expected:
                raise ValueError('global timeline payload changed')
        self.identity = hashlib.sha256(payload).hexdigest()
        self.predictions = [np.load(self.root/n, mmap_mode='r', allow_pickle=False)
                            for n in self.manifest['prediction_files']]
        self.variance_clock = np.load(self.root/'variance-clock.npy', mmap_mode='r', allow_pickle=False)
        self.variance = np.load(self.root/'variance.npy', mmap_mode='r', allow_pickle=False)
        self.trade_window = RollingTradeWindow(self.bundle)

    def predictions_for(self, prepared):
        """Reuse one frozen exogenous prediction pass across independent arms."""
        if (prepared.manifest_id != self.bundle.input_manifest_id
                or prepared.inputs.get('loaded_window_ns') is not None
                or prepared.window_binding is not None):
            raise ValueError('global prediction cache requires its complete admitted input owner')
        values = [_immutable_array(a) for a in self.predictions]
        count = self.manifest['head_count']
        return PreparedPublicPredictions(prepared,
            (*values[:count],dict(zip(self.manifest['feature_names'],values[count:],strict=True))))

    @deferred_input_gc()
    def prepare(self, begin_ns, end_ns, *, tick_size, feature_begin_ns=None):
        # Incremental feature consumers can be dormant for days. Keep their
        # exact intervening timeline without dragging the market tape back to
        # the same date. No feature cursor is advanced or observation skipped.
        feature_begin_ns = begin_ns if feature_begin_ns is None else feature_begin_ns
        if (type(feature_begin_ns) is not int
                or not self.bundle.manifest['plan']['start_ns'] <= feature_begin_ns <= begin_ns):
            raise ValueError('feature context must cover the market window')
        inputs = load_public_replay_inputs(self.bundle, tick_size=tick_size,
                                          window_ns=(begin_ns, end_ns),
                                          trade_window=self.trade_window)
        left, right = np.searchsorted(self.variance_clock, [(feature_begin_ns+999_999)//10**6-1000,
                                                           (end_ns+999_999)//10**6-1000])
        prepared = PreparedReplayInputs.create(inputs, tick_size,
            _immutable_array(self.variance_clock[left:right]), _immutable_array(self.variance[left:right]))
        left, right = np.searchsorted(self.predictions[0], [(feature_begin_ns+999_999)//10**6,
                                                          (end_ns+999_999)//10**6])
        values = [_immutable_array(a[left:right]) for a in self.predictions]
        count = self.manifest['head_count']
        predictions = PreparedPublicPredictions(prepared,
            (*values[:count], dict(zip(self.manifest['feature_names'], values[count:], strict=True))))
        binding = PublicWindowBinding(self.identity, begin_ns, end_ns, predictions)
        # Replace the prepared immutable shell before handing either owner out.
        prepared = replace(prepared, window_binding=binding)
        object.__setattr__(predictions, 'prepared', prepared)
        return prepared, predictions
