"""The formal replay engine's explicit common-data consumer adapter.

Only data loading is performed here. Economic execution still belongs to
models.backtest_tick; an adapter is not economic or live admission.
"""

from __future__ import annotations

from decimal import Decimal
from dataclasses import dataclass, fields, field, replace
import hashlib
import pickle
from types import MappingProxyType

import numpy as np
import pandas as pd

from data.runtime import ConsumerBundle, ObservationProfile, PublicInputStream
from models.tick_data_types import (HistoricalBBOData, HistoricalExchangeBookEvent, HistoricalL2Data,
                                    book_observation_times_us, book_usable_mask)


def _immutable_array(array):
    array = np.asarray(array)
    if array.dtype.hasobject:
        raise ValueError('immutable replay arrays require numeric storage')
    return np.frombuffer(array.tobytes(), dtype=array.dtype).reshape(array.shape)


@dataclass(frozen=True)
class _ExecutionFrame:
    """Fresh frame shell over immutable numeric columns; never exposed externally.

    Object-valued frames conservatively keep the ordinary copying route.
    Index and attrs are independently restored because callers may mutate them.
    """
    columns: tuple
    metadata: bytes

    @classmethod
    def create(cls, frame):
        if (not frame.columns.is_unique or any(not isinstance(dtype, np.dtype) or dtype.hasobject
                                               for dtype in frame.dtypes)):
            return None
        return cls(tuple((name, _immutable_array(frame[name].values)) for name in frame.columns),
                   pickle.dumps((frame.index, frame.columns, frame.attrs), protocol=pickle.HIGHEST_PROTOCOL))

    def frame(self):
        index, columns, attrs = pickle.loads(self.metadata)
        result = pd.DataFrame(dict(self.columns), index=index, copy=False)
        result.columns = columns
        result.attrs = attrs
        return result


@dataclass(frozen=True)
class _AccountTrades:
    # Serialized private ownership, not a frozen wrapper around a mutable frame.
    payload: bytes
    identity: bytes
    execution: _ExecutionFrame | None = field(default=None, compare=False, repr=False)

    def copy(self):
        return pickle.loads(self.payload)

    def execution_frame(self):
        return self.copy() if self.execution is None else self.execution.frame()


@dataclass(frozen=True)
class PreparedReplayInputs:
    """Admitted market inputs only: no account, model, RNG or consumed cursor.

    Admission snapshots trades into private immutable bytes. Account selection
    is performed once; execution gets an independent frame shell over immutable
    numeric storage, with conservative deserialization for object columns.
    BBO/L2 arrays own immutable byte storage.
    Exchange tape iteration creates a fresh source cursor.
    """
    inputs: MappingProxyType
    tick_size: float
    variance_ts: np.ndarray
    variance: np.ndarray
    manifest_id: str
    _clocks: dict = field(default_factory=dict, compare=False, repr=False)
    _accounts: dict = field(default_factory=dict, compare=False, repr=False)
    _book_inputs: dict = field(default_factory=dict, compare=False, repr=False)
    _execution_inputs: dict = field(default_factory=dict, compare=False, repr=False)
    _source_trades: bytes = field(default=b'', compare=False, repr=False)
    _book_coverage: dict = field(default_factory=dict, compare=False, repr=False)
    _clock_arrays: dict = field(default_factory=dict, compare=False, repr=False)
    _clock_frames: dict = field(default_factory=dict, compare=False, repr=False)
    _response_cache: list = field(default_factory=list, compare=False, repr=False)

    def response_row_cache(self):
        from data.response_cursor import ResponseRowGroupCache
        if not self._response_cache:
            self._response_cache.append(ResponseRowGroupCache())
        return self._response_cache[0]

    def clock_arrays(self, clock_key):
        """Exact numeric event columns, owned by immutable bytes, not a frame view."""
        if clock_key not in self._clocks:
            raise ValueError('event arrays require an admitted clock')
        if clock_key not in self._clock_arrays:
            owner = self._clock_frames.get(clock_key)
            if owner is not None:
                columns = dict(owner.columns)
                self._clock_arrays[clock_key] = tuple(columns[name]
                    for name in ('transact_time', 'price', 'quantity'))
            else:
                frame, _ = self._clocks[clock_key]
                self._clock_arrays[clock_key] = tuple(_immutable_array(frame[name].values)
                    for name in ('transact_time', 'price', 'quantity'))
        return self._clock_arrays[clock_key]

    def historical_book_coverage(self, clock_key, bbo, l2, freshness_ms, calculate):
        """Reuse scalar coverage only for this owner's admitted static clock.

        Called by the controlled account path before checkpoint consumption.
        External books or unbound clocks fall back to the ordinary validator.
        Threshold decisions deliberately remain outside this cache.
        """
        if clock_key not in self._clocks:
            return None
        if any(book is not None and book is not self.inputs[name]
               for name, book in (('bbo', bbo), ('l2', l2))):
            return None
        key = (clock_key, bbo is not None, l2 is not None, freshness_ms)
        if key not in self._book_coverage:
            frame, _ = self._clocks[clock_key]
            timeline = frame['transact_time'].to_numpy(dtype=np.int64, copy=False)
            self._book_coverage[key] = calculate(timeline, bbo, l2, freshness_ms)
        return self._book_coverage[key]

    def account_trades(self, start_ms, end_ms, *, quantity_rule):
        if quantity_rule != 'all_public_volume_eligible':
            raise ValueError('unsupported account quantity eligibility')
        key = (start_ms, end_ms, quantity_rule)
        if key not in self._accounts:
            source = pickle.loads(self._source_trades)
            mask = source.transact_time >= start_ms
            if end_ms is not None:
                mask &= source.transact_time <= end_ms
            frame = source[mask].copy()
            frame['normal_quantity'] = frame['quantity']
            payload = pickle.dumps(frame, protocol=pickle.HIGHEST_PROTOCOL)
            self._accounts[key] = _AccountTrades(payload, hashlib.sha256(payload).digest(),
                                                 _ExecutionFrame.create(frame))
        return self._accounts[key]

    def book_inputs(self, name, book, *, allow_source_regression):
        if book is not self.inputs[name]:
            raise ValueError('book input belongs to another prepared owner')
        key = (name, allow_source_regression)
        if key not in self._book_inputs:
            observation = _immutable_array(book_observation_times_us(
                book, allow_source_regression=allow_source_regression))
            usable = _immutable_array(book_usable_mask(book)) if name == 'bbo' else None
            deltas = np.diff(np.asarray(book.ts_ms, dtype=np.int64))
            positive = deltas[deltas > 0]
            resolution = float(np.median(positive)) if positive.size else None
            self._book_inputs[key] = observation, usable, resolution
        return self._book_inputs[key]

    def execution_inputs(self, clock_key, *, cumulative=False):
        if clock_key not in self._clocks:
            raise ValueError('execution arrays require an admitted clock')
        key = (clock_key, cumulative)
        if key not in self._execution_inputs:
            frame, _ = self._clocks[clock_key]
            maker = frame['is_buyer_maker'].to_numpy(dtype=np.uint8, copy=False)
            seller = maker == 1
            eligible = (frame['_is_execution_trade'].to_numpy(dtype=np.bool_, copy=False)
                        if '_is_execution_trade' in frame else np.ones(len(frame), dtype=np.bool_))
            values = (maker, seller, eligible)
            if cumulative:
                qty = np.where(eligible, np.asarray(frame.quantity, dtype=np.float64), 0.)
                values = (qty, np.concatenate(([0.], np.cumsum(np.where(seller, 0., qty)))),
                          np.concatenate(([0.], np.cumsum(np.where(seller, qty, 0.)))),
                          np.maximum.accumulate(np.where(eligible, np.arange(len(frame)), -1)))
            self._execution_inputs[key] = tuple(_immutable_array(a) for a in values)
        return self._execution_inputs[key]

    def immutable_clone_inputs(self, predictions=None):
        """Explicit admitted owners only, never runtime discovery or rehashing."""
        arrays = []
        for name in ('bbo', 'l2'):
            book = self.inputs[name]
            arrays.extend(getattr(book, f.name) for f in fields(book)
                          if isinstance(getattr(book, f.name), np.ndarray))
        for observation, usable, _ in self._book_inputs.values():
            arrays.append(observation)
            if usable is not None:
                arrays.append(usable)
        for values in self._execution_inputs.values():
            arrays.extend(values)
        for values in self._clock_arrays.values():
            arrays.extend(values)
        if predictions is not None:
            if not isinstance(predictions, PreparedPublicPredictions) or predictions.prepared is not self:
                raise ValueError('clone predictions belong to another prepared owner')
            arrays.extend(predictions.values[:-1])
            arrays.extend(predictions.values[-1].values())
        return tuple(arrays)

    def event_clock(self, trades, builder, *, _account=None, _identity_out=None, **kwargs):
        """External frames are fully hashed; internal accounts use their snapshot.

        The private account route always builds from owned bytes, not from the
        caller's mutable frame. Internal numeric clocks use independent frame
        shells over immutable columns; external frames retain deep copies.
        """
        digest = hashlib.sha256()
        if _account is not None:
            if not any(_account is value for value in self._accounts.values()):
                raise ValueError('account input belongs to another prepared owner')
            digest.update(_account.identity)
        else:
            digest.update(pd.util.hash_pandas_object(trades, index=True).values)
            digest.update(repr(tuple(zip(trades.columns, map(str, trades.dtypes), strict=True))).encode())
            digest.update(repr(trades.attrs).encode())
        for key, value in sorted(kwargs.items()):
            digest.update(key.encode())
            if key in {'bbo_data', 'l2_data'}:
                if value is not self.inputs[key.removesuffix('_data')]:
                    return builder(trades, **kwargs)
                digest.update(str(id(value)).encode())
            elif isinstance(value, np.ndarray):
                array = np.ascontiguousarray(value)
                digest.update(str((array.shape, array.dtype.str)).encode())
                digest.update(memoryview(array).cast('B'))
            else:
                digest.update(repr(value).encode())
        key = digest.digest()
        if key not in self._clocks:
            self._clocks[key] = builder(_account.copy() if _account is not None else trades, **kwargs)
        if _identity_out is not None:
            _identity_out.append(key)
        frame, count = self._clocks[key]
        if _account is not None:
            if key not in self._clock_frames:
                self._clock_frames[key] = _ExecutionFrame.create(frame)
                if self._clock_frames[key] is not None:
                    # Coverage/execution helpers need a frame view, not another
                    # full mutable copy of the already frozen event columns.
                    self._clocks[key] = self._clock_frames[key].frame(), count
            owned = self._clock_frames[key]
            if owned is not None:
                return owned.frame(), count
        return frame.copy(deep=True), count

    @classmethod
    def create(cls, inputs, tick_size, variance_ts, variance):
        inputs = dict(inputs)
        for name in ("bbo", "l2"):
            inputs[name] = replace(inputs[name], **{
                descriptor.name: _immutable_array(getattr(inputs[name], descriptor.name))
                for descriptor in fields(inputs[name])
                if isinstance(getattr(inputs[name], descriptor.name), np.ndarray)})
        for array in (variance_ts, variance):
            array.flags.writeable = False
        return cls(MappingProxyType(inputs), tick_size, variance_ts, variance,
                   inputs["bundle"].input_manifest_id,
                   _source_trades=pickle.dumps(inputs['trades'], protocol=pickle.HIGHEST_PROTOCOL))


@dataclass(frozen=True)
class PreparedPublicPredictions:
    """One parent's market-only predictions; never an advanced account state.

    This in-memory owner is tied to the exact prepared input object. Ordinary
    checkpoint binding reuses the digest of this immutable prediction owner.
    """
    prepared: PreparedReplayInputs
    values: tuple
    _bindings: dict = field(default_factory=dict, compare=False, repr=False)

    def __post_init__(self):
        object.__setattr__(self, 'values', (*self.values[:-1], MappingProxyType(dict(self.values[-1]))))
        arrays = (*self.values[:-1], *self.values[-1].values())
        for array in arrays:
            base = array
            while isinstance(base, np.ndarray):
                if base.flags.writeable:
                    raise ValueError('prepared predictions require immutable byte ownership')
                base = base.base
            if not isinstance(base, bytes):
                raise ValueError('prepared predictions require immutable byte ownership')

    @classmethod
    def create(cls, prepared, signal_engine, cross_columns):
        values = public_predictions(prepared.inputs['frames'], signal_engine, cross_columns)
        # Own immutable bytes, not views the predictor can subsequently mutate.
        def freeze(array):
            array = np.asarray(array)
            return np.frombuffer(array.tobytes(), dtype=array.dtype).reshape(array.shape)
        return cls(prepared, (*[freeze(a) for a in values[:-1]],
                             MappingProxyType({name: freeze(a) for name, a in values[-1].items()})))

    def for_inputs(self, prepared):
        if prepared is not self.prepared:
            raise ValueError('prepared predictions belong to another input owner')
        # A fresh mapping prevents consumers changing this owner's feature map.
        return (*self.values[:-1], dict(self.values[-1]))


class PublicExchangeBookTape:
    def __init__(self, bundle, *, tick_size):
        self.bundle = bundle
        self.tick_size = Decimal(str(tick_size))
        self.day_start_ns = bundle.manifest["plan"]["start_ns"]
        if self.tick_size <= 0:
            raise ValueError("explicit positive price tick required")

    def __iter__(self):
        meta = self.bundle.manifest
        plan = meta["plan"]
        profile = ObservationProfile(**plan["observation_profile"])
        source = PublicInputStream(self.bundle.source_paths(), profile=profile,
            start_ns=plan["start_ns"], end_ns=plan["end_ns"], market_id=plan["market_id"],
            input_contract_id=meta["input_contract_id"], verified_files=self.bundle.verified)
        for scheduled, message in source._channel("incremental_book_L2"):
            levels = []
            for side, price, quantity in message.levels:
                tick = price/self.tick_size
                if tick != tick.to_integral_value():
                    raise ValueError("price cannot be represented by replay tick contract")
                levels.append((side, int(tick), float(quantity)))
            yield HistoricalExchangeBookEvent(market_id=message.market_id, event_type=message.kind,
                exchange_ts_ns=scheduled, exchange_ts_source="unknown", local_receive_ts_ns=0,
                levels=tuple(levels), source=message.source_file_id,
                source_ordinal=message.source_ordinal, sequence_scope="provider_ordered")


def load_public_replay_inputs(root, *, tick_size):
    """Keep exchange tape and delivered quote state separate, with no fallback.

    Ceil nanoseconds at the legacy millisecond ABI so sub-ms readiness is never
    exposed early. Invalid/stale observations remain explicit invalidations.
    """
    bundle = root if isinstance(root, ConsumerBundle) else ConsumerBundle(root)
    meta, plan = bundle.manifest, bundle.manifest["plan"]
    profile = ObservationProfile(**plan["observation_profile"])
    source = PublicInputStream(bundle.source_paths(), profile=profile,
        start_ns=plan["start_ns"], end_ns=plan["end_ns"], market_id=plan["market_id"],
        input_contract_id=meta["input_contract_id"], verified_files=bundle.verified)
    rows = []
    for scheduled, trade in source._channel("trades"):
        if scheduled < plan["start_ns"]:
            continue
        rows.append({"transact_time": (scheduled+999_999)//1_000_000,
                     "price": float(trade.price), "quantity": float(trade.quantity),
                     "is_buyer_maker": trade.aggressor_side == "sell", "trade_id": trade.trade_id,
                     "source_timestamp_us": trade.source_timestamp_us,
                     "normal_quantity": np.nan})
    trades = pd.DataFrame(rows, columns=["transact_time", "price", "quantity", "is_buyer_maker",
                                        "trade_id", "source_timestamp_us", "normal_quantity"])
    del rows
    trades.attrs["input_contract_id"] = meta["input_contract_id"]
    trades.attrs["normal_quantity_observability"] = "unavailable"
    trades.attrs["source_clock_policy"] = profile.clock_policy
    count = meta["files"]["depth"]["rows"]
    ts, observed, versions = (np.empty(count, dtype=np.int64) for _ in range(3))
    usable = np.empty(count, dtype=bool)
    matrices = {name: np.full((count, 20), np.nan)
                for name in ("bid_px", "bid_qty", "ask_px", "ask_qty")}
    start = 0
    for depth in bundle.batches("depth"):
        stop = start + depth.num_rows
        ts[start:stop] = (depth["ready_ns"].to_numpy()+999_999)//1_000_000
        observed[start:stop] = depth["source_asof_ns"].to_numpy()//1000
        versions[start:stop] = depth["book_version"].to_numpy()
        usable[start:stop] = (depth["valid"].to_numpy(zero_copy_only=False)
                             & ~depth["stale"].to_numpy(zero_copy_only=False))
        for name, matrix in matrices.items():
            for offset, levels in enumerate(depth[name].to_pylist()):
                matrix[start + offset, :len(levels)] = levels
        start = stop
    # L2 has no separate usable mask at the legacy ABI. Invalidate its prices
    # and sizes as well, so a depth lookup cannot bypass BBO freshness.
    for matrix in matrices.values():
        matrix[~usable] = np.nan
    # Repeat timer samples are not new source messages, even if the message
    # itself left all level quantities unchanged.
    source_observed = np.r_[True, versions[1:] != versions[:-1]] if len(ts) else np.empty(0, dtype=bool)
    bbo = HistoricalBBOData(ts, matrices["bid_px"][:, 0], matrices["ask_px"][:, 0],
        matrices["bid_qty"][:, 0], matrices["ask_qty"][:, 0], source="public_delivered_depth",
        observation_ts_us=observed, source_observed=source_observed, usable=usable)
    l2 = HistoricalL2Data(ts, matrices["bid_px"], matrices["bid_qty"], matrices["ask_px"], matrices["ask_qty"],
        source="public_delivered_depth", observation_ts_us=observed, source_observed=source_observed)
    bars = bundle.table("bars").to_pandas()
    for name in ("open", "high", "low", "close", "volume", "turnover"):
        bars[name] = pd.to_numeric(bars[name], errors="coerce")
    bars["trade_count"] = bars["individual_count"].astype(float)
    # Compatibility base + 1000ms equals actual ready time, NOT source start.
    bars.index = pd.Index((bars["ready_ns"].to_numpy()+999_999)//1_000_000-1000, name="ready_base_ms")
    bars.attrs["availability_clock"] = "ready_base_ms_plus_1000"
    bars.attrs["input_contract_id"] = meta["input_contract_id"]
    return {"bundle": bundle, "trades": trades, "bars": bars, "bbo": bbo, "l2": l2,
            "exchange_book_event_tape": PublicExchangeBookTape(bundle, tick_size=tick_size),
            "frames": tuple(bundle.frames()), "contract_parity": "shared_input_adapter",
            "native_observation_parity": "not_proven", "economic_admission": False}


def public_predictions(frames, signal_engine, cross_columns):
    """Prediction timestamp is the actual frame-ready boundary, not t+10s."""
    predictions = (signal_engine.compute_feature_frames(frames)
                   if hasattr(signal_engine, "compute_feature_frames") else
                   [signal_engine.compute_signal(feature_frame=f, decision_ns=f.cutoff_ns) for f in frames])
    if not predictions:
        raise ValueError("no public feature frames in the requested interval")
    timestamps = np.asarray([(f.cutoff_ns+999_999)//1_000_000 for f in frames], dtype=np.int64)
    heads = [np.asarray([getattr(p, name) for p in predictions], dtype=np.float64)
             for name in ("touch_conditioned_up_probability_10000ms", "absolute_price_variance_rate_10000ms", "touch_conditioned_price_change_fraction_10000ms", "touch_side_adverse_probability_bid_10000ms", "touch_side_adverse_probability_ask_10000ms")]
    # Explicit unavailable reference columns, not neutralized fake observations.
    cross = [np.full(len(frames), np.nan) for _ in cross_columns]
    mappings = [dict(f.values) for f in frames]
    values = {name: np.asarray([row[name] for row in mappings], dtype=np.float64)
              for name, _ in frames[0].values}
    return (timestamps, *heads, *cross, values)
