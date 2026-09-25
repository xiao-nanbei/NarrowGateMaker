"""The formal replay engine's explicit common-data consumer adapter.

Only data loading is performed here. Economic execution still belongs to
models.backtest_tick; an adapter is not economic or live admission.
"""

from __future__ import annotations

from decimal import Decimal
from dataclasses import dataclass, fields
import hashlib
from types import MappingProxyType

import numpy as np
import pandas as pd

from data.runtime import ConsumerBundle, ObservationProfile, PublicInputStream
from models.tick_data_types import HistoricalBBOData, HistoricalExchangeBookEvent, HistoricalL2Data


@dataclass(frozen=True)
class PreparedReplayInputs:
    """Admitted market inputs only: no account, model, RNG or consumed cursor.

    DataFrames are private cold-path owners; callers must not mutate them.
    Execution obtains its own filtered trade table. Shared numeric views are
    read-only, and exchange tape iteration creates a fresh source cursor.
    """
    inputs: MappingProxyType
    tick_size: float
    variance_ts: np.ndarray
    variance: np.ndarray
    manifest_id: str

    @classmethod
    def create(cls, inputs, tick_size, variance_ts, variance):
        for name in ("bbo", "l2"):
            for field in fields(inputs[name]):
                value = getattr(inputs[name], field.name)
                if isinstance(value, np.ndarray):
                    value.flags.writeable = False
        for array in (variance_ts, variance):
            array.flags.writeable = False
        return cls(MappingProxyType(inputs), tick_size, variance_ts, variance,
                   hashlib.sha256((inputs["bundle"].root/"manifest.json").read_bytes()).hexdigest())


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
            input_contract_id=meta["input_contract_id"])
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
    bundle = ConsumerBundle(root)
    meta, plan = bundle.manifest, bundle.manifest["plan"]
    profile = ObservationProfile(**plan["observation_profile"])
    source = PublicInputStream(bundle.source_paths(), profile=profile,
        start_ns=plan["start_ns"], end_ns=plan["end_ns"], market_id=plan["market_id"],
        input_contract_id=meta["input_contract_id"])
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
