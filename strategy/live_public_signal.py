"""Execution-v1 market transport consumer, usable in offline assembly.

Deployment authority belongs to live startup, not a network-free model object.
Constructing this consumer does not grant permission to connect or trade.
"""

import json
from pathlib import Path
from threading import RLock
import time

import lightgbm as lgb

from live.feature_protocol import LIVE_INPUT_CONTRACT, LiveExecutionFeatures, validate_live_feature_support
from strategy.public_model_contract import validate_public_bundle
from strategy.signal import Bar1s, SignalEngine


class LivePublicSignalEngine(SignalEngine):
    def __init__(self, *args, **kwargs):
        self._protocol_lock = RLock()
        self._live_features = None
        self._last_published_bar = -1
        self._published_generation = None
        super().__init__(*args, **kwargs)
        if not self._enable_ml:
            raise ValueError("execution-v1 live requires an explicitly selected complete model")
        self._cpp_feature_engine = None
        self._cpp_signal_features_enabled = False
        self._live_features = LiveExecutionFeatures(
            symbol=self._symbol, start_ns=time.time_ns(),
            allowed_lateness_ns=100_000_000, max_book_age_ns=1_000_000_000,
        )
        self._live_features.connection(connected=True, now_ns=time.time_ns())

    def _initialize_current_models(self, *, model_dir=None):
        root = Path(model_dir or self._model_dir)
        metadata = validate_public_bundle(root, expected_symbol=self._symbol, live=False)
        manifest = json.loads((root / "public_input_model.json").read_text())
        validate_live_feature_support(manifest, trade_source="individual")
        models = {name: lgb.Booster(model_file=str(root / f"{name}.txt")) for name in metadata}
        for name, model in models.items():
            if model.feature_name() != metadata[name]["feature_cols"]:
                raise ValueError("model file feature order differs from frozen metadata")
        # Only the transport binding is projected in memory. Frozen files,
        # source/training/label identity and all learned weights stay unchanged.
        projected = {name: {**meta, "training_input_contract_id": meta["input_contract_id"],
                           "input_contract_id": LIVE_INPUT_CONTRACT} for name, meta in metadata.items()}
        with self._model_runtime_lock:
            self._models = models
            self._model_metadata = projected
            self._model_feature_cols = {name: meta["feature_cols"] for name, meta in metadata.items()}
            self._model_feature_schema = tuple(next(iter(self._model_feature_cols.values())))
            self._native_model_bundle = None
            self._native_inference_requested = False

    def prefill_from_agg_trades(self, trades):
        # REST aggregate prefill cannot seed this model's individual history.
        return None

    def on_agg_trade(self, *args, **kwargs):
        raise ValueError("execution-v1 requires native individual @trade, not aggTrade")

    def _publish_bars(self):
        bars = self._live_features.features.bars.get(self._live_features.market_id, ())
        with self._lock:
            if self._published_generation != self._live_features.generation:
                self._bar_buffer.clear()
                self._close_history.clear()
                self._sign_history.clear()
                self._signed_vol_cumsum = 0.0
                self._warmup_count = 0
                self._last_depth = None
                self._published_generation = self._live_features.generation
            for bar in bars:
                if bar.end_ns <= self._last_published_bar:
                    continue
                self._last_published_bar = bar.end_ns
                if bar.coverage != "observed" or bar.close is None:
                    continue
                # Compatibility view for existing price/variance/risk consumers;
                # model features always come from the shared immutable Bar.
                values = {k: float(getattr(bar, k)) for k in
                          ("open", "high", "low", "close", "volume", "buy_volume", "sell_volume")}
                self._finalize_bar(Bar1s(
                    ts=bar.start_ns // 1_000_000, **values,
                    trade_count=bar.individual_count, buy_count=bar.buy_count, sell_count=bar.sell_count,
                    quote_qty=float(bar.turnover), buy_quote_qty=float(bar.buy_turnover),
                    sell_quote_qty=float(bar.sell_turnover),
                ))

    def on_trade(self, event, *, receive_ts_ns=None):
        with self._protocol_lock:
            now = time.time_ns()
            if not self._live_features.connected:
                self._live_features.connection(connected=True, now_ns=now)
            self._live_features.individual_trade(event, receive_ns=receive_ts_ns or now, ready_ns=now)
            self._publish_bars()

    def on_depth(self, event, *, receive_ts_ns=None):
        with self._protocol_lock:
            now = time.time_ns()
            if not self._live_features.connected:
                return
            self._live_features.partial_depth(event, receive_ns=receive_ts_ns or now, ready_ns=now)
            self._publish_bars()
            super().on_depth(event, receive_ts_ns=receive_ts_ns or now)

    def market_disconnected(self):
        with self._protocol_lock:
            self._live_features.connection(connected=False, now_ns=time.time_ns())
            self._publish_bars()

    @property
    def is_warmed_up(self):
        with self._protocol_lock:
            if self._live_features is None or not self._live_features.connected:
                return False
            values = dict(self._live_features.frame(time.time_ns()).values)
            self._publish_bars()
            return values["mid"] is not None and values["volume_60s"] is not None

    def compute_signal(self, *, perf_timings=None, **kwargs):
        if kwargs:
            raise ValueError("live execution-v1 does not accept offline override frames")
        with self._protocol_lock:
            now = time.time_ns()
            if not self._live_features.connected:
                raise ValueError("execution-v1 trade source disconnected")
            frame = self._live_features.frame(now)
            self._publish_bars()
            values = dict(frame.values)
            if values["mid"] is None or values["volume_60s"] is None:
                raise ValueError("execution-v1 input warmup or coverage incomplete")
        if perf_timings is not None:
            perf_timings["signal_compute_path"] = "execution_v1_individual"
        return self.consume_feature_frame(frame, decision_ns=now)
