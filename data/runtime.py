"""Shared source-fact -> visible-input stream for feature and replay consumers.

No exchange connectivity, model fitting, account reset or alternate raw source.
The source-time proxy is an explicit approximation, never mapper verification.
"""

from __future__ import annotations

import heapq
import hashlib
import json
import math
import os
import shutil
from dataclasses import asdict, dataclass, replace
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

from data.facts import _digest, _real, read_facts, save_private_json
from data.observation import (
    CONTRACT, DeliveryQueue, ExecutionFeatures, LatencyProfile, TradeContribution,
    VisibleTradeWindows, historical_trade,
)
from data.tardis_input import BookMessage, ObservableBook, TradeExecution


@dataclass(frozen=True)
class ObservationProfile:
    profile_id: str
    clock_policy: str
    market_delay_ns: int
    processing_ns: int
    allowed_lateness_ns: int
    max_book_age_ns: int
    depth_period_ns: int = 100_000_000
    depth_phase_ns: int = 0
    feature_period_ns: int = 1_000_000_000
    trade_coverage: str = "unknown"
    tie_policy: str = "source_then_publication_then_delivery_then_timer"
    measured_latency_path: str | None = None
    measured_latency_sha256: str | None = None
    measured_latency_market_id: str | None = None
    latency_seed: int = 42

    def __post_init__(self):
        if not self.profile_id or self.clock_policy not in {"strict_exchange", "source_timestamp_proxy"}:
            raise ValueError("explicit clock policy/profile identity required")
        if min(self.market_delay_ns, self.processing_ns, self.allowed_lateness_ns, self.max_book_age_ns) < 0:
            raise ValueError("negative observation timing")
        if (self.depth_period_ns <= 0 or self.feature_period_ns <= 0
                or not 0 <= self.depth_phase_ns < self.depth_period_ns):
            raise ValueError("invalid observation cadence")
        if self.trade_coverage not in {"observed", "unknown", "partial", "missing"}:
            raise ValueError("invalid trade coverage policy")
        if self.tie_policy != "source_then_publication_then_delivery_then_timer":
            raise ValueError("unsupported tie policy")
        binding = (self.measured_latency_path, self.measured_latency_sha256, self.measured_latency_market_id)
        if any(binding) and not all(binding):
            raise ValueError("complete measured latency identity required")
        if all(binding) and (self.market_delay_ns or self.processing_ns):
            raise ValueError("measured pairs replace fixed delay/service; cannot double count")

    def require_market(self, market_id: str) -> None:
        """A measured delivery sample belongs to one exact perpetual market."""
        if self.measured_latency_market_id is None:
            return
        prefix = "binance_futures:perpetual:"
        if (not market_id.startswith(prefix)
                or self.measured_latency_market_id != "binance:perp:" + market_id[len(prefix):]):
            raise ValueError("measured latency profile market identity mismatch")


def _require_observation_scenario(plan, profile):
    """Verify an explicitly declared scenario without changing legacy plans."""
    scenario = plan.get("observation_scenario")
    if scenario is None:
        return
    if (scenario.get("schema") != "data.observation_scenario.v1"
            or scenario.get("market_id") != plan.get("market_id")
            or scenario.get("native_observation_parity") != "not_proven"):
        raise ValueError("observation scenario identity/market/parity mismatch")
    classification = scenario.get("classification")
    if classification == "simulated_not_measured":
        if (profile.measured_latency_path is not None or profile.market_delay_ns <= 0
                or not scenario.get("parameter_basis")
                or scenario.get("provider_local_timestamp_as_receive") is not False):
            raise ValueError("simulated scenario needs positive delay and explicit provenance")
    elif classification == "market_measured":
        if profile.measured_latency_path is None:
            raise ValueError("measured scenario needs measured latency evidence")
    else:
        raise ValueError("unknown observation scenario classification")


class MeasuredDelivery:
    """Paired empirical lag/service scenario, not native packet reconstruction.

    Draws use stable event identities, independent of batching or warmup length.
    Same-message lag/service pairing is retained; historical burst chronology
    and cross-channel correlations are not reconstructed. Measured service is
    treated as a modeled processor cost, not proof of intrinsic CPU time.
    """

    def __init__(self, profile):
        self.seed, self.groups = profile.latency_seed, {}
        path = Path(profile.measured_latency_path)
        if _digest(path) != profile.measured_latency_sha256:
            raise ValueError("measured latency binding changed")
        data = json.loads(path.read_text())
        if data.get("schema") != "market_data_latency_profile.v1":
            raise ValueError("unsupported measured latency profile")
        for channel in ("depth", "trade"):
            groups = [g for g in data.get("groups", []) if
                g.get("market_id") == profile.measured_latency_market_id
                and g.get("event_type") == channel and g.get("transport") == "websocket"]
            if len(groups) != 1:
                raise ValueError("one exact measured group per depth/trade required")
            group = groups[0]
            pairs = group.get("simulation_clock_pair_samples_ms", [])
            if (group.get("simulation_clock_pair_columns") != ["transport_lag_ms", "feature_latency_ms"]
                    or group.get("simulation_clock_pair_semantics") != "all_observed_same_message_pairs"
                    or not pairs or group.get("rows") != len(pairs)
                    or any(len(p) != 2 or any(not math.isfinite(x) or x < 0 for x in p) for p in pairs)):
                raise ValueError("complete nonnegative paired latency samples required")
            self.groups[channel] = tuple(tuple(round(x * 1_000_000) for x in p) for p in pairs)

    def draw(self, channel, event_id):
        key = f"paired_message_v1:{self.seed}:{channel}:{event_id}".encode()
        pairs = self.groups[channel]
        lag, service = pairs[int.from_bytes(hashlib.sha256(key).digest()[:8], "little") % len(pairs)]
        return dict(market_delay_ns=lag, processing_ns=service)


@dataclass(frozen=True)
class InputTick:
    now_ns: int
    exchange_events: tuple
    observations: tuple
    bars: tuple
    feature_frame: object | None
    exchange_book: object


class ExchangeOutcomeBars:
    """Offline outcome-only OHLC on the declared exchange/proxy schedule.

    Never delivered to ExecutionFeatures. Source regressions keep the stream's
    explicitly modeled schedule; these are not claimed as exact exchange Bars.
    Empty observed buckets retain null OHLC, not carried trade prices.
    """

    period = 1_000_000_000

    def __init__(self, start_ns, coverage):
        self.start = start_ns
        self.coverage = coverage
        self.row = self._empty()

    def _empty(self):
        known = self.coverage == "observed"
        return dict(start_ns=self.start, end_ns=self.start+self.period,
            coverage=self.coverage, open=None, high=None, low=None, close=None,
            volume=Decimal(0) if known else None, turnover=Decimal(0) if known else None,
            individual_count=0 if known else None)

    def advance(self, now_ns, events=()):
        if now_ns < self.start:
            return ()  # source context before selected outcome interval
        closed = []
        while self.start+self.period <= now_ns:
            closed.append(self.row)
            self.start += self.period
            self.row = self._empty()
        for event in events:
            if not isinstance(event, TradeExecution):
                continue
            row, price = self.row, event.price
            row["open"] = price if row["open"] is None else row["open"]
            row["high"] = price if row["high"] is None else max(row["high"], price)
            row["low"] = price if row["low"] is None else min(row["low"], price)
            row["close"] = price
            if self.coverage == "observed":
                row["volume"] += event.quantity
                row["turnover"] += price*event.quantity
                row["individual_count"] += 1
        return tuple(closed)


def bundle_paths(root, *, start=None, end=None, relocated=False):
    root = Path(root)
    manifest = json.loads((root / "manifest.json").read_text())
    if manifest.get("schema") == "data.facts.v1":
        return [root]
    if manifest.get("schema") != "data.full_calendar.v1" or manifest.get("source_profile") != "tardis_only":
        raise ValueError("a bound source-fact calendar is required")
    rows = [r for r in manifest["days"] if (start is None or r["calendar_date"] >= start)
            and (end is None or r["calendar_date"] <= end)]
    if not rows or (start and rows[0]["calendar_date"] != start) or (end and rows[-1]["calendar_date"] != end):
        raise ValueError("missing requested calendar interval")
    first = date.fromisoformat(start or manifest["start"])
    last = date.fromisoformat(end or manifest["end"])
    expected = [(first + timedelta(days=i)).isoformat() for i in range((last-first).days+1)]
    if not expected or [r["calendar_date"] for r in rows] != expected:
        raise ValueError("calendar dates missing, duplicated or reordered")
    symbols = manifest.get("symbols", [])
    if not symbols or len(set(symbols)) != len(symbols):
        raise ValueError("required calendar symbols missing or duplicated")
    paths = []
    for row in rows:
        if row["status"] != "content_scanned":
            raise ValueError(f"unready required date: {row['calendar_date']}")
        path = root / row["calendar_date"] if relocated else Path(row["bundle"])
        if _digest(path / "manifest.json") != row["manifest_sha256"]:
            raise ValueError("calendar source binding changed")
        daily = json.loads((path / "manifest.json").read_text())
        if (daily.get("schema") != "data.facts.v1" or
                [(f["symbol"], f["channel"]) for f in daily["files"]] !=
                [(s, c) for s in symbols for c in ("incremental_book_L2", "trades")]):
            raise ValueError("required channel identity mismatch")
        paths.append(path)
    return paths


def _require_market_source_bundles(bundles, market_id):
    """Refuse a valid but wrong-symbol fact list before publishing an empty view."""
    prefix = "binance_futures:perpetual:"
    if not market_id.startswith(prefix) or not market_id[len(prefix):]:
        raise ValueError("consumer market identity must name one Binance perpetual")
    symbol = market_id[len(prefix):]
    required = {(symbol, "incremental_book_L2"), (symbol, "trades")}
    for bundle in bundles:
        manifest = json.loads((bundle / "manifest.json").read_text())
        available = {(spec["symbol"], spec["channel"]) for spec in manifest["files"]}
        if manifest.get("schema") != "data.facts.v1" or not required <= available:
            raise ValueError("consumer source bundle market/channel identity mismatch")


class PublicInputStream:
    """Bounded streaming scheduler; consumer clocks never use provider receive.

    A channel's original order is retained. Proxy mode uses a distinct modeled
    schedule for regressions and records the adjustment; raw timestamps remain.
    Strict mode refuses uncertain clocks instead of silently selecting a proxy.
    """

    def __init__(self, bundles, *, profile, start_ns, end_ns, market_id,
                 input_contract_id, verify=True):
        if start_ns >= end_ns or start_ns % 1_000_000_000:
            raise ValueError("aligned nonempty observation interval required")
        if isinstance(profile, ObservationProfile):
            profile.require_market(market_id)
        self.bundles = tuple(Path(p) for p in bundles)
        self.profile, self.start_ns, self.end_ns = profile, start_ns, end_ns
        self.market_id, self.input_contract_id, self.verify = market_id, input_contract_id, verify
        self.stats = dict(book_events=0, trade_events=0, duplicate_trades=0,
            source_regressions=0, max_schedule_adjustment_ns=0, future_fill_violations=0,
            invalid_book_observations=0, stale_book_observations=0, max_book_age_ns=0,
            late_trades=0, frames=0, source_clock_policy=profile.clock_policy,
            capture_completeness="unknown", native_observation_parity="not_proven")

    def _channel(self, channel):
        previous = -1
        # This stream selects one market. Retain its last nonempty ID high-water
        # mark across empty bundles; overlapping facts require reconciliation.
        previous_max = None
        ordinal = 0
        for bundle in self.bundles:
            manifest = json.loads((bundle / "manifest.json").read_text())
            specs = [s for s in manifest["files"] if s["channel"] == channel and self.market_id.endswith(":"+s["symbol"])]
            for spec in specs:
                if channel == "trades":
                    lo, hi = spec["quality"].get("trade_id_min"), spec["quality"].get("trade_id_max")
                    if previous_max is not None and lo is not None and lo <= previous_max:
                        # Do not reinterpret a calendar overlap as two economic
                        # executions. A frozen globally deduplicated bundle is
                        # required until its exact overlap is reconciled.
                        raise ValueError("overlapping cross-bundle trade IDs require reconciled facts")
                    if hi is not None:
                        previous_max = hi
            for event in read_facts(bundle, verify=self.verify, channels={channel}):
                if event.market_id != self.market_id:
                    continue
                source = event.exchange_ts_ns
                if source is None:
                    if self.profile.clock_policy == "strict_exchange":
                        raise ValueError("unverified source clock cannot enter strict consumer")
                    source = event.source_timestamp_us * 1000
                schedule = source
                if schedule < previous:
                    self.stats["source_regressions"] += 1
                    if self.profile.clock_policy == "strict_exchange":
                        raise ValueError("source clock regression in strict consumer")
                    schedule = previous
                    self.stats["max_schedule_adjustment_ns"] = max(self.stats["max_schedule_adjustment_ns"], schedule-source)
                previous = schedule
                event = replace(event, source_ordinal=ordinal)
                ordinal += 1
                if schedule >= self.end_ns:
                    return
                yield schedule, event

    def __iter__(self):
        profile = self.profile
        measured = MeasuredDelivery(profile) if profile.measured_latency_path else None
        queue = DeliveryQueue(LatencyProfile(profile.market_delay_ns, profile.processing_ns, profile.profile_id))
        windows = VisibleTradeWindows(start_ns=self.start_ns, allowed_lateness_ns=profile.allowed_lateness_ns,
            market_id=self.market_id, input_contract_id=self.input_contract_id, coverage=profile.trade_coverage)
        features = ExecutionFeatures(self.input_contract_id, market_id=self.market_id, max_book_age_ns=profile.max_book_age_ns)
        book = ObservableBook()
        channels = [iter(self._channel(c)) for c in ("incremental_book_L2", "trades")]
        pending = []
        for rank, channel in enumerate(channels):
            item = next(channel, None)
            if item:
                heapq.heappush(pending, (item[0], rank, item[1]))
        period = profile.depth_period_ns
        depth_at = ((self.start_ns-profile.depth_phase_ns+period-1)//period)*period+profile.depth_phase_ns
        frame_at = ((self.start_ns+profile.feature_period_ns-1)//profile.feature_period_ns)*profile.feature_period_ns
        while pending or depth_at < self.end_ns or frame_at < self.end_ns:
            delivery_at = min(queue._received[0][0] if queue._received else self.end_ns,
                              queue._ready[0][0] if queue._ready else self.end_ns)
            timer_at = windows.next_start+windows.period+windows.lateness
            now = min(pending[0][0] if pending else self.end_ns, depth_at, frame_at, delivery_at, timer_at)
            if now >= self.end_ns:
                break
            exchange = []
            while pending and pending[0][0] <= now:
                schedule, rank, event = heapq.heappop(pending)
                exchange.append(event)
                if isinstance(event, BookMessage):
                    book.apply(event)
                    self.stats["book_events"] += 1
                else:
                    self.stats["trade_events"] += 1
                    # Proxy conversion is local to the explicit observation
                    # scenario; frozen source facts keep exchange_ts=None.
                    timed = event if event.exchange_ts_ns is not None else replace(event, exchange_ts_ns=event.source_timestamp_us*1000)
                    contribution = historical_trade(timed)
                    if schedule >= self.start_ns:
                        queue.publish(event_id=contribution.event_id, market_id=self.market_id,
                            input_contract_id=self.input_contract_id, origin="source_trade_event",
                            source_asof_ns=contribution.exchange_ts_ns, publish_ns=schedule,
                            payload=contribution, coverage=profile.trade_coverage,
                            **(measured.draw("trade", contribution.event_id) if measured else {}))
                item = next(channels[rank], None)
                if item:
                    heapq.heappush(pending, (item[0], rank, item[1]))
            if now < self.start_ns:
                continue  # caller includes warmup in start_ns, not future backfill
            if now == depth_at:
                view = book.view(20)
                if view.source_asof_us is not None:
                    age = now-view.source_asof_us*1000
                    if age < 0:
                        raise ValueError("future book publication")
                    self.stats["max_book_age_ns"] = max(self.stats["max_book_age_ns"], age)
                    self.stats["invalid_book_observations"] += not view.valid
                    self.stats["stale_book_observations"] += age > profile.max_book_age_ns
                    queue.publish(event_id=f"depth:{now}", market_id=self.market_id,
                        input_contract_id=self.input_contract_id, origin="derived_partial_depth20",
                        source_asof_ns=view.source_asof_us*1000, publish_ns=now, payload=view,
                        **(measured.draw("depth", f"depth:{now}") if measured else {}))
                depth_at += period
            observations = queue.advance(now)
            trades = tuple(o for o in observations if isinstance(o.payload, TradeContribution))
            bars = windows.advance(now, trades)
            self.stats["late_trades"] = windows.late_count
            features.advance(now, observations, ((self.market_id, b) for b in bars))
            frame = None
            if now == frame_at:
                frame = features.frame(now)
                self.stats["frames"] += 1
                frame_at += profile.feature_period_ns
            yield InputTick(now, tuple(exchange), observations, bars, frame, book.view(1))


def derive_inputs(plan, output):
    """Create-only, bounded-memory consumer bundle through the shared stream.

    This is feature generation, not labels/training/economic replay. The private
    plan declares source interval, warmup and all simulated clock assumptions.
    """
    from uuid import uuid4
    import pyarrow as pa
    import pyarrow.parquet as pq
    from data.observation import EXECUTION_FEATURE_NAMES, FEATURE_CONTRACT
    from data.tardis_input import CONTRACT as INPUT_CONTRACT

    output = _real(Path(output))
    if output.exists():
        raise FileExistsError("consumer bundle exists; no implicit overwrite")
    if "source_bundles" in plan:
        if "facts_root" in plan or not plan["source_bundles"]:
            raise ValueError("choose one explicit source-bundle list or facts root")
        bundles = []
        for source in plan["source_bundles"]:
            path = _real(Path(source["path"]))
            if _digest(path / "manifest.json") != source["sha256"]:
                raise ValueError("sequence source manifest changed")
            if path in bundles:
                raise ValueError("duplicate sequence source bundle")
            bundles.append(path)
    else:
        root = _real(Path(plan["facts_root"]))
        bundles = bundle_paths(root, start=plan.get("source_start_day"), end=plan.get("source_end_day"))
    profile = ObservationProfile(**plan["observation_profile"])
    profile.require_market(plan["market_id"])
    _require_observation_scenario(plan, profile)
    _require_market_source_bundles(bundles, plan["market_id"])
    stream = PublicInputStream(bundles, profile=profile, start_ns=plan["start_ns"],
        end_ns=plan["end_ns"], market_id=plan["market_id"], input_contract_id=INPUT_CONTRACT)
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = output.with_name(output.name+"."+uuid4().hex+".part")
    stage.mkdir(mode=0o700)
    common = {b"input_contract_id": INPUT_CONTRACT.encode(), b"observation_contract_id": CONTRACT.encode(),
              b"feature_contract_id": FEATURE_CONTRACT.encode()}
    schemas = {
        "features": pa.schema([("cutoff_ns", pa.int64()), ("max_dependency_ready_ns", pa.int64())]
            + [(n, pa.float64()) for n in EXECUTION_FEATURE_NAMES]
            + [("validity_mask", pa.list_(pa.bool_())),
               ("source_ages_ns", pa.map_(pa.string(), pa.int64()))], metadata=common),
        "bars": pa.schema([(n, pa.int64()) for n in ("start_ns", "end_ns", "ready_ns", "last_trade_ns")]
            + [(n, pa.string()) for n in ("coverage", "open", "high", "low", "close", "volume", "turnover",
               "buy_volume", "sell_volume", "buy_turnover", "sell_turnover")]
            + [(n, pa.int64()) for n in ("individual_count", "native_packet_count", "buy_count", "sell_count", "observed_event_count")], metadata=common),
        "depth": pa.schema([("publish_ns", pa.int64()), ("ready_ns", pa.int64()), ("source_asof_ns", pa.int64()),
            ("book_version", pa.int64()), ("valid", pa.bool_()), ("stale", pa.bool_())]
            + [(n, pa.list_(pa.float64())) for n in ("bid_px", "bid_qty", "ask_px", "ask_qty")], metadata=common),
    }
    outcomes = None
    if plan.get("include_outcome_bars", False):
        outcomes = ExchangeOutcomeBars(plan["start_ns"], profile.trade_coverage)
        schemas["outcome_bars"] = pa.schema(
            [("start_ns", pa.int64()), ("end_ns", pa.int64()), ("coverage", pa.string())]
            + [(n, pa.string()) for n in ("open", "high", "low", "close", "volume", "turnover")]
            + [("individual_count", pa.int64())],
            metadata={**common, b"purpose": b"offline_outcome_only_not_strategy_visible",
                      b"clock_policy": profile.clock_policy.encode()})
    writers, buffers, counts = {}, {name: [] for name in schemas}, dict.fromkeys(schemas, 0)
    try:
        for name, schema in schemas.items():
            writers[name] = pq.ParquetWriter(stage / (name+".parquet"), schema, compression="zstd")

        def write(name, row):
            buffers[name].append(row)
            counts[name] += 1
            if len(buffers[name]) >= 4096:
                writers[name].write_table(pa.Table.from_pylist(buffers[name], schema=schemas[name]))
                buffers[name].clear()

        def write_outcomes(rows):
            for row in rows:
                write("outcome_bars", {k: str(v) if isinstance(v, Decimal) else v for k, v in row.items()})

        for tick in stream:
            if outcomes is not None:
                write_outcomes(outcomes.advance(tick.now_ns, tick.exchange_events))
            for bar in tick.bars:
                row = asdict(bar)
                for field in schemas["bars"]:
                    if pa.types.is_string(field.type) and row[field.name] is not None:
                        row[field.name] = str(row[field.name])
                write("bars", row)
            for observation in tick.observations:
                if isinstance(observation.payload, TradeContribution):
                    continue
                view = observation.payload
                write("depth", {"publish_ns": observation.publish_ns, "ready_ns": observation.ready_ns,
                    "source_asof_ns": observation.source_asof_ns, "book_version": view.version, "valid": view.valid,
                    "stale": tick.now_ns-observation.source_asof_ns > profile.max_book_age_ns,
                    "bid_px": [float(p) for p, _ in view.bids], "bid_qty": [float(q) for _, q in view.bids],
                    "ask_px": [float(p) for p, _ in view.asks], "ask_qty": [float(q) for _, q in view.asks]})
            frame = tick.feature_frame
            if frame is not None:
                write("features", {"cutoff_ns": frame.cutoff_ns, "max_dependency_ready_ns": frame.max_dependency_ready_ns,
                    **{k: float(v) if v is not None else None for k, v in frame.values},
                    "validity_mask": [v for _, v in frame.validity_mask],
                    "source_ages_ns": list(frame.source_ages_ns)})
        if outcomes is not None:
            write_outcomes(outcomes.advance(plan["end_ns"]))
        for name in schemas:
            if buffers[name]:
                writers[name].write_table(pa.Table.from_pylist(buffers[name], schema=schemas[name]))
            writers[name].close()
        result = {"schema": "data.consumer_bundle.v1", "visibility": "local_only_do_not_publish",
            "input_contract_id": INPUT_CONTRACT, "observation_contract_id": CONTRACT,
            "feature_contract_id": FEATURE_CONTRACT, "feature_names": list(EXECUTION_FEATURE_NAMES),
            "plan": plan, "source_bundles": [{"path": str(p), "sha256": _digest(p/"manifest.json")} for p in bundles],
            "files": {n: {"file": n+".parquet", "sha256": _digest(stage/(n+".parquet")), "rows": counts[n]} for n in schemas},
            "stats": stream.stats, "training": "not_run", "economic_replay": "not_run",
            "economic_admission": False, "research_use": "unchanged_requires_explicit_split_manifest"}
        save_private_json(stage/"manifest.json", result)
        os.rename(stage, output)
        return result
    except BaseException:
        for writer in writers.values():
            writer.close()
        shutil.rmtree(stage)
        raise


class ConsumerBundle:
    """Single verified reader for preprocessing, model and replay callers."""

    def __init__(self, root):
        from data.observation import FEATURE_CONTRACT, EXECUTION_FEATURE_NAMES
        from data.tardis_input import CONTRACT as INPUT_CONTRACT
        self.root = _real(Path(root))
        self.manifest = json.loads((self.root/"manifest.json").read_text())
        if (self.manifest.get("schema") != "data.consumer_bundle.v1"
                or self.manifest.get("input_contract_id") != INPUT_CONTRACT
                or self.manifest.get("observation_contract_id") != CONTRACT
                or self.manifest.get("feature_contract_id") != FEATURE_CONTRACT
                or self.manifest.get("feature_names") != list(EXECUTION_FEATURE_NAMES)):
            raise ValueError("legacy/unknown consumer bundle forbidden")
        profile = ObservationProfile(**self.manifest["plan"]["observation_profile"])
        profile.require_market(self.manifest["plan"]["market_id"])
        _require_observation_scenario(self.manifest["plan"], profile)
        self.verified = {}

    def source_paths(self):
        paths = []
        for source in self.manifest["source_bundles"]:
            path = _real(Path(source["path"]))
            if _digest(path / "manifest.json") != source["sha256"]:
                raise ValueError("consumer source manifest changed")
            paths.append(path)
        return paths

    def stream(self):
        """Replay the bound observation scenario, not provider receipt clocks.

        Re-iterable callers create a fresh stream for each pass. The source
        identities are checked before any event is exposed.
        """
        plan = self.manifest["plan"]
        return PublicInputStream(
            self.source_paths(), profile=ObservationProfile(**plan["observation_profile"]),
            start_ns=plan["start_ns"], end_ns=plan["end_ns"], market_id=plan["market_id"],
            input_contract_id=self.manifest["input_contract_id"],
        )

    def _verified_parquet(self, name):
        import pyarrow.parquet as pq
        spec = self.manifest["files"][name]
        path = _real(self.root/spec["file"])
        if path.parent != self.root:
            raise ValueError("consumer artifact escapes bundle")
        stat = path.stat()
        identity = (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
        if self.verified.get(name) != identity:
            if _digest(path) != spec["sha256"]:
                raise ValueError("consumer artifact checksum mismatch")
            self.verified[name] = identity
        reader = pq.ParquetFile(path)
        for key in ("input_contract_id", "observation_contract_id", "feature_contract_id"):
            if (reader.schema_arrow.metadata or {}).get(key.encode()) != self.manifest[key].encode():
                raise ValueError("consumer contract metadata mismatch")
        if reader.metadata.num_rows != spec["rows"]:
            raise ValueError("consumer row count mismatch")
        return reader

    def table(self, name):
        return self._verified_parquet(name).read()

    def batches(self, name, *, batch_size=8192):
        """Bounded expansion with the same identity/schema/row-count checks."""
        if type(batch_size) is not int or batch_size <= 0:
            raise ValueError("positive integer batch size required")
        yield from self._verified_parquet(name).iter_batches(batch_size=batch_size)

    def frames(self):
        from data.observation import FeatureFrame
        names = self.manifest["feature_names"]
        for row in self.table("features").to_pylist():
            frame = FeatureFrame(self.manifest["input_contract_id"], CONTRACT, self.manifest["feature_contract_id"],
                row["cutoff_ns"], row["max_dependency_ready_ns"], tuple((n, row[n]) for n in names),
                tuple(row.get("source_ages_ns") or ()),
                tuple(zip(names, row["validity_mask"], strict=True)))
            if frame.max_dependency_ready_ns is not None and frame.max_dependency_ready_ns > frame.cutoff_ns:
                raise ValueError("future dependency in persisted feature frame")
            yield frame
