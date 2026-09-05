"""Shared tick replay data-window loading and slicing helpers."""

from __future__ import annotations

import hashlib
import inspect
import json
import os
import pickle
import sys
from contextlib import suppress
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data import normalized_l2_registry as l2_registry  # noqa: E402
from models import backtest_tick as bt  # noqa: E402
from models.book_source_contract import (  # noqa: E402
    book_source_contract,
    enforce_book_source_contract,
)
from models.cache_tier_lru import (  # noqa: E402
    record_cache_access,
    register_cache_write,
)
from models.replay_cache_components import (  # noqa: E402
    MarketContextPayload,
    canonical_sha256,
    file_reference,
    file_sha256,
    load_market_context,
    load_model_overlay,
    market_context_identity,
    missing_file_reference,
    model_overlay_identity,
    write_market_context,
    write_model_overlay,
)
from models.tick_data_types import HistoricalBBOData, HistoricalL2Data  # noqa: E402

# v13 binds the source authority of the selected normalized book.  This keeps
# provider-local Tardis sensitivity windows from being reused as native formal
# lifecycle or exact-queue evidence.
WINDOW_CACHE_VERSION = 13
WINDOW_COMPONENT_CACHE_VERSION = 1
WINDOW_COMPONENT_CACHE_V2_VERSION = 2


def _record_cache_hit(path: Path) -> None:
    with suppress(Exception):
        record_cache_access(path)


def _register_cache_artifact(path: Path) -> None:
    with suppress(Exception):
        register_cache_write(path)


def _causal_context_days(day: str, warmup_days: int) -> list[str]:
    target = pd.Timestamp(day, tz="UTC")
    count = max(0, int(warmup_days))
    return [
        (target - pd.Timedelta(days=offset)).strftime("%Y-%m-%d") for offset in range(count, -1, -1)
    ]


def _resolve_project_path(raw: Any) -> Path | None:
    if raw is None or str(raw).strip() == "":
        return None
    path = Path(str(raw)).expanduser()
    if not path.is_absolute():
        path = ROOT / path
    return path.resolve()


def _formal_quality_allowed_days(params: dict[str, Any]) -> tuple[str, ...]:
    raw = params.get("_formal_quality_allowed_days", ())
    values = (raw,) if isinstance(raw, str) else tuple(raw)
    days = tuple(sorted({str(value)[:10] for value in values}))
    for day in days:
        try:
            parsed = pd.Timestamp(day)
        except Exception as exc:
            raise ValueError(f"invalid formal quality day: {day}") from exc
        if len(day) != 10 or parsed.strftime("%Y-%m-%d") != day:
            raise ValueError(f"invalid formal quality day: {day}")
    return days


def _book_source_contract(day: str) -> dict[str, Any]:
    return book_source_contract(
        day,
        bbo_dir=bt.BBO_DIR,
        l2_dir=bt.L2_DIR,
    )


def _enforce_book_source_contract(
    day: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    return enforce_book_source_contract(
        day,
        params,
        bbo_dir=bt.BBO_DIR,
        l2_dir=bt.L2_DIR,
    )


def _file_signature(path: Path) -> tuple[str, int, int]:
    try:
        stat = path.stat()
        return (str(path.resolve()), int(stat.st_size), int(stat.st_mtime_ns))
    except FileNotFoundError:
        return (str(path.resolve()), -1, -1)


def _glob_signatures(directory: Path, patterns: tuple[str, ...]) -> list[tuple[str, int, int]]:
    out: list[tuple[str, int, int]] = []
    for pattern in patterns:
        out.extend(_file_signature(path) for path in sorted(directory.glob(pattern)))
    return out


def _model_artifact_signatures(model_dir: Path) -> list[tuple[str, int, int]]:
    if not model_dir.exists():
        return [_file_signature(model_dir)]
    # ML replay arrays depend on the LightGBM model files, meta feature lists,
    # and bundle metadata that controls feature variants.  Do not include nested
    # historical outputs here; the hot cache key should track only direct inputs.
    return _glob_signatures(model_dir, ("*.txt", "*_meta.json", "bundle_meta.json"))


def _feature_source_signatures(
    feature_dir: Path, context_days: list[str],
) -> list[tuple[str, int, int]]:
    roots = {feature_dir.resolve()}
    warmup = os.environ.get("MM_FEATURE_WARMUP_DIR", "").strip()
    if warmup:
        roots.add(Path(warmup).expanduser().resolve())
    signatures = []
    for root in sorted(roots):
        signatures.extend(_glob_signatures(
            root, tuple(f"features_{day}.parquet" for day in context_days),
        ))
        signatures.append(_file_signature(root / "causal_feature_manifest.json"))
    return signatures


def _quality_policy_signatures() -> list[tuple[str, int, int]]:
    signatures = [_file_signature(ROOT / "data_quality.py")]
    audit_override = os.environ.get("MM_CRYPTOHFT_BAD_DAYS_CSV")
    audit_path = (
        Path(audit_override).expanduser().resolve()
        if audit_override
        else ROOT / "logs" / "data_audit" / "cryptohft_bad_days_20250801_20260624.csv"
    )
    signatures.append(_file_signature(audit_path))
    signatures.append(_file_signature(bt.BBO_DIR.parent / "daily_quality.csv"))
    signatures.append(_file_signature(bt.BBO_DIR.parent / "manifest.json"))
    return signatures


def _window_source_signature(
    day: str,
    *,
    load_ml: bool,
    run_ml_inference: bool,
    feature_dir: Path,
    execution_trade_source: str,
    market_context_warmup_days: int,
) -> tuple[tuple[str, int, int], ...]:
    symbol = str(bt.SYMBOL).upper()
    signatures: list[tuple[str, int, int]] = []
    if execution_trade_source == "trades":
        signatures.extend(
            _glob_signatures(
                bt.RAW_TRADES_DIR / symbol,
                (
                    f"{symbol}-trades-{day}.csv",
                    f"{symbol}-trades-{day}.csv.gz",
                ),
            )
        )
    else:
        signatures.extend(
            _glob_signatures(
                bt.RAW_DIR,
                (
                    f"{symbol}-aggTrades-{day}.csv",
                    f"{symbol}-aggTrades-{day}.csv.gz",
                ),
            )
        )
    context_days = _causal_context_days(day, market_context_warmup_days)
    signatures.extend(
        _glob_signatures(
            bt.BARS_DIR,
            tuple(f"{symbol}-1s-{context_day}.parquet" for context_day in context_days),
        )
    )
    signatures.extend(
        _glob_signatures(
            bt.BBO_DIR,
            tuple(
                pattern
                for context_day in context_days
                for pattern in (
                    f"{symbol}-bbo-{context_day}.parquet",
                    f"{symbol}-bookTicker-{context_day}.parquet",
                )
            ),
        )
    )
    signatures.extend(
        _glob_signatures(
            bt.L2_DIR,
            tuple(
                pattern
                for context_day in context_days
                for pattern in (
                    f"{symbol}-l2-{context_day}.parquet",
                    f"{symbol}-depth20-{context_day}.parquet",
                )
            ),
        )
    )
    if load_ml:
        signatures.extend(_feature_source_signatures(feature_dir, context_days))
        if run_ml_inference:
            signatures.extend(_model_artifact_signatures(bt.MODEL_DIR))
    signatures.extend(_quality_policy_signatures())
    return tuple(sorted(signatures))


def _reference_role(path: str) -> str:
    normalized = str(path).lower()
    name = Path(path).name.lower()
    if "aggtrades" in name or "-trades-" in name:
        return "execution_trades"
    if "-bbo-" in name or "bookticker" in name:
        return "normalized_bbo"
    if "-l2-" in name or "depth20" in name:
        return "normalized_l2"
    if "-1s-" in name:
        return "one_second_bars"
    if "feature" in name:
        return "causal_features"
    if name.endswith(".txt") or "bundle_meta" in name or "_meta" in name:
        return "model_bundle"
    if "quality" in normalized or "manifest" in name:
        return "data_identity"
    return "source_contract"


def _signature_references(
    signatures: tuple[tuple[str, int, int], ...] | list[tuple[str, int, int]],
) -> tuple[dict[str, Any], ...]:
    references: list[dict[str, Any]] = []
    for path_text, size, mtime_ns in sorted(signatures):
        path = Path(path_text).expanduser().resolve()
        role = _reference_role(str(path))
        logical_source = f"{role}/{path.name}"
        if int(size) < 0:
            references.append(
                missing_file_reference(
                    path,
                    role=role,
                    logical_source=logical_source,
                )
            )
            continue
        manifest_identity = _trusted_source_sha256(path)
        if manifest_identity is None:
            if int(size) > 128 * 1024 * 1024:
                raise ValueError(
                    "source exceeds direct-hash fallback limit and has no trusted "
                    f"producer manifest SHA256: {path}"
                )
            sha256 = _cached_direct_source_sha256(str(path), int(size), int(mtime_ns))
            provenance: dict[str, Any] = {"kind": "direct_file_sha256"}
        else:
            sha256, provenance = manifest_identity
        references.append(
            file_reference(
                path,
                role=role,
                logical_source=logical_source,
                sha256=sha256,
                hash_provenance=provenance,
                direct_hash_max_bytes=128 * 1024 * 1024,
            )
        )
    return tuple(references)


@lru_cache(maxsize=512)
def _cached_direct_source_sha256(
    path_text: str,
    size_bytes: int,
    mtime_ns: int,
) -> str:
    del size_bytes, mtime_ns
    return file_sha256(Path(path_text))


@lru_cache(maxsize=128)
def _cached_json_manifest(
    path_text: str,
    size_bytes: int,
    mtime_ns: int,
) -> dict[str, Any]:
    del size_bytes, mtime_ns
    return json.loads(Path(path_text).read_text(encoding="utf-8"))


def _read_json_manifest(path: Path) -> dict[str, Any] | None:
    try:
        stat = path.stat()
        return _cached_json_manifest(str(path.resolve()), int(stat.st_size), int(stat.st_mtime_ns))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None


def _manifest_provenance(path: Path, *, field: str) -> dict[str, Any]:
    stat = path.stat()
    return {
        "kind": "producer_manifest_sha256",
        "manifest_path": str(path.resolve()),
        "manifest_sha256": _cached_direct_source_sha256(
            str(path.resolve()), int(stat.st_size), int(stat.st_mtime_ns)
        ),
        "field": field,
    }


def _trusted_source_sha256(
    path: Path,
) -> tuple[str, dict[str, Any]] | None:
    """Resolve producer-bound content hashes without hashing large payloads."""

    adjacent_meta = path.with_name(f"{path.name}.meta.json")
    meta = _read_json_manifest(adjacent_meta)
    if meta is not None and meta.get("output_sha256"):
        return str(meta["output_sha256"]), _manifest_provenance(
            adjacent_meta, field="output_sha256"
        )

    feature_manifest = path.parent / "causal_feature_manifest.json"
    feature_payload = _read_json_manifest(feature_manifest)
    if feature_payload is not None:
        for item in feature_payload.get("daily_files", []):
            if Path(str(item.get("file", ""))).name == path.name and item.get("sha256"):
                return str(item["sha256"]), _manifest_provenance(
                    feature_manifest, field="daily_files[].sha256"
                )

    book_manifest = bt.BBO_DIR.parent / "manifest.json"
    book_payload = _read_json_manifest(book_manifest)
    if book_payload is not None and path.parent in {bt.BBO_DIR, bt.L2_DIR}:
        relative = str(path.relative_to(book_manifest.parent))
        for item in book_payload.get("files", []):
            if item.get("destination_relative_path") != relative:
                continue
            source_identity = dict(item.get("source_identity", {}))
            if source_identity.get("sha256"):
                return str(source_identity["sha256"]), _manifest_provenance(
                    book_manifest, field="files[].source_identity.sha256"
                )

    if path.parent.parent == bt.RAW_TRADES_DIR:
        data_root = bt.RAW_TRADES_DIR.parent
        for manifest_path in sorted(data_root.glob("trade_features_*/manifest.json")):
            payload = _read_json_manifest(manifest_path)
            if payload is None:
                continue
            for item in payload.get("daily_files", []):
                if Path(str(item.get("raw_file", ""))).name != path.name:
                    continue
                if item.get("raw_sha256"):
                    return str(item["raw_sha256"]), _manifest_provenance(
                        manifest_path, field="daily_files[].raw_sha256"
                    )
    return None


def _callable_identity(callable_value: Any) -> dict[str, str]:
    try:
        source = inspect.getsource(callable_value)
    except (OSError, TypeError):
        source = repr(callable_value)
    return {
        "module": str(getattr(callable_value, "__module__", "")),
        "qualname": str(getattr(callable_value, "__qualname__", "")),
        "source_sha256": hashlib.sha256(source.encode("utf-8")).hexdigest(),
    }


def _market_context_transform_identity() -> str:
    return canonical_sha256(
        {
            "schema": "narrowgate.market_context_transform.v2",
            "aggtrade_dtypes": {
                name: np.dtype(dtype).name
                for name, dtype in sorted(bt.AGGTRADE_DTYPES.items())
            },
            "functions": [
                _callable_identity(callable_value)
                for callable_value in (
                    bt._read_aggtrade_csv,
                    bt._read_individual_trade_csv,
                    bt.load_execution_trades,
                    bt.load_1s_bars,
                    bt.build_rolling_variance,
                    bt.build_trade_intensity,
                    bt.build_squared_returns,
                    _variance_from_trades,
                )
            ],
        }
    )


@dataclass
class WindowData:
    trades: pd.DataFrame
    var_ts_ms: np.ndarray
    var_ssq: np.ndarray
    var_ti: np.ndarray | None
    var_retsq: np.ndarray | None
    bbo_data: Any
    l2_data: Any
    execution_trade_source: str = "aggTrades"
    ml_data: Any = None
    ml_cache: dict[bool, Any] = field(default_factory=dict)
    toxicity_horizon_s: int = 10
    reference_event_tapes: Any = None
    campaign_repair_data: Any = None
    campaign_repair_model: Any = None
    historical_global_flow_data: Any = None
    book_source_authority: str = "unclassified"
    book_dataset_version: str = ""
    formal_lifecycle_replay_eligible: bool = False
    provider_sensitivity_replay_eligible: bool = False
    exact_queue_policy_eligible: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "trades": self.trades,
            "var_ts_ms": self.var_ts_ms,
            "var_ssq": self.var_ssq,
            "var_ti": self.var_ti,
            "var_retsq": self.var_retsq,
            "bbo_data": self.bbo_data,
            "l2_data": self.l2_data,
            "execution_trade_source": self.execution_trade_source,
            "ml_data": self.ml_data,
            "ml_cache": self.ml_cache,
            "toxicity_horizon_s": self.toxicity_horizon_s,
            "reference_event_tapes": self.reference_event_tapes,
            "campaign_repair_data": self.campaign_repair_data,
            "campaign_repair_model": self.campaign_repair_model,
            "historical_global_flow_data": self.historical_global_flow_data,
            "book_source_authority": self.book_source_authority,
            "book_dataset_version": self.book_dataset_version,
            "formal_lifecycle_replay_eligible": self.formal_lifecycle_replay_eligible,
            "provider_sensitivity_replay_eligible": self.provider_sensitivity_replay_eligible,
            "exact_queue_policy_eligible": self.exact_queue_policy_eligible,
        }


@dataclass
class WindowMarketContext:
    """Strategy-independent, model-independent replay window component."""

    trades: pd.DataFrame
    var_ts_ms: np.ndarray
    var_ssq: np.ndarray
    var_ti: np.ndarray | None
    var_retsq: np.ndarray | None
    bbo_data: Any
    l2_data: Any
    execution_trade_source: str
    book_source_authority: str
    book_dataset_version: str
    formal_lifecycle_replay_eligible: bool
    provider_sensitivity_replay_eligible: bool
    exact_queue_policy_eligible: bool


@dataclass
class WindowModelOverlay:
    """Model-bound predictions layered over one market-context component."""

    ml_data: Any
    toxicity_horizon_s: int


def assemble_window_data(
    market_context: WindowMarketContext,
    *,
    ml_data: Any,
    toxicity_horizon_s: int,
    with_ml_cache: bool,
) -> WindowData:
    """Assemble the replay view in memory without publishing another cache."""

    return WindowData(
        trades=market_context.trades,
        var_ts_ms=market_context.var_ts_ms,
        var_ssq=market_context.var_ssq,
        var_ti=market_context.var_ti,
        var_retsq=market_context.var_retsq,
        bbo_data=market_context.bbo_data,
        l2_data=market_context.l2_data,
        execution_trade_source=market_context.execution_trade_source,
        ml_data=ml_data,
        ml_cache={} if with_ml_cache else {},
        toxicity_horizon_s=int(toxicity_horizon_s),
        book_source_authority=market_context.book_source_authority,
        book_dataset_version=market_context.book_dataset_version,
        formal_lifecycle_replay_eligible=(market_context.formal_lifecycle_replay_eligible),
        provider_sensitivity_replay_eligible=(market_context.provider_sensitivity_replay_eligible),
        exact_queue_policy_eligible=market_context.exact_queue_policy_eligible,
    )


def _variance_from_trades(trades: pd.DataFrame) -> tuple[np.ndarray, np.ndarray, None, None]:
    trades_sec = trades.copy()
    trades_sec["ts_sec"] = (trades_sec["transact_time"] // 1000) * 1000
    close_1s = trades_sec.groupby("ts_sec")["price"].last()
    diffs = close_1s.diff().fillna(0).values
    ssq = pd.Series(diffs).rolling(60, min_periods=10).var().ffill().bfill().values
    ssq = ssq.clip(min=1e-6)
    return close_1s.index.values.astype("int64"), ssq, None, None


def _resolve_cache_dir(cache_dir: str | Path | None) -> Path | None:
    raw = cache_dir if cache_dir is not None else os.environ.get("NARROWGATE_TICK_WINDOW_CACHE_DIR")
    if raw is None or str(raw).strip() == "":
        return None
    return Path(raw).expanduser().resolve()


def _normalize_execution_trade_source(value: Any) -> str:
    normalized = str(value or "aggTrades").strip().lower()
    if normalized in {"trade", "trades", "individual"}:
        return "trades"
    if normalized in {"aggtrade", "aggtrades"}:
        return "aggTrades"
    raise ValueError("execution_trade_source must be aggTrades or trades")


def load_replay_aggregate_parents(
    day: str, params: dict[str, Any],
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """Load packet identity after any value-cache hit, never reuse an old shape."""
    frames, identity = [], []
    symbol = str(params.get("symbol") or bt.SYMBOL).upper()
    for source_day in _causal_context_days(day, int(params.get("market_context_warmup_days", 1))):
        stem = f"{symbol}-aggTrades-{source_day}.csv"
        paths = [path for path in (bt.RAW_DIR / stem, bt.RAW_DIR / f"{stem}.gz") if path.is_file()]
        if len(paths) != 1:
            raise ValueError(f"message delivery requires one retained aggTrade source: {stem}")
        path = paths[0]
        frame = bt._read_aggtrade_csv(path, include_trade_ids=True)
        frames.append(frame)
        identity.append({"path": str(path.resolve()), "sha256": file_sha256(path),
                         "rows": len(frame)})
    return pd.concat(frames, ignore_index=True), identity


def execution_message_delivery_params(
    window: dict[str, Any], *, symbol: str, profile: dict[str, Any], seed: int,
    parent_trades: pd.DataFrame, parent_source_identity: list[dict[str, Any]],
    unmatched_child_mode: str = "error",
) -> dict[str, Any]:
    """Delay frozen execution inputs without changing exchange matching rows.

    Parent packets own trade visibility. Frozen bars/predictions are released
    behind explicit source-completion barriers; their values are not recomputed
    from delayed feeds. This is an execution-source diagnostic, not exact live
    feature reconstruction or a recovered shared-socket message chronology.
    """
    from models.exchange_book_replay import HistoricalMessageDeliverySchedule
    from research.system_engineering.audit.market_data_latency import MarketDataLatencySimulator

    trades = window["trades"]
    if "trade_id" not in trades:
        raise ValueError("message delivery requires retained individual trade IDs for parent mapping")
    for name in ("agg_trade_id", "first_trade_id", "last_trade_id", "transact_time"):
        if name not in parent_trades:
            raise ValueError(f"aggregate parent input is missing {name}")
    child_ids = trades["trade_id"].to_numpy(dtype=np.int64, copy=False)
    first = parent_trades["first_trade_id"].to_numpy(dtype=np.int64, copy=False)
    last = parent_trades["last_trade_id"].to_numpy(dtype=np.int64, copy=False)
    parent_ms = parent_trades["transact_time"].to_numpy(dtype=np.int64, copy=False)
    if (not len(first) or not len(child_ids) or np.any(first > last)
            or np.any(first[1:] <= last[:-1]) or np.any(child_ids[1:] <= child_ids[:-1])
            or np.any(parent_ms[1:] < parent_ms[:-1])):
        raise ValueError("aggregate parent/individual child identity must be ordered and nonoverlapping")
    owner = np.searchsorted(last, child_ids, side="left")
    visible_child_mask = (owner < len(first)) & (
        first[np.minimum(owner, len(first) - 1)] <= child_ids
    )
    if unmatched_child_mode not in {"error", "matching_only"}:
        raise ValueError("unknown unmatched child visibility mode")
    if not np.all(visible_child_mask) and unmatched_child_mode == "error":
        raise ValueError("aggregate parent ranges do not cover every individual execution")
    if not np.any(visible_child_mask):
        raise ValueError("no individual execution has a retained aggregate parent")
    # An aggregate's T can be its first child's timestamp. Its callback
    # cannot expose later children before those executions have happened.
    parent_complete_ms = parent_ms.copy()
    np.maximum.at(
        parent_complete_ms, owner[visible_child_mask],
        trades["transact_time"].to_numpy(dtype=np.int64, copy=False)[visible_child_mask],
    )

    simulator = MarketDataLatencySimulator(profile)
    delivery, source_stats = {}, {}
    market_id = f"binance:perp:{symbol.upper()}"
    for feed, event_type, source_ms in (
        ("bbo", "book", getattr(window.get("bbo_data"), "ts_ms", None)),
        ("depth", "depth", getattr(window.get("l2_data"), "ts_ms", None)),
        ("trade", "trade", parent_ms),
    ):
        if source_ms is None or not len(source_ms):
            raise ValueError(f"message delivery requires retained {feed} source rows")
        exchange = np.asarray(source_ms, dtype=np.int64) * 1_000_000
        receive, ready = simulator.message_clock_arrays(
            exchange, market_id=market_id, event_type=event_type,
            transport="websocket", seed=seed,
        )
        floor_added_ns = np.zeros(len(exchange), dtype=np.int64)
        if feed == "trade":
            floor_added_ns = np.maximum(parent_complete_ms * 1_000_000 - receive, 0)
            # Shift both clocks together to preserve observed service time.
            receive = receive + floor_added_ns
            ready = ready + floor_added_ns
        schedule = HistoricalMessageDeliverySchedule(
            exchange, receive, ready, serialize_callback_service=True,
        )
        delivery[feed] = {
            "exchange_ts_ns": schedule.exchange_ns_for_channel(),
            "receive_ts_ns": schedule.receive_ns_for_channel(),
            "feature_ready_ts_ns": schedule.ready_ns_for_channel(),
        }
        source_stats[feed] = schedule.stats_dict()
        if feed == "trade":
            source_stats[feed].update({
                "child_completion_floor_model": "receive_not_before_last_retained_child_execution",
                "child_completion_floor_count": int(np.count_nonzero(floor_added_ns)),
                "child_completion_floor_added_ms_total": float(floor_added_ns.sum()) / 1_000_000,
                "child_completion_floor_added_ms_max": float(floor_added_ns.max()) / 1_000_000,
            })
    own_last_child = np.searchsorted(child_ids, last, side="right") - 1
    valid_last = (own_last_child >= 0) & (
        child_ids[np.maximum(own_last_child, 0)] >= first
    ) & visible_child_mask[np.maximum(own_last_child, 0)]
    # Empty parents carry forward only an already-visible valid child. Never
    # turn searchsorted's preceding unrelated/gap row into a new parent child.
    delivery["trade"]["last_child_row_index"] = np.maximum.accumulate(
        np.where(valid_last, own_last_child, -1)
    )
    delivery["trade"]["visible_child_mask"] = visible_child_mask
    if "price" in parent_trades:
        delivery["trade"]["mark_price"] = parent_trades["price"].to_numpy(
            dtype=np.float64, copy=True,
        )

    unavailable_ns = np.iinfo(np.int64).max
    variance_ms = np.asarray(window["var_ts_ms"], dtype=np.int64)
    # Live commits a 1s aggregate when a later aggTrade advances its bucket.
    # The next packet only supplies the completion clock, never its values.
    closing_parent = np.searchsorted(parent_ms, variance_ms + 1_000, side="left")
    variance_ready = np.full(len(variance_ms), unavailable_ns, dtype=np.int64)
    has_closer = closing_parent < len(parent_ms)
    variance_ready[has_closer] = delivery["trade"]["feature_ready_ts_ns"][
        closing_parent[has_closer]
    ]

    def derived_clock(source_ms: np.ndarray, ready: np.ndarray) -> dict[str, np.ndarray]:
        exchange = source_ms * 1_000_000
        ready = np.maximum.accumulate(np.maximum(exchange, ready))
        return {"exchange_ts_ns": exchange, "receive_ts_ns": ready,
                "feature_ready_ts_ns": ready}

    delivery["variance"] = derived_clock(variance_ms, variance_ready)
    prediction_withheld_prefix_count = 0
    if window.get("ml_data") is not None:
        prediction_ms = np.asarray(window["ml_data"][0], dtype=np.int64)
        prediction_ns = prediction_ms * 1_000_000
        ready = prediction_ns.copy()
        for feed in ("bbo", "depth", "variance"):
            source = delivery[feed]
            index = np.searchsorted(source["exchange_ts_ns"], prediction_ns, side="left") - 1
            available = index >= 0
            dependency = np.full(len(index), unavailable_ns, dtype=np.int64)
            dependency[available] = source["feature_ready_ts_ns"][index[available]]
            ready = np.maximum(ready, dependency)
        complete = np.flatnonzero(ready < unavailable_ns)
        if not len(complete):
            raise ValueError("no frozen prediction has complete causal source context")
        prediction_withheld_prefix_count = int(complete[0])
        # Unavailable leading values must never be selected, but cannot poison
        # all later buckets through an infinite HOL barrier. Give the prefix
        # its first complete successor's release boundary: strictly-before
        # latest-index lookup then jumps directly to that successor.
        ready[:prediction_withheld_prefix_count] = ready[prediction_withheld_prefix_count]
        delivery["prediction"] = derived_clock(prediction_ms, ready)
    for clock in delivery.values():
        for values in clock.values():
            values.setflags(write=False)
    return {
        "exec_book_visibility_mode": "message_schedule", "_exec_message_delivery": delivery,
        "exec_message_delivery_input_semantics": {
            "profile_id": simulator.profile_id, "market_id": market_id,
            "transport": "websocket", "sampling": "same_message_pairs_once_per_source_row",
            "source_scheduler": "per_feed_callback_FIFO_not_recovered_shared_socket_order",
            "source_stats": source_stats, "aggregate_parent_sources": parent_source_identity,
            "aggregate_parent_count": len(parent_trades), "individual_child_count": len(trades),
            "trade_parent_mapping": "retained_first_last_trade_ids_matching_only_unmatched",
            "unmatched_child_mode": unmatched_child_mode,
            "unmatched_child_count": int(np.count_nonzero(~visible_child_mask)),
            "unmatched_child_ids": child_ids[~visible_child_mask].tolist(),
            "trade_mark_price_semantics": (
                "retained_aggregate_price" if "price" in parent_trades
                else "carry_forward_previous_valid_visible_child"
            ),
            "prediction_withheld_prefix_count": prediction_withheld_prefix_count,
            "variance_readiness": "next_aggTrade_callback_commits_completed_1s_bar",
            "prediction_readiness": (
                "conservative_frozen_bucket_release_after_prior_execution_BBO_depth_and_bar_inputs"
            ),
            "derived_clock_receive_semantics": "dependency_ready_barrier_not_measured_receive",
            "limitations": [
                "Frozen bar/feature/prediction values are delayed, not recomputed from late feeds.",
                "No complete feature dependency manifest; unmodeled external feeds are not covered.",
                "Per-message draws do not recover burst correlation or shared-socket feed order.",
                "Independent per-feed callback FIFO is a model, not captured target chronology.",
                "Trade child-completion floors are causal model adjustments, not observed delays.",
                "Incomplete leading predictions stay withheld until a complete successor supersedes them.",
                "No subsequent parent packet means the remaining frozen bar is unavailable.",
            ],
        },
    }


def _prediction_context_bounds(day: str, params: dict[str, Any]) -> dict[str, int]:
    if not params.get("runtime_compute_clock"):
        return {}
    midnight = int(pd.Timestamp(day, tz="UTC").value // 1_000_000)
    start = params.get("replay_event_clock_start_ts_ms", midnight)
    end = params.get("replay_event_clock_end_ts_ms", midnight + 86_400_000 - 1)
    if (
        type(start) is not int or type(end) is not int
        or not midnight <= start <= end < midnight + 86_400_000
    ):
        raise ValueError("runtime compute window bounds must lie within the target UTC day")
    return {"prediction_context_start_ms": start, "prediction_context_end_ms": end}


def _window_cache_path(
    cache_dir: Path,
    day: str,
    params: dict[str, Any],
    *,
    load_ml: bool,
    require_ml: bool,
    run_ml_inference: bool,
    feature_dir: Path,
    require_target_feature_files: bool,
    cross_market_enabled: bool,
    with_ml_cache: bool,
    require_historical_bbo: bool,
) -> Path:
    execution_trade_source = _normalize_execution_trade_source(
        params.get("execution_trade_source", "aggTrades")
    )
    market_context_warmup_days = max(
        0,
        int(params.get("market_context_warmup_days", 1) or 0),
    )
    payload = {
        "version": WINDOW_CACHE_VERSION,
        "transform_identity_sha256": _market_context_transform_identity(),
        "symbol": str(bt.SYMBOL).upper(),
        "day": day,
        "load_ml": bool(load_ml),
        "require_ml": bool(require_ml),
        "run_ml_inference": bool(run_ml_inference),
        "require_target_feature_files": bool(require_target_feature_files),
        "cross_market_enabled": bool(cross_market_enabled),
        "with_ml_cache": bool(with_ml_cache),
        "require_historical_bbo": bool(require_historical_bbo),
        "toxicity_horizon_s": int(params.get("toxicity_horizon_s", 10)),
        **_prediction_context_bounds(day, params),
        "model_dir": (str(bt.MODEL_DIR.resolve()) if load_ml and run_ml_inference else ""),
        "features_dir": str(feature_dir) if load_ml else "",
        "execution_trade_source": execution_trade_source,
        "market_context_warmup_days": market_context_warmup_days,
        "formal_quality_allowed_days": _formal_quality_allowed_days(params),
        "formal_quality_day_manifest_sha256": str(
            params.get("_formal_quality_day_manifest_sha256", "") or ""
        ),
        "book_source_authority": str(params.get("_book_source_authority", "unclassified")),
        "book_dataset_version": str(params.get("_book_dataset_version", "")),
        "book_manifest_sha256": str(params.get("_book_manifest_sha256", "")),
        "source_signature": _window_source_signature(
            day,
            load_ml=load_ml,
            run_ml_inference=run_ml_inference,
            feature_dir=feature_dir,
            execution_trade_source=execution_trade_source,
            market_context_warmup_days=market_context_warmup_days,
        ),
    }
    digest = hashlib.sha256(repr(sorted(payload.items())).encode("utf-8")).hexdigest()[:16]
    safe_symbol = str(bt.SYMBOL).lower()
    return cache_dir / f"{safe_symbol}_{day}_tick_window_v{WINDOW_CACHE_VERSION}_{digest}.pkl"


def _window_market_context_cache_path(
    cache_dir: Path,
    day: str,
    params: dict[str, Any],
) -> Path:
    execution_trade_source = _normalize_execution_trade_source(
        params.get("execution_trade_source", "aggTrades")
    )
    warmup_days = max(
        0,
        int(params.get("market_context_warmup_days", 1) or 0),
    )
    payload = {
        "schema_version": "narrowgate.window_market_context.v1",
        "component_cache_version": WINDOW_COMPONENT_CACHE_VERSION,
        "transform_identity_sha256": _market_context_transform_identity(),
        "symbol": str(bt.SYMBOL).upper(),
        "day": str(day),
        "execution_trade_source": execution_trade_source,
        "market_context_warmup_days": warmup_days,
        "book_source_authority": str(params.get("_book_source_authority", "unclassified")),
        "book_dataset_version": str(params.get("_book_dataset_version", "")),
        "source_signature": _window_source_signature(
            day,
            load_ml=False,
            run_ml_inference=False,
            feature_dir=Path(bt.FEATURES_DIR),
            execution_trade_source=execution_trade_source,
            market_context_warmup_days=warmup_days,
        ),
    }
    digest = hashlib.sha256(repr(sorted(payload.items())).encode("utf-8")).hexdigest()[:20]
    safe_symbol = str(bt.SYMBOL).lower()
    return (
        cache_dir
        / "components_v1"
        / "market_context"
        / f"{safe_symbol}_{day}_market_context_{digest}.pkl"
    )


def _window_model_overlay_cache_path(
    cache_dir: Path,
    day: str,
    params: dict[str, Any],
    *,
    feature_dir: Path,
    run_ml_inference: bool,
    cross_market_enabled: bool,
    market_context_path: Path,
) -> Path:
    execution_trade_source = _normalize_execution_trade_source(
        params.get("execution_trade_source", "aggTrades")
    )
    warmup_days = max(
        0,
        int(params.get("market_context_warmup_days", 1) or 0),
    )
    payload = {
        "schema_version": "narrowgate.window_model_overlay.v1",
        "component_cache_version": WINDOW_COMPONENT_CACHE_VERSION,
        "symbol": str(bt.SYMBOL).upper(),
        "day": str(day),
        "market_context_identity": market_context_path.name,
        **_prediction_context_bounds(day, params),
        "toxicity_horizon_s": int(params.get("toxicity_horizon_s", 10)),
        "cross_market_enabled": bool(cross_market_enabled),
        "run_ml_inference": bool(run_ml_inference),
        "model_dir": str(bt.MODEL_DIR.resolve()) if run_ml_inference else "",
        "features_dir": str(feature_dir),
        "source_signature": _window_source_signature(
            day,
            load_ml=True,
            run_ml_inference=run_ml_inference,
            feature_dir=feature_dir,
            execution_trade_source=execution_trade_source,
            market_context_warmup_days=warmup_days,
        ),
    }
    digest = hashlib.sha256(repr(sorted(payload.items())).encode("utf-8")).hexdigest()[:20]
    safe_symbol = str(bt.SYMBOL).lower()
    return (
        cache_dir
        / "components_v1"
        / "model_overlay"
        / f"{safe_symbol}_{day}_model_overlay_{digest}.pkl"
    )


def _window_market_context_v2_identity(
    day: str,
    params: dict[str, Any],
) -> tuple[dict[str, Any], tuple[dict[str, Any], ...]]:
    execution_trade_source = _normalize_execution_trade_source(
        params.get("execution_trade_source", "aggTrades")
    )
    warmup_days = max(
        0,
        int(params.get("market_context_warmup_days", 1) or 0),
    )
    signatures = _window_source_signature(
        day,
        load_ml=False,
        run_ml_inference=False,
        feature_dir=Path(bt.FEATURES_DIR),
        execution_trade_source=execution_trade_source,
        market_context_warmup_days=warmup_days,
    )
    references = _signature_references(signatures)
    identity = market_context_identity(
        symbol=str(bt.SYMBOL),
        day=day,
        warmup_days=warmup_days,
        source_references=references,
        book_source_authority=str(params.get("_book_source_authority", "unclassified")),
        book_dataset_version=str(params.get("_book_dataset_version", "")),
        transform_identity_sha256=_market_context_transform_identity(),
    )
    return identity, references


def _window_model_overlay_v2_identity(
    day: str,
    params: dict[str, Any],
    *,
    feature_dir: Path,
    run_ml_inference: bool,
    cross_market_enabled: bool,
    market_context_identity_sha256: str,
) -> dict[str, Any]:
    warmup_days = max(
        0,
        int(params.get("market_context_warmup_days", 1) or 0),
    )
    context_days = _causal_context_days(day, warmup_days)
    feature_signatures = _feature_source_signatures(feature_dir, context_days)
    model_signatures = _model_artifact_signatures(bt.MODEL_DIR) if run_ml_inference else []
    identity = model_overlay_identity(
        symbol=str(bt.SYMBOL),
        day=day,
        market_context_identity_sha256=market_context_identity_sha256,
        feature_source_identity=_signature_references(feature_signatures),
        model_bundle_identity=_signature_references(model_signatures),
        toxicity_horizon_s=int(params.get("toxicity_horizon_s", 10)),
        cross_market_enabled=cross_market_enabled,
        run_ml_inference=run_ml_inference,
    )
    identity.update(_prediction_context_bounds(day, params))
    return identity


def _load_cached_window(path: Path) -> WindowData | None:
    try:
        with path.open("rb") as fh:
            cached = pickle.load(fh)
        if isinstance(cached, WindowData):
            _record_cache_hit(path)
            print(f"  Tick window cache hit: {path}")
            return cached
        print(f"  [WARN] Ignoring incompatible tick window cache: {path}")
    except Exception as exc:
        print(f"  [WARN] Failed to read tick window cache {path}: {exc}")
    return None


def _write_cached_window(path: Path, window: WindowData) -> None:
    tmp: Path | None = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        # 多 worker 可能同时构建同一个 day/model 的窗口。临时文件名必须
        # 每个进程唯一，否则一个 worker replace 后另一个 worker 会找不到
        # 共享的 `.tmp`，产生误导性的 cache write warning。
        tmp = path.with_name(f".{path.name}.{os.getpid()}.{id(window)}.tmp")
        with tmp.open("wb") as fh:
            pickle.dump(window, fh, protocol=pickle.HIGHEST_PROTOCOL)
        tmp.replace(path)
        _register_cache_artifact(path)
        print(f"  Tick window cache saved: {path}")
    except Exception as exc:
        print(f"  [WARN] Failed to write tick window cache {path}: {exc}")
        if tmp is not None:
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass


def _load_component(path: Path, expected_type: type[Any]) -> Any | None:
    try:
        with path.open("rb") as handle:
            cached = pickle.load(handle)
        if isinstance(cached, expected_type):
            _record_cache_hit(path)
            print(f"  Replay component cache hit: {path}")
            return cached
        print(f"  [WARN] Ignoring incompatible replay component cache: {path}")
    except Exception as exc:
        print(f"  [WARN] Failed to read replay component cache {path}: {exc}")
    return None


def _write_component(path: Path, component: Any) -> None:
    temporary: Path | None = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{os.getpid()}.{id(component)}.tmp")
        with temporary.open("wb") as handle:
            pickle.dump(component, handle, protocol=pickle.HIGHEST_PROTOCOL)
        temporary.replace(path)
        _register_cache_artifact(path)
        print(f"  Replay component cache saved: {path}")
    except Exception as exc:
        print(f"  [WARN] Failed to write replay component cache {path}: {exc}")
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except Exception:
                pass


def load_tick_window(
    day: str,
    params: dict[str, Any],
    *,
    load_ml: bool = True,
    require_ml: bool = True,
    run_ml_inference: bool | None = None,
    feature_dir: str | Path | None = None,
    require_target_feature_files: bool = False,
    cross_market_enabled: bool = True,
    with_ml_cache: bool = False,
    require_historical_bbo: bool = True,
    require_formal_l2: bool | None = None,
    verify_formal_l2_hashes: bool | None = None,
    cache_dir: str | Path | None = None,
    refresh_cache: bool = False,
) -> WindowData:
    if len(str(day)) != 10 or "/" in str(day):
        raise SystemExit(f"Tick window loader expects one UTC day YYYY-MM-DD, got: {day}")
    formal_l2_required = (
        bool(params.get("require_formal_l2", False))
        if require_formal_l2 is None
        else bool(require_formal_l2)
    )
    formal_l2_verify_hashes = (
        bool(params.get("verify_formal_l2_hashes", False))
        if verify_formal_l2_hashes is None
        else bool(verify_formal_l2_hashes)
    )
    market_context_days = _causal_context_days(
        day,
        int(params.get("market_context_warmup_days", 1) or 0),
    )
    if formal_l2_required:
        dataset_root = bt.BBO_DIR.parent.resolve()
        expected_bbo = dataset_root / "bbo"
        expected_l2 = dataset_root / "l2"
        if bt.BBO_DIR.resolve() != expected_bbo or bt.L2_DIR.resolve() != expected_l2:
            raise SystemExit(
                "formal L2 replay requires sibling bbo/l2 directories under "
                f"{l2_registry.DATASET_VERSION}: "
                f"{bt.BBO_DIR} | {bt.L2_DIR}"
            )
        try:
            l2_registry.require_formal_days(
                dataset_root,
                market_context_days,
                verify_hashes=formal_l2_verify_hashes,
            )
        except l2_registry.FormalEligibilityError as exc:
            raise SystemExit(f"{day}: formal L2/context gate failed: {exc}") from exc
    if require_historical_bbo or formal_l2_required:
        book_contract = _enforce_book_source_contract(day, params)
    else:
        book_contract = {
            "source_authority": "unclassified",
            "dataset_version": "",
            "formal_lifecycle_replay_eligible": False,
            "provider_sensitivity_replay_eligible": False,
            "exact_queue_policy_eligible": False,
        }
    cache_identity_params = {
        **params,
        "_book_source_authority": str(book_contract["source_authority"]),
        "_book_dataset_version": str(book_contract["dataset_version"]),
    }
    # 中文说明：窗口 loader 是 replay/A-B 共享入口。不要依赖调用方已经
    # configure_symbol 到正确模型目录；并行 worker 或单独脚本很容易先把全局
    # MODEL_DIR 重置成 symbol 默认目录。params 来自 live/config 的 resolved_model_dir
    # 时，这里强制恢复同一 bundle，保证 cache key 和 ML predictions 都同源。
    model_dir_override = (
        params.get("resolved_model_dir")
        or params.get("model_dir_override")
        or params.get("model_dir")
    )
    resolved_model_dir = _resolve_project_path(model_dir_override)
    if resolved_model_dir is not None:
        bt.configure_symbol(
            params.get("symbol") or bt.SYMBOL, model_dir_override=resolved_model_dir
        )
    resolved_cache_dir = _resolve_cache_dir(cache_dir)
    resolved_run_ml_inference = (
        bool(params.get("ml_enabled", True)) if run_ml_inference is None else bool(run_ml_inference)
    )
    if feature_dir is not None:
        resolved_feature_dir = Path(feature_dir).expanduser().resolve()
    elif str(os.environ.get("MM_FEATURE_DIR", "")).strip():
        resolved_feature_dir = Path(os.environ["MM_FEATURE_DIR"]).expanduser().resolve()
    elif load_ml and resolved_run_ml_inference:
        # Use the model's recorded panel as a default locator, not as an
        # equality constraint on new-date inference.  load_ml_predictions
        # validates the actual panel's feature and causal-clock interface.
        resolved_feature_dir = bt.resolve_ml_feature_dir()
    else:
        resolved_feature_dir = Path(bt.FEATURES_DIR).expanduser().resolve()
    formal_quality_allowed_days = _formal_quality_allowed_days(params)
    cache_path = None
    market_context_cache_path = None
    market_context_v2_identity_payload: dict[str, Any] | None = None
    market_context_v2_references: tuple[dict[str, Any], ...] = ()
    market_context: WindowMarketContext | None = None
    if resolved_cache_dir is not None:
        cache_path = _window_cache_path(
            resolved_cache_dir,
            day,
            params,
            load_ml=load_ml,
            require_ml=require_ml,
            run_ml_inference=resolved_run_ml_inference,
            feature_dir=resolved_feature_dir,
            require_target_feature_files=require_target_feature_files,
            cross_market_enabled=cross_market_enabled,
            with_ml_cache=with_ml_cache,
            require_historical_bbo=require_historical_bbo,
        )
        if cache_path.exists() and not refresh_cache:
            cached = _load_cached_window(cache_path)
            if cached is not None:
                if load_ml and resolved_run_ml_inference and cached.ml_data is not None:
                    bt._load_ml_inference_metadata(
                        resolved_feature_dir,
                        toxicity_horizon_s=int(params.get("toxicity_horizon_s", 10)),
                    )
                return cached

        market_context_v2_identity_payload, market_context_v2_references = (
            _window_market_context_v2_identity(day, cache_identity_params)
        )
        cached_v2 = (
            None
            if refresh_cache
            else load_market_context(
                cache_root=resolved_cache_dir,
                identity=market_context_v2_identity_payload,
                source_references=market_context_v2_references,
            )
        )
        if cached_v2 is not None:
            if require_historical_bbo or formal_l2_required:
                bbo_data = bt.load_bbo_data(
                    days=market_context_days,
                    quality_allowed_days=formal_quality_allowed_days,
                )
                l2_data = bt.load_l2_data(
                    days=market_context_days,
                    quality_allowed_days=formal_quality_allowed_days,
                )
            else:
                bbo_data = None
                l2_data = None
            metadata = cached_v2.metadata
            market_context = WindowMarketContext(
                trades=cached_v2.trades,
                var_ts_ms=cached_v2.var_ts_ms,
                var_ssq=cached_v2.var_ssq,
                var_ti=cached_v2.var_ti,
                var_retsq=cached_v2.var_retsq,
                bbo_data=bbo_data,
                l2_data=l2_data,
                execution_trade_source=str(metadata["execution_trade_source"]),
                book_source_authority=str(metadata["book_source_authority"]),
                book_dataset_version=str(metadata["book_dataset_version"]),
                formal_lifecycle_replay_eligible=bool(metadata["formal_lifecycle_replay_eligible"]),
                provider_sensitivity_replay_eligible=bool(
                    metadata["provider_sensitivity_replay_eligible"]
                ),
                exact_queue_policy_eligible=bool(metadata["exact_queue_policy_eligible"]),
            )
            print("  Replay component cache hit: market_context_day_v2")

        market_context_cache_path = _window_market_context_cache_path(
            resolved_cache_dir,
            day,
            params,
        )
        if market_context is None and market_context_cache_path.exists() and not refresh_cache:
            market_context = _load_component(
                market_context_cache_path,
                WindowMarketContext,
            )

    # 这个 helper 是所有 tick replay/A-B 的日度策略状态边界。Inventory、
    # guard、queue 和 markout EMA 仍由 simulator 在目标日 fresh/live start；
    # rolling market context 必须因果跨过 UTC 午夜，否则首分钟并不等价于 live。
    print(f"\nLoading tick window {day} ...")
    if market_context is None:
        days = [day]
        execution_trade_source = _normalize_execution_trade_source(
            params.get("execution_trade_source", "aggTrades")
        )
        trades = bt.load_execution_trades(
            days=days,
            source=execution_trade_source,
            quality_allowed_days=formal_quality_allowed_days,
        )
        formal_replay = bool(params.get("strict_calibration", False)) or (
            str(params.get("replay_purpose", "")).strip().lower() == "formal"
        )
        bars = bt.load_1s_bars(
            days=market_context_days,
            quality_allowed_days=formal_quality_allowed_days,
            require_dense_source=formal_replay,
        )
        bars = bt.require_formal_dense_1s_timeline(bars, params)
        if bars is None:
            var_ts_ms, var_ssq, var_ti, var_retsq = _variance_from_trades(trades)
        else:
            var_ts_ms, var_ssq = bt.build_rolling_variance(bars)
            _, var_ti = bt.build_trade_intensity(bars)
            _, var_retsq = bt.build_squared_returns(bars)

        # Receive-time replay may need a book snapshot from before midnight.
        # This component remains action/model independent and is shared by all
        # downstream arms that bind the same source and cutoff identity.
        bbo_data = bt.load_bbo_data(
            days=market_context_days,
            quality_allowed_days=formal_quality_allowed_days,
        )
        l2_data = bt.load_l2_data(
            days=market_context_days,
            quality_allowed_days=formal_quality_allowed_days,
        )
        if require_historical_bbo and bbo_data is None and l2_data is None:
            raise SystemExit(f"{day}: historical BBO/L2 required but not found")
        market_context = WindowMarketContext(
            trades=trades,
            var_ts_ms=var_ts_ms,
            var_ssq=var_ssq,
            var_ti=var_ti,
            var_retsq=var_retsq,
            bbo_data=bbo_data,
            l2_data=l2_data,
            execution_trade_source=execution_trade_source,
            book_source_authority=str(book_contract["source_authority"]),
            book_dataset_version=str(book_contract["dataset_version"]),
            formal_lifecycle_replay_eligible=bool(
                book_contract["formal_lifecycle_replay_eligible"]
            ),
            provider_sensitivity_replay_eligible=bool(
                book_contract["provider_sensitivity_replay_eligible"]
            ),
            exact_queue_policy_eligible=bool(book_contract["exact_queue_policy_eligible"]),
        )
        if resolved_cache_dir is not None and bool(params.get("window_cache_write_enabled", True)):
            if market_context_v2_identity_payload is None:
                (
                    market_context_v2_identity_payload,
                    market_context_v2_references,
                ) = _window_market_context_v2_identity(
                    day,
                    cache_identity_params,
                )
            write_market_context(
                cache_root=resolved_cache_dir,
                identity=market_context_v2_identity_payload,
                payload=MarketContextPayload(
                    trades=market_context.trades,
                    var_ts_ms=market_context.var_ts_ms,
                    var_ssq=market_context.var_ssq,
                    var_ti=market_context.var_ti,
                    var_retsq=market_context.var_retsq,
                    metadata={
                        "execution_trade_source": (market_context.execution_trade_source),
                        "book_source_authority": (market_context.book_source_authority),
                        "book_dataset_version": market_context.book_dataset_version,
                        "formal_lifecycle_replay_eligible": (
                            market_context.formal_lifecycle_replay_eligible
                        ),
                        "provider_sensitivity_replay_eligible": (
                            market_context.provider_sensitivity_replay_eligible
                        ),
                        "exact_queue_policy_eligible": (market_context.exact_queue_policy_eligible),
                    },
                    source_references=market_context_v2_references,
                ),
            )
            if market_context_cache_path is not None and bool(
                params.get("legacy_component_v1_write_enabled", False)
            ):
                _write_component(market_context_cache_path, market_context)

    trades = market_context.trades
    var_ts_ms = market_context.var_ts_ms
    var_ssq = market_context.var_ssq
    var_ti = market_context.var_ti
    var_retsq = market_context.var_retsq
    bbo_data = market_context.bbo_data
    l2_data = market_context.l2_data
    execution_trade_source = market_context.execution_trade_source
    if require_historical_bbo and bbo_data is None and l2_data is None:
        raise SystemExit(f"{day}: historical BBO/L2 required but not found")

    toxicity_horizon_s = int(params.get("toxicity_horizon_s", 10))
    ml_data = None
    if load_ml:
        model_overlay_cache_path = None
        model_overlay: WindowModelOverlay | None = None
        model_overlay_v2_identity_payload: dict[str, Any] | None = None
        if resolved_cache_dir is not None and market_context_v2_identity_payload is not None:
            model_overlay_v2_identity_payload = _window_model_overlay_v2_identity(
                day,
                cache_identity_params,
                feature_dir=resolved_feature_dir,
                run_ml_inference=resolved_run_ml_inference,
                cross_market_enabled=cross_market_enabled,
                market_context_identity_sha256=canonical_sha256(market_context_v2_identity_payload),
            )
            cached_overlay = (
                None
                if refresh_cache
                else load_model_overlay(
                    cache_root=resolved_cache_dir,
                    identity=model_overlay_v2_identity_payload,
                )
            )
            if cached_overlay is not None:
                model_overlay = WindowModelOverlay(
                    ml_data=cached_overlay,
                    toxicity_horizon_s=toxicity_horizon_s,
                )
                print("  Replay component cache hit: model_overlay_day")

        if (
            model_overlay is None
            and resolved_cache_dir is not None
            and market_context_cache_path is not None
        ):
            model_overlay_cache_path = _window_model_overlay_cache_path(
                resolved_cache_dir,
                day,
                params,
                feature_dir=resolved_feature_dir,
                run_ml_inference=resolved_run_ml_inference,
                cross_market_enabled=cross_market_enabled,
                market_context_path=market_context_cache_path,
            )
            if model_overlay_cache_path.exists() and not refresh_cache:
                model_overlay = _load_component(
                    model_overlay_cache_path,
                    WindowModelOverlay,
                )
        if model_overlay is not None:
            if model_overlay.toxicity_horizon_s != toxicity_horizon_s:
                raise RuntimeError("model overlay toxicity horizon mismatch")
            if resolved_run_ml_inference and model_overlay.ml_data is not None:
                bt._load_ml_inference_metadata(
                    resolved_feature_dir, toxicity_horizon_s=toxicity_horizon_s,
                )
            ml_data = model_overlay.ml_data
        else:
            ml_data = bt.load_ml_predictions(
                trades,
                toxicity_horizon_s=toxicity_horizon_s,
                cross_market_enabled=cross_market_enabled,
                allow_missing_features=bool(params.get("allow_missing_ml_features", False)),
                run_model_inference=resolved_run_ml_inference,
                feature_dir=resolved_feature_dir,
                require_target_feature_files=bool(require_target_feature_files),
                **_prediction_context_bounds(day, params),
            )
            if ml_data is not None and bool(params.get("window_cache_write_enabled", True)):
                if resolved_cache_dir is not None and model_overlay_v2_identity_payload is not None:
                    write_model_overlay(
                        cache_root=resolved_cache_dir,
                        identity=model_overlay_v2_identity_payload,
                        ml_data=ml_data,
                    )
                if model_overlay_cache_path is not None and bool(
                    params.get("legacy_component_v1_write_enabled", False)
                ):
                    _write_component(
                        model_overlay_cache_path,
                        WindowModelOverlay(
                            ml_data=ml_data,
                            toxicity_horizon_s=toxicity_horizon_s,
                        ),
                    )
        if ml_data is None:
            if require_ml:
                raise SystemExit(f"{day}: ML predictions required for tick replay")
            print("  [WARN] ML unavailable; running without ML data")

    window = assemble_window_data(
        market_context,
        ml_data=ml_data,
        toxicity_horizon_s=toxicity_horizon_s,
        with_ml_cache=with_ml_cache,
    )
    if (
        cache_path is not None
        and bool(params.get("legacy_monolithic_window_cache_write_enabled", False))
        and bool(params.get("window_cache_write_enabled", True))
    ):
        _write_cached_window(cache_path, window)
    return window


def load_tick_window_dict(day: str, params: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
    return load_tick_window(day, params, **kwargs).to_dict()


def parse_bound(value: str | None, *, is_end: bool) -> int | None:
    if not value:
        return None
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = pd.Timestamp(value, tz="UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    if is_end and len(value) == 10:
        timestamp = timestamp + pd.Timedelta(days=1)
    return int(timestamp.value // 1_000_000)


def slice_tuple_by_first_array(data, start_ms: int | None, end_ms: int | None):
    if data is None:
        return None
    ts = np.asarray(data[0])
    mask = np.ones(len(ts), dtype=bool)
    if start_ms is not None:
        mask &= ts >= start_ms
    if end_ms is not None:
        mask &= ts < end_ms
    if not np.any(mask):
        return data
    return tuple(
        np.asarray(item)[mask] if hasattr(item, "__len__") and len(item) == len(ts) else item
        for item in data
    )


def slice_history_object(data, start_ms: int | None, end_ms: int | None):
    if data is None:
        return None
    ts = np.asarray(data.ts_ms)
    mask = np.ones(len(ts), dtype=bool)
    if start_ms is not None:
        mask &= ts >= start_ms
    if end_ms is not None:
        mask &= ts < end_ms
    if not np.any(mask):
        return data
    if isinstance(data, HistoricalBBOData):
        return HistoricalBBOData(
            ts[mask],
            data.best_bid[mask],
            data.best_ask[mask],
            data.bid_qty[mask],
            data.ask_qty[mask],
            data.source,
        )
    if isinstance(data, HistoricalL2Data):
        return HistoricalL2Data(
            ts[mask],
            data.bid_px[mask],
            data.bid_qty[mask],
            data.ask_px[mask],
            data.ask_qty[mask],
            data.source,
        )
    return data


def slice_window(
    window: dict[str, Any], start_ms: int | None, end_ms: int | None
) -> dict[str, Any]:
    if start_ms is None and end_ms is None:
        return window
    out = dict(window)
    trades = out["trades"]
    mask = pd.Series(True, index=trades.index)
    if start_ms is not None:
        mask &= trades["transact_time"] >= start_ms
    if end_ms is not None:
        mask &= trades["transact_time"] < end_ms
    sliced_trades = trades.loc[mask].copy()
    if sliced_trades.empty:
        raise ValueError("Date slice has no trades")
    out["trades"] = sliced_trades

    var_ts = np.asarray(out["var_ts_ms"])
    var_mask = np.ones(len(var_ts), dtype=bool)
    if start_ms is not None:
        var_mask &= var_ts >= start_ms
    if end_ms is not None:
        var_mask &= var_ts < end_ms
    if np.any(var_mask):
        out["var_ts_ms"] = var_ts[var_mask]
        out["var_ssq"] = np.asarray(out["var_ssq"])[var_mask]
        if out.get("var_ti") is not None and len(out["var_ti"]) == len(var_ts):
            out["var_ti"] = np.asarray(out["var_ti"])[var_mask]
        if out.get("var_retsq") is not None and len(out["var_retsq"]) == len(var_ts):
            out["var_retsq"] = np.asarray(out["var_retsq"])[var_mask]

    # trade/variance/ML/BBO/L2 必须用同一 UTC 毫秒边界切片；
    # 否则 quote context 和成交路径会出现看似微小但会放大的错位。
    out["bbo_data"] = slice_history_object(out.get("bbo_data"), start_ms, end_ms)
    out["l2_data"] = slice_history_object(out.get("l2_data"), start_ms, end_ms)
    out["ml_data"] = slice_tuple_by_first_array(out.get("ml_data"), start_ms, end_ms)
    print(
        f"  Date slice: {len(out['trades']):,} trades "
        f"({pd.Timestamp(int(out['trades']['transact_time'].iloc[0]), unit='ms', tz='UTC')} -> "
        f"{pd.Timestamp(int(out['trades']['transact_time'].iloc[-1]), unit='ms', tz='UTC')})"
    )
    return out
