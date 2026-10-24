"""Historical sensitivity adapter for the causal cross-venue fair price.

The authoritative live node consumes AWS receive-time BBO events. Historical
archives expose right-edge one-second trade bars instead. This adapter keeps
that distinction explicit: it reuses the same past-only estimator, but every
output is marked transport-unsupported and may only support historical
sensitivity or Development replay.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import math
import os
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from data_paths import cache_root, data_root, normalized_l2_root
from strategy.cross_venue_fair_price import (
    FAIR_PRICE_SCHEMA_VERSION,
    CrossVenueFairPriceConfig,
    CrossVenueFairPriceEstimator,
    CrossVenueFairPriceState,
    FairPriceSource,
)

ROOT = Path(__file__).resolve().parents[4]
HISTORICAL_SCHEMA_VERSION = "cross_venue_causal_fair_price_trade_1s.v1"
HISTORICAL_SOURCE_KIND = "historical_trade_bar_1s"
HISTORICAL_CACHE_NODE = "cross_venue_fair_price_trade_1s_v1"
VENUES = ("bitget", "bybit", "okx")
MARKETS = ("spot", "perp")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(payload: Mapping[str, Any]) -> str:
    raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _day_before(day: str) -> str:
    return (date.fromisoformat(str(day)) - timedelta(days=1)).isoformat()


def _day_bounds_ns(day: str) -> tuple[int, int]:
    start = int(pd.Timestamp(day, tz="UTC").value)
    return start, start + 86_400 * 1_000_000_000


def _external_root(project_data_root: Path | None = None) -> Path:
    base = data_root(ROOT) if project_data_root is None else Path(project_data_root)
    return base / "external_venues"


def source_paths(
    day: str,
    *,
    project_data_root: Path | None = None,
    include_warmup: bool = True,
) -> dict[str, Path]:
    """Return every raw input consumed by one target-day cache."""

    external = _external_root(project_data_root)
    l2_root = (
        normalized_l2_root(ROOT)
        if project_data_root is None
        else Path(project_data_root) / "normalized_l2_100ms_v2"
    )
    days = (_day_before(day), str(day)) if include_warmup else (str(day),)
    paths: dict[str, Path] = {}
    for context_day in days:
        for venue in VENUES:
            for market in MARKETS:
                paths[f"{context_day}:{venue}:{market}"] = (
                    external
                    / venue
                    / market
                    / "BTCUSDT"
                    / "features_1s"
                    / f"BTCUSDT-{venue}-trades-1s-{context_day}.parquet"
                )
        paths[f"{context_day}:anchor"] = (
            external
            / "binance"
            / "spot"
            / "USDCUSDT"
            / "bars_1s"
            / f"USDCUSDT-1s-{context_day}.parquet"
        )
        paths[f"{context_day}:execution_bbo"] = (
            l2_root / "bbo" / f"BTCUSDC-bbo-{context_day}.parquet"
        )
    return paths


def source_manifest(
    day: str,
    *,
    project_data_root: Path | None = None,
) -> list[dict[str, Any]]:
    manifest: list[dict[str, Any]] = []
    for label, path in sorted(
        source_paths(day, project_data_root=project_data_root).items()
    ):
        if not path.is_file():
            raise FileNotFoundError(f"missing fair-price source {label}: {path}")
        stat = path.stat()
        manifest.append(
            {
                "label": label,
                "path": str(path.resolve()),
                "size_bytes": int(stat.st_size),
                "sha256": _sha256_file(path),
            }
        )
    return manifest


@dataclass(frozen=True)
class HistoricalFairPriceData:
    feature_ready_ts_ns: np.ndarray
    fair_price: np.ndarray
    gain: np.ndarray
    confidence: np.ndarray
    dispersion_bps: np.ndarray
    valid_venues: np.ndarray
    minimum_basis_samples: np.ndarray
    lead_variance_bps2: np.ndarray
    noise_variance_bps2: np.ndarray
    max_source_age_ms: np.ndarray
    valid: np.ndarray
    reason: np.ndarray
    omitted_venue: str = ""
    source: str = HISTORICAL_SCHEMA_VERSION

    def __post_init__(self) -> None:
        ts = np.asarray(self.feature_ready_ts_ns, dtype=np.int64)
        if ts.ndim != 1 or (ts.size and np.any(np.diff(ts) <= 0)):
            raise ValueError("historical fair-price timestamps must be unique/sorted")
        object.__setattr__(self, "feature_ready_ts_ns", ts)
        float_fields = (
            "fair_price",
            "gain",
            "confidence",
            "dispersion_bps",
            "lead_variance_bps2",
            "noise_variance_bps2",
            "max_source_age_ms",
        )
        for name in float_fields:
            values = np.asarray(getattr(self, name), dtype=np.float64)
            if values.ndim != 1 or values.size != ts.size:
                raise ValueError(f"historical fair-price field {name} length mismatch")
            object.__setattr__(self, name, values)
        int_fields = ("valid_venues", "minimum_basis_samples", "valid")
        for name in int_fields:
            values = np.asarray(getattr(self, name), dtype=np.int32)
            if values.ndim != 1 or values.size != ts.size:
                raise ValueError(f"historical fair-price field {name} length mismatch")
            object.__setattr__(self, name, values)
        reasons = np.asarray(self.reason, dtype=str)
        if reasons.ndim != 1 or reasons.size != ts.size:
            raise ValueError("historical fair-price reason length mismatch")
        object.__setattr__(self, "reason", reasons)
        omitted = str(self.omitted_venue).lower()
        if omitted and omitted not in VENUES:
            raise ValueError(f"unsupported omitted venue: {omitted}")
        object.__setattr__(self, "omitted_venue", omitted)

    def to_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "feature_ready_ts_ns": self.feature_ready_ts_ns,
                "fair_price": self.fair_price,
                "gain": self.gain,
                "confidence": self.confidence,
                "dispersion_bps": self.dispersion_bps,
                "valid_venues": self.valid_venues,
                "minimum_basis_samples": self.minimum_basis_samples,
                "lead_variance_bps2": self.lead_variance_bps2,
                "noise_variance_bps2": self.noise_variance_bps2,
                "max_source_age_ms": self.max_source_age_ms,
                "valid": self.valid,
                "reason": self.reason,
            }
        )

    @classmethod
    def from_frame(
        cls,
        frame: pd.DataFrame,
        *,
        omitted_venue: str = "",
        source: str = HISTORICAL_SCHEMA_VERSION,
    ) -> HistoricalFairPriceData:
        return cls(
            feature_ready_ts_ns=frame["feature_ready_ts_ns"].to_numpy(),
            fair_price=frame["fair_price"].to_numpy(),
            gain=frame["gain"].to_numpy(),
            confidence=frame["confidence"].to_numpy(),
            dispersion_bps=frame["dispersion_bps"].to_numpy(),
            valid_venues=frame["valid_venues"].to_numpy(),
            minimum_basis_samples=frame["minimum_basis_samples"].to_numpy(),
            lead_variance_bps2=frame["lead_variance_bps2"].to_numpy(),
            noise_variance_bps2=frame["noise_variance_bps2"].to_numpy(),
            max_source_age_ms=frame["max_source_age_ms"].to_numpy(),
            valid=frame["valid"].to_numpy(),
            reason=frame["reason"].astype(str).to_numpy(),
            omitted_venue=omitted_venue,
            source=source,
        )


class HistoricalFairPriceCursor:
    """Backward-only cursor over sensitivity-only fair-price states."""

    def __init__(
        self,
        data: HistoricalFairPriceData,
        *,
        max_state_age_ms: float = 2_000.0,
    ) -> None:
        self.data = data
        self.max_state_age_ms = max(0.0, float(max_state_age_ms))
        self._last_decision_ts_ns = 0

    def asof(
        self,
        decision_ts_ns: int,
        *,
        local_mid: float,
    ) -> CrossVenueFairPriceState:
        decision_ns = int(decision_ts_ns)
        if decision_ns < self._last_decision_ts_ns:
            raise ValueError("historical fair-price decision clock regressed")
        self._last_decision_ts_ns = decision_ns
        index = int(
            np.searchsorted(
                self.data.feature_ready_ts_ns,
                decision_ns,
                side="right",
            )
            - 1
        )
        local = float(local_mid)
        reason = "before_first_historical_state"
        if index >= 0:
            age_ms = (
                decision_ns - int(self.data.feature_ready_ts_ns[index])
            ) / 1_000_000.0
            reason = str(self.data.reason[index])
        else:
            age_ms = math.inf
        valid = bool(
            index >= 0
            and int(self.data.valid[index]) == 1
            and 0.0 <= age_ms <= self.max_state_age_ms
            and math.isfinite(local)
            and local > 0.0
        )
        if index >= 0 and int(self.data.valid[index]) == 1 and not valid:
            reason = "stale_historical_fair_price"
        fair = float(self.data.fair_price[index]) if index >= 0 else math.nan
        gain = float(self.data.gain[index]) if valid else 0.0
        raw_lead_bps = (
            math.log(fair / local) * 10_000.0
            if valid and fair > 0.0
            else math.nan
        )
        shift = gain * (fair - local) if valid else 0.0
        venue_count = int(self.data.valid_venues[index]) if index >= 0 else 0
        ids = tuple(
            venue
            for venue in VENUES
            if venue != str(self.data.omitted_venue)
        )[: max(0, venue_count)]
        return CrossVenueFairPriceState(
            schema_version=FAIR_PRICE_SCHEMA_VERSION,
            decision_ts_ns=decision_ns,
            valid=valid,
            reason="valid" if valid else reason,
            local_mid=local,
            fair_price=fair,
            raw_lead_bps=raw_lead_bps,
            gain=gain,
            center_shift_price=shift,
            center_shift_bps=(shift / local * 10_000.0 if valid else 0.0),
            confidence=(float(self.data.confidence[index]) if valid else 0.0),
            dispersion_bps=(
                float(self.data.dispersion_bps[index]) if index >= 0 else math.nan
            ),
            valid_venues=venue_count,
            venue_ids=ids,
            minimum_basis_samples=(
                int(self.data.minimum_basis_samples[index]) if index >= 0 else 0
            ),
            lead_variance_bps2=(
                float(self.data.lead_variance_bps2[index]) if index >= 0 else 0.0
            ),
            noise_variance_bps2=(
                float(self.data.noise_variance_bps2[index]) if index >= 0 else 0.0
            ),
            max_source_age_ms=(
                float(self.data.max_source_age_ms[index]) + max(0.0, age_ms)
                if index >= 0
                else math.inf
            ),
            max_feed_latency_ms=math.nan,
            max_feature_latency_ms=math.nan,
            source_kinds=(HISTORICAL_SOURCE_KIND,),
            transport_supported=False,
            venues={},
        )


def _trade_state(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(
        path,
        columns=[
            "event_second_ts_ms",
            "timestamp",
            "last_event_ts_ms",
            "close",
            "trade_count",
        ],
    )
    timestamp = pd.to_numeric(frame["timestamp"], errors="coerce")
    second = pd.to_numeric(frame["event_second_ts_ms"], errors="coerce")
    event = pd.to_numeric(frame["last_event_ts_ms"], errors="coerce")
    close = pd.to_numeric(frame["close"], errors="coerce")
    count = pd.to_numeric(frame["trade_count"], errors="coerce")
    valid = (
        timestamp.notna()
        & second.notna()
        & event.notna()
        & close.gt(0.0)
        & count.gt(0.0)
        & timestamp.eq(second + 1_000.0)
        & event.le(timestamp)
    )
    output = pd.DataFrame(
        {
            "ready_ns": timestamp.loc[valid].round().astype("int64") * 1_000_000,
            "exchange_ns": event.loc[valid].round().astype("int64") * 1_000_000,
            "mid": close.loc[valid].astype(float),
        }
    )
    return output.sort_values("ready_ns").drop_duplicates("ready_ns", keep="last")


def _anchor_state(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    starts = pd.to_numeric(pd.Index(frame.index), errors="coerce")
    close = pd.to_numeric(frame["close"], errors="coerce").to_numpy()
    count = (
        pd.to_numeric(frame["trade_count"], errors="coerce").to_numpy()
        if "trade_count" in frame
        else np.ones(len(frame), dtype=float)
    )
    valid = np.isfinite(starts) & np.isfinite(close) & (close > 0.0) & (count > 0.0)
    output = pd.DataFrame(
        {
            "ready_ns": (starts[valid].round().astype("int64") + 1_000)
            * 1_000_000,
            "mid": close[valid],
        }
    )
    return output.sort_values("ready_ns").drop_duplicates("ready_ns", keep="last")


def _execution_mid_state(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(
        path,
        columns=["timestamp", "best_bid", "best_ask"],
    )
    ts = pd.to_numeric(frame["timestamp"], errors="coerce")
    bid = pd.to_numeric(frame["best_bid"], errors="coerce")
    ask = pd.to_numeric(frame["best_ask"], errors="coerce")
    valid = ts.notna() & bid.gt(0.0) & ask.gt(bid)
    output = pd.DataFrame(
        {
            "ready_ns": ts.loc[valid].round().astype("int64") * 1_000_000,
            "mid": 0.5 * (bid.loc[valid].astype(float) + ask.loc[valid].astype(float)),
        }
    )
    return output.sort_values("ready_ns").drop_duplicates("ready_ns", keep="last")


def _concat_context(
    paths: Mapping[str, Path],
    labels: Iterable[str],
    loader,
) -> pd.DataFrame:
    pieces = [loader(paths[label]) for label in labels]
    return (
        pd.concat(pieces, ignore_index=True)
        .sort_values("ready_ns")
        .drop_duplicates("ready_ns", keep="last")
    )


def _asof_arrays(
    grid_ns: np.ndarray,
    state: pd.DataFrame,
    *,
    include_exchange: bool,
) -> dict[str, np.ndarray]:
    ready = state["ready_ns"].to_numpy(dtype=np.int64, copy=False)
    index = np.searchsorted(ready, grid_ns, side="right") - 1
    supported = index >= 0
    safe = np.maximum(index, 0)
    output = {
        "ready_ns": np.where(supported, ready[safe], 0),
        "mid": np.where(
            supported,
            state["mid"].to_numpy(dtype=np.float64, copy=False)[safe],
            np.nan,
        ),
    }
    if include_exchange:
        exchange = state["exchange_ns"].to_numpy(dtype=np.int64, copy=False)
        output["exchange_ns"] = np.where(supported, exchange[safe], 0)
    return output


def _build_frame(
    day: str,
    paths: Mapping[str, Path],
    *,
    omitted_venue: str,
    config: CrossVenueFairPriceConfig,
) -> pd.DataFrame:
    warmup = _day_before(day)
    warmup_start, _ = _day_bounds_ns(warmup)
    _, target_end = _day_bounds_ns(day)
    grid_ns = np.arange(
        warmup_start + 1_000_000_000,
        target_end + 1,
        1_000_000_000,
        dtype=np.int64,
    )
    source_arrays: dict[tuple[str, str], dict[str, np.ndarray]] = {}
    for venue in VENUES:
        if venue == omitted_venue:
            continue
        for market in MARKETS:
            labels = (f"{warmup}:{venue}:{market}", f"{day}:{venue}:{market}")
            source_arrays[(venue, market)] = _asof_arrays(
                grid_ns,
                _concat_context(paths, labels, _trade_state),
                include_exchange=True,
            )
    anchor = _asof_arrays(
        grid_ns,
        _concat_context(
            paths,
            (f"{warmup}:anchor", f"{day}:anchor"),
            _anchor_state,
        ),
        include_exchange=False,
    )
    execution = _asof_arrays(
        grid_ns,
        _concat_context(
            paths,
            (f"{warmup}:execution_bbo", f"{day}:execution_bbo"),
            _execution_mid_state,
        ),
        include_exchange=False,
    )

    estimator = CrossVenueFairPriceEstimator(config)
    target_start, _ = _day_bounds_ns(day)
    rows: list[dict[str, Any]] = []
    for index, decision_ns in enumerate(grid_ns):
        local_ready = int(execution["ready_ns"][index])
        local_age_ms = (int(decision_ns) - local_ready) / 1_000_000.0
        local_mid = (
            float(execution["mid"][index])
            if local_ready > 0 and 0.0 <= local_age_ms <= 500.0
            else math.nan
        )
        sources: list[FairPriceSource] = []
        for (venue, market), values in source_arrays.items():
            ready_ns = int(values["ready_ns"][index])
            exchange_ns = int(values["exchange_ns"][index])
            event_age_ms = (int(decision_ns) - exchange_ns) / 1_000_000.0
            sources.append(
                FairPriceSource(
                    venue=venue,
                    market_type=market,
                    bid=math.nan,
                    ask=math.nan,
                    exchange_ts_ns=exchange_ns,
                    local_receive_ts_ns=ready_ns,
                    feature_ready_ts_ns=ready_ns,
                    valid=bool(
                        ready_ns > 0
                        and exchange_ns > 0
                        and 0.0 <= event_age_ms <= config.max_source_age_ms
                    ),
                    mid_override=float(values["mid"][index]),
                    source_kind=HISTORICAL_SOURCE_KIND,
                    transport_supported=False,
                )
            )
        state = estimator.observe(
            decision_ts_ns=int(decision_ns),
            local_mid=local_mid,
            stablecoin_mid=float(anchor["mid"][index]),
            stablecoin_feature_ready_ts_ns=int(anchor["ready_ns"][index]),
            sources=sources,
        )
        if int(decision_ns) <= target_start:
            continue
        rows.append(
            {
                "feature_ready_ts_ns": int(decision_ns),
                "fair_price": float(state.fair_price),
                "gain": float(state.gain),
                "confidence": float(state.confidence),
                "dispersion_bps": float(state.dispersion_bps),
                "valid_venues": int(state.valid_venues),
                "minimum_basis_samples": int(state.minimum_basis_samples),
                "lead_variance_bps2": float(state.lead_variance_bps2),
                "noise_variance_bps2": float(state.noise_variance_bps2),
                "max_source_age_ms": float(state.max_source_age_ms),
                "valid": int(bool(state.valid)),
                "reason": str(state.reason),
            }
        )
    output = pd.DataFrame(rows)
    if len(output) != 86_400:
        raise RuntimeError(f"{day}: fair-price cache rows={len(output)}, expected 86400")
    if output["feature_ready_ts_ns"].duplicated().any():
        raise RuntimeError(f"{day}: duplicate fair-price feature-ready timestamps")
    return output


def _cache_identity(
    day: str,
    *,
    omitted_venue: str,
    config: CrossVenueFairPriceConfig,
    sources: list[dict[str, Any]],
) -> dict[str, Any]:
    implementation = {
        "adapter_path": str(Path(__file__).resolve()),
        "adapter_sha256": _sha256_file(Path(__file__).resolve()),
        "estimator_path": str(
            (ROOT / "strategy" / "cross_venue_fair_price.py").resolve()
        ),
        "estimator_sha256": _sha256_file(
            ROOT / "strategy" / "cross_venue_fair_price.py"
        ),
    }
    return {
        "schema_version": HISTORICAL_SCHEMA_VERSION,
        "fair_price_schema_version": FAIR_PRICE_SCHEMA_VERSION,
        "utc_day": str(day),
        "warmup_day": _day_before(day),
        "omitted_venue": str(omitted_venue),
        "source_kind": HISTORICAL_SOURCE_KIND,
        "transport_supported": False,
        "config": asdict(config),
        "implementation": implementation,
        "sources": sources,
    }


def build_or_load_daily_fair_price(
    day: str,
    *,
    omitted_venue: str = "",
    config: CrossVenueFairPriceConfig | None = None,
    project_data_root: Path | None = None,
    cache_dir: Path | None = None,
    force_rebuild: bool = False,
) -> tuple[HistoricalFairPriceData, dict[str, Any]]:
    """Build/load one source-identified day on the internal cache disk."""

    omitted = str(omitted_venue).lower().strip()
    if omitted and omitted not in VENUES:
        raise ValueError(f"unsupported omitted venue: {omitted}")
    cfg = config or CrossVenueFairPriceConfig()
    sources = source_manifest(day, project_data_root=project_data_root)
    identity = _cache_identity(
        day,
        omitted_venue=omitted,
        config=cfg,
        sources=sources,
    )
    digest = _canonical_sha256(identity)
    base = (
        Path(cache_dir)
        if cache_dir is not None
        else cache_root(ROOT) / "replay_dag" / HISTORICAL_CACHE_NODE
    )
    variant = f"leave_{omitted}_out" if omitted else "all_venues"
    target_dir = base / variant
    target = target_dir / f"{day}-{digest[:16]}.parquet"
    manifest_path = target.with_suffix(target.suffix + ".manifest.json")
    if target.is_file() and manifest_path.is_file() and not force_rebuild:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (
            str(manifest.get("cache_identity_sha256", "")) == digest
            and str(manifest.get("output_sha256", "")) == _sha256_file(target)
        ):
            data = HistoricalFairPriceData.from_frame(
                pd.read_parquet(target),
                omitted_venue=omitted,
                source=str(target),
            )
            return data, manifest

    paths = source_paths(day, project_data_root=project_data_root)
    frame = _build_frame(
        day,
        paths,
        omitted_venue=omitted,
        config=cfg,
    )
    target_dir.mkdir(parents=True, exist_ok=True)
    temp = target.with_suffix(target.suffix + f".tmp.{os.getpid()}")
    frame.to_parquet(temp, index=False, compression="zstd")
    os.replace(temp, target)
    manifest = {
        **identity,
        "cache_identity_sha256": digest,
        "output_path": str(target.resolve()),
        "output_sha256": _sha256_file(target),
        "rows": int(len(frame)),
        "valid_rows": int(frame["valid"].sum()),
        "valid_fraction": float(frame["valid"].mean()),
        "reason_counts": {
            str(key): int(value)
            for key, value in frame["reason"].value_counts().items()
        },
    }
    manifest_temp = manifest_path.with_suffix(
        manifest_path.suffix + f".tmp.{os.getpid()}"
    )
    manifest_temp.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(manifest_temp, manifest_path)
    data = HistoricalFairPriceData.from_frame(
        frame,
        omitted_venue=omitted,
        source=str(target),
    )
    return data, manifest


def audit_daily_source_coverage(
    days: Iterable[str],
    *,
    project_data_root: Path | None = None,
) -> pd.DataFrame:
    """Cheap per-file identity/support audit before cache construction."""

    rows: list[dict[str, Any]] = []
    for day in (str(value) for value in days):
        for label, path in sorted(
            source_paths(day, project_data_root=project_data_root).items()
        ):
            row: dict[str, Any] = {
                "target_day": day,
                "source_label": label,
                "path": str(path),
                "exists": int(path.is_file()),
                "rows": 0,
            }
            if path.is_file():
                row["rows"] = int(pq.ParquetFile(path).metadata.num_rows)
                row["size_bytes"] = int(path.stat().st_size)
            rows.append(row)
    return pd.DataFrame(rows)


def _cache_build_job(
    payload: tuple[str, str, str | None, str | None, bool],
) -> dict[str, Any]:
    day, omitted, project_data_root, cache_dir, force_rebuild = payload
    _, manifest = build_or_load_daily_fair_price(
        day,
        omitted_venue=omitted,
        project_data_root=(
            Path(project_data_root) if project_data_root is not None else None
        ),
        cache_dir=Path(cache_dir) if cache_dir is not None else None,
        force_rebuild=bool(force_rebuild),
    )
    return {
        "day": str(day),
        "omitted_venue": str(omitted),
        "variant": f"leave_{omitted}_out" if omitted else "all_venues",
        "output_path": str(manifest["output_path"]),
        "output_sha256": str(manifest["output_sha256"]),
        "cache_identity_sha256": str(manifest["cache_identity_sha256"]),
        "rows": int(manifest["rows"]),
        "valid_rows": int(manifest["valid_rows"]),
        "valid_fraction": float(manifest["valid_fraction"]),
        "reason_counts": dict(manifest.get("reason_counts") or {}),
    }


def build_cache_universe(
    days: Iterable[str],
    *,
    workers: int = 1,
    project_data_root: Path | None = None,
    cache_dir: Path | None = None,
    force_rebuild: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build all/LOO variants and return cache plus common-support audits."""

    ordered_days = tuple(dict.fromkeys(str(day) for day in days))
    if not ordered_days:
        raise ValueError("fair-price cache universe is empty")
    variants = ("", *VENUES)
    jobs = [
        (
            day,
            omitted,
            str(project_data_root) if project_data_root is not None else None,
            str(cache_dir) if cache_dir is not None else None,
            bool(force_rebuild),
        )
        for day in ordered_days
        for omitted in variants
    ]
    if int(workers) <= 1:
        rows = [_cache_build_job(job) for job in jobs]
    else:
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=int(workers)
        ) as executor:
            rows = list(executor.map(_cache_build_job, jobs))
    caches = pd.DataFrame(rows).sort_values(
        ["day", "variant"], ignore_index=True
    )
    expected_variants = {
        "all_venues",
        "leave_bitget_out",
        "leave_bybit_out",
        "leave_okx_out",
    }
    common_rows: list[dict[str, Any]] = []
    for day in ordered_days:
        day_rows = caches[caches["day"].eq(day)]
        if set(day_rows["variant"]) != expected_variants:
            raise RuntimeError(f"{day}: fair-price cache variants are incomplete")
        valid_by_variant: dict[str, np.ndarray] = {}
        timestamps: np.ndarray | None = None
        for row in day_rows.itertuples(index=False):
            frame = pd.read_parquet(
                row.output_path,
                columns=["feature_ready_ts_ns", "valid"],
            )
            current_ts = frame["feature_ready_ts_ns"].to_numpy(
                dtype=np.int64, copy=False
            )
            if timestamps is None:
                timestamps = current_ts
            elif not np.array_equal(timestamps, current_ts):
                raise RuntimeError(f"{day}: LOO timestamp denominators drifted")
            valid_by_variant[str(row.variant)] = (
                frame["valid"].to_numpy(dtype=np.int8, copy=False) == 1
            )
        common_valid = np.logical_and.reduce(
            [valid_by_variant[name] for name in sorted(expected_variants)]
        )
        common_rows.append(
            {
                "day": day,
                "rows": int(common_valid.size),
                "common_valid_rows": int(common_valid.sum()),
                "common_valid_fraction": float(common_valid.mean()),
                **{
                    f"{name}_valid_rows": int(valid_by_variant[name].sum())
                    for name in sorted(expected_variants)
                },
            }
        )
    return caches, pd.DataFrame(common_rows)


def load_common_support_variants(
    cache_variants: pd.DataFrame | Path,
    day: str,
) -> tuple[dict[str, HistoricalFairPriceData], dict[str, Any]]:
    """Load hash-verified all/LOO caches on one identical visibility mask."""

    audit = (
        pd.read_csv(cache_variants)
        if isinstance(cache_variants, Path)
        else cache_variants.copy()
    )
    day_rows = audit[audit["day"].astype(str).eq(str(day))].copy()
    expected = {
        "all_venues": "",
        "leave_bitget_out": "bitget",
        "leave_bybit_out": "bybit",
        "leave_okx_out": "okx",
    }
    if set(day_rows["variant"].astype(str)) != set(expected):
        raise ValueError(f"{day}: cache variant audit is incomplete")
    frames: dict[str, pd.DataFrame] = {}
    timestamps: np.ndarray | None = None
    for row in day_rows.itertuples(index=False):
        variant = str(row.variant)
        path = Path(str(row.output_path))
        if not path.is_file() or _sha256_file(path) != str(row.output_sha256):
            raise ValueError(f"{day}: cache artifact drifted for {variant}")
        frame = pd.read_parquet(path)
        current_ts = frame["feature_ready_ts_ns"].to_numpy(
            dtype=np.int64, copy=False
        )
        if timestamps is None:
            timestamps = current_ts
        elif not np.array_equal(timestamps, current_ts):
            raise ValueError(f"{day}: cache variant timestamps drifted")
        frames[variant] = frame
    common_valid = np.logical_and.reduce(
        [
            frames[variant]["valid"].to_numpy(dtype=np.int8, copy=False) == 1
            for variant in sorted(expected)
        ]
    )
    outputs: dict[str, HistoricalFairPriceData] = {}
    for variant, omitted in expected.items():
        frame = frames[variant].copy()
        original_valid = frame["valid"].to_numpy(dtype=np.int8, copy=False) == 1
        frame["valid"] = common_valid.astype(np.int8)
        frame.loc[original_valid & ~common_valid, "reason"] = (
            "loo_common_support_unavailable"
        )
        source_path = str(
            day_rows.loc[
                day_rows["variant"].astype(str).eq(variant), "output_path"
            ].iloc[0]
        )
        outputs[variant] = HistoricalFairPriceData.from_frame(
            frame,
            omitted_venue=omitted,
            source=f"{source_path}|common_loo_support",
        )
    return outputs, {
        "day": str(day),
        "rows": int(common_valid.size),
        "common_valid_rows": int(common_valid.sum()),
        "common_valid_fraction": float(common_valid.mean()),
        "identical_timestamp_denominator": True,
        "identical_validity_denominator": bool(
            all(
                np.array_equal(
                    data.valid == 1,
                    common_valid,
                )
                for data in outputs.values()
            )
        ),
    }


def _atomic_write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def _cli() -> None:
    parser = argparse.ArgumentParser(
        description="Build causal fair-price all/LOO Development caches"
    )
    parser.add_argument("--spec", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--project-data-root", type=Path)
    parser.add_argument("--cache-dir", type=Path)
    parser.add_argument("--force-rebuild", action="store_true")
    args = parser.parse_args()

    spec_path = args.spec.expanduser().resolve()
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    panels = spec.get("panels") or spec.get("split") or {}
    days = tuple(str(day) for day in panels.get("development_days") or ())
    if not days:
        raise ValueError("spec does not contain Development days")
    if len(days) != len(set(days)) or tuple(sorted(days)) != days:
        raise ValueError("Development days must be unique and chronological")

    source_audit = audit_daily_source_coverage(
        days,
        project_data_root=args.project_data_root,
    )
    if not source_audit["exists"].eq(1).all():
        missing = source_audit[source_audit["exists"].ne(1)]
        raise FileNotFoundError(
            "fair-price source universe is incomplete: "
            + ", ".join(missing["path"].astype(str).head(10))
        )
    caches, common = build_cache_universe(
        days,
        workers=max(1, int(args.workers)),
        project_data_root=args.project_data_root,
        cache_dir=args.cache_dir,
        force_rebuild=bool(args.force_rebuild),
    )

    output_dir = args.output_dir.expanduser().resolve()
    source_path = output_dir / "source_coverage.csv"
    cache_path = output_dir / "cache_variants.csv"
    common_path = output_dir / "common_support.csv"
    output_dir.mkdir(parents=True, exist_ok=True)
    source_audit.to_csv(source_path, index=False)
    caches.to_csv(cache_path, index=False)
    common.to_csv(common_path, index=False)
    manifest = {
        "schema_version": "cross_venue_causal_fair_price_cache_universe.v1",
        "historical_schema_version": HISTORICAL_SCHEMA_VERSION,
        "source_kind": HISTORICAL_SOURCE_KIND,
        "transport_supported": False,
        "spec_path": str(spec_path),
        "spec_sha256": _sha256_file(spec_path),
        "development_days": list(days),
        "development_day_count": len(days),
        "variant_count": 4,
        "cache_row_count": int(len(caches)),
        "common_valid_rows": int(common["common_valid_rows"].sum()),
        "common_rows": int(common["rows"].sum()),
        "common_valid_fraction": float(
            common["common_valid_rows"].sum() / common["rows"].sum()
        ),
        "minimum_daily_common_valid_fraction": float(
            common["common_valid_fraction"].min()
        ),
        "source_coverage_csv": str(source_path),
        "source_coverage_sha256": _sha256_file(source_path),
        "cache_variants_csv": str(cache_path),
        "cache_variants_sha256": _sha256_file(cache_path),
        "common_support_csv": str(common_path),
        "common_support_sha256": _sha256_file(common_path),
        "permissions": {
            "prediction_authorized": False,
            "action_experiment_authorized": False,
            "live_deployment_authorized": False,
        },
    }
    manifest["canonical_manifest_sha256"] = _canonical_sha256(manifest)
    _atomic_write_text(
        output_dir / "manifest.json",
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
    )


if __name__ == "__main__":
    _cli()
