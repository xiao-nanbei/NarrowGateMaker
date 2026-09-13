"""Shared infrastructure for paired fixed-spread research."""

from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from data.normalized_l2_registry import DAILY_QUALITY_FILENAME
from models import backtest_tick as bt
from models.backtest_config import (
    disable_ml_params,
    load_tick_base_params,
    validate_formal_replay_calibration,
)

ROOT = Path(__file__).resolve().parents[4]

DEFAULT_DISTANCES = (
    0,
    1,
    2,
    5,
    10,
    20,
    40,
    60,
    80,
    100,
    120,
    140,
    160,
    180,
    200,
    220,
    240,
    280,
    320,
    400,
    500,
    600,
    800,
    1000,
    1200,
)
SMOKE_DISTANCES = (0, 1, 20, 80, 140, 220, 400, 600, 1200)


def _safe_ratio(numerator: float, denominator: float) -> float:
    return float(numerator) / float(denominator) if denominator > 0 else np.nan


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{_process_id()}.tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def _process_id() -> int:
    import os

    return os.getpid()


def _atomic_write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{_process_id()}.tmp")
    frame.to_csv(tmp, index=False)
    tmp.replace(path)


def _git_identity() -> dict[str, Any]:
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            text=True,
        ).strip()
        diff = subprocess.check_output(
            ["git", "diff", "--binary", "HEAD"],
            cwd=ROOT,
        )
        untracked = subprocess.check_output(
            ["git", "ls-files", "--others", "--exclude-standard"],
            cwd=ROOT,
            text=True,
        ).splitlines()
    except (OSError, subprocess.CalledProcessError):
        return {"commit": "", "dirty": True, "dirty_patch_sha256": ""}
    digest = hashlib.sha256(diff)
    for relative in sorted(untracked):
        path = ROOT / relative
        digest.update(relative.encode("utf-8"))
        if path.is_file():
            digest.update(path.read_bytes())
    return {
        "commit": commit,
        "dirty": bool(diff or untracked),
        "dirty_patch_sha256": digest.hexdigest(),
    }


def audit_execution_trade_inputs(days: list[str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for day in days:
        path = bt.RAW_TRADES_DIR / "BTCUSDC" / f"BTCUSDC-trades-{day}.csv"
        if not path.is_file():
            raise FileNotFoundError(f"individual-trade input is missing: {path}")
        seen_true = False
        seen_false = False
        bytes_scanned = 0
        carry = b""
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                bytes_scanned += len(chunk)
                lines = (carry + chunk).split(b"\n")
                carry = lines.pop()
                for line in lines:
                    value = line.rsplit(b",", 1)[-1].strip().lower()
                    seen_true = seen_true or value == b"true"
                    seen_false = seen_false or value == b"false"
                if seen_true and seen_false:
                    break
        if carry and not (seen_true and seen_false):
            value = carry.rsplit(b",", 1)[-1].strip().lower()
            seen_true = seen_true or value == b"true"
            seen_false = seen_false or value == b"false"
        if not (seen_true and seen_false):
            raise ValueError(
                f"{day}: individual-trade tape lacks BUY/SELL taker-side "
                f"support: true={seen_true} false={seen_false} path={path}"
            )
        stat = path.stat()
        rows.append(
            {
                "day": day,
                "path": str(path.resolve()),
                "size_bytes": int(stat.st_size),
                "mtime_ns": int(stat.st_mtime_ns),
                "has_buyer_maker_true": seen_true,
                "has_buyer_maker_false": seen_false,
                "preflight_bytes_scanned": bytes_scanned,
            }
        )
    return pd.DataFrame(rows)


def load_quality(dataset_root: Path) -> pd.DataFrame:
    path = dataset_root / DAILY_QUALITY_FILENAME
    frame = pd.read_csv(path)
    required = {"day", "rebuilt", "formal_eligible", "bbo_sha256", "l2_sha256"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(
            f"normalized L2 quality registry missing columns: {sorted(missing)}"
        )
    frame = frame.copy()
    frame["day"] = pd.to_datetime(frame["day"], utc=True).dt.strftime("%Y-%m-%d")
    for column in ("rebuilt", "formal_eligible"):
        frame[column] = (
            frame[column]
            .astype(str)
            .str.strip()
            .str.lower()
            .isin({"1", "true", "yes", "y"})
        )
    if frame["day"].duplicated().any():
        raise ValueError(f"duplicate days in {path}")
    return frame.sort_values("day").reset_index(drop=True)


def select_days(
    quality: pd.DataFrame,
    *,
    panel: str,
    requested_days: list[str] | None,
    max_days: int | None,
) -> list[str]:
    eligible = quality.loc[quality["rebuilt"], "day"].tolist()
    if panel == "formal":
        eligible = quality.loc[quality["formal_eligible"], "day"].tolist()
    eligible_set = set(eligible)
    if requested_days:
        normalized = [
            pd.Timestamp(day, tz="UTC").strftime("%Y-%m-%d")
            for day in requested_days
        ]
        missing = sorted(set(normalized).difference(eligible_set))
        if missing:
            raise ValueError(f"requested days are outside panel={panel}: {missing}")
        eligible = normalized
    if max_days is not None:
        eligible = eligible[: max(0, int(max_days))]
    if not eligible:
        raise ValueError(f"panel={panel} selected no days")
    return eligible


def build_research_params(
    *,
    config_path: Path,
    strict_calibration: bool,
    queue_calibration_path: Path | None,
    latency_telemetry_path: Path | None,
    latency_mode: str,
) -> dict[str, Any]:
    params = load_tick_base_params(
        symbol="BTCUSDC",
        config_path=config_path,
        configure_symbol=bt.configure_symbol,
        require_historical_bbo=True,
        include_fill_probability=True,
        include_queue_calibration=True,
        queue_calibration_path=queue_calibration_path,
        strict_calibration=strict_calibration,
    )
    q90_separate_treatment = {
        "treatment_id": "buy_exposure_adverse_q90_cancel_reentry_v1",
        "enabled_in_operational_config": bool(
            params.get("dynamic_fill_hazard_action_enabled", False)
        ),
        "policy_path": str(
            params.get("dynamic_fill_hazard_action_policy_path", "") or ""
        ),
        "policy_sha256": str(
            params.get("dynamic_fill_hazard_action_policy_sha256", "") or ""
        ),
        "replay_status": "excluded_frozen_separate_treatment",
    }
    disable_ml_params(params)
    params.update(
        {
            "ml_enabled": False,
            "buy_fill_selection_live_enabled": False,
            "adverse_guard_enabled": False,
            "defense_guard_enabled": False,
            "local_extreme_guard_enabled": False,
            "campaign_soft_control_enabled": False,
            "campaign_stop_add_enabled": False,
            "post_fill_quote_response_enabled": False,
            "multi_market_policy_enabled": False,
            "dynamic_fill_hazard_action_enabled": False,
            "_frozen_separate_treatments": [q90_separate_treatment],
            "random_passive_enabled": False,
            "use_bar_pricing": False,
            "replay_event_clock": "merged",
            "replay_clock_interval_ms": 100,
            "requote_clock": "fixed",
            "execution_trade_source": "trades",
            "market_context_warmup_days": 0,
            "initial_inventory": 0.0,
            "initial_entry_price": 0.0,
            "collect_curves": False,
            "trace_decisions_max": 0,
            "trace_quotes_max": 0,
            "trace_fills_max": 0,
            "require_historical_bbo": True,
            "replay_purpose": "formal" if strict_calibration else "descriptive",
            "latency_sampler_version": bt.LATENCY_SAMPLER_VERSION,
        }
    )
    if latency_telemetry_path is not None:
        samples = bt._load_live_perf_latency_samples(  # noqa: SLF001
            latency_telemetry_path,
            mode=latency_mode,
        )
        params["_new_order_latency_samples_ms"] = samples[
            "new_order_latency_samples_ms"
        ]
        params["_cancel_order_latency_samples_ms"] = samples[
            "cancel_order_latency_samples_ms"
        ]
        params["live_perf_telemetry_path"] = str(latency_telemetry_path)
        params["live_perf_latency_mode"] = latency_mode
    if strict_calibration:
        validate_formal_replay_calibration(params, require_latency=True)
    return params


def _bootstrap_ratio_ci(
    frame: pd.DataFrame,
    *,
    numerator: str,
    denominator: str,
    reps: int,
    seed: int,
) -> tuple[float, float]:
    if reps <= 0 or frame.empty:
        return np.nan, np.nan
    values = frame[[numerator, denominator]].to_numpy(dtype=np.float64)
    rng = np.random.default_rng(seed)
    estimates = np.empty(reps, dtype=np.float64)
    n_days = len(values)
    for index in range(reps):
        sample = values[rng.integers(0, n_days, size=n_days)]
        denominator_sum = float(sample[:, 1].sum())
        estimates[index] = (
            float(sample[:, 0].sum()) / denominator_sum
            if denominator_sum > 0.0
            else np.nan
        )
    finite = estimates[np.isfinite(estimates)]
    if finite.size == 0:
        return np.nan, np.nan
    return tuple(np.quantile(finite, [0.025, 0.975]).tolist())
