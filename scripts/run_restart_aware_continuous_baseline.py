#!/usr/bin/env python3
"""Run the current baseline on the frozen restart-aware 40-anchor calendar."""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import math
import os
import sys
import tempfile
import time
from collections import OrderedDict
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_paths import data_root, window_cache_root  # noqa: E402

DATA_ROOT = data_root(ROOT)
CALENDAR_MANIFEST = (
    ROOT
    / "research/shared/replay_lifecycle/docs/"
    "calendar_continuity_manifest_20260417_20260730_v1.json"
)
BASELINE_POINTER = (
    ROOT
    / "research/families/f10_live_replay_attribution/docs/"
    "operational_baseline_current.json"
)
MODEL_DIR = (
    ROOT
    / "models/saved_btcusdc_causal_v12_expanded_source_aware_semantics_v6_20260802_live_canary"
)
FEATURE_DIR = (
    DATA_ROOT
    / "features_btcusdc_causal_v12_ranked_toxicity_f09_40d_20260802"
)
BOOK_ROOT = DATA_ROOT / "normalized_l2_100ms_v2_20260727"
QUEUE_PATH = (
    DATA_ROOT
    / "reports/formal_recalibration_20260715/"
    "BTCUSDC-queue-calibration-v3-fit-20260710_11-q070.json"
)
LATENCY_PATH = (
    DATA_ROOT
    / "reports/formal_recalibration_20260715/"
    "ec2_aws_tokyo_2vcpu4g_20260710_14_rest_latency.csv.gz"
)
COVERAGE_REPORT = (
    DATA_ROOT
    / "reports/continuous_calendar_substrate_v1_20260803/"
    "normalized_coverage_final.json"
)
FRESH_BASELINE_REPORT = (
    ROOT
    / "research/families/f10_live_replay_attribution/docs/"
    "current_live_held_ber_replay_baseline_40d_20260809.json"
)
CACHE_DIR = window_cache_root(ROOT)
DEFAULT_OUTPUT = (
    DATA_ROOT
    / "reports/current_live_held_ber_baseline_restart_aware_continuous_40anchor_v1_20260809"
)
FEATURE_MANIFEST_SHA256 = (
    "1e21d6a8aee511d1ebed8db04d76c5b6d8803724999c284b2256bd83f5d36a90"
)
COVERAGE_REPORT_SHA256 = (
    "2f927c1ee3be75087585c5e6ddd73c9bc2f91a97c86990a6e0943af45ed7981e"
)
DAY_MS = 86_400_000
EPS = 1e-10


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
        temporary = Path(handle.name)
    os.replace(temporary, path)


def require_sha256(path: Path, expected: str, label: str) -> None:
    observed = sha256_file(path)
    if observed != expected:
        raise RuntimeError(f"{label} hash mismatch: {observed} != {expected}")


class CausalMarkStore:
    def __init__(self, day_rows: dict[str, dict[str, Any]], max_cached_days: int = 2):
        self._day_rows = day_rows
        self._cache: OrderedDict[str, tuple[np.ndarray, np.ndarray]] = OrderedDict()
        self._max_cached_days = int(max_cached_days)
        self._used_sources: dict[str, dict[str, Any]] = {}

    def _load(self, day: str) -> tuple[np.ndarray, np.ndarray]:
        cached = self._cache.pop(day, None)
        if cached is not None:
            self._cache[day] = cached
            return cached
        row = self._day_rows.get(day)
        if row is None:
            raise FileNotFoundError(f"mark source missing from calendar manifest for {day}")
        official_raw = str(row.get("official_btcusdc_trade_path", "") or "").strip()
        official_path = Path(official_raw) if official_raw else None
        if official_path is not None and official_path.is_file():
            frame = pd.read_csv(
                official_path,
                usecols=["time", "price"],
                dtype={"time": "int64", "price": "float64"},
            )
            ts = frame["time"].to_numpy(dtype=np.int64, copy=True)
            px = frame["price"].to_numpy(dtype=np.float64, copy=True)
            source = {
                "authority": "official_btcusdc_individual_trades",
                "path": str(official_path),
            }
        else:
            provider = dict(row.get("provider_normalized") or {})
            bbo = dict((provider.get("outputs") or {}).get("bbo") or {})
            path_raw = str(bbo.get("path", "") or "").strip()
            expected_sha256 = str(bbo.get("sha256", "") or "").strip()
            bbo_path = Path(path_raw) if path_raw else None
            if (
                not bool(provider.get("all_outputs_admitted"))
                or not bool(provider.get("daily_mark_available"))
                or bbo_path is None
                or not bbo_path.is_file()
                or len(expected_sha256) != 64
            ):
                raise FileNotFoundError(f"admitted causal mark source missing for {day}")
            require_sha256(bbo_path, expected_sha256, f"{day} provider BBO mark")
            frame = pd.read_parquet(
                bbo_path,
                columns=["timestamp", "best_bid", "best_ask"],
            )
            ts = frame["timestamp"].to_numpy(dtype=np.int64, copy=True)
            bid = frame["best_bid"].to_numpy(dtype=np.float64, copy=True)
            ask = frame["best_ask"].to_numpy(dtype=np.float64, copy=True)
            if np.any(bid <= 0.0) or np.any(ask < bid):
                raise RuntimeError(f"invalid provider BBO mark source: {bbo_path}")
            px = (bid + ask) / 2.0
            source = {
                "authority": "tardis_provider_normalized_bbo_mid",
                "path": str(bbo_path),
                "sha256": expected_sha256,
                "clock_source": str(provider.get("clock_source", "")),
                "policy_visible": bool(provider.get("policy_visible")),
            }
        if ts.size == 0 or np.any(ts[1:] < ts[:-1]) or np.any(~np.isfinite(px)):
            raise RuntimeError(f"invalid causal mark source for {day}")
        payload = (ts, px)
        self._used_sources[day] = source
        self._cache[day] = payload
        while len(self._cache) > self._max_cached_days:
            self._cache.popitem(last=False)
        return payload

    def at_or_before(self, ts_ms: int) -> float:
        probe = datetime.fromtimestamp(int(ts_ms) / 1_000.0, tz=UTC)
        day = probe.date().isoformat()
        for _ in range(3):
            if day in self._day_rows:
                ts, px = self._load(day)
                idx = int(np.searchsorted(ts, int(ts_ms), side="right") - 1)
                if idx >= 0:
                    return float(px[idx])
            day = (date.fromisoformat(day) - timedelta(days=1)).isoformat()
        raise RuntimeError(f"no official BTCUSDC mark at or before {ts_ms}")

    def source_manifest(self) -> list[dict[str, Any]]:
        return [
            {"day": day, **source}
            for day, source in sorted(self._used_sources.items())
        ]


def validate_identities() -> dict[str, Any]:
    pointer = json.loads(BASELINE_POINTER.read_text(encoding="utf-8"))
    identity_path = ROOT / str(pointer["identity_path"])
    config_path = ROOT / str(pointer["live_config_path"])
    require_sha256(identity_path, str(pointer["identity_sha256"]), "baseline identity")
    require_sha256(config_path, str(pointer["live_config_sha256"]), "baseline config")
    require_sha256(
        MODEL_DIR / "bundle_meta.json",
        str(pointer["bundle_meta_sha256"]),
        "causal-v12 bundle",
    )
    require_sha256(
        FEATURE_DIR / "causal_feature_manifest.json",
        FEATURE_MANIFEST_SHA256,
        "frozen 40-day feature manifest",
    )
    require_sha256(COVERAGE_REPORT, COVERAGE_REPORT_SHA256, "normalized coverage report")
    if pointer.get("backtest_control_arm") != (
        "causal_v12_ml_on_q90_action_off_buy_fill_selection_action_off"
    ):
        raise RuntimeError("operational pointer does not select the expected control arm")
    expected_flags = {
        "ml_enabled": True,
        "dynamic_fill_hazard_shadow_enabled": True,
        "dynamic_fill_hazard_action_enabled": False,
        "buy_fill_selection_shadow_enabled": True,
        "buy_fill_selection_live_enabled": False,
    }
    for name, expected in expected_flags.items():
        if bool(pointer.get(name)) != expected:
            raise RuntimeError(f"baseline pointer flag mismatch: {name}")
    return {
        "pointer": pointer,
        "identity_path": identity_path,
        "config_path": config_path,
    }


def build_params(day: str, config_path: Path) -> dict[str, Any]:
    from models import backtest_tick as bt
    from models.backtest_config import (
        load_tick_base_params,
        validate_formal_replay_calibration,
    )
    from models.replay_contract import configure_fixed_latency_distribution

    bt.BBO_DIR = BOOK_ROOT / "bbo"
    bt.L2_DIR = BOOK_ROOT / "l2"
    bt.configure_symbol("BTCUSDC", model_dir_override=MODEL_DIR)
    params = load_tick_base_params(
        symbol="BTCUSDC",
        config_path=config_path,
        configure_symbol=bt.configure_symbol,
        require_historical_bbo=True,
        queue_calibration_path=QUEUE_PATH,
        strict_calibration=True,
    )
    params.update(
        {
            "execution_trade_source": "trades",
            "market_context_warmup_days": 1,
            "replay_event_clock": "merged",
            "replay_clock_interval_ms": 100,
            "queue_ahead_mode": "exact_level",
            "queue_l2_cancel_ahead_enabled": False,
            "collect_curves": False,
            "trace_fills_max": 100_000,
            "trace_quotes_max": 0,
            "dynamic_fill_hazard_shadow_enabled": True,
            "dynamic_fill_hazard_action_enabled": False,
            "dynamic_fill_hazard_cpp_parity_enabled": False,
            "buy_fill_selection_shadow_enabled": True,
            "buy_fill_selection_live_enabled": False,
            "ml_enabled": True,
            "model_dir": str(MODEL_DIR),
            "resolved_model_dir": str(MODEL_DIR),
            "markout_side_asymmetry_sign": 1.0,
            "window_cache_write_enabled": False,
            "legacy_monolithic_window_cache_write_enabled": False,
            "replay_purpose": "restart_aware_continuous_baseline_diagnostic",
            "replay_promotion_eligible": False,
            "_formal_quality_allowed_days": [
                (date.fromisoformat(day) - timedelta(days=1)).isoformat(),
                day,
            ],
            "sync_adjust_replay_mode": "disabled",
            "sync_adjust_stress_seed": 20260729,
            "sync_adjust_stress_interval_s": 21_600.0,
        }
    )
    samples = bt._load_live_perf_latency_samples(LATENCY_PATH, mode="avg")
    params["_new_order_latency_samples_ms"] = samples["new_order_latency_samples_ms"]
    params["_cancel_order_latency_samples_ms"] = samples[
        "cancel_order_latency_samples_ms"
    ]
    configure_fixed_latency_distribution(
        params,
        scenario="baseline",
        profile_id="aws_tokyo_2vcpu4g_amzn2023_rest_20260710_14",
        environment="aws-ap-northeast-1-tokyo",
        baseline_clip_quantile=0.99,
    )
    validate_formal_replay_calibration(params, require_latency=True)
    if bool(params.get("buy_fill_selection_live_enabled")):
        raise RuntimeError("current baseline unexpectedly enables BUY fill-selection action")
    if bool(params.get("dynamic_fill_hazard_action_enabled")):
        raise RuntimeError("current baseline unexpectedly enables q90 action")
    return params


def _bootstrap_ci(values: np.ndarray, *, seed: int = 20260803) -> list[float]:
    if values.size == 0:
        return [0.0, 0.0]
    rng = np.random.default_rng(seed)
    draws = rng.choice(values, size=(10_000, values.size), replace=True).mean(axis=1)
    return [float(np.quantile(draws, 0.025)), float(np.quantile(draws, 0.975))]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--max-segments", type=int, default=0)
    args = parser.parse_args()
    if args.max_segments < 0:
        raise ValueError("max-segments cannot be negative")
    output_root = args.output_root.expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    os.chdir(ROOT)

    from models import backtest_tick as bt
    from models.data_windows import load_tick_window
    from models.replay.continuous_accounting import ContinuousAccountingLedger
    from models.replay.continuous_calendar import CalendarReplayPlan, ReplayMode
    from models.replay.continuous_tick_runner import (
        SCHEMA_VERSION as RUNNER_SCHEMA,
    )
    from models.replay.continuous_tick_runner import (
        assert_planned_shutdown_drained,
        build_active_segments,
        expected_segment_local_pnl_delta,
        requires_new_campaign_id,
    )
    from models.replay.replay_state_checkpoint import ContinuousReplayState

    started = time.perf_counter()
    identities = validate_identities()
    calendar_payload = json.loads(CALENDAR_MANIFEST.read_text(encoding="utf-8"))
    plan = CalendarReplayPlan.from_manifest(
        CALENDAR_MANIFEST,
        mode=ReplayMode.ANCHOR_PANEL_CONTINUOUS,
    )
    cancel_drain_ms = int(calendar_payload["cancel_drain_ms"])
    all_segments = build_active_segments(plan, cancel_drain_ms=cancel_drain_ms)
    segments = (
        all_segments[: args.max_segments]
        if args.max_segments
        else all_segments
    )
    complete_run = len(segments) == len(all_segments)
    day_rows = {
        str(row["day"]): row
        for row in calendar_payload["day_sources"]
        if plan.calendar_start_day <= str(row["day"]) <= plan.calendar_end_day
    }
    marks = CausalMarkStore(day_rows)
    ledger: ContinuousAccountingLedger | None = None
    campaign_ordinal = 0
    previous_end_ts: int | None = None
    previous_end_mark: float | None = None
    previous_gap_id = ""
    segment_rows: list[dict[str, Any]] = []
    current_day = ""
    window = None
    params: dict[str, Any] | None = None

    def record_gap_parts(
        *,
        gap_id: str,
        start_ts_ms: int,
        end_ts_ms: int,
        start_mark: float,
        end_mark: float,
    ) -> None:
        nonlocal ledger
        assert ledger is not None
        cursor = int(start_ts_ms)
        cursor_mark = float(start_mark)
        part = 0
        boundary = ((cursor // DAY_MS) + 1) * DAY_MS
        while boundary <= int(end_ts_ms):
            boundary_mark = marks.at_or_before(boundary)
            if boundary > cursor:
                part += 1
                ledger.record_gap(
                    gap_id=f"{gap_id}#P{part:03d}",
                    start_ts_ms=cursor,
                    end_ts_ms=boundary,
                    start_mark_price=cursor_mark,
                    end_mark_price=boundary_mark,
                )
            ledger.close_utc_day(
                day_end_ts_ms=boundary,
                mark_price=boundary_mark,
            )
            cursor = boundary
            cursor_mark = boundary_mark
            boundary += DAY_MS
        if cursor < int(end_ts_ms):
            part += 1
            ledger.record_gap(
                gap_id=f"{gap_id}#P{part:03d}",
                start_ts_ms=cursor,
                end_ts_ms=int(end_ts_ms),
                start_mark_price=cursor_mark,
                end_mark_price=float(end_mark),
            )

    for segment in segments:
        if segment.day != current_day:
            del window
            gc.collect()
            current_day = segment.day
            params = build_params(current_day, identities["config_path"])
            window = load_tick_window(
                current_day,
                params,
                load_ml=True,
                require_ml=True,
                run_ml_inference=True,
                feature_dir=FEATURE_DIR,
                require_target_feature_files=True,
                cross_market_enabled=True,
                with_ml_cache=False,
                require_historical_bbo=True,
                require_formal_l2=False,
                cache_dir=CACHE_DIR,
                refresh_cache=False,
            )
            if window.book_source_authority != "native_formal_lifecycle":
                raise RuntimeError(
                    f"{current_day} source authority={window.book_source_authority}"
                )
        assert window is not None and params is not None
        mask = (
            (window.trades["transact_time"] >= segment.start_ts_ms)
            & (window.trades["transact_time"] < segment.end_ts_ms)
        )
        segment_trades = window.trades.loc[mask].copy()
        if segment_trades.empty:
            raise RuntimeError(f"{segment.segment_id} has no execution trades")
        real_execution_trade_rows = int(len(segment_trades))
        scheduler_sentinel_added = False
        if not segment.terminal_censor:
            sentinel_ts = int(segment.end_ts_ms - 1)
            if int(segment_trades["transact_time"].iloc[-1]) < sentinel_ts:
                sentinel = segment_trades.iloc[[-1]].copy()
                sentinel.loc[:, "transact_time"] = sentinel_ts
                sentinel.loc[:, "price"] = marks.at_or_before(sentinel_ts)
                quantity_column = (
                    "quantity" if "quantity" in sentinel.columns else "qty"
                )
                sentinel.loc[:, quantity_column] = 0.0
                sentinel.loc[:, "is_buyer_maker"] = False
                segment_trades = pd.concat(
                    [segment_trades, sentinel],
                    ignore_index=True,
                )
                scheduler_sentinel_added = True
        first_ts = int(segment_trades["transact_time"].iloc[0])
        first_price = float(segment_trades["price"].iloc[0])
        if ledger is None:
            state = ContinuousReplayState(
                arm_id=str(identities["pointer"]["baseline_id"]),
                checkpoint_ts_ms=first_ts,
                cash_usdc=0.0,
                position_btc=0.0,
                average_entry_price=0.0,
                cumulative_realized_pnl_usdc=0.0,
                cumulative_fees_usdc=0.0,
                equity_anchor_usdc=0.0,
                last_mark_price=first_price,
                cumulative_pnl_usdc=0.0,
                feature_warmup_ready=True,
                quoting_enabled=True,
            )
            ledger = ContinuousAccountingLedger(state)
        else:
            assert previous_end_ts is not None and previous_end_mark is not None
            record_gap_parts(
                gap_id=previous_gap_id,
                start_ts_ms=previous_end_ts,
                end_ts_ms=first_ts,
                start_mark=previous_end_mark,
                end_mark=first_price,
            )
            ledger.resume_after_warmup(
                decision_ts_ms=first_ts,
                feature_ready_ts_ms=first_ts,
            )
        ledger.mark(first_ts, first_price)
        segment_start_equity = ledger.equity_usdc
        initial_inventory = float(ledger.state.position_btc)
        initial_entry = float(ledger.state.average_entry_price)
        run_params = dict(params)
        run_params["initial_inventory"] = initial_inventory
        run_params["initial_entry_price"] = initial_entry
        run_params["planned_quote_stop_ts_ms"] = int(
            segment.planned_quote_stop_ts_ms
        )
        simulated_at = time.perf_counter()
        result = bt._simulate_tick_with_engine(
            "cpp",
            segment_trades,
            window.var_ts_ms,
            window.var_ssq,
            run_params,
            ml_data=window.ml_data,
            bbo_data=window.bbo_data,
            l2_data=window.l2_data,
            var_ti=window.var_ti,
            var_retsq=window.var_retsq,
        )
        if not segment.terminal_censor:
            assert_planned_shutdown_drained(result)
        fill_trace = sorted(
            list(result.get("_fill_trace") or []),
            key=lambda row: (int(row["fill_ts"]), int(row.get("order_id", -1))),
        )
        if len(fill_trace) != int(result["fills_total"]):
            raise RuntimeError(f"{segment.segment_id} fill trace was truncated")
        for fill in fill_trace:
            side = str(fill["side"]).upper()
            quantity = float(fill["fill_qty"])
            price = float(fill["quote_px"])
            fee_usdc = float(fill["fill_fee_usdc"])
            if fee_usdc < 0:
                raise RuntimeError("continuous accounting does not accept an unbound rebate")
            new_id = None
            if requires_new_campaign_id(
                ledger.state.position_btc,
                side=side,
                quantity_btc=quantity,
            ):
                campaign_ordinal += 1
                new_id = f"EC-{campaign_ordinal:08d}"
            ledger.fill(
                ts_ms=int(fill["fill_ts"]),
                side=side,
                quantity_btc=quantity,
                price=price,
                fee_usdc=fee_usdc,
                new_campaign_id=new_id,
            )
        final_mark = float(result["terminal_mark_price"])
        final_event_ts = int(segment_trades["transact_time"].iloc[-1])
        ledger.mark(final_event_ts, final_mark)
        observed_delta = ledger.equity_usdc - segment_start_equity
        expected_delta = expected_segment_local_pnl_delta(
            terminal_mtm_pnl_usdc=float(result["terminal_mtm_pnl"]),
            initial_inventory_btc=initial_inventory,
            initial_entry_price=initial_entry,
            first_mark_price=first_price,
        )
        accounting_error = observed_delta - expected_delta
        if abs(accounting_error) > 1e-6:
            raise RuntimeError(
                f"{segment.segment_id} C++/continuous accounting error={accounting_error}; "
                f"observed_delta={observed_delta}; expected_delta={expected_delta}; "
                f"terminal_mtm={result['terminal_mtm_pnl']}; "
                f"initial_inventory={initial_inventory}; initial_entry={initial_entry}; "
                f"first_mark={first_price}; final_mark={final_mark}"
            )
        if not math.isclose(
            ledger.state.position_btc,
            float(result["final_inventory"]),
            rel_tol=0.0,
            abs_tol=1e-10,
        ):
            raise RuntimeError(f"{segment.segment_id} inventory parity failed")

        boundary_mark = marks.at_or_before(segment.end_ts_ms)
        if final_event_ts < segment.end_ts_ms:
            ledger.record_gap(
                gap_id=f"{segment.gap_after_id}#CANCEL_DRAIN_TAIL",
                start_ts_ms=final_event_ts,
                end_ts_ms=segment.end_ts_ms,
                start_mark_price=final_mark,
                end_mark_price=boundary_mark,
            )
        if segment.end_ts_ms % DAY_MS == 0:
            ledger.close_utc_day(
                day_end_ts_ms=segment.end_ts_ms,
                mark_price=boundary_mark,
            )
        if not segment.terminal_censor:
            ledger.enter_planned_restart(segment.end_ts_ms)
        previous_end_ts = segment.end_ts_ms
        previous_end_mark = boundary_mark
        previous_gap_id = segment.gap_after_id
        row = {
            **asdict(segment),
            "quality_grade": str(day_rows[segment.day]["quality_grade"]),
            "source_authority": str(window.book_source_authority),
            "execution_trade_rows": real_execution_trade_rows,
            "scheduler_sentinel_added": scheduler_sentinel_added,
            "fills_bid": int(result["fills_bid"]),
            "fills_ask": int(result["fills_ask"]),
            "fills_total": int(result["fills_total"]),
            "initial_inventory_btc": initial_inventory,
            "final_inventory_btc": float(result["final_inventory"]),
            "active_abs_inventory_time_btc_s": float(
                result["abs_inventory_time_s"]
            ),
            "active_pnl_delta_usdc": observed_delta,
            "accounting_error_usdc": accounting_error,
            "planned_quote_stop_trigger_ts_ms": int(
                result["planned_quote_stop_trigger_ts_ms"]
            ),
            "planned_shutdown_orders_at_trigger": int(
                result["planned_shutdown_orders_at_trigger"]
            ),
            "planned_shutdown_remaining_order_count": int(
                result["planned_shutdown_open_order_count"]
                + result["planned_shutdown_pending_new_order_count"]
                + result["planned_shutdown_pending_cancel_order_count"]
            ),
            "terminal_censor": bool(segment.terminal_censor),
            "runtime_s": time.perf_counter() - simulated_at,
        }
        segment_rows.append(row)
        print(
            f"DONE {segment.segment_id} {segment.day} "
            f"pnl_delta={observed_delta:+.6f} fills={row['fills_total']} "
            f"inventory={row['final_inventory_btc']:+.6f} "
            f"runtime={row['runtime_s']:.2f}s",
            flush=True,
        )
        del result, fill_trace, segment_trades
        gc.collect()

    assert ledger is not None
    if complete_run:
        if previous_end_ts is None or previous_end_mark is None:
            raise RuntimeError("complete run lost its final maintenance boundary")
        calendar_end = (
            int(
                datetime.fromisoformat(plan.calendar_end_day)
                .replace(tzinfo=UTC)
                .timestamp()
                * 1_000
            )
            + DAY_MS
        )
        if previous_end_ts < calendar_end:
            final_mark = marks.at_or_before(calendar_end)
            record_gap_parts(
                gap_id=previous_gap_id,
                start_ts_ms=previous_end_ts,
                end_ts_ms=calendar_end,
                start_mark=previous_end_mark,
                end_mark=final_mark,
            )
        if len(ledger.daily_slices) != plan.calendar_day_count:
            raise RuntimeError(
                "daily accounting denominator mismatch: "
                f"{len(ledger.daily_slices)} != {plan.calendar_day_count}"
            )

    segment_frame = pd.DataFrame(segment_rows)
    daily_frame = pd.DataFrame(asdict(row) for row in ledger.daily_slices)
    campaign_frame = pd.DataFrame(asdict(row) for row in ledger.closed_campaigns)
    gap_frame = pd.DataFrame(asdict(row) for row in ledger.gap_carries)
    segment_path = output_root / "segments.parquet"
    daily_path = output_root / "daily.parquet"
    campaign_path = output_root / "campaigns.parquet"
    gap_path = output_root / "gap_carry.parquet"
    segment_frame.to_parquet(segment_path, index=False)
    daily_frame.to_parquet(daily_path, index=False)
    campaign_frame.to_parquet(campaign_path, index=False)
    gap_frame.to_parquet(gap_path, index=False)

    daily_values = (
        daily_frame["pnl_usdc"].to_numpy(dtype=np.float64)
        if not daily_frame.empty
        else np.empty(0, dtype=np.float64)
    )
    campaign_values = (
        campaign_frame["value_usdc"].to_numpy(dtype=np.float64)
        if not campaign_frame.empty
        else np.empty(0, dtype=np.float64)
    )
    active_inventory_time = float(
        segment_frame["active_abs_inventory_time_btc_s"].sum()
    )
    gap_inventory_time = float(
        sum(
            abs(row.position_btc) * (row.end_ts_ms - row.start_ts_ms) / 1_000.0
            for row in ledger.gap_carries
        )
    )
    fresh_payload = json.loads(FRESH_BASELINE_REPORT.read_text(encoding="utf-8"))
    fresh_pnl = float(fresh_payload["economics"]["terminal_mtm_pnl_usdc"])
    audit = ledger.accounting_audit()
    q10 = float(np.quantile(campaign_values, 0.10)) if campaign_values.size else 0.0
    multi = (
        campaign_frame[campaign_frame["peak_abs_inventory_btc"] >= 0.0015]
        if not campaign_frame.empty
        else campaign_frame
    )
    report = {
        "schema_version": "current_operational_baseline_restart_aware_continuous.v1",
        "created_at_utc": datetime.now(UTC).isoformat(),
        "complete_run": complete_run,
        "runner_schema": RUNNER_SCHEMA,
        "baseline_id": identities["pointer"]["baseline_id"],
        "calendar": {
            "mode": plan.mode.value,
            "calendar_start_day": plan.calendar_start_day,
            "calendar_end_day": plan.calendar_end_day,
            "calendar_days": plan.calendar_day_count,
            "anchor_active_days": len(plan.active_days),
            "active_segments": len(segments),
            "frozen_active_segments": len(all_segments),
            "restart_intervals": len(plan.restart_intervals),
            "timeline_sha256": plan.timeline_sha256,
            "provider_l2_used_for_queue_or_policy": False,
            "provider_normalized_bbo_used_for_offline_inventory_mark": any(
                row["authority"] == "tardis_provider_normalized_bbo_mid"
                for row in marks.source_manifest()
            ),
            "offline_mark_sources": marks.source_manifest(),
        },
        "execution": {
            "engine": "cpp",
            "ml": "causal_v12_semantics_v6_on",
            "q90_shadow": True,
            "q90_action": False,
            "buy_fill_selection_shadow": False,
            "buy_fill_selection_action": False,
            "queue": "native_formal_exact_level_on_anchor_segments",
            "cache_read_root": str(CACHE_DIR),
            "cache_writes_enabled": False,
            "non_cache_output_root": str(output_root),
        },
        "result": {
            "continuous_terminal_mtm_pnl_usdc": float(
                ledger.state.cumulative_pnl_usdc
            ),
            "pnl_per_anchor_active_day_usdc": float(
                ledger.state.cumulative_pnl_usdc / len(plan.active_days)
            ),
            "pnl_per_calendar_day_usdc": float(
                ledger.state.cumulative_pnl_usdc / plan.calendar_day_count
            ),
            "positive_calendar_days": int(np.sum(daily_values > 0.0)),
            "positive_calendar_day_rate": float(
                np.mean(daily_values > 0.0) if daily_values.size else 0.0
            ),
            "mean_daily_pnl_ci95_bootstrap_usdc": _bootstrap_ci(daily_values),
            "fills_total": int(segment_frame["fills_total"].sum()),
            "fills_bid": int(segment_frame["fills_bid"].sum()),
            "fills_ask": int(segment_frame["fills_ask"].sum()),
            "final_inventory_btc": float(ledger.state.position_btc),
            "active_abs_inventory_time_btc_s": active_inventory_time,
            "gap_abs_inventory_time_btc_s": gap_inventory_time,
            "continuous_abs_inventory_time_btc_s": (
                active_inventory_time + gap_inventory_time
            ),
            "gap_inventory_pnl_usdc": float(audit["gap_inventory_pnl_usdc"]),
            "closed_campaigns": int(len(campaign_frame)),
            "closed_campaign_value_usdc": float(campaign_values.sum()),
            "campaign_q10_usdc": q10,
            "campaign_cvar10_usdc": float(
                campaign_values[campaign_values <= q10].mean()
                if campaign_values.size
                else 0.0
            ),
            "multi_level_campaigns": int(len(multi)),
            "multi_level_terminal_value_usdc": float(
                multi["value_usdc"].sum() if not multi.empty else 0.0
            ),
            "max_segment_accounting_error_usdc": float(
                segment_frame["accounting_error_usdc"].abs().max()
            ),
            "planned_shutdown_failures": int(
                (
                    (~segment_frame["terminal_censor"])
                    & (
                        segment_frame["planned_shutdown_remaining_order_count"]
                        != 0
                    )
                ).sum()
            ),
            "daily_additivity_error_usdc": float(
                audit["closed_daily_additivity_error_usdc"]
            ),
        },
        "fresh_start_comparison": {
            "fresh_40_day_current_baseline_pnl_usdc": fresh_pnl,
            "continuous_minus_fresh_usdc": float(
                ledger.state.cumulative_pnl_usdc - fresh_pnl
            ),
            "comparison_is_causal_tail_governance_evidence": False,
            "interpretation": (
                "calendar availability, planned restart state transport, and continuous "
                "inventory accounting differ jointly from the daily fresh-start diagnostic"
            ),
        },
        "identity": {
            "baseline_pointer": {
                "path": str(BASELINE_POINTER),
                "sha256": sha256_file(BASELINE_POINTER),
            },
            "baseline_identity": {
                "path": str(identities["identity_path"]),
                "sha256": sha256_file(identities["identity_path"]),
            },
            "live_config": {
                "path": str(identities["config_path"]),
                "sha256": sha256_file(identities["config_path"]),
            },
            "calendar_manifest": {
                "path": str(CALENDAR_MANIFEST),
                "sha256": sha256_file(CALENDAR_MANIFEST),
            },
            "coverage_report": {
                "path": str(COVERAGE_REPORT),
                "sha256": sha256_file(COVERAGE_REPORT),
            },
            "feature_manifest": {
                "path": str(FEATURE_DIR / "causal_feature_manifest.json"),
                "sha256": sha256_file(FEATURE_DIR / "causal_feature_manifest.json"),
            },
            "model_bundle_meta": {
                "path": str(MODEL_DIR / "bundle_meta.json"),
                "sha256": sha256_file(MODEL_DIR / "bundle_meta.json"),
            },
            "queue_calibration": {
                "path": str(QUEUE_PATH),
                "sha256": sha256_file(QUEUE_PATH),
            },
            "latency_samples": {
                "path": str(LATENCY_PATH),
                "sha256": sha256_file(LATENCY_PATH),
            },
            "backtest_tick": {
                "path": str(ROOT / "models/backtest_tick.py"),
                "sha256": sha256_file(ROOT / "models/backtest_tick.py"),
            },
            "continuous_tick_runner": {
                "path": str(ROOT / "models/replay/continuous_tick_runner.py"),
                "sha256": sha256_file(ROOT / "models/replay/continuous_tick_runner.py"),
            },
            "runner": {
                "path": str(Path(__file__).resolve()),
                "sha256": sha256_file(Path(__file__).resolve()),
            },
        },
        "permissions": {
            "diagnostic_only": True,
            "independent_confirmation": False,
            "action_authority": False,
            "live_authority": False,
        },
        "runtime_s": time.perf_counter() - started,
    }
    report_path = output_root / "report.json"
    atomic_json(report_path, report)
    manifest = {
        "schema_version": "current_operational_baseline_restart_aware_continuous.manifest.v1",
        "files": {
            name: {"path": str(path), "sha256": sha256_file(path)}
            for name, path in {
                "segments": segment_path,
                "daily": daily_path,
                "campaigns": campaign_path,
                "gap_carry": gap_path,
                "report": report_path,
            }.items()
        },
    }
    atomic_json(output_root / "manifest.json", manifest)
    print(json.dumps(report["result"], indent=2, sort_keys=True), flush=True)
    print(json.dumps(report["fresh_start_comparison"], indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
