"""Prefix-preserving market-window stitching for F05 ADD-vs-WAIT forks.

The assignment and release locators in the frozen F05 panel are indexes into
the first day's arrays.  Generic time sorting can renumber those indexes, so
this module keeps every first-day array byte-for-byte in place and appends
only strictly later rows from the natural next UTC day.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd

from models.tick_data_types import HistoricalBBOData, HistoricalL2Data


class F05WindowStitchError(RuntimeError):
    """Raised when a continuation day cannot preserve the frozen prefix."""


@dataclass(frozen=True, slots=True)
class F05ReplayDay:
    day: str
    window: Any
    ml_data: tuple[Any, ...]
    identities: Mapping[str, str]

    def validate(self) -> None:
        date.fromisoformat(self.day)
        if getattr(self.window, "ml_data", None) is not None:
            raise F05WindowStitchError("F05 market window must be model-free")
        if not isinstance(self.ml_data, tuple) or not self.ml_data:
            raise F05WindowStitchError("F05 continuation lacks its ML overlay")
        if any(len(str(value)) != 64 for value in self.identities.values()):
            raise F05WindowStitchError("F05 day identity is incomplete")


def _require_natural_successor(first_day: str, second_day: str) -> None:
    expected = date.fromisoformat(first_day) + timedelta(days=1)
    if date.fromisoformat(second_day) != expected:
        raise F05WindowStitchError(
            f"continuation must use natural D+1: {first_day} -> {second_day}"
        )


def _validate_clock(clock: np.ndarray, *, role: str) -> np.ndarray:
    values = np.asarray(clock, dtype=np.int64)
    if values.ndim != 1 or len(values) == 0:
        raise F05WindowStitchError(f"{role} clock is empty or non-vector")
    if np.any(values[1:] <= values[:-1]):
        raise F05WindowStitchError(f"{role} clock is not strictly increasing")
    return values


def _append_arrays(
    first: Sequence[Any],
    second: Sequence[Any],
    *,
    role: str,
    validate_overlap_values: bool = True,
) -> tuple[np.ndarray, ...]:
    if len(first) != len(second) or len(first) < 2:
        raise F05WindowStitchError(f"{role} ABI differs across days")
    first_ts = _validate_clock(np.asarray(first[0]), role=f"{role} first")
    second_ts = _validate_clock(np.asarray(second[0]), role=f"{role} second")
    cut = int(np.searchsorted(second_ts, first_ts[-1], side="right"))
    overlap_ts = second_ts[:cut]
    if len(overlap_ts):
        positions = np.searchsorted(first_ts, overlap_ts)
        if np.any(positions >= len(first_ts)) or not np.array_equal(
            first_ts[positions], overlap_ts
        ):
            raise F05WindowStitchError(
                f"{role} continuation contains a pre-prefix timestamp"
            )
        if validate_overlap_values:
            for index, (left_raw, right_raw) in enumerate(
                zip(first[1:], second[1:], strict=True), start=1
            ):
                left = np.asarray(left_raw)
                right = np.asarray(right_raw)
                if left.shape[1:] != right.shape[1:]:
                    raise F05WindowStitchError(f"{role} value {index} shape drifted")
                if not np.array_equal(left[positions], right[:cut], equal_nan=True):
                    raise F05WindowStitchError(
                        f"{role} overlap changed frozen prefix value {index}"
                    )
    output: list[np.ndarray] = [np.concatenate((first_ts, second_ts[cut:]))]
    for index, (left_raw, right_raw) in enumerate(
        zip(first[1:], second[1:], strict=True), start=1
    ):
        left = np.asarray(left_raw)
        right = np.asarray(right_raw)
        if len(left) != len(first_ts) or len(right) != len(second_ts):
            raise F05WindowStitchError(f"{role} value {index} is misaligned")
        output.append(np.concatenate((left, right[cut:]), axis=0))
    return tuple(output)


def _stitch_trades(first: pd.DataFrame, second: pd.DataFrame) -> pd.DataFrame:
    for name, frame in (("first", first), ("second", second)):
        if "transact_time" not in frame.columns or frame.empty:
            raise F05WindowStitchError(f"{name} trade tape is invalid")
        clock = frame["transact_time"].to_numpy(dtype=np.int64, copy=False)
        if np.any(clock[1:] < clock[:-1]):
            raise F05WindowStitchError(f"{name} trade tape moved backward")
    first_last = int(first["transact_time"].iloc[-1])
    second_first = int(second["transact_time"].iloc[0])
    if second_first <= first_last:
        raise F05WindowStitchError("trade continuation overlaps the frozen day")
    return pd.concat((first, second), ignore_index=True)


def _stitch_bbo(first: HistoricalBBOData, second: HistoricalBBOData) -> HistoricalBBOData:
    values = _append_arrays(
        (
            first.ts_ms,
            first.best_bid,
            first.best_ask,
            first.bid_qty,
            first.ask_qty,
        ),
        (
            second.ts_ms,
            second.best_bid,
            second.best_ask,
            second.bid_qty,
            second.ask_qty,
        ),
        role="BBO",
    )
    return HistoricalBBOData(*values, source="f05_native_cross_day_prefix_preserved")


def _stitch_l2(first: HistoricalL2Data, second: HistoricalL2Data) -> HistoricalL2Data:
    values = _append_arrays(
        (
            first.ts_ms,
            first.bid_px,
            first.bid_qty,
            first.ask_px,
            first.ask_qty,
        ),
        (
            second.ts_ms,
            second.bid_px,
            second.bid_qty,
            second.ask_px,
            second.ask_qty,
        ),
        role="L2",
    )
    return HistoricalL2Data(*values, source="f05_native_cross_day_prefix_preserved")


def _stitch_ml(first: tuple[Any, ...], second: tuple[Any, ...]) -> tuple[Any, ...]:
    if len(first) != len(second):
        raise F05WindowStitchError("ML overlay ABI differs across days")
    first_clock = _validate_clock(np.asarray(first[0]), role="ML first")
    second_clock = _validate_clock(np.asarray(second[0]), role="ML second")
    if second_clock[0] <= first_clock[-1]:
        raise F05WindowStitchError("ML continuation overlaps frozen visibility rows")
    output: list[Any] = []
    for index, (left_raw, right_raw) in enumerate(zip(first, second, strict=True)):
        if isinstance(left_raw, Mapping):
            if not isinstance(right_raw, Mapping) or tuple(left_raw) != tuple(right_raw):
                raise F05WindowStitchError("ML feature mapping ABI drifted")
            output.append(
                {
                    str(name): np.concatenate(
                        (np.asarray(left_raw[name]), np.asarray(right_raw[name]))
                    )
                    for name in left_raw
                }
            )
            continue
        left = np.asarray(left_raw)
        right = np.asarray(right_raw)
        if left.ndim != 1 or right.ndim != 1:
            raise F05WindowStitchError(f"ML array {index} is not one-dimensional")
        if len(left) != len(first_clock) or len(right) != len(second_clock):
            raise F05WindowStitchError(f"ML array {index} is misaligned")
        output.append(np.concatenate((left, right)))
    return tuple(output)


def stitch_two_days(
    first: F05ReplayDay,
    second: F05ReplayDay,
) -> tuple[Any, tuple[Any, ...], dict[str, Any]]:
    """Return one continuous engine input while preserving every D locator."""

    first.validate()
    second.validate()
    _require_natural_successor(first.day, second.day)
    first_window = first.window
    second_window = second.window
    var_ssq = _append_arrays(
        (first_window.var_ts_ms, first_window.var_ssq),
        (second_window.var_ts_ms, second_window.var_ssq),
        role="variance ssq",
        validate_overlap_values=False,
    )
    var_ti = None
    if first_window.var_ti is not None or second_window.var_ti is not None:
        if first_window.var_ti is None or second_window.var_ti is None:
            raise F05WindowStitchError("variance TI availability differs across days")
        var_ti = _append_arrays(
            (first_window.var_ts_ms, first_window.var_ti),
            (second_window.var_ts_ms, second_window.var_ti),
            role="variance TI",
            validate_overlap_values=False,
        )[1]
    var_retsq = None
    if first_window.var_retsq is not None or second_window.var_retsq is not None:
        if first_window.var_retsq is None or second_window.var_retsq is None:
            raise F05WindowStitchError("variance return-square availability differs")
        var_retsq = _append_arrays(
            (first_window.var_ts_ms, first_window.var_retsq),
            (second_window.var_ts_ms, second_window.var_retsq),
            role="variance return-square",
            validate_overlap_values=False,
        )[1]
    bbo = _stitch_bbo(first_window.bbo_data, second_window.bbo_data)
    l2 = _stitch_l2(first_window.l2_data, second_window.l2_data)
    window = SimpleNamespace(
        trades=_stitch_trades(first_window.trades, second_window.trades),
        var_ts_ms=var_ssq[0],
        var_ssq=var_ssq[1],
        var_ti=var_ti,
        var_retsq=var_retsq,
        bbo_data=bbo,
        l2_data=l2,
        ml_data=None,
    )
    ml_data = _stitch_ml(first.ml_data, second.ml_data)
    prefix = {
        "trade_rows": len(first_window.trades),
        "variance_rows": len(first_window.var_ts_ms),
        "bbo_rows": len(first_window.bbo_data.ts_ms),
        "l2_rows": len(first_window.l2_data.ts_ms),
        "ml_rows": len(first.ml_data[0]),
    }
    if not np.array_equal(
        window.bbo_data.ts_ms[: prefix["bbo_rows"]], first_window.bbo_data.ts_ms
    ) or not np.array_equal(
        window.l2_data.ts_ms[: prefix["l2_rows"]], first_window.l2_data.ts_ms
    ):
        raise F05WindowStitchError("stitcher renumbered the frozen market prefix")
    audit = {
        "schema_version": "f05_ema_add_wait_two_day_window.v1",
        "first_day": first.day,
        "second_day": second.day,
        "first_day_prefix_rows": prefix,
        "source_identities": {
            first.day: dict(first.identities),
            second.day: dict(second.identities),
        },
        "utc_midnight_resets_state": False,
        "planned_restart_between_days": False,
    }
    return window, ml_data, audit


__all__ = [
    "F05ReplayDay",
    "F05WindowStitchError",
    "stitch_two_days",
]
