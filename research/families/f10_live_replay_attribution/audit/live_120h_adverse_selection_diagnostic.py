"""Recompute the frozen 120-hour live adverse-selection diagnostic.

This module validates observational evidence only. It does not fit a model,
estimate a treatment effect, or authorize a strategy change.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

SCHEMA_VERSION = "live_120h_adverse_selection_diagnostic.v1"
IDENTITY = "live_120h_adverse_selection_diagnostic_v1"
ROOT = Path(__file__).resolve().parents[4]
DEFAULT_SPEC = (
    ROOT
    / "research"
    / "families"
    / "f10_live_replay_attribution"
    / "docs"
    / "live_120h_adverse_selection_diagnostic_v1_spec_20260730.json"
)

EXACT_FILL_COLUMNS = frozenset(
    {
        "timestamp",
        "side",
        "role",
        "commission",
        "age_ms",
        "entry_edge_bps",
        "observation_delay_10s",
        "value_10s_bps",
        "value_10s_usdc",
        "market_move_10s_bps",
    }
)
CAMPAIGN_COLUMNS = frozenset(
    {
        "campaign_sequence",
        "opening_side",
        "max_abs_position",
        "pnl_usdc",
    }
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_spec_sha256(payload: Mapping[str, Any]) -> str:
    normalized = dict(payload)
    normalized.pop("canonical_spec_sha256", None)
    raw = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def validate_spec(payload: Mapping[str, Any]) -> None:
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected live diagnostic schema")
    if payload.get("identity") != IDENTITY:
        raise ValueError("unexpected live diagnostic identity")
    frozen_hash = str(payload.get("canonical_spec_sha256", ""))
    if len(frozen_hash) != 64 or canonical_spec_sha256(payload) != frozen_hash:
        raise ValueError("live diagnostic spec hash mismatch")
    if payload.get("estimand", {}).get("unit") != "maker_signed_bps_and_USDC":
        raise ValueError("live diagnostic unit drifted")
    if payload.get("horizon", {}).get("observation_rule") != (
        "first_quote_decision_at_or_after_fill_plus_10s"
    ):
        raise ValueError("live diagnostic horizon drifted")
    permissions = payload.get("permissions") or {}
    if any(bool(value) for value in permissions.values()):
        raise ValueError("live observational evidence cannot grant authority")
    implementation = payload.get("implementation_identity") or {}
    expected_paths = {
        "evaluator_sha256": Path(__file__).resolve(),
        "test_sha256": ROOT / "tests" / "test_live_120h_adverse_selection.py",
    }
    for key, path in expected_paths.items():
        if not path.is_file() or sha256_file(path) != str(implementation.get(key, "")):
            raise ValueError(f"live diagnostic implementation drifted: {key}")


def _require_columns(frame: pd.DataFrame, required: frozenset[str], name: str) -> None:
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{name} is missing columns: {', '.join(missing)}")


def _mean(series: pd.Series) -> float:
    return float(pd.to_numeric(series, errors="raise").mean())


def _fill_summary(
    frame: pd.DataFrame,
    *,
    total_fills: int | None = None,
) -> dict[str, float | int]:
    total = len(frame) if total_fills is None else int(total_fills)
    return {
        "fills": total,
        "valid_10s": int(len(frame)),
        "coverage_10s": float(len(frame) / total),
        "entry_edge_bps_mean": _mean(frame["entry_edge_bps"]),
        "market_move_10s_bps_mean": _mean(frame["market_move_10s_bps"]),
        "value_10s_bps_mean": _mean(frame["value_10s_bps"]),
        "value_10s_win_rate": float((frame["value_10s_bps"] > 0.0).mean()),
        "value_10s_usdc_sum": float(frame["value_10s_usdc"].sum()),
    }


def _campaign_summary(
    frame: pd.DataFrame,
    *,
    minimum_order_size_btc: float,
) -> dict[str, float | int]:
    wins = frame.loc[frame["pnl_usdc"] > 0.0, "pnl_usdc"]
    losses = frame.loc[frame["pnl_usdc"] < 0.0, "pnl_usdc"]
    multi = frame.loc[
        frame["max_abs_position"] > float(minimum_order_size_btc) + 1e-12
    ]
    shorts = frame.loc[frame["opening_side"].eq("SHORT")]
    return {
        "campaigns": int(len(frame)),
        "win_rate": float((frame["pnl_usdc"] > 0.0).mean()),
        "average_win_usdc": float(wins.mean()),
        "average_loss_usdc": float(losses.mean()),
        "pnl_usdc_sum": float(frame["pnl_usdc"].sum()),
        "multi_inventory_campaigns": int(len(multi)),
        "multi_inventory_rate": float(len(multi) / len(frame)),
        "multi_inventory_pnl_usdc_sum": float(multi["pnl_usdc"].sum()),
        "multi_inventory_share_of_signed_loss": float(
            multi["pnl_usdc"].sum() / frame["pnl_usdc"].sum()
        ),
        "short_campaigns": int(len(shorts)),
        "short_campaign_pnl_usdc_sum": float(shorts["pnl_usdc"].sum()),
    }


def evaluate_frames(
    exact_fills: pd.DataFrame,
    campaigns: pd.DataFrame,
    *,
    minimum_order_size_btc: float = 0.001,
) -> dict[str, Any]:
    _require_columns(exact_fills, EXACT_FILL_COLUMNS, "exact fills")
    _require_columns(campaigns, CAMPAIGN_COLUMNS, "campaigns")
    if exact_fills.empty or campaigns.empty:
        raise ValueError("live diagnostic inputs cannot be empty")

    fills = exact_fills.copy()
    numeric_fill_columns = (
        "timestamp",
        "commission",
        "age_ms",
        "entry_edge_bps",
        "observation_delay_10s",
        "value_10s_bps",
        "value_10s_usdc",
        "market_move_10s_bps",
    )
    for column in numeric_fill_columns:
        fills[column] = pd.to_numeric(fills[column], errors="coerce")
    if not fills["side"].isin(("BUY", "SELL")).all():
        raise ValueError("exact fills contain an invalid side")

    valid = fills.loc[fills["value_10s_bps"].notna()].copy()
    if valid.empty:
        raise ValueError("exact fills contain no usable 10-second observations")
    required_valid = (
        "entry_edge_bps",
        "market_move_10s_bps",
        "value_10s_bps",
        "value_10s_usdc",
        "observation_delay_10s",
        "age_ms",
    )
    if valid.loc[:, required_valid].isna().any().any():
        raise ValueError("usable exact fills contain an incomplete value row")
    accounting_error = (
        valid["entry_edge_bps"]
        + valid["market_move_10s_bps"]
        - valid["value_10s_bps"]
    ).abs()
    if float(accounting_error.max()) > 1e-9:
        raise ValueError("spread plus market move does not equal maker value")

    valid["day"] = pd.to_datetime(
        valid["timestamp"], unit="s", utc=True
    ).dt.strftime("%Y-%m-%d")
    daily = (
        valid.groupby("day", sort=True)
        .agg(
            fills=("value_10s_bps", "size"),
            value_10s_bps_mean=("value_10s_bps", "mean"),
            value_10s_usdc_sum=("value_10s_usdc", "sum"),
            value_10s_win_rate=("value_10s_bps", lambda x: (x > 0.0).mean()),
        )
        .reset_index()
    )

    campaigns = campaigns.copy()
    for column in ("max_abs_position", "pnl_usdc"):
        campaigns[column] = pd.to_numeric(campaigns[column], errors="raise")
    if campaigns["campaign_sequence"].duplicated().any():
        raise ValueError("campaign evidence contains duplicate campaign_sequence")

    side = {}
    for key, all_group in fills.groupby("side", sort=True):
        valid_group = valid.loc[valid["side"].eq(key)]
        side[str(key)] = _fill_summary(
            valid_group,
            total_fills=len(all_group),
        )
    side_role = {}
    for (side_key, role), all_group in fills.groupby(
        ["side", "role"], sort=True
    ):
        valid_group = valid.loc[
            valid["side"].eq(side_key) & valid["role"].eq(role)
        ]
        side_role[f"{side_key}_{role}"] = _fill_summary(
            valid_group,
            total_fills=len(all_group),
        )
    age_slices = {}
    age_masks = {
        "under_1s": valid["age_ms"] < 1000.0,
        "4p5_to_5p5s": valid["age_ms"].between(4500.0, 5500.0),
    }
    for name, mask in age_masks.items():
        group = valid.loc[mask]
        age_slices[name] = {
            "fills": int(len(group)),
            "value_10s_bps_mean": _mean(group["value_10s_bps"]),
            "value_10s_win_rate": float((group["value_10s_bps"] > 0.0).mean()),
        }

    return {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "observational_only": True,
        "all": _fill_summary(valid, total_fills=len(fills)),
        "by_side": side,
        "by_side_role": side_role,
        "daily": daily.to_dict(orient="records"),
        "all_daily_value_means_negative": bool(
            daily["value_10s_bps_mean"].lt(0.0).all()
        ),
        "age_slices": age_slices,
        "observation_delay_10s": {
            "mean_s": _mean(valid["observation_delay_10s"]),
            "median_s": float(valid["observation_delay_10s"].median()),
            "p90_s": float(valid["observation_delay_10s"].quantile(0.90)),
        },
        "commission": {
            "nonzero_rows": int(fills["commission"].ne(0.0).sum()),
            "sum_reported_asset_units": float(fills["commission"].sum()),
        },
        "campaigns": _campaign_summary(
            campaigns,
            minimum_order_size_btc=minimum_order_size_btc,
        ),
        "contracts": {
            "maker_value_accounting_max_abs_error_bps": float(
                accounting_error.max()
            ),
            "minimum_order_size_btc": float(minimum_order_size_btc),
            "restart_sensitivity_bound_to_source_logs": False,
            "action_or_live_authorization": False,
        },
    }


def _assert_close(
    actual: Any,
    expected: Any,
    *,
    path: str,
    atol: float,
) -> None:
    if isinstance(expected, Mapping):
        if not isinstance(actual, Mapping):
            raise ValueError(f"frozen claim type drifted at {path}")
        for key, value in expected.items():
            if key not in actual:
                raise ValueError(f"frozen claim missing at {path}.{key}")
            _assert_close(
                actual[key],
                value,
                path=f"{path}.{key}",
                atol=atol,
            )
        return
    if isinstance(expected, list):
        if not isinstance(actual, list) or len(actual) != len(expected):
            raise ValueError(f"frozen claim length drifted at {path}")
        for index, value in enumerate(expected):
            _assert_close(
                actual[index],
                value,
                path=f"{path}[{index}]",
                atol=atol,
            )
        return
    if isinstance(expected, bool) or isinstance(expected, str):
        if actual != expected:
            raise ValueError(f"frozen claim drifted at {path}")
        return
    if isinstance(expected, int):
        if int(actual) != expected:
            raise ValueError(f"frozen claim drifted at {path}")
        return
    if not np.isclose(float(actual), float(expected), atol=atol, rtol=0.0):
        raise ValueError(
            f"frozen claim drifted at {path}: {actual!r} != {expected!r}"
        )


def evaluate_spec(spec: Mapping[str, Any]) -> dict[str, Any]:
    validate_spec(spec)
    loaded: dict[str, Path] = {}
    for name, identity in (spec.get("artifacts") or {}).items():
        path = Path(str(identity.get("path", ""))).expanduser()
        if not path.is_file() or sha256_file(path) != str(identity.get("sha256", "")):
            raise ValueError(f"live diagnostic artifact identity drifted: {name}")
        loaded[str(name)] = path

    required = {"exact_mid_fills", "campaigns"}
    if not required.issubset(loaded):
        raise ValueError("live diagnostic spec is missing an authoritative artifact")
    report = evaluate_frames(
        pd.read_csv(loaded["exact_mid_fills"]),
        pd.read_csv(loaded["campaigns"]),
    )
    _assert_close(
        report,
        spec.get("frozen_claims") or {},
        path="report",
        atol=float(spec.get("claim_tolerance", 1e-12)),
    )
    report["artifact_sha256"] = {
        name: sha256_file(path) for name, path in loaded.items()
    }
    report["spec_sha256"] = canonical_spec_sha256(spec)
    return report


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC)
    parser.add_argument("--output-json", type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    spec = json.loads(args.spec.read_text())
    report = evaluate_spec(spec)
    payload = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output_json is None:
        print(payload, end="")
    else:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
