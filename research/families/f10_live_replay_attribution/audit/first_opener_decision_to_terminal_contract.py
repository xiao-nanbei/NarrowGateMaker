"""Contract for decision-visible first-opener value attribution.

The identity is observational and conditional on an opener order eventually
producing the campaign's first fill. It cannot establish operational quote
value or authorize an action without a later opportunity-denominator study.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

SCHEMA_VERSION = "sell_first_fill_conditional_value_feasibility.v3"
TRACE_SCHEMA_VERSION = "first_opener_decision_to_terminal_trace.v2"
IDENTITY = "sell_first_fill_conditional_value_feasibility_v3"
PRIMARY_ESTIMAND = "decision_to_flat_or_day_end_mtm_usdc"
ROOT = Path(__file__).resolve().parents[4]
MODEL_FEATURES = (
    "quote_distance_ticks",
    "decision_spread_ticks",
    "book_imbalance",
    "microprice_shift_bps",
    "near_depth_total_btc",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
)

REQUIRED_TRACE_COLUMNS = frozenset(
    {
        "trace_schema_version",
        "day",
        "quality_grade",
        "campaign_id",
        "decision_id",
        "decision_ts_ms",
        "order_id",
        "order_submit_ts_ms",
        "order_activation_ts_ms",
        "fill_ts_ms",
        "fill_price",
        "campaign_terminal_ts_ms",
        "campaign_terminal_kind",
        "campaign_closed",
        "campaign_censored",
        "campaign_terminal_inventory_btc",
        "campaign_terminal_mark_price_usdc_per_btc",
        "side",
        "inventory_role",
        "campaign_opened_by_fill",
        "exact_decision_order_fill_join",
        "decision_visible_feature_ready_ts_max_ms",
        "bbo_feature_ready_ts_ms",
        "l2_feature_ready_ts_ms",
        "decision_visible_bbo_cutoff_ts_ms",
        "decision_bbo_source_ts_ms",
        "decision_bbo_source_kind",
        "decision_visible_l2_cutoff_ts_ms",
        "decision_l2_source_ts_ms",
        "decision_visible_trade_cutoff_ts_ms",
        "exec_depth_visibility_source_offset_ms",
        "feature_clock_source",
        "decision_equity_usdc",
        "campaign_terminal_equity_usdc",
        "inventory_before_fill_btc",
        "inventory_after_fill_btc",
        "first_opener_fill_qty_btc",
        "remaining_qty_before_fill_btc",
        "remaining_qty_after_fill_btc",
        "first_fill_is_partial",
        "fill_while_cancel_pending",
        "cancel_request_ts_ms",
        "decision_mid_usdc_per_btc",
        "decision_best_bid_usdc_per_btc",
        "decision_best_ask_usdc_per_btc",
        "order_price_usdc_per_btc",
        "quote_distance_ticks",
        "decision_spread_ticks",
        "queue_ahead_btc",
        "queue_ahead_available",
        "queue_ahead_source",
        "queue_ahead_asof_ts_ns",
        "queue_ahead_decision_authority",
        "book_imbalance",
        "microprice_shift_bps",
        "near_depth_total_btc",
        "l2_book_refresh_ratio",
        "l2_book_cancel_ratio",
        "maker_fee_rate",
        "maker_fee_asset",
        "first_fill_fee_usdc",
        PRIMARY_ESTIMAND,
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
        raise ValueError("unexpected first-opener feasibility schema")
    if payload.get("identity") != IDENTITY:
        raise ValueError("unexpected first-opener feasibility identity")
    if payload.get("status") != (
        "frozen_before_v3_native_development_outcome_read"
    ):
        raise ValueError("first-opener feasibility status drifted")
    frozen_hash = str(payload.get("canonical_spec_sha256", ""))
    if len(frozen_hash) != 64 or canonical_spec_sha256(payload) != frozen_hash:
        raise ValueError("first-opener feasibility spec hash mismatch")

    estimand = payload.get("estimand") or {}
    if estimand.get("primary") != PRIMARY_ESTIMAND:
        raise ValueError("first-opener feasibility changed its primary estimand")
    if estimand.get("unit") != "USDC_per_first_opener_fill_decision":
        raise ValueError("first-opener feasibility unit drifted")
    if not bool(estimand.get("fill_conditioned_observational", False)):
        raise ValueError("first-opener feasibility must declare fill conditioning")
    if bool(estimand.get("operational_quote_value", True)):
        raise ValueError("first-opener feasibility cannot claim operational value")

    hypothesis = payload.get("hypothesis_source") or {}
    for label, identity in (
        ("120h hypothesis report", hypothesis.get("report_identity") or {}),
        ("120h hypothesis errata", hypothesis.get("errata_identity") or {}),
    ):
        path = ROOT / str(identity.get("path", ""))
        if not path.is_file() or sha256_file(path) != str(
            identity.get("sha256", "")
        ):
            raise ValueError(f"first-opener {label} identity drifted")
    if (
        hypothesis.get("role") != "hypothesis_generation_only"
        or not bool(hypothesis.get("never_training_or_confirmation_data", False))
    ):
        raise ValueError("first-opener hypothesis-source boundary drifted")

    panels = payload.get("panels") or {}
    primary = tuple(panels.get("development_primary_grade_a_days") or ())
    sensitivity = tuple(
        panels.get("development_sensitivity_grade_b_days") or ()
    )
    if len(primary) != 22 or len(sensitivity) != 11:
        raise ValueError("first-opener feasibility must preserve the 22A/11B split")
    if set(primary) & set(sensitivity):
        raise ValueError("first-opener primary and sensitivity days overlap")
    if primary != tuple(sorted(primary)) or sensitivity != tuple(
        sorted(sensitivity)
    ):
        raise ValueError("first-opener feasibility days must be chronological")
    if panels.get("grade_b_policy") != (
        "past_grade_a_model_transport_sensitivity_only_never_pooled_into_primary_gate"
    ):
        raise ValueError("first-opener Grade-B policy drifted")

    quality = payload.get("quality_identity") or {}
    for label, identity in (
        (
            "normalized L2 manifest",
            quality.get("normalized_l2_manifest_identity") or {},
        ),
        (
            "normalized L2 daily quality",
            quality.get("normalized_l2_daily_quality_identity") or {},
        ),
    ):
        path = Path(str(identity.get("path", ""))).expanduser().resolve()
        if not path.is_file() or sha256_file(path) != str(
            identity.get("sha256", "")
        ):
            raise ValueError(f"first-opener {label} identity drifted")
    if not bool(
        quality.get("target_and_previous_natural_day_must_be_formal", False)
    ):
        raise ValueError("first-opener D-1 formal context contract drifted")

    lifecycle = payload.get("native_lifecycle_contract") or {}
    if set(lifecycle.get("required_columns") or ()) != set(REQUIRED_TRACE_COLUMNS):
        raise ValueError("first-opener native lifecycle columns drifted")
    if lifecycle.get("join") != "decision_id_to_order_id_to_fill_to_campaign":
        raise ValueError("first-opener feasibility requires an exact lifecycle join")
    if not bool(lifecycle.get("coverage_must_equal_one_or_fail", False)):
        raise ValueError("first-opener feasibility cannot filter unmatched rows")
    if lifecycle.get("coverage_denominator") != (
        "campaigns_opened_by_an_order_submitted_with_true_opener_role"
    ):
        raise ValueError("first-opener feasibility denominator drifted")
    if float(lifecycle.get("minimum_true_opener_share_of_all_campaigns", 0.0)) != 0.99:
        raise ValueError("first-opener feasibility operational support gate drifted")
    if not bool(
        lifecycle.get(
            "cross_boundary_pending_cancel_reopen_is_unsupported_not_relabelled",
            False,
        )
    ):
        raise ValueError("first-opener feasibility must preserve reopen identity")

    features = payload.get("decision_visible_features") or {}
    forbidden = set(features.get("post_decision_diagnostic_only") or ())
    if not {"order_age_to_fill_ms", "active_age_to_fill_ms"}.issubset(forbidden):
        raise ValueError("first-opener order-age diagnostics must remain post-decision")
    model_features = set(features.get("model_features") or ())
    if model_features & forbidden:
        raise ValueError("first-opener model contains a post-decision feature")
    if tuple(features.get("model_features") or ()) != MODEL_FEATURES:
        raise ValueError("first-opener decision-visible model feature identity drifted")
    if not bool(features.get("queue_is_diagnostic_only", False)):
        raise ValueError("exchange-time queue must remain diagnostic-only")
    if not bool(features.get("source_asof_clock_required", False)):
        raise ValueError("first-opener source as-of clocks are required")
    if (
        int(features.get("minimum_unique_values_per_panel_side", 0)) != 2
        or float(
            features.get(
                "maximum_dominant_value_fraction_per_panel_side",
                0.0,
            )
        )
        != 0.995
        or float(
            features.get(
                "minimum_nonconstant_day_fraction_per_panel_side",
                0.0,
            )
        )
        != 0.25
    ):
        raise ValueError("first-opener feature support identity drifted")
    market = payload.get("market_contract") or {}
    if market.get("symbol") != "BTCUSDC" or float(
        market.get("tick_size_usdc_per_btc", 0.0)
    ) != 0.1:
        raise ValueError("first-opener market geometry contract drifted")

    chronology = payload.get("chronological_evaluation") or {}
    expected_grade_a = tuple(
        chronology.get("expected_grade_a_scored_oof_days") or ()
    )
    expected_grade_b = tuple(
        chronology.get("expected_grade_b_transport_oof_days") or ()
    )
    if (
        int(chronology.get("minimum_train_days", 0)) != 8
        or int(chronology.get("embargo_calendar_days", 0)) != 1
        or int(chronology.get("maximum_test_block_days", 0)) != 1
        or int(chronology.get("grade_b_transport_test_block_days", 0)) != 1
        or len(expected_grade_a) != 13
        or len(expected_grade_b) != 6
        or not set(expected_grade_a).issubset(primary)
        or not set(expected_grade_b).issubset(sensitivity)
        or chronology.get("grade_b_model_source")
        != "rolling_past_grade_a_refit_only"
    ):
        raise ValueError("first-opener chronological OOF identity drifted")

    inference = payload.get("inference") or {}
    if (
        int(inference.get("minimum_grade_a_oof_days", 0)) != 13
        or int(inference.get("minimum_grade_b_transport_oof_days", 0)) != 6
        or float(inference.get("minimum_selective_value_gap_usdc", -1.0))
        != 0.0
        or float(inference.get("candidate_rate_min", 0.0)) != 0.1
        or float(inference.get("candidate_rate_max", 0.0)) != 0.3
    ):
        raise ValueError("first-opener inference gate identity drifted")

    implementation = payload.get("implementation_identity") or {}
    for key, path in (
        ("contract_module_sha256", Path(__file__).resolve()),
        (
            "contract_test_sha256",
            ROOT / "tests" / "test_first_opener_decision_to_terminal_contract.py",
        ),
        (
            "evaluator_module_sha256",
            ROOT
            / "research"
            / "families"
            / "f05_fill_quality_quote_ev"
            / "audit"
            / "sell_first_fill_conditional_value.py",
        ),
        (
            "evaluator_test_sha256",
            ROOT / "tests" / "test_sell_first_fill_conditional_value.py",
        ),
    ):
        if not path.is_file() or sha256_file(path) != str(
            implementation.get(key, "")
        ):
            raise ValueError(f"first-opener implementation drifted: {key}")

    permissions = payload.get("permissions") or {}
    required_permissions = {
        "development_outcome_read",
        "validation_read",
        "sealed_holdout_read",
        "action_experiment_authorized",
        "live_deployment_authorized",
    }
    if set(permissions) != required_permissions or any(
        bool(value) for value in permissions.values()
    ):
        raise ValueError("first-opener feasibility cannot grant authority")


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    values = pd.to_numeric(frame[column], errors="coerce")
    if values.isna().any() or not np.isfinite(values.to_numpy(dtype=float)).all():
        raise ValueError(f"first-opener trace has invalid {column}")
    return values


def validate_native_trace(
    frame: pd.DataFrame,
    spec: Mapping[str, Any],
) -> pd.DataFrame:
    validate_spec(spec)
    panels = spec["panels"]
    grade_by_day = {
        **{
            str(day): "A"
            for day in panels["development_primary_grade_a_days"]
        },
        **{
            str(day): "B"
            for day in panels["development_sensitivity_grade_b_days"]
        },
    }
    return validate_native_trace_mechanics(
        frame,
        tick_size_usdc_per_btc=float(
            spec["market_contract"]["tick_size_usdc_per_btc"]
        ),
        quality_grade_by_day=grade_by_day,
    )


def validate_native_trace_mechanics(
    frame: pd.DataFrame,
    *,
    tick_size_usdc_per_btc: float,
    quality_grade_by_day: Mapping[str, str],
) -> pd.DataFrame:
    """Validate trace mechanics without requiring private frozen evidence."""

    missing = sorted(REQUIRED_TRACE_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError("first-opener native trace is missing: " + ", ".join(missing))
    if frame.empty:
        raise ValueError("first-opener native trace is empty")
    if not frame["trace_schema_version"].eq(TRACE_SCHEMA_VERSION).all():
        raise ValueError("first-opener native trace schema drifted")
    if frame.duplicated(["day", "campaign_id"]).any():
        raise ValueError("first-opener native trace has multiple rows per campaign")
    if frame["decision_id"].astype(str).duplicated().any():
        raise ValueError("first-opener decision_id is not unique")
    if frame.assign(_order_id=frame["order_id"].astype(str)).duplicated(
        ["day", "_order_id"]
    ).any():
        raise ValueError("first-opener day/order_id is not unique")
    if not frame["side"].astype(str).str.upper().isin(("BUY", "SELL")).all():
        raise ValueError("first-opener side is invalid")
    if not frame["inventory_role"].astype(str).eq("opener").all():
        raise ValueError("first-opener feasibility accepts opener decisions only")
    if not _numeric(frame, "exact_decision_order_fill_join").eq(1).all():
        raise ValueError("first-opener lifecycle join is not exact")

    decision_ts = _numeric(frame, "decision_ts_ms")
    submit_ts = _numeric(frame, "order_submit_ts_ms")
    activation_ts = _numeric(frame, "order_activation_ts_ms")
    fill_ts = _numeric(frame, "fill_ts_ms")
    terminal_ts = _numeric(frame, "campaign_terminal_ts_ms")
    feature_ready_ts = _numeric(
        frame, "decision_visible_feature_ready_ts_max_ms"
    )
    bbo_ready_ts = _numeric(frame, "bbo_feature_ready_ts_ms")
    l2_ready_ts = _numeric(frame, "l2_feature_ready_ts_ms")
    bbo_cutoff_ts = _numeric(frame, "decision_visible_bbo_cutoff_ts_ms")
    bbo_source_ts = _numeric(frame, "decision_bbo_source_ts_ms")
    l2_cutoff_ts = _numeric(frame, "decision_visible_l2_cutoff_ts_ms")
    l2_source_ts = _numeric(frame, "decision_l2_source_ts_ms")
    trade_cutoff_ts = _numeric(frame, "decision_visible_trade_cutoff_ts_ms")
    depth_source_offset_ms = _numeric(
        frame, "exec_depth_visibility_source_offset_ms"
    )
    queue_asof_ts_ns = _numeric(frame, "queue_ahead_asof_ts_ns")
    bbo_source_kind = frame["decision_bbo_source_kind"].astype(str)
    bbo_l2_fallback = bbo_source_kind.eq("l2_fallback")
    expected_bbo_ready_ts = decision_ts - (bbo_cutoff_ts - bbo_source_ts)
    expected_l2_ready_ts = decision_ts - (l2_cutoff_ts - l2_source_ts)
    if (
        (feature_ready_ts != decision_ts).any()
        or (bbo_ready_ts > decision_ts).any()
        or (l2_ready_ts > decision_ts).any()
        or (bbo_ready_ts != expected_bbo_ready_ts).any()
        or (l2_ready_ts != expected_l2_ready_ts).any()
        or (bbo_source_ts <= 0).any()
        or (l2_source_ts <= 0).any()
        or (bbo_source_ts > bbo_cutoff_ts).any()
        or (
            bbo_cutoff_ts
            - np.where(bbo_l2_fallback, depth_source_offset_ms, 0.0)
            > decision_ts
        ).any()
        or (l2_source_ts > l2_cutoff_ts).any()
        or ((l2_cutoff_ts - depth_source_offset_ms) > decision_ts).any()
        or (trade_cutoff_ts > decision_ts).any()
        or (queue_asof_ts_ns > decision_ts * 1_000_000).any()
        or (submit_ts != decision_ts).any()
        or (activation_ts < submit_ts).any()
        or (fill_ts < activation_ts).any()
        or (terminal_ts < fill_ts).any()
    ):
        raise ValueError("first-opener lifecycle or feature clock is non-causal")
    if (
        bbo_l2_fallback
        & (
            (bbo_source_ts != l2_source_ts)
            | (bbo_cutoff_ts != l2_cutoff_ts)
        )
    ).any():
        raise ValueError("first-opener source identity is internally inconsistent")
    if not bbo_source_kind.isin(("bbo", "l2_fallback")).all():
        raise ValueError("first-opener BBO source kind drifted")
    if not frame["feature_clock_source"].astype(str).eq(
        "modeled_receive_ready_source_asof_at_submit"
    ).all():
        raise ValueError("first-opener feature clock source identity drifted")
    if not frame["queue_ahead_decision_authority"].astype(str).eq(
        "exchange_time_diagnostic_only"
    ).all():
        raise ValueError("first-opener queue authority drifted")
    ordered_clock = frame.assign(
        _decision_ts=decision_ts,
        _trade_cutoff=trade_cutoff_ts,
    ).sort_values(["day", "_decision_ts", "decision_id"], kind="stable")
    if ordered_clock.groupby("day", sort=False)["_trade_cutoff"].diff().lt(0).any():
        raise ValueError("first-opener trade visibility cursor moved backward")
    for feature in MODEL_FEATURES:
        _numeric(frame, feature)

    tick_size = float(tick_size_usdc_per_btc)
    if not np.isfinite(tick_size) or tick_size <= 0.0:
        raise ValueError("first-opener tick size is invalid")
    decision_mid = _numeric(frame, "decision_mid_usdc_per_btc")
    decision_bid = _numeric(frame, "decision_best_bid_usdc_per_btc")
    decision_ask = _numeric(frame, "decision_best_ask_usdc_per_btc")
    order_price = _numeric(frame, "order_price_usdc_per_btc")
    spread_ticks = _numeric(frame, "decision_spread_ticks")
    quote_distance_ticks = _numeric(frame, "quote_distance_ticks")
    side = frame["side"].astype(str).str.upper()
    expected_distance = np.where(
        side.eq("BUY"),
        np.maximum(0.0, decision_mid - order_price),
        np.maximum(0.0, order_price - decision_mid),
    ) / tick_size
    if (
        (decision_bid <= 0.0).any()
        or (decision_ask <= decision_bid).any()
        or not np.allclose(
            decision_mid,
            0.5 * (decision_bid + decision_ask),
            atol=1e-9,
            rtol=0.0,
        )
        or not np.allclose(
            spread_ticks,
            (decision_ask - decision_bid) / tick_size,
            atol=1e-9,
            rtol=0.0,
        )
        or not np.allclose(
            quote_distance_ticks,
            expected_distance,
            atol=1e-9,
            rtol=0.0,
        )
    ):
        raise ValueError("first-opener submit-time market geometry drifted")

    grade_by_day = {
        str(day): str(grade) for day, grade in quality_grade_by_day.items()
    }
    if not grade_by_day or not set(grade_by_day.values()).issubset({"A", "B"}):
        raise ValueError("first-opener public quality mapping is invalid")
    observed_days = set(frame["day"].astype(str))
    if not observed_days.issubset(grade_by_day):
        raise ValueError("first-opener trace read a day outside frozen Development")
    expected_grades = frame["day"].astype(str).map(grade_by_day)
    if not frame["quality_grade"].astype(str).eq(expected_grades).all():
        raise ValueError("first-opener trace quality grade drifted")

    decision_equity = _numeric(frame, "decision_equity_usdc")
    terminal_equity = _numeric(frame, "campaign_terminal_equity_usdc")
    logged_value = _numeric(frame, PRIMARY_ESTIMAND)
    expected_value = terminal_equity - decision_equity
    if not np.allclose(logged_value, expected_value, atol=1e-9, rtol=0.0):
        raise ValueError("first-opener decision-to-terminal accounting drifted")
    if "inventory_btc" in frame and not np.allclose(
        _numeric(frame, "inventory_btc"), 0.0, atol=1e-10, rtol=0.0
    ):
        raise ValueError("first-opener decision did not begin flat")
    inventory_after = _numeric(frame, "inventory_after_fill_btc")
    side = frame["side"].astype(str).str.upper()
    if ((side.eq("BUY") & (inventory_after <= 0.0)) | (
        side.eq("SELL") & (inventory_after >= 0.0)
    )).any():
        raise ValueError("first-opener fill did not open the declared campaign side")
    if not _numeric(frame, "campaign_opened_by_fill").eq(1).all():
        raise ValueError("first-opener row was not the campaign-opening fill")
    if not frame["maker_fee_asset"].astype(str).eq("USDC").all():
        raise ValueError("first-opener maker fee asset identity drifted")
    expected_fee = (
        _numeric(frame, "fill_price")
        * _numeric(frame, "first_opener_fill_qty_btc")
        * _numeric(frame, "maker_fee_rate")
    )
    if not np.allclose(
        _numeric(frame, "first_fill_fee_usdc"),
        expected_fee,
        atol=1e-12,
        rtol=0.0,
    ):
        raise ValueError("first-opener fee accounting drifted")
    terminal_kind = frame["campaign_terminal_kind"].astype(str)
    closed = _numeric(frame, "campaign_closed").astype(int)
    censored = _numeric(frame, "campaign_censored").astype(int)
    if (
        (~terminal_kind.isin(("flat", "day_end_mtm_censored"))).any()
        or ((closed + censored) != 1).any()
        or (terminal_kind.eq("flat") != closed.eq(1)).any()
    ):
        raise ValueError("first-opener terminal/censor identity drifted")
    return frame.copy()


def validate_quality_identity(spec: Mapping[str, Any]) -> pd.DataFrame:
    validate_spec(spec)
    identity = spec.get("quality_identity") or {}
    path = Path(str(identity.get("path", ""))).expanduser()
    if not path.is_file() or sha256_file(path) != str(identity.get("sha256", "")):
        raise ValueError("first-opener quality ledger identity drifted")
    quality = pd.read_csv(path, dtype={"day": str, "quality_grade": str})
    if quality["day"].duplicated().any():
        raise ValueError("first-opener quality ledger contains duplicate days")
    by_day = quality.set_index("day")
    panels = spec["panels"]
    expected = {
        **{
            str(day): "A"
            for day in panels["development_primary_grade_a_days"]
        },
        **{
            str(day): "B"
            for day in panels["development_sensitivity_grade_b_days"]
        },
    }
    missing = sorted(set(expected) - set(by_day.index))
    if missing:
        raise ValueError("first-opener quality ledger is missing: " + ", ".join(missing))
    for day, grade in expected.items():
        row = by_day.loc[day]
        if str(row["quality_grade"]) != grade:
            raise ValueError(f"first-opener quality grade drifted for {day}")
        if not bool(row.get("native_sequence_eligible", False)) or not bool(
            row.get("normalized_formal_eligible", False)
        ):
            raise ValueError(f"first-opener native market-data identity failed for {day}")
        if grade == "A" and not bool(
            row.get("formal_training_replay_eligible", False)
        ):
            raise ValueError(f"first-opener Grade-A day is not formally eligible: {day}")
    return quality.loc[quality["day"].isin(expected)].copy()


def required_trace_columns() -> Sequence[str]:
    return tuple(sorted(REQUIRED_TRACE_COLUMNS))
