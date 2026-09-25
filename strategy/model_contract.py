"""Strict runtime contract for a configured 13-head LightGBM bundle."""

from __future__ import annotations

import math
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from features.feature_dag import TEN_SECOND_CAUSAL_GRAPH

REQUIRED_MODEL_HEADS = (
    "touch_conditioned_up_probability_10000ms", "touch_conditioned_up_probability_30000ms", "touch_conditioned_up_probability_60000ms",
    "absolute_price_variance_rate_10000ms", "absolute_price_variance_rate_30000ms", "absolute_price_variance_rate_60000ms",
    "touch_conditioned_price_change_fraction_10000ms", "touch_conditioned_price_change_fraction_30000ms", "touch_conditioned_price_change_fraction_60000ms",
    "touch_side_adverse_probability_bid_5000ms", "touch_side_adverse_probability_ask_5000ms",
    "touch_side_adverse_probability_bid_10000ms", "touch_side_adverse_probability_ask_10000ms",
)

ABSOLUTE_PRICE_VARIANCE_SEMANTICS = "fixed_forward_h_absolute_price_variance"
VARIANCE_UNIT_CONTRACT_SCHEMA = "narrowgate.absolute_price_variance_unit_contract.v1"
ABSOLUTE_PRICE_VARIANCE_UNITS = "(quote/base)^2_per_second"
ABSOLUTE_PRICE_VARIANCE_SAMPLE_PERIOD_S = 1.0
REQUIRED_FEATURE_SEMANTICS_VERSION = 6
REQUIRED_FEATURE_DAG_ID = TEN_SECOND_CAUSAL_GRAPH.graph_id
REQUIRED_FEATURE_DAG_SHA256 = TEN_SECOND_CAUSAL_GRAPH.sha256()
REQUIRED_LABEL_SEMANTICS_VERSION = 3
REQUIRED_LABEL_WINDOW_SEMANTICS = "left_closed_right_open_[t,t+h)"
REQUIRED_CALENDAR_TIMESTAMP_SEMANTICS = (
    "preserve_datetime_physical_unit_ms_us_ns_before_epoch_conversion"
)
PRIVATE_DEPLOYMENT_AUTHORITY = "private_deployment_authorized"
F03_DIRECT_QUOTE_ACTION_SCHEMA = "narrowgate.f03.direct_quote_action.v1"
F03_DIRECT_QUOTE_ACTION_EVENT_TYPE = "decision_to_fixed_horizon_return"
F03_DIRECT_QUOTE_ACTION_PRICE_ORIGIN = "decision_mid"
F03_DIRECT_QUOTE_ACTION_RETURN_UNIT = "fraction"
F03_DIRECT_QUOTE_ACTION_CONSUMER = "quote_center_shift"

_MODEL_QUOTE_ASSET_SUFFIXES = (
    "FDUSD",
    "USDC",
    "USDT",
    "BUSD",
    "TUSD",
    "DAI",
    "USD",
)


def absolute_price_variance_unit_contract(symbol: str) -> dict[str, Any]:
    """Derive the variance-rate unit contract from one canonical symbol."""
    normalized = str(symbol or "").strip().upper().replace("/", "").replace("-", "")
    for quote_asset in _MODEL_QUOTE_ASSET_SUFFIXES:
        if normalized.endswith(quote_asset) and len(normalized) > len(quote_asset):
            base_asset = normalized[: -len(quote_asset)]
            return {
                "schema_version": VARIANCE_UNIT_CONTRACT_SCHEMA,
                "symbol": normalized,
                "base_asset": base_asset,
                "quote_asset": quote_asset,
                "variance_units": ABSOLUTE_PRICE_VARIANCE_UNITS,
                "sample_period_s": ABSOLUTE_PRICE_VARIANCE_SAMPLE_PERIOD_S,
            }
    raise ValueError(f"cannot derive base/quote assets from model symbol {symbol!r}")


def validate_variance_unit_contract(
    contract: Any,
    *,
    symbol: str,
) -> dict[str, Any]:
    expected = absolute_price_variance_unit_contract(symbol)
    if not isinstance(contract, dict) or contract != expected:
        raise ValueError(
            "volatility_unit_contract must exactly match the symbol-derived "
            "absolute-price variance-rate contract"
        )
    return dict(expected)






def f03_direct_quote_action_contract(meta: Mapping[str, Any]) -> dict[str, Any]:
    """Return a fail-closed, explicitly declared F03 quote-action contract.

    Existing F03 ``touch_conditioned_price_change_fraction_10000ms`` labels are fill-conditioned outcomes spanning
    10--20 seconds.  Their historical name is not permission to consume them
    as a point-horizon quote-center action.  A future action model must carry
    this separate contract; absence deliberately returns an incompatible
    identity so ML inference with ``ret_skew == 0`` remains a no-op.
    """

    raw = meta.get("direct_quote_action")
    if raw is None:
        return {"compatible": False, "horizon_s": 0.0}
    if not isinstance(raw, Mapping):
        raise ValueError("F03 direct_quote_action must be a mapping")
    horizon_s = float(raw.get("horizon_s", 0.0) or 0.0)
    expected = {
        "schema_version": F03_DIRECT_QUOTE_ACTION_SCHEMA,
        "compatible": True,
        "event_type": F03_DIRECT_QUOTE_ACTION_EVENT_TYPE,
        "price_origin": F03_DIRECT_QUOTE_ACTION_PRICE_ORIGIN,
        "return_unit": F03_DIRECT_QUOTE_ACTION_RETURN_UNIT,
        "consumer": F03_DIRECT_QUOTE_ACTION_CONSUMER,
    }
    for key, value in expected.items():
        if raw.get(key) != value:
            raise ValueError(
                f"F03 direct_quote_action {key}={raw.get(key)!r}; expected {value!r}"
            )
    if not math.isfinite(horizon_s) or horizon_s <= 0.0:
        raise ValueError("F03 direct_quote_action horizon_s must be finite and positive")
    return {**expected, "horizon_s": horizon_s}








def resolve_model_authorization_manifest(
    model_dir: Path,
    metadata: Mapping[str, Mapping[str, Any]],
) -> Path:
    """Resolve the sole current hash-bound authorization; never inherit old grants."""
    from strategy.public_model_contract import validate_public_bundle
    root = Path(model_dir).expanduser().resolve()
    symbols = {str(meta.get("symbol") or "BTCUSDC") for meta in metadata.values()}
    if len(symbols) != 1:
        raise ValueError("model metadata must bind one symbol")
    validate_public_bundle(root, expected_symbol=symbols.pop(), live=True)
    candidate = root / "live_input_authorization.json"
    if candidate.is_symlink() or not candidate.is_file():
        raise ValueError("authorization must be a canonical regular file")
    return candidate








def validate_model_bundle(
    model_dir: Path,
    *,
    allow_research_only: bool = False,
    require_live_authorization: bool = False,
    expected_symbol: str | None = None,
) -> dict[str, dict[str, Any]]:
    """Validate the sole semantic bundle protocol, including deployment authority."""
    from strategy.public_model_contract import validate_public_bundle
    root = Path(model_dir).expanduser().resolve()
    if not root.is_dir():
        raise ValueError(f"model bundle directory does not exist: {root}")
    if not (root / "public_input_model.json").is_file():
        raise ValueError("semantic_model_bundle manifest required; retired model bundle rejected")
    return validate_public_bundle(
        root, expected_symbol=expected_symbol or "BTCUSDC",
        live=require_live_authorization or not allow_research_only,
    )
