"""Explicit common-input fixed-parameter replay; historical rankings stay frozen."""

from copy import deepcopy
import math
from pathlib import Path

from data.runtime import ConsumerBundle


def _validate_effective_quote_change(common_params, changes):
    """Reject legacy aliases that cannot reach the current quote coefficients."""
    for name, value in changes.items():
        if name not in common_params:
            raise ValueError(f"{name} requires an explicit B0 value")
        if not math.isfinite(float(value)):
            raise ValueError(f"{name} must be finite")
        if value == common_params[name]:
            continue  # The explicitly named B0 arm is allowed.
        if name in {"eta_inventory", "risk_per_order", "a_spread", "kappa"} and value <= 0:
            raise ValueError(f"{name} must be positive")
        if name == "kappa" and (
            common_params.get("execution_intensity_slope") is not None
            or (common_params.get("historical_p3_scalar_adapter_enabled", True)
                and float(common_params.get("p3_kappa_eff", 0.0)) > 0.0)
        ):
            raise ValueError("kappa is masked by the effective distance-decay coefficient")
        if name == "max_spread_bps" and (
            common_params.get("dynamic_cap_enabled", False)
            and float(common_params.get("dynamic_cap_base_bps", 0.0)) > 0.0
        ):
            raise ValueError("max_spread_bps is masked by dynamic_cap_base_bps")


def _require_frozen_f03_p3(common_params):
    """An F03-model comparison must not silently lose its frozen P3 projection."""
    if not (
        common_params.get("fill_probability_calibrated") is True
        and common_params.get("p3_identity_required") is True
        and float(common_params.get("p3_delta_star", 0.0)) > 0.0
        and float(common_params.get("p3_kappa_eff", 0.0)) > 0.0
        and len(str(common_params.get("fill_probability_artifact_sha256", ""))) == 64
    ):
        raise ValueError("F03 model replay requires the loaded frozen P3 identity")


def iter_parameter_candidates(root, candidates, *, common_params, model_dir=None):
    """Run independent candidate accounts through the maintained input reader.

    This interface deliberately limits varying fields to quote parameters.
    Accounting/delivery/fill settings cannot vary by candidate. Funding remains
    unknown in this engine's diagnostic result; no all-in ranking is invented.
    """
    from models.backtest_tick import prepare_public_inputs, simulate_prepared_inputs

    bundle = ConsumerBundle(root)
    bundle.source_paths()
    if not candidates:
        raise ValueError("explicit candidates required")
    allowed = {"eta_inventory", "a_spread", "risk_per_order", "kappa", "max_spread_bps"}
    for name, changes in candidates.items():
        if not name or not changes or set(changes) - allowed:
            raise ValueError("candidate may vary only declared quote parameters")
        _validate_effective_quote_change(common_params, changes)
    if common_params.get("ml_enabled") is True and model_dir is None:
        raise ValueError("ml_enabled F01 replay requires an explicit frozen model_dir")
    if model_dir is not None and common_params.get("ml_enabled") is not True:
        raise ValueError("model_dir requires explicit ml_enabled=true")
    if model_dir is not None:
        _require_frozen_f03_p3(common_params)
    prepared = prepare_public_inputs(root, tick_size=common_params["tick_size"])
    for name, changes in candidates.items():
        engine = None
        if model_dir is not None:
            from strategy.signal import SignalEngine
            engine = SignalEngine.from_public_models(
                Path(model_dir), symbol="BTCUSDC",
                ret_demean_halflife=int(common_params.get("ret_demean_halflife", 0)),
            )
        yield name, simulate_prepared_inputs(
            prepared, {**deepcopy(common_params), **changes}, signal_engine=engine,
        )


def replay_parameter_candidates(root, candidates, *, common_params, model_dir=None):
    """Compatible materialized result; input preparation is shared once."""
    return dict(iter_parameter_candidates(
        root, candidates, common_params=common_params, model_dir=model_dir,
    ))


def replay_economic_candidates(root, candidates, *, common_params,
                               initial_capital, max_mark_age_ns, trace_limit,
                               funding=None, model_dir=None):
    """Settle each independent candidate with the shared complete-trace ledger.

    No ranking or holdout selection is performed. Missing funding stays unknown;
    a truncated trace fails instead of becoming a complete economic result.
    Historical closure statuses and parameter rankings are not inputs here.
    """
    from models.replay.public_accounting import settle_public_replay

    if type(trace_limit) is not int or trace_limit <= 0:
        raise ValueError("positive integer trace limit required")
    if not math.isfinite(initial_capital) or initial_capital <= 0:
        raise ValueError("positive finite initial capital required")
    if type(max_mark_age_ns) is not int or max_mark_age_ns < 0:
        raise ValueError("nonnegative integer valuation age required")
    params = deepcopy(common_params)
    if params.get("initial_live_state") or params.get("replay_initial_state_mode", "fresh_start") != "fresh_start":
        raise ValueError("economic candidates require fresh independent accounts")
    params.update(replay_initial_state_mode="fresh_start", cold_flat_clock_start=True,
                  trace_fills_max=trace_limit, record_utc_accounting=True)
    results = iter_parameter_candidates(
        root, candidates, common_params=params, model_dir=model_dir,
    )
    return {
        name: {
            "replay": result,
            "accounting": settle_public_replay(
                root, result, initial_capital=initial_capital,
                max_mark_age_ns=max_mark_age_ns, funding=deepcopy(funding)),
        }
        for name, result in results
    }
