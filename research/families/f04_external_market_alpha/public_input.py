"""Market-specific causal reference panels, without legacy zero imputation."""

import math

import pandas as pd

from data.feature_cursor import FeatureCursor
from data.observation import model_row


def _require_observation_market_binding(market, cursor):
    """Reject a measured delivery profile borrowed from another market.

    A fixed, explicitly declared timing scenario has no measured-market ID;
    a measured profile must name this exact Binance perpetual, not merely a
    market with the same quote currency or a similar latency distribution.
    """
    measured = cursor.bundle.manifest["plan"]["observation_profile"].get(
        "measured_latency_market_id")
    if measured is None:
        return
    prefix = "binance_futures:perpetual:"
    if not market.startswith(prefix) or measured != "binance:perp:" + market[len(prefix):]:
        raise ValueError("measured latency profile market identity mismatch")


def build_reference_panel(markets, decisions_ns, *, columns, max_age_ns, missing_policy):
    """Return ready-time as-of features under explicitly named market columns.

    ``markets`` maps the exact manifest market ID to its consumer bundle.
    Quote currencies remain distinct; this function does not perform FX or
    manufacture absent venues. It is not the historical native-flow model ABI.
    """
    if not markets or set(markets) != set(columns):
        raise ValueError("explicit market bundles and per-market columns required")
    cursors = {market: FeatureCursor(root) for market, root in markets.items()}
    for market, cursor in cursors.items():
        if cursor.bundle.manifest["plan"]["market_id"] != market:
            raise ValueError("reference market identity mismatch")
        _require_observation_market_binding(market, cursor)
    rows = []
    for decision in decisions_ns:
        row = {"decision_ns": decision}
        for market, cursor in cursors.items():
            features = cursor.row(decision, columns=columns[market],
                                  missing_policy=missing_policy, max_age_ns=max_age_ns)
            row.update({f"{market}/{name}": value for name, value in features.items()})
            row[f"{market}/frame_cutoff_ns"] = cursor.at(
                decision, max_age_ns=max_age_ns).cutoff_ns
        rows.append(row)
    result = pd.DataFrame(rows)
    result.attrs.update(input_mode="public_consumer_bundle", native_observation_parity="not_proven",
                        currency_conversion="none", missing_policy=missing_policy)
    return result


class ReferenceSignalAdapter:
    """Explicit new-model ABI; never loads historical external-venue weights.

    The supplied predictor declares and verifies its complete feature contract.
    It receives execution and market-namespaced ready-time features, not raw
    reference prices converted to the execution currency by assumption.
    """

    def __init__(self, execution_root, markets, predictor, *, contract):
        self.execution = FeatureCursor(execution_root)
        self.references = {market: FeatureCursor(root) for market, root in markets.items()}
        self.predictor, self.contract = predictor, dict(contract)
        if not markets or set(markets) != set(contract["reference_columns"]):
            raise ValueError("explicit reference market columns required")
        if predictor.input_contract != contract:
            raise ValueError("reference model input contract mismatch")
        if contract["schema"] != "research.reference_signal.v1":
            raise ValueError("unknown reference signal contract")
        if contract["missing_policy"] not in {"reject", "native_nan"}:
            raise ValueError("explicit missing policy required")
        if type(contract["max_age_ns"]) is not int or contract["max_age_ns"] < 0:
            raise ValueError("nonnegative reference age required")
        if not contract.get("model_id") or not contract.get("output_contract_id"):
            raise ValueError("model and quote-output identities required")
        if contract["execution_market"] != self.execution.bundle.manifest["plan"]["market_id"]:
            raise ValueError("execution market identity mismatch")
        _require_observation_market_binding(contract["execution_market"], self.execution)
        for market, cursor in self.references.items():
            if market == contract["execution_market"] or cursor.bundle.manifest["plan"]["market_id"] != market:
                raise ValueError("reference market identity mismatch")
            _require_observation_market_binding(market, cursor)
        expected = {"execution": self.execution.input_manifest_id,
                    **{market: cursor.input_manifest_id for market, cursor in self.references.items()}}
        if contract["input_manifest_ids"] != expected:
            raise ValueError("reference run input binding mismatch")
        self.observations = []

    def compute_signal(self, *, feature_frame, decision_ns):
        contract = self.contract
        metadata = {key: self.execution.bundle.manifest[key] for key in (
            "input_contract_id", "observation_contract_id", "feature_contract_id")}
        metadata.update(feature_cols=contract["execution_columns"], missing_policy=contract["missing_policy"])
        execution = dict(zip(contract["execution_columns"],
            model_row(feature_frame, metadata, decision_ns=decision_ns), strict=True))
        references, cutoffs = {}, {}
        for market, cursor in self.references.items():
            references[market] = cursor.row(decision_ns,
                columns=contract["reference_columns"][market],
                missing_policy=contract["missing_policy"], max_age_ns=contract["max_age_ns"])
            cutoffs[market] = cursor.at(decision_ns, max_age_ns=contract["max_age_ns"]).cutoff_ns
        prediction = self.predictor.predict(execution=execution, references=references,
                                            decision_ns=decision_ns)
        names = ("touch_conditioned_up_probability_10000ms", "absolute_price_variance_rate_10000ms", "touch_conditioned_price_change_fraction_10000ms", "touch_side_adverse_probability_bid_10000ms", "touch_side_adverse_probability_ask_10000ms")
        if not all(math.isfinite(float(getattr(prediction, name))) for name in names):
            raise ValueError("reference predictor returned nonfinite quote output")
        if prediction.absolute_price_variance_rate_10000ms < 0 or any(not 0 <= getattr(prediction, name) <= 1
                for name in ("touch_conditioned_up_probability_10000ms", "touch_side_adverse_probability_bid_10000ms", "touch_side_adverse_probability_ask_10000ms")):
            raise ValueError("reference predictor returned invalid probability or volatility")
        self.observations.append({"decision_ns": decision_ns, "reference_cutoffs": cutoffs})
        return prediction


def replay_reference_strategy(execution_root, markets, predictor, *, contract, params,
                              initial_capital, max_mark_age_ns, funding=None):
    """Run reference predictions through the existing quote/order/fill executor.

    Legacy cross-market columns remain unavailable. This explicit predictor ABI
    is separate from those columns. Funding is a separate verified input.
    """
    from models.backtest_tick import simulate_public_inputs
    from models.replay.public_accounting import settle_public_replay

    if params.get("initial_live_state") or params.get("replay_initial_state_mode", "fresh_start") != "fresh_start":
        raise ValueError("reference replay requires independent fresh account")
    if params.get("trace_fills_max", 0) <= 0:
        raise ValueError("explicit bounded complete fill tracing required")
    adapter = ReferenceSignalAdapter(execution_root, markets, predictor, contract=contract)
    result = simulate_public_inputs(execution_root, params, signal_engine=adapter)
    result["reference_signal_contract"] = dict(contract)
    result["reference_observations"] = adapter.observations
    result["reference_native_parity"] = "not_proven"
    result["accounting"] = settle_public_replay(execution_root, result,
        initial_capital=initial_capital, max_mark_age_ns=max_mark_age_ns, funding=funding)
    for key in ("economic_complete", "all_in_net_pnl", "funding_cashflow"):
        result[key] = result["accounting"][key]
    return result
