"""Strategy-visible side flow using the common window implementation."""

import pandas as pd

from data.runtime import ConsumerBundle


def load_visible_side_flow(root):
    """Read closed causal Bars, not exchange-time or native aggTrade proxies.

    No individual interarrival or native packet streak is inferred. Unknown
    coverage and counts remain null; publication lateness was handled by the
    bound shared producer, not retrospectively recalculated here.
    """
    bundle = ConsumerBundle(root)
    bundle.source_paths()
    result = bundle.table("bars").to_pandas()
    for name in ("volume", "turnover", "buy_volume", "sell_volume", "buy_turnover", "sell_turnover"):
        result[name] = pd.to_numeric(result[name], errors="raise")
    if (result["ready_ns"] < result["end_ns"]).any():
        raise ValueError("unfinished trade window exposed to strategy")
    if result["ready_ns"].isna().any() or not result["ready_ns"].is_monotonic_increasing:
        raise ValueError("invalid visible trade window ordering")
    result.attrs.update(input_contract_id=bundle.manifest["input_contract_id"],
                        observation_contract_id=bundle.manifest["observation_contract_id"],
                        count_semantics="individual_executions_not_native_packets",
                        native_observation_parity="not_proven")
    return result
