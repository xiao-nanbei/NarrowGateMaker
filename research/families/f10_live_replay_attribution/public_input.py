"""Attribution of an explicit new replay epoch, not historical live parity."""

from models.replay.continuous_accounting import marked_equity_change
from data.feature_cursor import FeatureCursor


def attribute_interval(root, start, end, *, fees_usdc, funding_cashflow_usdc,
                       max_mark_age_ms, run_identity):
    required = {"input_manifest_id", "observation_contract_id", "execution_contract_id", "epoch_id"}
    if set(run_identity) < required or any(not run_identity.get(key) for key in required):
        raise ValueError("explicit input, delivery, execution and account epoch required")
    if max_mark_age_ms is None:
        raise ValueError("new-source accounting requires an explicit mark age policy")
    cursor = FeatureCursor(root)
    cursor.require_binding([run_identity, start, end])
    if run_identity["observation_contract_id"] != cursor.bundle.manifest["observation_contract_id"]:
        raise ValueError("attribution observation contract mismatch")
    result = marked_equity_change(start, end, fees_usdc=fees_usdc,
                                  funding_cashflow_usdc=funding_cashflow_usdc,
                                  max_mark_age_ms=max_mark_age_ms)
    return {**result, "run_identity": dict(run_identity), "native_live_parity": "not_proven"}
