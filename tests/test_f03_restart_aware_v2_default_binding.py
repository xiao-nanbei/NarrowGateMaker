from __future__ import annotations

import json

import pytest

from models.replay import continuous_accounting
from models.replay.continuous_accounting import ContinuousAccountingLedger
from models.replay.replay_state_checkpoint import (
    ContinuousReplayState,
    EconomicCampaignState,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_restart_aware_continuous_ab as f03,
)
from scripts import run_restart_aware_continuous_baseline as runner


def test_public_default_documents_bind_prospective_accounting_v2() -> None:
    binding = f03.validate_default_binding_documents()

    assert f03.DEFAULT_SPEC.name.endswith("v2_preflight_20260825.json")
    assert f03.DEFAULT_AMENDMENT.name.endswith(
        "v2_execution_binding_amendment_20260825.json"
    )
    assert binding["continuous_accounting_contract_id"] == (
        continuous_accounting.SCHEMA_VERSION
    )
    assert binding["fee_accounting_semantics"] == (
        continuous_accounting.FEE_ACCOUNTING_SEMANTICS
    )
    assert binding["fill_trace_ordering_contract_id"] == (
        f03.FILL_TRACE_ORDERING_IDENTITY
    )
    assert len(binding["identity_sha256"]) == 64

    predecessor = json.loads(
        (
            f03.DEFAULT_SPEC.parent
            / "causal_v12_1s_restart_aware_continuous_calendar_ab_v1_"
            "preflight_20260805.json"
        ).read_text(encoding="utf-8")
    )
    assert predecessor["comparison"]["continuous_accounting_contract"][
        "identity"
    ] == "continuous_accounting_contract.v1"


def test_default_preflight_reaches_only_the_intentional_candidate_gate() -> None:
    with pytest.raises(f03.F03ContinuousABPreflightError, match="candidate identity"):
        f03.validate_preflight()


def test_signed_usdc_fee_binding_accepts_rebate_and_rejects_legacy_trace() -> None:
    binding = runner.validate_continuous_accounting_fee_binding()
    signed_fill = {
        "fill_fee_usdc": -0.005,
        "fill_fee_asset": "USDC",
        "fill_fee_semantics": continuous_accounting.FEE_ACCOUNTING_SEMANTICS,
    }

    assert runner.bound_fill_fee_usdc(
        signed_fill,
        fee_binding=binding,
    ) == pytest.approx(-0.005)

    legacy_fill = dict(signed_fill)
    legacy_fill.pop("fill_fee_asset")
    with pytest.raises(RuntimeError, match="lacks the signed USDC"):
        runner.bound_fill_fee_usdc(legacy_fill, fee_binding=binding)

    legacy_binding = dict(binding)
    legacy_binding["contract_id"] = "continuous_accounting_contract.v1"
    with pytest.raises(RuntimeError, match="lacks the signed USDC"):
        runner.bound_fill_fee_usdc(signed_fill, fee_binding=legacy_binding)


def _long_ledger() -> ContinuousAccountingLedger:
    return ContinuousAccountingLedger(
        ContinuousReplayState(
            arm_id="control",
            checkpoint_ts_ms=1_000,
            cash_usdc=-100.0,
            position_btc=1.0,
            average_entry_price=100.0,
            cumulative_realized_pnl_usdc=0.0,
            cumulative_fees_usdc=0.0,
            equity_anchor_usdc=0.0,
            last_mark_price=100.0,
            cumulative_pnl_usdc=0.0,
            economic_campaign=EconomicCampaignState(
                campaign_id="LONG-1",
                side="LONG",
                start_ts_ms=1_000,
                start_equity_usdc=0.0,
                peak_abs_inventory_btc=1.0,
            ),
        )
    )


def _signed_fee_binding() -> dict[str, str]:
    return {
        "contract_id": continuous_accounting.SCHEMA_VERSION,
        "fee_asset": "USDC",
        "fee_accounting_semantics": (
            continuous_accounting.FEE_ACCOUNTING_SEMANTICS
        ),
    }


def _same_tick_inverse_order_id_trace() -> list[dict[str, object]]:
    common = {
        "fill_ts": 2_000,
        "side": "SELL",
        "fill_qty": 1.0,
        "fill_fee_asset": "USDC",
        "fill_fee_semantics": continuous_accounting.FEE_ACCOUNTING_SEMANTICS,
    }
    return [
        {
            **common,
            "fill_sequence": 0,
            "order_id": 20,
            "quote_px": 99.0,
            "fill_fee_usdc": 0.001,
            "inventory_before_fill": 1.0,
            "inventory_after_fill": 0.0,
        },
        {
            **common,
            "fill_sequence": 1,
            "order_id": 10,
            "quote_px": 101.0,
            "fill_fee_usdc": -0.001,
            "inventory_before_fill": 0.0,
            "inventory_after_fill": -1.0,
        },
    ]


def test_runner_keeps_same_tick_ioc_then_passive_native_fill_sequence() -> None:
    ledger = _long_ledger()
    # Deliberately present the rows out of container order: immutable sequence,
    # not timestamp or order id, remains the physical execution authority.
    trace = list(reversed(_same_tick_inverse_order_id_trace()))

    campaign_ordinal = runner.apply_native_fill_trace(
        ledger=ledger,
        fill_trace=trace,
        expected_fill_count=2,
        fee_binding=_signed_fee_binding(),
        campaign_ordinal=1,
        segment_id="same-tick-oracle",
    )

    assert campaign_ordinal == 2
    assert ledger.closed_campaigns[0].value_usdc == pytest.approx(-1.001)
    assert ledger.state.position_btc == pytest.approx(-1.0)
    assert ledger.state.economic_campaign is not None
    assert ledger.state.economic_campaign.side == "SHORT"
    assert ledger.state.cumulative_fees_usdc == pytest.approx(0.0)
    assert ledger.state.equity_usdc == pytest.approx(0.0)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda rows: rows[1].__setitem__("fill_sequence", 2), "not contiguous"),
        (
            lambda rows: rows[1].__setitem__("inventory_before_fill", 0.25),
            "inventory_before",
        ),
        (
            lambda rows: rows[1].__setitem__("inventory_after_fill", -0.5),
            "inventory_after",
        ),
    ],
)
def test_runner_fill_sequence_and_inventory_path_fail_closed(
    mutation: object,
    message: str,
) -> None:
    ledger = _long_ledger()
    trace = _same_tick_inverse_order_id_trace()
    mutation(trace)  # type: ignore[operator]

    with pytest.raises(RuntimeError, match=message):
        runner.apply_native_fill_trace(
            ledger=ledger,
            fill_trace=trace,
            expected_fill_count=2,
            fee_binding=_signed_fee_binding(),
            campaign_ordinal=1,
            segment_id="invalid-trace",
        )

    assert ledger.state.position_btc == pytest.approx(1.0)
    assert ledger.closed_campaigns == []
