from __future__ import annotations

import json
from pathlib import Path

import pytest

from models.audit import experiment_scorecard_v2
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_continuous_scorecard_v1 as subject,
)


def _write(path: Path, payload: object) -> Path:
    path.write_text(
        json.dumps(payload, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    return path


def _fixture(tmp_path: Path) -> dict[str, Path]:
    day_count = 71
    terminal_deltas = [0.2] * 50 + [-0.1] * 21
    closed_deltas = [value / 2.0 for value in terminal_deltas]
    terminal_total = sum(terminal_deltas)
    closed_total = sum(closed_deltas)
    adapter_identity = "a" * 64
    postrun = {
        "schema_version": subject.POSTRUN_SCHEMA_VERSION,
        "identity": subject.POSTRUN_IDENTITY,
        "audit_passed": True,
        "read_only": True,
        "economic_effect_estimate_computed": False,
        "permissions": {field: False for field in subject.PERMISSION_FIELDS},
        "plan": {"adapter_plan_identity_sha256": adapter_identity},
        "execution": {
            "receipt_count": 104,
            "same_random_path_all_epochs": True,
            "quote_stop_and_zero_remaining_orders_all_epochs": True,
            "warmup_past_only_all_epochs": True,
            "strict_queue_authority_claim_count": 0,
            "receive_time_transport_authority_claim_count": 0,
        },
        "accounting": {
            "utc_day_count": day_count,
            "paired_dates_identical": True,
            "calendar_continuous": True,
            "tolerance_usdc": 1e-6,
            "maximum_abs_reconciliation_error_usdc": 1e-10,
        },
        "candidate_policy_audit": {
            "evaluations": 1000,
            "nonbaseline": 400,
        },
        "runtime_modes": {
            "control": {"control": 104},
            "candidate": {
                "owner_policy": 60,
                "missing_m2_control_fallback": 44,
            },
        },
    }
    control = {
        "terminal_mtm_pnl_usdc": -20.0,
        "closed_campaign_value_usdc": -15.0,
        "campaign_q10_usdc": -0.10,
        "campaign_cvar10_usdc": -0.20,
        "fill_count": 1000,
        "utc_day_count": day_count,
        "max_abs_inventory_btc": 0.020,
        "full_abs_inventory_time_btc_s": None,
    }
    candidate = {
        "terminal_mtm_pnl_usdc": -20.0 + terminal_total,
        "closed_campaign_value_usdc": -15.0 + closed_total,
        "campaign_q10_usdc": -0.08,
        "campaign_cvar10_usdc": -0.15,
        "fill_count": 950,
        "utc_day_count": day_count,
        "max_abs_inventory_btc": 0.018,
        "full_abs_inventory_time_btc_s": None,
    }
    final_report = {
        "schema_version": subject.CONTINUOUS_REPORT_SCHEMA_VERSION,
        "identity": subject.CONTINUOUS_IDENTITY,
        "status": "owner_restart_aware_continuous_historical_economics_complete",
        "epoch_count": 104,
        "adapter_plan_identity_sha256": adapter_identity,
        "permissions": {field: False for field in subject.PERMISSION_FIELDS},
        "evidence_scope": {
            "exchange_time": True,
            "modeled_queue": True,
            "restart_aware_continuous": True,
            "daily_fresh_start": False,
            "strict_queue": False,
            "receive_time_transport": False,
        },
        "economics": {
            "arms": {"control": control, "candidate": candidate},
            "paired": {
                "terminal_mtm_pnl_delta_usdc": terminal_total,
                "closed_campaign_value_delta_usdc": closed_total,
                "campaign_q10_delta_usdc": 0.02,
                "campaign_cvar10_delta_usdc": 0.05,
                "max_abs_inventory_delta_btc": -0.002,
                "fill_retention": 0.95,
                "daily_terminal_pnl": {
                    "day_count": day_count,
                    "mean_delta_usdc_per_day": terminal_total / day_count,
                    "total_delta_usdc": terminal_total,
                    "ci95_mean_delta_usdc_per_day": [-0.01, 0.25],
                    "bootstrap_draws": 99_999,
                    "bootstrap_seed": 20260812,
                },
                "daily_closed_campaign_value": {
                    "day_count": day_count,
                    "mean_delta_usdc_per_day": closed_total / day_count,
                    "total_delta_usdc": closed_total,
                    "ci95_mean_delta_usdc_per_day": [-0.005, 0.125],
                    "bootstrap_draws": 99_999,
                    "bootstrap_seed": 20260813,
                },
            },
        },
    }
    rows = []
    for index, (terminal, closed) in enumerate(
        zip(terminal_deltas, closed_deltas, strict=True), start=1
    ):
        rows.append(
            {
                "day": f"2026-01-{index:02d}",
                "control_pnl_usdc": 0.0,
                "candidate_pnl_usdc": terminal,
                "delta_pnl_usdc": terminal,
                "control_closed_campaign_value_usdc": 0.0,
                "candidate_closed_campaign_value_usdc": closed,
                "delta_closed_campaign_value_usdc": closed,
            }
        )
    paired_daily = {
        "schema_version": subject.PAIRED_DAILY_SCHEMA_VERSION,
        "identity": subject.CONTINUOUS_IDENTITY,
        "rows": rows,
    }
    owner_decision = {
        "schema_version": subject.OWNER_DAILY_SCHEMA_VERSION,
        "identity": subject.OWNER_DAILY_IDENTITY,
        "evidence_route": "owner_risk_accepted_outcome_informed_successor",
        "panel": {"days": 50},
        "hard_gate": {"passed": False},
        "owner_decision": {
            "daily_hard_gate_passed": False,
            "advance_to_restart_aware_continuous_confirmation": True,
            "outcome_informed": True,
        },
        "permissions": {field: False for field in subject.PERMISSION_FIELDS},
    }
    contract = experiment_scorecard_v2.score_profile_contract(subject.PROFILE_ID)
    return {
        "postrun": _write(tmp_path / "postrun.json", postrun),
        "final": _write(tmp_path / "report.json", final_report),
        "daily": _write(tmp_path / "paired_daily.json", paired_daily),
        "owner": _write(tmp_path / "owner_decision.json", owner_decision),
        "contract": _write(tmp_path / "profile_contract.json", contract),
    }


def _build(paths: dict[str, Path]) -> dict:
    return subject.build_report(
        postrun_audit_path=paths["postrun"],
        final_report_path=paths["final"],
        paired_daily_path=paths["daily"],
        owner_daily_50d_decision_path=paths["owner"],
        score_profile_contract_path=paths["contract"],
    )


def test_builder_separates_evidence_layers_and_never_grants_authority(
    tmp_path: Path,
) -> None:
    report = _build(_fixture(tmp_path))

    assert report["evidence_layers"]["oof_research"] == {
        "identity": subject.OOF_RESEARCH_IDENTITY,
        "status": "research_gate_failed",
        "research_supported": False,
        "result_recomputed_by_this_builder": False,
        "corroborating_frozen_field": (
            "owner_daily_50d_decision.permissions.research_supported=false"
        ),
    }
    assert report["evidence_layers"]["owner_daily_50d"]["hard_gate_passed"] is False
    assert (
        report["evidence_layers"]["owner_restart_aware_continuous"]["status"]
        == "historical_economics_complete"
    )
    assert report["permissions"] == {field: False for field in subject.PERMISSION_FIELDS}
    assert report["scorecard"]["formal_scorecard_passed"] is False
    assert report["scorecard"]["ranking_score"] is None
    normalized = dict(report)
    expected = normalized.pop("report_sha256")
    assert subject.canonical_sha256(normalized) == expected


def test_builder_reports_calculable_economics_and_missing_profile_gates(
    tmp_path: Path,
) -> None:
    report = _build(_fixture(tmp_path))

    economics = report["economics"]
    assert economics["terminal_mtm"]["total_delta_usdc"] == pytest.approx(7.9)
    assert economics["closed_campaign_value"]["total_delta_usdc"] == pytest.approx(3.95)
    assert economics["positive_terminal_day"] == {
        "positive_days": 50,
        "day_count": 71,
        "rate": pytest.approx(50 / 71),
    }
    assert economics["campaign_q10"]["candidate_minus_control_usdc"] == pytest.approx(0.02)
    assert economics["campaign_cvar10"]["candidate_minus_control_usdc"] == pytest.approx(0.05)
    assert economics["fills"]["retention"] == pytest.approx(0.95)
    assert economics["maximum_absolute_inventory"][
        "control_minus_candidate_avoidance_btc"
    ] == pytest.approx(0.002)
    assert economics["policy_mechanics"]["nonbaseline_rate"] == pytest.approx(0.4)

    compatibility = report["score_profile_compatibility"]
    assert compatibility["exact_contract_match"] is True
    assert compatibility["models_audit_score_canonical_evidence_compatible"] is False
    assert compatibility["models_audit_score_canonical_evidence_invoked"] is False
    assert "required_profile_metric_missing:negative_terminal_protection" in report["hard_blockers"]
    assert "full_abs_inventory_time_btc_s_missing" in report["hard_blockers"]
    assert "receive_time_transport_authority_missing" in report["hard_blockers"]


def test_contract_mismatch_is_reported_not_rewritten(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    bad_contract = json.loads(paths["contract"].read_text(encoding="utf-8"))
    bad_contract["profile_sha256"] = "f" * 64
    _write(paths["contract"], bad_contract)

    report = _build(paths)

    compatibility = report["score_profile_compatibility"]
    assert compatibility["exact_contract_match"] is False
    assert compatibility["profile_modified"] is False
    assert "action_defense_v2_profile_contract_mismatch" in report["hard_blockers"]
    assert report["scorecard"]["formal_scorecard_generated"] is False


def test_builder_rejects_paired_daily_economic_drift(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    daily = json.loads(paths["daily"].read_text(encoding="utf-8"))
    daily["rows"][0]["delta_pnl_usdc"] += 1.0
    _write(paths["daily"], daily)

    with pytest.raises(
        subject.OwnerContinuousScorecardError,
        match="paired PnL identity",
    ):
        _build(paths)


def test_builder_rejects_any_authority_claim(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    final = json.loads(paths["final"].read_text(encoding="utf-8"))
    final["permissions"]["action_authorized"] = True
    _write(paths["final"], final)

    with pytest.raises(
        subject.OwnerContinuousScorecardError,
        match="granted action_authorized",
    ):
        _build(paths)


def test_builder_accepts_frozen_owner_legacy_strict_queue_denial(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    owner = json.loads(paths["owner"].read_text(encoding="utf-8"))
    owner["permissions"].pop("strict_queue_authority")
    owner["permissions"]["strict_native_queue_authority"] = False
    owner["permissions"]["continuous_replay_authority"] = False
    _write(paths["owner"], owner)

    report = _build(paths)

    assert report["permissions"] == {field: False for field in subject.PERMISSION_FIELDS}


def test_builder_rejects_legacy_strict_queue_authority(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    owner = json.loads(paths["owner"].read_text(encoding="utf-8"))
    owner["permissions"].pop("strict_queue_authority")
    owner["permissions"]["strict_native_queue_authority"] = True
    _write(paths["owner"], owner)

    with pytest.raises(
        subject.OwnerContinuousScorecardError,
        match="strict queue authority",
    ):
        _build(paths)


def test_builder_rejects_conflicting_strict_queue_permissions(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    owner = json.loads(paths["owner"].read_text(encoding="utf-8"))
    owner["permissions"]["strict_native_queue_authority"] = True
    _write(paths["owner"], owner)

    with pytest.raises(
        subject.OwnerContinuousScorecardError,
        match="strict_native_queue_authority",
    ):
        _build(paths)


def test_atomic_writer_requires_explicit_replace(tmp_path: Path) -> None:
    report = _build(_fixture(tmp_path))
    output = tmp_path / "independent-scorecard-audit.json"

    subject.write_report(output, report)
    assert json.loads(output.read_text(encoding="utf-8")) == report
    with pytest.raises(subject.OwnerContinuousScorecardError, match="already exists"):
        subject.write_report(output, report)
    subject.write_report(output, report, replace=True)
    assert json.loads(output.read_text(encoding="utf-8")) == report
