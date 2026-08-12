from __future__ import annotations

import copy
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    decision_visible_negative_fill_value_evidence as m0,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    decision_visible_negative_fill_value_runner as m0_runner,
)
from research.families.f10_live_replay_attribution.audit import (
    first_add_decision_to_terminal_contract as f10_contract,
)

ROOT = Path(__file__).resolve().parents[1]
F10_SPEC_PATH = (
    ROOT
    / "research"
    / "families"
    / "f10_live_replay_attribution"
    / "docs"
    / "first_add_decision_to_terminal_loss_diagnostic_v1_spec_20260729.json"
)
METHOD_PATH = (
    ROOT
    / "research"
    / "families"
    / "f05_fill_quality_quote_ev"
    / "docs"
    / "decision_visible_negative_fill_value_evidence_m0_v1_method_20260729.json"
)
METHOD_V1_1_PATH = (
    ROOT
    / "research"
    / "families"
    / "f05_fill_quality_quote_ev"
    / "docs"
    / "decision_visible_negative_fill_value_evidence_m0_v1_1_method_20260729.json"
)


def _f10_spec() -> dict:
    return json.loads(F10_SPEC_PATH.read_text(encoding="utf-8"))


def _method_contract() -> dict:
    return json.loads(METHOD_PATH.read_text(encoding="utf-8"))


def _native_frame() -> pd.DataFrame:
    spec = _f10_spec()
    primary = tuple(spec["panels"]["development_primary_grade_a_days"])
    sensitivity = tuple(spec["panels"]["development_sensitivity_grade_b_days"])
    rows: list[dict] = []
    rng = np.random.default_rng(712_029)
    for grade, days in (("A", primary), ("B", sensitivity)):
        for day_number, day in enumerate(days):
            day_start_ms = int(pd.Timestamp(day, tz="UTC").timestamp() * 1_000)
            day_effect = 0.0015 * np.sin(day_number / 3.0)
            for side in ("BUY", "SELL"):
                side_sign = 1.0 if side == "BUY" else -1.0
                for row_number in range(40):
                    local_toxicity = float(rng.normal())
                    flow = float(rng.normal())
                    inventory = side_sign * (0.002 + 0.0002 * row_number)
                    campaign_pnl = 0.01 * np.cos(row_number)
                    campaign_mae = -0.005 * row_number
                    quote_distance = 135.0 + row_number
                    queue_ahead = 0.01 + 0.002 * row_number
                    microprice = float(rng.normal())
                    refresh = float(rng.uniform(0.2, 0.8))
                    cancel = float(rng.uniform(0.2, 0.8))
                    target = (
                        0.25 * campaign_pnl
                        + 0.08 * campaign_mae
                        - 0.12 * local_toxicity
                        - 0.05 * side_sign * flow
                        + 0.02 * side_sign * microprice
                        + day_effect
                        + rng.normal(0.0, 0.001)
                    )
                    campaign_id = f"{side}-{row_number}"
                    uid = f"{day}-{campaign_id}"
                    decision_ts = day_start_ms + 60_000 + row_number * 1_000
                    decision_equity = 10.0 + 0.01 * row_number
                    rows.append(
                        {
                            "trace_schema_version": (
                                f10_contract.TRACE_SCHEMA_VERSION
                            ),
                            "day": day,
                            "quality_grade": grade,
                            "campaign_id": campaign_id,
                            "decision_id": f"decision-{uid}",
                            "decision_ts_ms": decision_ts,
                            "order_id": f"order-{uid}",
                            "order_submit_ts_ms": decision_ts + 1,
                            "fill_ts_ms": decision_ts + 20,
                            "campaign_terminal_ts_ms": decision_ts + 10_000,
                            "side": side,
                            "inventory_role": "add",
                            "exact_decision_order_fill_join": 1,
                            "decision_visible_feature_ready_ts_max_ms": decision_ts,
                            "decision_equity_usdc": decision_equity,
                            "campaign_terminal_equity_usdc": (
                                decision_equity + target
                            ),
                            m0.TARGET_COLUMN: target,
                            "inventory_btc": inventory,
                            "campaign_age_ms": 2_000 + 100 * row_number,
                            "campaign_pnl_so_far_usdc": campaign_pnl,
                            "campaign_mae_so_far_usdc": campaign_mae,
                            "exposure_increasing_fill_count_so_far": (
                                row_number % 3
                            ),
                            "reducing_fill_count_so_far": row_number % 2,
                            "quote_distance_ticks": quote_distance,
                            "queue_ahead_btc": queue_ahead,
                            "microprice_shift_bps": microprice,
                            "l2_book_refresh_ratio": refresh,
                            "l2_book_cancel_ratio": cancel,
                            "local_toxicity": local_toxicity,
                            "parent_aggtrade_flow_imbalance": flow,
                        }
                    )
    return pd.DataFrame(rows)


def _fit_identity(parquet_path: Path) -> dict:
    spec = _f10_spec()
    method = _method_contract()
    payload = {
        "schema_version": m0.FIT_SCHEMA_VERSION,
        "identity": m0.IDENTITY,
        "status": "frozen_native_f10_artifact_development_only",
        "method_contract_identity": {
            "path": str(METHOD_PATH.resolve()),
            "sha256": m0.sha256_file(METHOD_PATH),
            "canonical_method_contract_sha256": method[
                "canonical_method_contract_sha256"
            ],
        },
        "f10_source": {
            "artifact_path": str(parquet_path.resolve()),
            "artifact_sha256": m0.sha256_file(parquet_path),
            "spec_path": str(F10_SPEC_PATH.resolve()),
            "spec_file_sha256": m0.sha256_file(F10_SPEC_PATH),
            "spec_canonical_sha256": spec["canonical_spec_sha256"],
            "trace_schema_version": f10_contract.TRACE_SCHEMA_VERSION,
            "exact_native_join_required": True,
            "feature_ready_clock_required": True,
            "producer_audit": {
                "candidate_campaigns": 3200,
                "emitted_rows": 3200,
                "exact_join_rows": 3200,
                "nearest_time_match_rows": 0,
                "feature_clock_violation_rows": 0,
            },
        },
        "authoritative_target": dict(method["authoritative_target"]),
        "panels": {
            "grade_a_primary_days": spec["panels"][
                "development_primary_grade_a_days"
            ],
            "grade_b_sensitivity_days": spec["panels"][
                "development_sensitivity_grade_b_days"
            ],
            "pooling": method["panels"]["pooling"],
            "grade_b_role": method["panels"]["grade_b_role"],
        },
        "model": dict(method["model"]),
        "chronology": dict(method["chronology"]),
        "high_risk": dict(method["high_risk"]),
        "inference": dict(method["inference"]),
        "outcome_access": dict(method["outcome_access"]),
        "permissions": dict(method["permissions"]),
    }
    payload["canonical_fit_identity_sha256"] = (
        m0.canonical_fit_identity_sha256(payload)
    )
    return payload


def _fit_identity_v1_1(parquet_path: Path) -> dict:
    payload = _fit_identity(parquet_path)
    method = json.loads(METHOD_V1_1_PATH.read_text(encoding="utf-8"))
    payload["identity"] = m0.IDENTITY_V1_1
    payload["method_contract_identity"] = {
        "path": str(METHOD_V1_1_PATH.resolve()),
        "sha256": m0.sha256_file(METHOD_V1_1_PATH),
        "canonical_method_contract_sha256": method[
            "canonical_method_contract_sha256"
        ],
    }
    payload["model"] = dict(method["model"])
    return _refreeze(payload)


@pytest.fixture
def frozen_input(tmp_path: Path) -> tuple[Path, dict]:
    path = tmp_path / "f10_native_first_add.parquet"
    _native_frame().to_parquet(path, index=False)
    return path, _fit_identity(path)


def _refreeze(payload: dict) -> dict:
    output = copy.deepcopy(payload)
    output.pop("canonical_fit_identity_sha256", None)
    output["canonical_fit_identity_sha256"] = (
        m0.canonical_fit_identity_sha256(output)
    )
    return output


def test_m0_runs_side_specific_past_only_without_pooling(
    frozen_input: tuple[Path, dict],
) -> None:
    _, identity = frozen_input
    result = m0.evaluate_frozen_f10_parquet(identity)

    assert set(result.oof_predictions["m0_panel"]) == {
        m0.PRIMARY_PANEL,
        m0.SENSITIVITY_PANEL,
    }
    assert set(result.oof_predictions["side"]) == {"BUY", "SELL"}
    for row in result.oof_predictions.itertuples(index=False):
        assert pd.Timestamp(row.outer_train_max_day) < (
            pd.Timestamp(row.day) - pd.Timedelta(days=1)
        )

    report = result.report
    assert report["panel_contract"] == {
        "grade_a_days": 24,
        "grade_b_days": 16,
        "pooled_fit": False,
        "pooled_metric": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    for panel in (m0.PRIMARY_PANEL, m0.SENSITIVITY_PANEL):
        for side in ("BUY", "SELL"):
            cell = report["panels"][panel][side]
            prediction = cell["direct_usdc_prediction"]
            assert prediction["local_microstructure_mse_usdc2"] < (
                prediction["campaign_state_mse_usdc2"]
            )
            assert cell["high_risk"]["mean_value_usdc"] < 0.0
            assert cell["cluster_bootstrap"]["method"] == (
                "day_then_campaign_cluster_percentile_bonferroni"
            )
            assert not cell["action_authorized"]
            assert not cell["live_authorized"]
    assert not report["permissions"]["action_registration"]
    assert not report["permissions"]["validation_read"]
    assert not report["permissions"]["sealed_holdout_read"]
    assert not report["permissions"]["live_deployment_authorized"]


def test_m0_rejects_f10_hash_before_parquet_read(
    frozen_input: tuple[Path, dict],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, identity = frozen_input
    broken = copy.deepcopy(identity)
    original = broken["f10_source"]["artifact_sha256"]
    broken["f10_source"]["artifact_sha256"] = (
        ("a" if original[0] != "a" else "b") + original[1:]
    )
    broken = _refreeze(broken)

    read_attempted = False

    def _forbidden_read(*args, **kwargs):  # noqa: ANN002, ANN003
        nonlocal read_attempted
        read_attempted = True
        raise AssertionError("parquet must not be read after an identity failure")

    monkeypatch.setattr(m0.pd, "read_parquet", _forbidden_read)
    with pytest.raises(ValueError, match="parquet SHA256 mismatch"):
        m0.evaluate_frozen_f10_parquet(broken)
    assert not read_attempted


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("join", "not exact"),
        ("clock", "non-causal"),
        ("grade", "quality grade drifted"),
        ("later_panel", "outside frozen Development"),
    ],
)
def test_m0_fails_closed_on_native_identity_errors(
    tmp_path: Path,
    mutation: str,
    message: str,
) -> None:
    frame = _native_frame()
    if mutation == "join":
        frame.loc[0, "exact_decision_order_fill_join"] = 0
    elif mutation == "clock":
        frame.loc[0, "decision_visible_feature_ready_ts_max_ms"] = (
            frame.loc[0, "decision_ts_ms"] + 1
        )
    elif mutation == "grade":
        frame.loc[0, "quality_grade"] = "B"
    elif mutation == "later_panel":
        frame.loc[0, "day"] = "2026-07-01"
    path = tmp_path / f"broken-{mutation}.parquet"
    frame.to_parquet(path, index=False)

    with pytest.raises(ValueError, match=message):
        m0.evaluate_frozen_f10_parquet(_fit_identity(path))


def test_m0_fit_identity_cannot_grant_action_or_pool_quality_panels(
    frozen_input: tuple[Path, dict],
) -> None:
    _, identity = frozen_input
    action = copy.deepcopy(identity)
    action["permissions"]["action_registration"] = True
    with pytest.raises(ValueError, match="forbidden authority"):
        m0.validate_fit_identity(_refreeze(action))

    pooled = copy.deepcopy(identity)
    pooled["panels"]["pooling"] = "allowed"
    with pytest.raises(ValueError, match="cannot pool"):
        m0.validate_fit_identity(_refreeze(pooled))


def test_m0_fit_freeze_binds_native_bytes_before_outcome_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trace_path = tmp_path / "native-trace.parquet"
    frame = _native_frame()
    frame.to_parquet(trace_path, index=False)
    counts = frame.groupby("day", sort=True).size()
    audit_path = tmp_path / "native-audit.parquet"
    pd.DataFrame(
        {
            "day": counts.index,
            "selected_campaign_count": counts.values,
            "emitted_row_count": counts.values,
            "exact_join_count": counts.values,
            "feature_clock_violation_count": 0,
            "open_record_count": 0,
        }
    ).to_parquet(audit_path, index=False)
    producer_spec_path = tmp_path / "producer-spec.json"
    producer_spec_path.write_text("{}\n", encoding="utf-8")
    manifest_path = tmp_path / "producer-manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "identity": "first_add_decision_to_terminal_native_producer_v1",
                "complete_development": True,
                "validation_read": False,
                "sealed_holdout_read": False,
                "trace_path": str(trace_path),
                "trace_sha256": m0.sha256_file(trace_path),
                "producer_audit_path": str(audit_path),
                "producer_audit_sha256": m0.sha256_file(audit_path),
                "producer_identity": {
                    "native_producer_spec": {
                        "path": str(producer_spec_path),
                        "sha256": m0.sha256_file(producer_spec_path),
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    original_read = m0_runner.pd.read_parquet

    def _audit_only_read(path, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        if Path(path).resolve() == trace_path.resolve():
            raise AssertionError("fit freeze must not read F10 outcome rows")
        return original_read(path, *args, **kwargs)

    monkeypatch.setattr(m0_runner.pd, "read_parquet", _audit_only_read)
    identity = m0_runner.build_fit_identity(
        native_manifest_path=manifest_path,
        method_path=METHOD_PATH,
    )

    m0.validate_fit_identity(identity)
    assert identity["f10_source"]["artifact_sha256"] == m0.sha256_file(
        trace_path
    )
    assert not identity["permissions"]["validation_read"]
    assert not identity["permissions"]["action_experiment_authorized"]


def test_m0_v1_1_preserves_unknown_queue_with_explicit_indicator(
    tmp_path: Path,
) -> None:
    frame = _native_frame()
    frame["queue_ahead_source"] = "native_exchange_book_exact"
    grade_b_index = frame.index[frame["quality_grade"].eq("B")][0]
    frame.loc[grade_b_index, "queue_ahead_btc"] = np.nan
    frame.loc[grade_b_index, "queue_ahead_source"] = "unknown"
    path = tmp_path / "f10-native-queue-missing.parquet"
    frame.to_parquet(path, index=False)

    result = m0.evaluate_frozen_f10_parquet(_fit_identity_v1_1(path))

    assert result.report["identity"] == m0.IDENTITY_V1_1
    assert result.report["model"]["local_incremental_feature_count"] == 8
    assert not result.report["permissions"]["action_registration"]
