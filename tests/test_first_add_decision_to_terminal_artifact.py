from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from research.families.f10_live_replay_attribution.audit import (
    first_add_decision_to_terminal_artifact as artifact,
)
from research.families.f10_live_replay_attribution.audit import (
    first_add_decision_to_terminal_contract as contract,
)

ROOT = Path(__file__).resolve().parents[1]
SPEC_PATH = (
    ROOT
    / "research"
    / "families"
    / "f10_live_replay_attribution"
    / "docs"
    / "first_add_decision_to_terminal_loss_diagnostic_v1_spec_20260729.json"
)


def _spec() -> dict:
    return json.loads(SPEC_PATH.read_text(encoding="utf-8"))


def _day_contract(spec: dict) -> dict[str, str]:
    panels = spec["panels"]
    return {
        **{
            str(day): "A"
            for day in panels["development_primary_grade_a_days"]
        },
        **{
            str(day): "B"
            for day in panels["development_sensitivity_grade_b_days"]
        },
    }


def _day_frame(day: str, grade: str, spec: dict) -> pd.DataFrame:
    rows: list[dict] = []
    features = spec["decision_visible_features"]
    feature_columns = [
        *features["campaign_state"],
        *features["local_microstructure"],
    ]
    for side_index, side in enumerate(("BUY", "SELL")):
        for cohort_index in range(2):
            suffix = f"{day}-{side}-{cohort_index}"
            decision_ts = 1_000 + side_index * 100 + cohort_index * 10
            value = (0.02 if cohort_index else -0.01) * (
                1.0 if side == "BUY" else 0.5
            )
            row = {
                "trace_schema_version": contract.TRACE_SCHEMA_VERSION,
                "day": day,
                "quality_grade": grade,
                "campaign_id": f"campaign-{suffix}",
                "decision_id": f"decision-{suffix}",
                "decision_ts_ms": decision_ts,
                "order_id": f"order-{suffix}",
                "order_submit_ts_ms": decision_ts + 1,
                "fill_ts_ms": decision_ts + 5,
                "campaign_terminal_ts_ms": decision_ts + 50,
                "side": side,
                "inventory_role": "add",
                "exact_decision_order_fill_join": 1,
                "decision_visible_feature_ready_ts_max_ms": decision_ts,
                "decision_equity_usdc": 10.0,
                "campaign_terminal_equity_usdc": 10.0 + value,
                contract.PRIMARY_ESTIMAND: value,
            }
            for feature_index, feature in enumerate(feature_columns):
                row[feature] = float(feature_index + cohort_index + 1)
            row["campaign_age_ms"] = 100.0 + cohort_index
            row["campaign_mae_so_far_usdc"] = 0.1
            row["exposure_increasing_fill_count_so_far"] = float(cohort_index)
            row["reducing_fill_count_so_far"] = 0.0
            row["quote_distance_ticks"] = float(cohort_index + 1)
            row["queue_ahead_btc"] = 0.001 * (cohort_index + 1)
            row["queue_ahead_source"] = "native_exchange_book_exact"
            rows.append(row)
    return pd.DataFrame(rows)


def _full_trace(spec: dict) -> pd.DataFrame:
    return pd.concat(
        [
            _day_frame(day, grade, spec)
            for day, grade in sorted(_day_contract(spec).items())
        ],
        ignore_index=True,
    )


def _day_audit(day: str, grade: str, rows: int = 4) -> dict:
    return {
        "day": day,
        "quality_grade": grade,
        "trace_schema_version": contract.TRACE_SCHEMA_VERSION,
        "selected_campaign_count": rows,
        "emitted_row_count": rows,
        "unique_campaign_count": rows,
        "exact_join_count": rows,
        "feature_clock_violation_count": 0,
        "open_record_count": 0,
        "coverage_complete": True,
    }


def _full_audit(spec: dict) -> pd.DataFrame:
    return pd.DataFrame(
        [
            _day_audit(day, grade)
            for day, grade in sorted(_day_contract(spec).items())
        ]
    )


def test_builder_calls_exact_frozen_days_and_requires_native_result_key() -> None:
    spec = _spec()
    seen: list[tuple[str, str]] = []

    def run_day(*, day: str, quality_grade: str, spec: dict) -> dict:
        seen.append((day, quality_grade))
        return {
            artifact.TRACE_RESULT_KEY: _day_frame(
                day, quality_grade, spec
            ).to_dict(orient="records"),
            artifact.TRACE_AUDIT_RESULT_KEY: _day_audit(day, quality_grade),
        }

    build = artifact.build_trace_from_days(spec, run_day)
    expected = sorted(_day_contract(spec).items())
    assert seen == expected
    assert build.trace["day"].nunique() == 40
    assert build.producer_audit["coverage_complete"].all()
    assert set(build.trace["analysis_panel"]) == {
        artifact.PRIMARY_PANEL,
        artifact.SENSITIVITY_PANEL,
    }

    with pytest.raises(ValueError, match=artifact.TRACE_RESULT_KEY):
        artifact.build_trace_from_days(spec, lambda **_: {})

    def missing_audit(*, day: str, quality_grade: str, spec: dict) -> dict:
        return {artifact.TRACE_RESULT_KEY: _day_frame(day, quality_grade, spec)}

    with pytest.raises(ValueError, match=artifact.TRACE_AUDIT_RESULT_KEY):
        artifact.build_trace_from_days(spec, missing_audit)


def test_trace_audit_rejects_later_or_incomplete_development() -> None:
    spec = _spec()
    trace = _full_trace(spec)

    later = trace.copy()
    later.loc[later.index[0], "day"] = "2026-07-01"
    with pytest.raises(ValueError, match="outside frozen Development|later-panel"):
        artifact.validate_trace_artifact(later, spec)

    missing_day = trace.loc[trace["day"].ne("2026-04-20")]
    with pytest.raises(ValueError, match="denominator is incomplete"):
        artifact.validate_trace_artifact(missing_day, spec)

    future_feature = trace.copy()
    future_feature.loc[future_feature.index[0], "decision_visible_feature_ready_ts_max_ms"] += 1
    with pytest.raises(ValueError, match="non-causal"):
        artifact.validate_trace_artifact(future_feature, spec)

    broken_audit = _full_audit(spec)
    broken_audit.loc[broken_audit.index[0], "open_record_count"] = 1
    with pytest.raises(ValueError, match="open_record_count=0"):
        artifact.validate_producer_audit(broken_audit, trace, spec)

    future_clock_audit = _full_audit(spec)
    future_clock_audit.loc[
        future_clock_audit.index[0], "feature_clock_violation_count"
    ] = 1
    with pytest.raises(ValueError, match="feature_clock_violation_count=0"):
        artifact.validate_producer_audit(future_clock_audit, trace, spec)

    incomplete_coverage = _full_audit(spec)
    incomplete_coverage.loc[incomplete_coverage.index[0], "coverage_complete"] = False
    with pytest.raises(ValueError, match="coverage_complete is false"):
        artifact.validate_producer_audit(incomplete_coverage, trace, spec)

    mismatched = _full_audit(spec)
    mismatched.loc[mismatched.index[0], "exact_join_count"] -= 1
    with pytest.raises(ValueError, match="selected/emitted/unique/exact_join"):
        artifact.validate_producer_audit(mismatched, trace, spec)


def test_evaluation_keeps_grade_b_separate_and_is_evidence_only() -> None:
    spec = _spec()
    evaluation = artifact.evaluate_trace(
        _full_trace(spec),
        _full_audit(spec),
        spec,
        bootstrap_draws=200,
        bootstrap_seed=17,
    )
    report = evaluation["report"]
    assert report["development_outcome_read"] is True
    assert report["ranking_score"] is None
    assert not any(report["permissions"].values())
    assert report["limitations"]["grade_b_is_sensitivity_only_and_never_pooled"]
    assert report["native_producer_audit"]["coverage_complete"] is True
    assert report["native_producer_audit"]["open_record_count"] == 0

    outcome = evaluation["outcome_summary"]
    assert set(outcome["analysis_panel"]) == {
        artifact.PRIMARY_PANEL,
        artifact.SENSITIVITY_PANEL,
    }
    assert not outcome["analysis_panel"].astype(str).str.contains("pooled").any()
    assert outcome["supported"].all()

    mechanism = evaluation["mechanism_summary"]
    assert set(mechanism["side"]) == {"BUY", "SELL"}
    assert mechanism["supported"].any()
    supported = mechanism.loc[mechanism["supported"]]
    assert supported["day_clusters"].ge(2).all()
    assert supported["lower_95"].notna().all()
    assert mechanism.loc[
        mechanism["feature"].eq("reducing_fill_count_so_far"), "supported"
    ].eq(False).all()
    assert mechanism["metric"].eq("within_day_high_minus_low_value_usdc").all()


def test_loader_checks_quality_and_writer_freezes_artifact_hashes(
    tmp_path: Path,
) -> None:
    called: list[str] = []

    def quality_validator(spec: dict) -> pd.DataFrame:
        called.append(spec["identity"])
        return pd.DataFrame(
            {
                "day": list(_day_contract(spec)),
                "quality_grade": list(_day_contract(spec).values()),
            }
        )

    spec, quality = artifact.load_frozen_spec(
        SPEC_PATH,
        quality_validator=quality_validator,
    )
    assert called == [contract.IDENTITY]
    assert len(quality) == 40

    evaluation = artifact.evaluate_trace(
        _full_trace(spec),
        _full_audit(spec),
        spec,
        bootstrap_draws=100,
        bootstrap_seed=23,
    )
    output = tmp_path / "f10-evidence"
    paths = artifact.write_evidence_artifacts(
        evaluation,
        output_dir=output,
        spec_path=SPEC_PATH,
        spec=spec,
        producer_identity=artifact.describe_callback(quality_validator),
    )
    assert output.is_dir()
    assert set(paths) == {
        "trace",
        "producer_audit",
        "daily_summary",
        "outcome_summary",
        "mechanism_summary",
        "report",
        "manifest",
    }

    manifest = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    assert manifest["mode"] == "development_observational_evidence_only"
    assert manifest["ranking_score"] is None
    assert not any(manifest["permissions"].values())
    for identity in manifest["artifacts"].values():
        path = Path(identity["path"])
        assert path.is_file()
        assert contract.sha256_file(path) == identity["sha256"]


def test_callback_identity_can_include_frozen_native_producer_spec(
    tmp_path: Path,
) -> None:
    native_spec = tmp_path / "native-producer-spec.json"
    native_spec.write_text('{"identity":"synthetic-native-producer"}\n', encoding="utf-8")

    def run_day(**_: object) -> dict:
        return {}

    run_day.producer_identity = lambda: {  # type: ignore[attr-defined]
        "native_producer_spec": {
            "path": str(native_spec),
            "sha256": contract.sha256_file(native_spec),
        }
    }
    identity = artifact.describe_callback(run_day)
    assert identity["native_producer_spec"] == {
        "path": str(native_spec.resolve()),
        "sha256": contract.sha256_file(native_spec),
    }


def test_prebuilt_identity_accepts_native_producer_manifest(
    tmp_path: Path,
) -> None:
    native_spec = tmp_path / "native-producer-spec.json"
    native_spec.write_text('{"identity":"synthetic-native-producer"}\n', encoding="utf-8")
    manifest = tmp_path / "producer-manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "producer_identity": {
                    "native_producer_spec": {
                        "path": str(native_spec),
                        "sha256": contract.sha256_file(native_spec),
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    identity = artifact.load_producer_identity(manifest)

    assert identity["native_producer_spec"] == {
        "path": str(native_spec.resolve()),
        "sha256": contract.sha256_file(native_spec),
    }


def test_trace_artifact_preserves_explicit_unknown_queue() -> None:
    spec = _spec()
    frame = _full_trace(spec)
    frame.loc[0, "queue_ahead_btc"] = np.nan
    frame.loc[0, "queue_ahead_source"] = "unknown"

    validated = artifact.validate_trace_artifact(frame, spec)

    assert np.isnan(validated.loc[0, "queue_ahead_btc"])
    assert validated.loc[0, "queue_ahead_available"] == 0
    assert validated["queue_ahead_available"].sum() == len(validated) - 1


def test_cli_requires_trace_audit_and_frozen_producer_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = _spec()
    trace_path = tmp_path / "trace.parquet"
    audit_path = tmp_path / "audit.parquet"
    _full_trace(spec).to_parquet(trace_path, index=False)
    _full_audit(spec).to_parquet(audit_path, index=False)

    native_spec = tmp_path / "native-producer-spec.json"
    native_spec.write_text('{"identity":"synthetic-native-producer"}\n', encoding="utf-8")
    identity_path = tmp_path / "producer-identity.json"
    identity_path.write_text(
        json.dumps(
            {
                "native_producer_spec": {
                    "path": str(native_spec),
                    "sha256": contract.sha256_file(native_spec),
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        artifact,
        "load_frozen_spec",
        lambda _path: (spec, pd.DataFrame()),
    )
    output = tmp_path / "cli-output"
    result = artifact.main(
        [
            "--spec",
            str(SPEC_PATH),
            "--input-trace",
            str(trace_path),
            "--input-audit",
            str(audit_path),
            "--producer-identity",
            str(identity_path),
            "--output-dir",
            str(output),
            "--bootstrap-draws",
            "100",
        ]
    )
    assert result == 0
    manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["producer"]["native_producer_spec"]["sha256"] == (
        contract.sha256_file(native_spec)
    )
    assert not any(manifest["permissions"].values())
