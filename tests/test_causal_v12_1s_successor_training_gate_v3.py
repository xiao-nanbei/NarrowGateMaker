from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_execution_identity as execution_identity,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_orico_source_spec as source_specs,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_parity_successor_gate as parity_gate,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_real_day_cpp_parity as real_parity,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_training as training,
)


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _fake_build_receipt() -> dict[str, object]:
    return {
        "receipt_sha256": "build-receipt",
        "f03_component_semantics": {"identity_sha256": "component-v3"},
    }


def _parity_report(
    path: Path,
    *,
    utc_day: str,
    role: str,
    build: dict[str, object],
) -> None:
    source_permissions = dict(execution_identity.SOURCE_PERMISSION_CONTRACT)
    if role == "provider":
        profile_id = execution_identity.PROVIDER_PROFILE_ID
    else:
        profile_id = source_specs.NATIVE_NORMALIZED_PROFILE
        source_permissions["feature_prediction_training_authority"] = False
    sample_indices = real_parity.python_oracle_sample_indices(real_parity.FULL_DAY_ROWS)
    day_start = int(
        datetime.strptime(utc_day, "%Y-%m-%d").replace(tzinfo=UTC).timestamp()
        * 1_000
    )
    sample_cutoffs = real_parity.python_oracle_sample_cutoffs(
        day_start,
        real_parity.FULL_DAY_ROWS,
    )
    sample_rows = len(sample_indices)
    feature_cells = sample_rows * len(real_parity.schema.TRAINABLE_FEATURE_ORDER)
    numeric_contract = real_parity.numeric_comparison_contract(
        rtol=real_parity.DEFAULT_RTOL,
        atol=real_parity.DEFAULT_ATOL,
    )
    numeric_contract_sha256 = real_parity._canonical_sha256(numeric_contract)
    reduction_feature_stats = {
        name: {
            "evaluated_cells": sample_rows,
            "passed_cells": sample_rows,
            "failed_cells": 0,
            "min_observation_count": int(
                name.removeprefix("taker_signed_quote_sum_").removesuffix("s")
            ),
            "max_observation_count": int(
                name.removeprefix("taker_signed_quote_sum_").removesuffix("s")
            ),
            "max_input_l1_scale": 1.0,
            "max_computed_envelope": 1e-12,
            "max_observed_abs_error": 0.0,
            "max_error_envelope_utilization": 0.0,
        }
        for name in real_parity.SIGNED_QUOTE_REDUCTION_FEATURES
    }
    payload = {
        "schema_version": real_parity.SCHEMA_VERSION,
        "identity": real_parity.IDENTITY,
        "status": "passed_complete_day_cpp_and_stratified_python_173_field_parity",
        "utc_day": utc_day,
        "complete_utc_day": True,
        "source_profile": {
            "profile_id": profile_id,
            "source_permissions": source_permissions,
        },
        "cutoffs": {
            "rows": real_parity.FULL_DAY_ROWS,
            "python_oracle_sample": {
                "rows": sample_rows,
                "index_list_sha256": real_parity._canonical_sha256(
                    list(sample_indices)
                ),
                "cutoff_list_sha256": real_parity._canonical_sha256(
                    list(sample_cutoffs)
                ),
            },
        },
        "feature_contract": {
            "feature_count": len(real_parity.schema.TRAINABLE_FEATURE_ORDER),
            "feature_names": list(real_parity.schema.TRAINABLE_FEATURE_ORDER),
            "feature_order_sha256": real_parity.schema.feature_order_sha256(),
            "feature_contract_sha256": (
                real_parity.full.full_feature_contract_fingerprint()
            ),
            "source_manifest_sha256": real_parity.schema.canonical_sha256(
                real_parity.schema.source_manifest_payload()
            ),
            "cpp_abi_version": real_parity.CPP_ABI_VERSION,
        },
        "parity": {
            "panel_python_tolerance_parity_rows": sample_rows,
            "panel_cpp_tolerance_parity_rows": real_parity.FULL_DAY_ROWS,
            "cpp_python_tolerance_parity_rows": sample_rows,
            "python_oracle_tolerance_parity_rows": sample_rows,
            "full_day_panel_cpp_tolerance_parity_rows": real_parity.FULL_DAY_ROWS,
            "numeric_comparison_contract": numeric_contract,
            "numeric_comparison_contract_sha256": numeric_contract_sha256,
            "signed_quote_reduction_envelope": {
                "contract_id": real_parity.SIGNED_QUOTE_REDUCTION_ERROR_CONTRACT,
                "numeric_comparison_contract_sha256": numeric_contract_sha256,
                "feature_allowlist": list(
                    real_parity.SIGNED_QUOTE_REDUCTION_FEATURES
                ),
                "total": {
                    "evaluated_cells": sample_rows
                    * len(real_parity.SIGNED_QUOTE_REDUCTION_FEATURES),
                    "passed_cells": sample_rows
                    * len(real_parity.SIGNED_QUOTE_REDUCTION_FEATURES),
                    "failed_cells": 0,
                    "min_observation_count": 5,
                    "max_observation_count": 60,
                    "max_input_l1_scale": 1.0,
                    "max_computed_envelope": 1e-12,
                    "max_observed_abs_error": 0.0,
                    "max_error_envelope_utilization": 0.0,
                },
                "by_feature": reduction_feature_stats,
                "comparison_stream_sha256": "a" * 64,
            },
            "panel_cpp_bitwise_exact_required": True,
            "panel_cpp_bitwise_exact_row_fingerprint_matches": (
                real_parity.FULL_DAY_ROWS
            ),
            "python_oracle_feature_cell_comparisons": feature_cells,
            "python_oracle_channel_comparisons": {
                channel: feature_cells
                for channel in real_parity.PYTHON_ORACLE_CHANNELS
            },
            "python_oracle_channel_mismatches": {
                channel: 0 for channel in real_parity.PYTHON_ORACLE_CHANNELS
            },
            "validity_mismatches": 0,
            "source_timestamp_mismatches": 0,
            "ready_timestamp_mismatches": 0,
            "observation_count_mismatches": 0,
            "lag_state_mismatches": 0,
            "cutoff_mismatches": 0,
            "comparison_stream_sha256": f"stream-{role}-{utc_day}",
        },
        "field_stats": {
            name: {"supported_rows": sample_rows, "unsupported_rows": 0}
            for name in real_parity.schema.TRAINABLE_FEATURE_ORDER
        },
        "full_day_panel_cpp_field_stats": {
            name: {
                "supported_rows": real_parity.FULL_DAY_ROWS,
                "unsupported_rows": 0,
            }
            for name in real_parity.schema.TRAINABLE_FEATURE_ORDER
        },
        "implementation_identity": {
            "runner_path": str(Path(real_parity.__file__).resolve()),
            "runner_sha256": execution_identity.sha256_file(
                Path(real_parity.__file__).resolve()
            ),
            "python_code": real_parity._current_python_code_identity(),
            "f03_component_semantics": build["f03_component_semantics"],
            "native_build_receipt": {
                "receipt_sha256": build["receipt_sha256"],
            },
        },
        "permissions": {
            "predictions_read": False,
            "economic_outcomes_read": False,
        },
    }
    payload["report_identity_sha256"] = real_parity._canonical_sha256(payload)
    _write_json(path, payload)


def test_loaded_module_reports_final_lifecycle_abi() -> None:
    module = SimpleNamespace(
        ORDER_LIFECYCLE_JOURNAL_V2_MIRROR_ABI_VERSION=(
            execution_identity.EXPECTED_ORDER_LIFECYCLE_JOURNAL_ABI
        )
    )
    assert execution_identity.reported_extension_abis(module) == {
        "order_lifecycle_journal_v2": (
            "order_lifecycle_journal_v2_cpp_event_stream_mirror.v2"
        )
    }


def test_loaded_module_rejects_wrong_lifecycle_abi() -> None:
    module = SimpleNamespace(ORDER_LIFECYCLE_JOURNAL_V2_MIRROR_ABI_VERSION="v1")
    with pytest.raises(execution_identity.ExecutionIdentityError, match="unsupported"):
        execution_identity.reported_extension_abis(module)


def test_explicit_p3_must_equal_config_resolved_path(tmp_path: Path) -> None:
    model_dir = tmp_path / "model"
    model_dir.mkdir()
    configured = model_dir / "fill_prob_params.json"
    configured.write_text("{}\n", encoding="utf-8")
    other = tmp_path / "other.json"
    other.write_text("{}\n", encoding="utf-8")
    config = tmp_path / "config.yaml"
    config.write_text(f"ml:\n  model_dir: {model_dir}\n", encoding="utf-8")
    with pytest.raises(execution_identity.ExecutionIdentityError, match="config-resolved"):
        execution_identity.validate_explicit_p3_identity(config, other)


def test_parity_gate_requires_early_and_late_days_per_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    build_path = tmp_path / "build.json"
    build_path.write_text("{}\n", encoding="utf-8")
    build = _fake_build_receipt()
    monkeypatch.setattr(
        execution_identity,
        "validate_native_build_receipt",
        lambda _path: build,
    )
    provider = tmp_path / "provider.json"
    native = tmp_path / "native.json"
    _parity_report(provider, utc_day="2025-08-01", role="provider", build=build)
    _parity_report(native, utc_day="2026-04-17", role="native", build=build)
    with pytest.raises(parity_gate.ParitySuccessorGateError, match="early and late"):
        parity_gate.freeze_training_parity_gate(
            tmp_path / "gate.json",
            provider_report_paths=[provider],
            native_report_paths=[native],
            native_build_receipt_path=build_path,
        )


def test_parity_gate_accepts_distinct_complete_early_late_sets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    build_path = tmp_path / "build.json"
    build_path.write_text("{}\n", encoding="utf-8")
    build = _fake_build_receipt()
    monkeypatch.setattr(
        execution_identity,
        "validate_native_build_receipt",
        lambda _path: build,
    )
    provider_paths = []
    native_paths = []
    for role, days, destination in (
        ("provider", ("2025-08-01", "2025-12-30"), provider_paths),
        ("native", ("2026-04-17", "2026-07-31"), native_paths),
    ):
        for day in reversed(days):
            path = tmp_path / f"{role}-{day}.json"
            _parity_report(path, utc_day=day, role=role, build=build)
            destination.append(path)
    gate_path = tmp_path / "gate.json"
    frozen = parity_gate.freeze_training_parity_gate(
        gate_path,
        provider_report_paths=provider_paths,
        native_report_paths=native_paths,
        native_build_receipt_path=build_path,
    )
    assert [row["utc_day"] for row in frozen["provider_complete_day_reports"]] == [
        "2025-08-01",
        "2025-12-30",
    ]
    assert [row["utc_day"] for row in frozen["native_complete_day_reports"]] == [
        "2026-04-17",
        "2026-07-31",
    ]
    assert parity_gate.validate_training_parity_gate(gate_path) == frozen


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("feature_names", "feature contract differs"),
        ("full_day_fingerprints", "fingerprints differ"),
        ("channel_denominator", "channel denominators differ"),
        ("reduction_envelope", "signed-quote reduction denominator"),
    ],
)
def test_parity_gate_rejects_incomplete_evidence_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    message: str,
) -> None:
    build_path = tmp_path / "build.json"
    build_path.write_text("{}\n", encoding="utf-8")
    build = _fake_build_receipt()
    monkeypatch.setattr(
        execution_identity,
        "validate_native_build_receipt",
        lambda _path: build,
    )
    report_path = tmp_path / "provider.json"
    _parity_report(
        report_path,
        utc_day="2025-08-01",
        role="provider",
        build=build,
    )
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if mutation == "feature_names":
        report["feature_contract"]["feature_names"][0] = "fabricated"
    elif mutation == "full_day_fingerprints":
        report["parity"]["panel_cpp_bitwise_exact_row_fingerprint_matches"] -= 1
    elif mutation == "channel_denominator":
        report["parity"]["python_oracle_channel_comparisons"]["value"] -= 1
    else:
        report["parity"]["signed_quote_reduction_envelope"]["total"][
            "passed_cells"
        ] -= 1
    report.pop("report_identity_sha256")
    report["report_identity_sha256"] = real_parity._canonical_sha256(report)
    _write_json(report_path, report)

    with pytest.raises(parity_gate.ParitySuccessorGateError, match=message):
        parity_gate._validate_report(
            report_path,
            role="provider",
            build_receipt=build,
        )


def test_old_synthetic_training_manifest_cannot_authorize(tmp_path: Path) -> None:
    manifest = tmp_path / "old.json"
    _write_json(
        manifest,
        {
            "schema_version": "causal_v12_1s_training_day_manifest.v1",
            "training_input_authorized": True,
            "days": [{"utc_day": "2025-08-01"}],
        },
    )
    with pytest.raises(training.OneSecondTrainingError, match="unsupported"):
        training.load_training_day_manifest(manifest)
