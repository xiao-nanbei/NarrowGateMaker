#!/usr/bin/env python3
"""Training-authority gate over complete provider/native F03 parity reports."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_execution_identity as execution_identity,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_orico_source_spec as source_specs,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_real_day_cpp_parity as parity,
)

SCHEMA_VERSION = "causal_v12_1s_real_day_parity_training_gate.v6"
STATUS = (
    "provider_and_native_early_late_full_cpp_stratified_python_"
    "173_field_parity_passed"
)
MIN_COMPLETE_DAYS_PER_SOURCE = 2


class ParitySuccessorGateError(ValueError):
    """Raised when parity evidence cannot authorize F03 training mechanics."""


def _load_json(path: Path, *, role: str) -> dict[str, Any]:
    resolved = path.expanduser().resolve(strict=True)
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ParitySuccessorGateError(f"{role} must be a JSON object")
    return payload


def _validate_report(
    path: Path,
    *,
    role: str,
    build_receipt: Mapping[str, Any],
) -> dict[str, Any]:
    report_path = path.expanduser().resolve(strict=True)
    report = _load_json(report_path, role=f"{role} parity report")
    report_identity = report.get("report_identity_sha256")
    unsigned_report = dict(report)
    unsigned_report.pop("report_identity_sha256", None)
    if report_identity != parity._canonical_sha256(unsigned_report):
        raise ParitySuccessorGateError(f"{role} parity report identity differs")
    if report.get("schema_version") != parity.SCHEMA_VERSION:
        raise ParitySuccessorGateError(f"{role} parity report schema differs")
    if report.get("identity") != parity.IDENTITY:
        raise ParitySuccessorGateError(f"{role} parity report identity name differs")
    if report.get("status") != (
        "passed_complete_day_cpp_and_stratified_python_173_field_parity"
    ):
        raise ParitySuccessorGateError(f"{role} parity did not pass")
    if report.get("complete_utc_day") is not True:
        raise ParitySuccessorGateError(f"{role} parity is not a complete UTC day")
    cutoffs = report.get("cutoffs")
    if not isinstance(cutoffs, Mapping) or cutoffs.get("rows") != parity.FULL_DAY_ROWS:
        raise ParitySuccessorGateError(f"{role} parity denominator is not 86,400")
    sample = cutoffs.get("python_oracle_sample")
    if not isinstance(sample, Mapping):
        raise ParitySuccessorGateError(f"{role} parity lacks Python-oracle sampling")
    expected_indices = parity.python_oracle_sample_indices(parity.FULL_DAY_ROWS)
    expected_day_start = int(
        datetime.strptime(str(report.get("utc_day")), "%Y-%m-%d")
        .replace(tzinfo=UTC)
        .timestamp()
        * 1_000
    )
    expected_cutoffs = parity.python_oracle_sample_cutoffs(
        expected_day_start,
        parity.FULL_DAY_ROWS,
    )
    expected_sample_rows = len(expected_indices)
    if sample.get("rows") != expected_sample_rows:
        raise ParitySuccessorGateError(f"{role} Python-oracle denominator differs")
    if sample.get("index_list_sha256") != parity._canonical_sha256(
        list(expected_indices)
    ):
        raise ParitySuccessorGateError(f"{role} Python-oracle index set differs")
    if sample.get("cutoff_list_sha256") != parity._canonical_sha256(
        list(expected_cutoffs)
    ):
        raise ParitySuccessorGateError(f"{role} Python-oracle cutoff set differs")
    feature = report.get("feature_contract")
    expected_feature_contract = {
        "feature_count": len(parity.schema.TRAINABLE_FEATURE_ORDER),
        "feature_names": list(parity.schema.TRAINABLE_FEATURE_ORDER),
        "feature_order_sha256": parity.schema.feature_order_sha256(),
        "feature_contract_sha256": parity.full.full_feature_contract_fingerprint(),
        "source_manifest_sha256": parity.schema.canonical_sha256(
            parity.schema.source_manifest_payload()
        ),
        "cpp_abi_version": parity.CPP_ABI_VERSION,
    }
    if feature != expected_feature_contract:
        raise ParitySuccessorGateError(f"{role} parity feature contract differs")
    result = report.get("parity")
    if not isinstance(result, Mapping):
        raise ParitySuccessorGateError(f"{role} parity result is missing")
    expected_numeric_contract = parity.numeric_comparison_contract(
        rtol=parity.DEFAULT_RTOL,
        atol=parity.DEFAULT_ATOL,
    )
    expected_numeric_contract_sha256 = parity._canonical_sha256(
        expected_numeric_contract
    )
    if result.get("numeric_comparison_contract") != expected_numeric_contract:
        raise ParitySuccessorGateError(
            f"{role} parity numeric-comparison contract differs"
        )
    if result.get("numeric_comparison_contract_sha256") != (
        expected_numeric_contract_sha256
    ):
        raise ParitySuccessorGateError(
            f"{role} parity numeric-comparison contract SHA256 differs"
        )
    envelope = result.get("signed_quote_reduction_envelope")
    if not isinstance(envelope, Mapping):
        raise ParitySuccessorGateError(
            f"{role} parity lacks signed-quote reduction evidence"
        )
    if envelope.get("contract_id") != parity.SIGNED_QUOTE_REDUCTION_ERROR_CONTRACT:
        raise ParitySuccessorGateError(
            f"{role} signed-quote reduction contract differs"
        )
    if envelope.get("numeric_comparison_contract_sha256") != (
        expected_numeric_contract_sha256
    ):
        raise ParitySuccessorGateError(
            f"{role} signed-quote evidence is not bound to the numeric contract"
        )
    if envelope.get("feature_allowlist") != list(
        parity.SIGNED_QUOTE_REDUCTION_FEATURES
    ):
        raise ParitySuccessorGateError(
            f"{role} signed-quote reduction allowlist differs"
        )
    expected_reduction_cells = expected_sample_rows * len(
        parity.SIGNED_QUOTE_REDUCTION_FEATURES
    )
    total_reduction = envelope.get("total")
    if not isinstance(total_reduction, Mapping) or any(
        total_reduction.get(key) != expected
        for key, expected in (
            ("evaluated_cells", expected_reduction_cells),
            ("passed_cells", expected_reduction_cells),
            ("failed_cells", 0),
        )
    ):
        raise ParitySuccessorGateError(
            f"{role} signed-quote reduction denominator or pass count differs"
        )
    if not 0.0 <= float(total_reduction.get("max_error_envelope_utilization", -1.0)) <= 1.0:
        raise ParitySuccessorGateError(
            f"{role} signed-quote reduction exceeded its forward-error envelope"
        )
    reduction_by_feature = envelope.get("by_feature")
    if not isinstance(reduction_by_feature, Mapping) or set(reduction_by_feature) != set(
        parity.SIGNED_QUOTE_REDUCTION_FEATURES
    ):
        raise ParitySuccessorGateError(
            f"{role} signed-quote per-feature evidence differs"
        )
    for name in parity.SIGNED_QUOTE_REDUCTION_FEATURES:
        feature_evidence = reduction_by_feature[name]
        window = int(name.removeprefix("taker_signed_quote_sum_").removesuffix("s"))
        if not isinstance(feature_evidence, Mapping) or any(
            feature_evidence.get(key) != expected
            for key, expected in (
                ("evaluated_cells", expected_sample_rows),
                ("passed_cells", expected_sample_rows),
                ("failed_cells", 0),
                ("min_observation_count", window),
                ("max_observation_count", window),
            )
        ):
            raise ParitySuccessorGateError(
                f"{role} signed-quote evidence differs for {name}"
            )
        if not 0.0 <= float(
            feature_evidence.get("max_error_envelope_utilization", -1.0)
        ) <= 1.0:
            raise ParitySuccessorGateError(
                f"{role} signed-quote envelope exceeded for {name}"
            )
    reduction_digest = envelope.get("comparison_stream_sha256")
    if not isinstance(reduction_digest, str) or len(reduction_digest) != 64 or any(
        character not in "0123456789abcdef" for character in reduction_digest
    ):
        raise ParitySuccessorGateError(
            f"{role} signed-quote comparison stream digest is malformed"
        )
    for key in (
        "panel_cpp_tolerance_parity_rows",
        "full_day_panel_cpp_tolerance_parity_rows",
    ):
        if result.get(key) != parity.FULL_DAY_ROWS:
            raise ParitySuccessorGateError(f"{role} parity has incomplete {key}")
    for key in (
        "panel_python_tolerance_parity_rows",
        "cpp_python_tolerance_parity_rows",
        "python_oracle_tolerance_parity_rows",
    ):
        if result.get(key) != expected_sample_rows:
            raise ParitySuccessorGateError(f"{role} parity has incomplete {key}")
    if result.get("panel_cpp_bitwise_exact_required") is not True:
        raise ParitySuccessorGateError(
            f"{role} parity does not require exact full-day fingerprints"
        )
    if result.get("panel_cpp_bitwise_exact_row_fingerprint_matches") != (
        parity.FULL_DAY_ROWS
    ):
        raise ParitySuccessorGateError(
            f"{role} full-day panel/C++ fingerprints differ"
        )
    expected_feature_cells = expected_sample_rows * len(
        parity.schema.TRAINABLE_FEATURE_ORDER
    )
    if result.get("python_oracle_feature_cell_comparisons") != expected_feature_cells:
        raise ParitySuccessorGateError(
            f"{role} Python-oracle feature-cell denominator differs"
        )
    expected_channels = {
        channel: expected_feature_cells for channel in parity.PYTHON_ORACLE_CHANNELS
    }
    if result.get("python_oracle_channel_comparisons") != expected_channels:
        raise ParitySuccessorGateError(
            f"{role} Python-oracle channel denominators differ"
        )
    if result.get("python_oracle_channel_mismatches") != {
        channel: 0 for channel in parity.PYTHON_ORACLE_CHANNELS
    }:
        raise ParitySuccessorGateError(
            f"{role} Python-oracle channel mismatches are nonzero"
        )
    for key in (
        "validity_mismatches",
        "source_timestamp_mismatches",
        "ready_timestamp_mismatches",
        "observation_count_mismatches",
        "lag_state_mismatches",
        "cutoff_mismatches",
    ):
        if result.get(key) != 0:
            raise ParitySuccessorGateError(f"{role} parity has nonzero {key}")
    fields = report.get("field_stats")
    expected_field_names = set(parity.schema.TRAINABLE_FEATURE_ORDER)
    if not isinstance(fields, Mapping) or set(fields) != expected_field_names:
        raise ParitySuccessorGateError(f"{role} parity field statistics are incomplete")
    if any(
        int(row.get("supported_rows", -1)) + int(row.get("unsupported_rows", -1))
        != expected_sample_rows
        for row in fields.values()
        if isinstance(row, Mapping)
    ) or any(not isinstance(row, Mapping) for row in fields.values()):
        raise ParitySuccessorGateError(f"{role} Python field denominators differ")
    full_fields = report.get("full_day_panel_cpp_field_stats")
    if not isinstance(full_fields, Mapping) or set(full_fields) != expected_field_names:
        raise ParitySuccessorGateError(
            f"{role} full-day C++ field statistics are incomplete"
        )
    if any(
        int(row.get("supported_rows", -1)) + int(row.get("unsupported_rows", -1))
        != parity.FULL_DAY_ROWS
        for row in full_fields.values()
        if isinstance(row, Mapping)
    ) or any(not isinstance(row, Mapping) for row in full_fields.values()):
        raise ParitySuccessorGateError(f"{role} full-day C++ field denominators differ")
    implementation = report.get("implementation_identity")
    if not isinstance(implementation, Mapping):
        raise ParitySuccessorGateError(f"{role} parity implementation identity is missing")
    if implementation.get("f03_component_semantics") != build_receipt.get(
        "f03_component_semantics"
    ):
        raise ParitySuccessorGateError(f"{role} parity used a different F03 component")
    runner_path = Path(str(implementation.get("runner_path", ""))).expanduser().resolve()
    expected_runner_path = Path(parity.__file__).resolve()
    if runner_path != expected_runner_path or implementation.get(
        "runner_sha256"
    ) != execution_identity.sha256_file(expected_runner_path):
        raise ParitySuccessorGateError(f"{role} parity runner identity differs")
    if implementation.get("python_code") != parity._current_python_code_identity():
        raise ParitySuccessorGateError(f"{role} parity Python code identity differs")
    report_build = implementation.get("native_build_receipt")
    if not isinstance(report_build, Mapping) or report_build.get("receipt_sha256") != build_receipt.get(
        "receipt_sha256"
    ):
        raise ParitySuccessorGateError(f"{role} parity used a different build receipt")
    source = report.get("source_profile")
    if not isinstance(source, Mapping):
        raise ParitySuccessorGateError(f"{role} parity lacks source profile binding")
    profile_id = source.get("profile_id")
    if role == "provider":
        if profile_id != execution_identity.PROVIDER_PROFILE_ID:
            raise ParitySuccessorGateError("provider parity profile differs")
        if source.get("source_permissions") != execution_identity.SOURCE_PERMISSION_CONTRACT:
            raise ParitySuccessorGateError("provider parity permission contract differs")
    else:
        native_profiles = {
            source_specs.NATIVE_NORMALIZED_PROFILE,
            source_specs.NATIVE_HISTORICAL_MINIMAL141_PROFILE,
        }
        if profile_id not in native_profiles:
            raise ParitySuccessorGateError("native parity profile is not native")
        permissions = source.get("source_permissions")
        if not isinstance(permissions, Mapping):
            raise ParitySuccessorGateError("native parity permissions are missing")
        for key in (
            "queue_authority",
            "order_lifecycle_authority",
            "fill_path_authority",
            "pnl_authority",
        ):
            if permissions.get(key) is not False:
                raise ParitySuccessorGateError(f"native parity must bind {key}=false")
    permissions = report.get("permissions")
    if not isinstance(permissions, Mapping):
        raise ParitySuccessorGateError(f"{role} parity permissions are missing")
    if permissions.get("predictions_read") is not False or permissions.get(
        "economic_outcomes_read"
    ) is not False:
        raise ParitySuccessorGateError(f"{role} parity read forbidden outcomes")
    return {
        **execution_identity.file_identity(report_path),
        "utc_day": report["utc_day"],
        "profile_id": profile_id,
        "comparison_stream_sha256": result["comparison_stream_sha256"],
    }


def _validate_report_set(
    paths: Sequence[Path],
    *,
    role: str,
    build_receipt: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if len(paths) < MIN_COMPLETE_DAYS_PER_SOURCE:
        raise ParitySuccessorGateError(
            f"{role} parity requires at least early and late complete days"
        )
    rows = [
        _validate_report(path, role=role, build_receipt=build_receipt) for path in paths
    ]
    days = [str(row["utc_day"]) for row in rows]
    if len(set(days)) != len(days):
        raise ParitySuccessorGateError(f"{role} parity days must be distinct")
    rows.sort(key=lambda row: str(row["utc_day"]))
    if str(rows[0]["utc_day"]) >= str(rows[-1]["utc_day"]):
        raise ParitySuccessorGateError(f"{role} parity lacks an early/late span")
    return rows


def freeze_training_parity_gate(
    output_path: Path,
    *,
    provider_report_paths: Sequence[Path],
    native_report_paths: Sequence[Path],
    native_build_receipt_path: Path,
) -> dict[str, Any]:
    build_path = native_build_receipt_path.expanduser().resolve(strict=True)
    build = execution_identity.validate_native_build_receipt(build_path)
    providers = _validate_report_set(
        provider_report_paths,
        role="provider",
        build_receipt=build,
    )
    natives = _validate_report_set(
        native_report_paths,
        role="native",
        build_receipt=build,
    )
    unsigned = {
        "schema_version": SCHEMA_VERSION,
        "status": STATUS,
        "created_at_utc": datetime.now(tz=UTC).isoformat(),
        "native_build_receipt": {
            **execution_identity.file_identity(build_path),
            "receipt_sha256": build["receipt_sha256"],
        },
        "f03_component_semantics_sha256": build["f03_component_semantics"][
            "identity_sha256"
        ],
        "provider_complete_day_reports": providers,
        "native_complete_day_reports": natives,
        "provider_complete_day_count": len(providers),
        "native_complete_day_count": len(natives),
        "feature_count": 173,
        "training_authorized": True,
        "predictions_read": False,
        "economic_outcomes_read": False,
        "queue_authority": False,
        "order_lifecycle_authority": False,
        "fill_path_authority": False,
        "pnl_authority": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    payload = {
        **unsigned,
        "parity_gate_identity_sha256": execution_identity.canonical_sha256(unsigned),
    }
    output = output_path.expanduser().resolve()
    if output.exists():
        return validate_training_parity_gate(output)
    execution_identity.write_json_fsync(output, payload)
    return payload


def validate_training_parity_gate(path: Path) -> dict[str, Any]:
    gate_path = path.expanduser().resolve(strict=True)
    payload = _load_json(gate_path, role="parity successor gate")
    if payload.get("schema_version") != SCHEMA_VERSION or payload.get("status") != STATUS:
        raise ParitySuccessorGateError("unsupported parity successor gate")
    identity = payload.get("parity_gate_identity_sha256")
    unsigned = dict(payload)
    unsigned.pop("parity_gate_identity_sha256", None)
    if identity != execution_identity.canonical_sha256(unsigned):
        raise ParitySuccessorGateError("parity successor gate SHA256 mismatch")
    if payload.get("training_authorized") is not True:
        raise ParitySuccessorGateError("parity successor gate does not authorize training")
    build_binding = payload.get("native_build_receipt")
    if not isinstance(build_binding, Mapping):
        raise ParitySuccessorGateError("parity successor gate lacks build receipt")
    build_path = Path(str(build_binding.get("path", ""))).expanduser().resolve(strict=True)
    build = execution_identity.validate_native_build_receipt(build_path)
    if build_binding.get("receipt_sha256") != build.get("receipt_sha256"):
        raise ParitySuccessorGateError("parity gate build receipt drifted")
    provider_bindings = payload.get("provider_complete_day_reports")
    native_bindings = payload.get("native_complete_day_reports")
    if not isinstance(provider_bindings, list) or not isinstance(native_bindings, list):
        raise ParitySuccessorGateError("parity successor gate lacks report sets")
    providers = _validate_report_set(
        [Path(str(row["path"])) for row in provider_bindings],
        role="provider",
        build_receipt=build,
    )
    natives = _validate_report_set(
        [Path(str(row["path"])) for row in native_bindings],
        role="native",
        build_receipt=build,
    )
    if providers != provider_bindings:
        raise ParitySuccessorGateError("provider parity report bindings drifted")
    if natives != native_bindings:
        raise ParitySuccessorGateError("native parity report bindings drifted")
    if payload.get("provider_complete_day_count") != len(providers):
        raise ParitySuccessorGateError("provider parity report count drifted")
    if payload.get("native_complete_day_count") != len(natives):
        raise ParitySuccessorGateError("native parity report count drifted")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--provider-report", type=Path, action="append", required=True)
    parser.add_argument("--native-report", type=Path, action="append", required=True)
    parser.add_argument("--native-build-receipt", type=Path, required=True)
    args = parser.parse_args()
    payload = freeze_training_parity_gate(
        args.output,
        provider_report_paths=args.provider_report,
        native_report_paths=args.native_report,
        native_build_receipt_path=args.native_build_receipt,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
