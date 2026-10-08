#!/usr/bin/env python3
"""Validate Python/C++ CIF state-update parity on a trained F07 artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import uuid
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from research.families.f07_active_order_continuation.audit import (
    order_lifecycle_v2_downstream_execution_amendment_v1_5 as provenance,
)
from research.families.f07_active_order_continuation.audit.active_order_competing_risk_cif_inference_v1_1 import (
    update_active_order_competing_risk_cif as python_update,
)
from research.families.f07_active_order_continuation.audit.active_order_lifecycle_cif_100ms_training_v1_5 import (
    IDENTITY as TRAINING_IDENTITY,
)
from research.families.f07_active_order_continuation.audit.active_order_lifecycle_cif_100ms_training_v1_5 import (
    _age_bin,
    kernel_rates_from_lifecycle_rates,
)

IDENTITY = provenance.PARITY_IDENTITY
SCHEMA_VERSION = provenance.PARITY_SCHEMA_VERSION
RTOL = 2e-14
ATOL = 2e-15


def _canonical_sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _artifact(path: Path) -> dict[str, object]:
    resolved = path.expanduser().resolve()
    return {
        "path": str(resolved),
        "size_bytes": int(resolved.stat().st_size),
        "sha256": _file_sha256(resolved),
    }


def _atomic_write_json(path: Path, payload: Mapping[str, object]) -> None:
    resolved = path.expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    temporary = resolved.with_name(f".{resolved.name}.partial-{os.getpid()}-{uuid.uuid4().hex}")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, resolved)
        descriptor = os.open(resolved.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.expanduser().resolve().read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("JSON object required")
    return payload


class ArtifactRateTable:
    def __init__(self, artifact: Mapping[str, object]) -> None:
        if artifact.get("identity") != TRAINING_IDENTITY:
            raise ValueError("unexpected CIF training artifact identity")
        expected_hash = str(artifact.get("canonical_artifact_sha256", ""))
        body = dict(artifact)
        body.pop("canonical_artifact_sha256", None)
        if _canonical_sha256(body) != expected_hash:
            raise ValueError("CIF training artifact canonical hash differs")
        self.cells = {
            (
                str(row["side"]),
                str(row["phase"]),
                int(row["risk_age_bin"]),
                str(row["remaining_class"]),
                int(row["utc_hour_bin"]),
            ): dict(row["rates_per_s"])
            for row in artifact["cells"]
        }
        self.parents = {
            (str(row["side"]), str(row["phase"]), int(row["risk_age_bin"])): dict(
                row["rates_per_s"]
            )
            for row in artifact["parent_rates"]
        }
        if not self.cells or not self.parents:
            raise ValueError("CIF rate table is empty")

    def rates(
        self,
        *,
        side: str,
        phase: str,
        age_s: float,
        remaining_class: str,
        utc_hour_bin: int,
    ) -> np.ndarray:
        age = _age_bin(age_s)
        key = (side, phase, age, remaining_class, int(utc_hour_bin))
        source = self.cells.get(key)
        if source is None:
            source = self.parents.get((side, phase, age))
        if source is None:
            raise KeyError(f"unsupported CIF state: {key}")
        return np.asarray(kernel_rates_from_lifecycle_rates(source), dtype=np.float64)


def _compare_result(python: Mapping[str, object], cpp: Mapping[str, object]) -> float:
    maximum = 0.0
    for key in (
        "hazards",
        "no_event_probability",
        "survival_before",
        "survival_after",
        "cif_before",
        "cif_after",
        "final_cif",
    ):
        left = np.asarray(python[key], dtype=np.float64)
        right = np.asarray(cpp[key], dtype=np.float64)
        if left.shape != right.shape:
            raise AssertionError(f"Python/C++ CIF shape differs: {key}")
        if left.size:
            maximum = max(maximum, float(np.max(np.abs(left - right))))
        np.testing.assert_allclose(right, left, rtol=RTOL, atol=ATOL)
    if int(python["final_last_edge"]) != int(cpp["final_last_edge"]):
        raise AssertionError("Python/C++ final grid edge differs")
    if not math.isclose(
        float(python["final_survival"]),
        float(cpp["final_survival"]),
        rel_tol=RTOL,
        abs_tol=ATOL,
    ):
        raise AssertionError("Python/C++ final survival differs")
    return maximum


def run_parity(
    *,
    artifact_path: Path,
    training_report_path: Path,
    amendment_path: Path,
    output_path: Path,
) -> dict[str, object]:
    import narrowgate_cpp as cpp

    model_path = artifact_path.expanduser().resolve()
    training_report_file = training_report_path.expanduser().resolve()
    amendment_file = amendment_path.expanduser().resolve()
    preliminary = _load_json(model_path)
    inputs = preliminary.get("input_artifacts")
    if not isinstance(inputs, Mapping):
        raise ValueError("CIF training artifact input identities are missing")
    plan_path = Path(str(inputs.get("execution_plan", {}).get("path", ""))).expanduser().resolve()
    panel_path = Path(str(inputs.get("panel_manifest", {}).get("path", ""))).expanduser().resolve()
    lockstep_path = Path(
        str(inputs.get("python_cpp_lockstep", {}).get("path", ""))
    ).expanduser().resolve()
    amendment, plan = provenance.validate_downstream_execution_amendment(
        amendment_file,
        plan_path=plan_path,
    )
    provenance.validate_panel_manifest_strict(panel_path, plan=plan)
    provenance.validate_lockstep_report_for_training(
        lockstep_path,
        plan_path=plan_path,
        panel_path=panel_path,
        amendment_path=amendment_file,
        amendment=amendment,
        plan=plan,
    )
    artifact = provenance.validate_training_artifact_for_parity(
        model_path,
        plan_path=plan_path,
        panel_path=panel_path,
        lockstep_report_path=lockstep_path,
        amendment_path=amendment_file,
        amendment=amendment,
        plan=plan,
    )
    provenance.validate_training_report_for_parity(
        training_report_file,
        artifact_path=model_path,
        artifact=artifact,
        plan_path=plan_path,
        panel_path=panel_path,
        lockstep_report_path=lockstep_path,
        amendment_path=amendment_file,
        amendment=amendment,
        plan=plan,
    )
    table = ArtifactRateTable(artifact)
    cpp_path = Path(cpp.__file__).expanduser().resolve()
    expected_cpp = plan["global_execution_identity"]["cpp_event_stream"]["module_artifact"]
    if cpp_path != Path(str(expected_cpp["path"])).expanduser().resolve():
        raise RuntimeError("loaded C++ module path differs from the training plan")
    if _file_sha256(cpp_path) != str(expected_cpp["sha256"]):
        raise RuntimeError("loaded C++ module hash differs from the training plan")

    state_families = sorted(
        {
            (side, phase, remaining, hour)
            for side, phase, _age, remaining, hour in table.cells
        }
    )
    trajectory_reports: list[dict[str, object]] = []
    maximum_difference = 0.0
    checkpoint_maximum_difference = 0.0
    for side, phase, remaining, hour in state_families:
        rates = []
        supported_edges = []
        for edge in range(1, 301):
            age_s = edge * 0.1
            try:
                row = table.rates(
                    side=side,
                    phase=phase,
                    age_s=age_s,
                    remaining_class=remaining,
                    utc_hour_bin=hour,
                )
            except KeyError:
                continue
            supported_edges.append(edge)
            rates.append(row)
        if not rates:
            continue
        # A trajectory must be contiguous for the native scheduler. Limit it to
        # the first contiguous supported prefix rather than filling a model gap.
        prefix = 1
        while prefix < len(supported_edges) and supported_edges[prefix] == (
            supported_edges[prefix - 1] + 1
        ):
            prefix += 1
        edges = np.asarray(supported_edges[:prefix], dtype=np.int64)
        rate_array = np.asarray(rates[:prefix], dtype=np.float64)
        if np.any(rate_array[:, 1] != 0.0):
            raise AssertionError("unclassified adverse-fill channel must remain zero")
        initial_cif = np.zeros(4, dtype=np.float64)
        python = python_update(
            edges=edges,
            rates_per_s=rate_array,
            initial_last_edge=int(edges[0]) - 1,
            initial_survival=1.0,
            initial_cif=initial_cif,
        )
        native = cpp.update_active_order_competing_risk_cif(
            edges,
            rate_array,
            int(edges[0]) - 1,
            1.0,
            initial_cif,
        )
        maximum_difference = max(maximum_difference, _compare_result(python, native))

        split = max(1, len(edges) // 2)
        first = cpp.update_active_order_competing_risk_cif(
            edges[:split],
            rate_array[:split],
            int(edges[0]) - 1,
            1.0,
            initial_cif,
        )
        second = cpp.update_active_order_competing_risk_cif(
            edges[split:],
            rate_array[split:],
            int(first["final_last_edge"]),
            float(first["final_survival"]),
            np.asarray(first["final_cif"], dtype=np.float64),
        )
        checkpoint_difference = max(
            abs(float(second["final_survival"]) - float(native["final_survival"])),
            float(
                np.max(
                    np.abs(
                        np.asarray(second["final_cif"], dtype=np.float64)
                        - np.asarray(native["final_cif"], dtype=np.float64)
                    )
                )
            ),
        )
        if checkpoint_difference > ATOL:
            raise AssertionError("C++ CIF checkpoint resume differs from single batch")
        checkpoint_maximum_difference = max(
            checkpoint_maximum_difference, checkpoint_difference
        )
        trajectory_reports.append(
            {
                "side": side,
                "phase": phase,
                "remaining_class": remaining,
                "utc_hour_bin": hour,
                "edge_count": int(len(edges)),
                "python_cpp_max_abs_difference": maximum_difference,
                "checkpoint_max_abs_difference": checkpoint_difference,
            }
        )

    gates = {
        "trained_artifact_bound": True,
        "training_report_bound": True,
        "report_artifact_plan_closure_bound": True,
        "downstream_implementation_hashes_bound": True,
        "cpp_module_bound_to_execution_plan": True,
        "representative_trajectory_count_positive": bool(trajectory_reports),
        "python_cpp_state_update_parity": maximum_difference <= ATOL,
        "cpp_checkpoint_resume_parity": checkpoint_maximum_difference <= ATOL,
        "adverse_fill_channel_fixed_zero": True,
        "economic_outcomes_not_read": True,
    }
    passed = bool(all(gates.values()))
    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": "passed" if passed else "failed_closed",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "downstream_execution_amendment": provenance.amendment_reference(
            amendment_file,
            amendment,
        ),
        "training_artifact": _artifact(model_path),
        "training_report": _artifact(training_report_file),
        "plan_sha256": plan["canonical_plan_sha256"],
        "cpp_module": _artifact(cpp_path),
        "trajectory_count": len(trajectory_reports),
        "maximum_python_cpp_abs_difference": maximum_difference,
        "maximum_checkpoint_abs_difference": checkpoint_maximum_difference,
        "trajectories": trajectory_reports,
        "gates": gates,
        "scope": dict(provenance.PARITY_SCOPE),
        "permissions": dict(provenance.PARITY_PERMISSIONS),
    }
    report["canonical_report_sha256"] = _canonical_sha256(report)
    _atomic_write_json(output_path, report)
    if not passed:
        raise RuntimeError("trained-artifact Python/C++ CIF parity failed closed")
    return report


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--training-report", type=Path, required=True)
    parser.add_argument("--execution-amendment", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    report = run_parity(
        artifact_path=args.artifact,
        training_report_path=args.training_report,
        amendment_path=args.execution_amendment,
        output_path=args.out,
    )
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
