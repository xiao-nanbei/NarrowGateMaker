#!/usr/bin/env python3
"""Run the frozen policy-visible decision-cadence P3 transport audit."""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from data_paths import resolve_portable_path
from research.families.f02_empirical_p3_touch.audit.p3_touch_decision_cadence_context import (
    load_f06_baseline_eligible_decisions,
)
from research.families.f02_empirical_p3_touch.audit.p3_touch_decision_cadence_transport import (
    ACTION_NAMES,
    DecisionCadenceOOFModels,
    TransportGates,
    aggregate_calibration_summaries,
    calibration_summary,
    evaluate_transport,
    load_official_aggressive_trades,
    score_decision_day,
    summarize_scored_day,
)
from research.families.f02_empirical_p3_touch.audit.p3_touch_policy_visible_decision_context import (
    FrozenPolicyVisibleBboSource,
    extract_policy_visible_decision_context,
)

IDENTITY = "p3_touch_policy_visible_decision_cadence_transport_v1"
SCHEMA_VERSION = "narrowgate_p3_touch_policy_visible_decision_cadence_transport.v1"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _artifact(path: Path) -> dict[str, Any]:
    resolved = path.expanduser().resolve()
    return {
        "path": str(resolved),
        "sha256": _sha256(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _canonical_sha256(payload: dict[str, Any], *, omit: str) -> str:
    normalized = dict(payload)
    normalized.pop(omit, None)
    return hashlib.sha256(
        json.dumps(
            normalized,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _load_spec(path: Path) -> dict[str, Any]:
    spec = json.loads(path.read_text(encoding="utf-8"))
    if spec.get("identity") != IDENTITY:
        raise ValueError("unexpected policy-visible transport identity")
    if spec.get("canonical_spec_identity_sha256") != _canonical_sha256(
        spec,
        omit="canonical_spec_identity_sha256",
    ):
        raise ValueError("policy-visible transport canonical hash mismatch")
    permissions = spec.get("permissions") or {}
    for field in (
        "quote_mapping_authority",
        "action_authority",
        "live_authority",
    ):
        if bool(permissions.get(field, False)):
            raise ValueError(f"transport spec unexpectedly grants {field}")
    return spec


def _verify_ref(ref: dict[str, Any], *, label: str) -> Path:
    path = resolve_portable_path(str(ref.get("path", ""))).resolve()
    expected = str(ref.get("sha256", ""))
    if not path.is_file() or len(expected) != 64 or _sha256(path) != expected:
        raise ValueError(f"{label} identity mismatch: {path}")
    return path


def _monotonicity(scored: pd.DataFrame) -> tuple[int, int]:
    if scored.empty:
        return 0, 0
    wide = scored.pivot(index="decision_id", columns="action", values="p_v4")
    complete = wide.dropna(subset=list(ACTION_NAMES))
    if complete.empty:
        return 0, 0
    values = complete.loc[:, list(ACTION_NAMES)].to_numpy(dtype=np.float64)
    differences = np.diff(values, axis=1)
    return int(np.sum(differences > 1e-12)), int(differences.size)


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def _verify_f06_visibility_identity(
    *,
    manifest_path: Path,
    mechanics_spec_path: Path,
    profile_path: Path,
    profile_sha256: str,
    visibility_seed: int,
) -> None:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    identity = manifest.get("book_visibility_profile") or {}
    if str(identity.get("sha256", "")) != str(profile_sha256):
        raise ValueError("F06 placement visibility profile hash does not match")
    if Path(str(identity.get("path", ""))).name != profile_path.name:
        raise ValueError("F06 placement visibility profile filename does not match")
    mechanics_spec = json.loads(mechanics_spec_path.read_text(encoding="utf-8"))
    source_identity = mechanics_spec.get("source_identity") or {}
    if str(source_identity.get("book_visibility_profile_sha256", "")) != str(profile_sha256):
        raise ValueError("F06 mechanics spec visibility profile hash does not match")
    if int(source_identity.get("latency_seed", -1)) != int(visibility_seed):
        raise ValueError("F06 mechanics spec visibility seed does not match")
    if str(source_identity.get("latency_sampler_version", "")) != ("keyed_splitmix64_v1"):
        raise ValueError("F06 mechanics spec visibility sampler does not match")


def run(spec_path: Path) -> dict[str, Any]:
    spec_path = spec_path.expanduser().resolve()
    spec = _load_spec(spec_path)
    for label, ref in spec["implementation_identities"].items():
        _verify_ref(ref, label=f"implementation {label}")

    inputs = spec["input_identities"]
    v4_1_path = _verify_ref(inputs["p3_v4_1_spec"], label="P3 v4.1 spec")
    v2_path = _verify_ref(inputs["p3_v2_artifact"], label="P3 v2 artifact")
    v2_report_path = _verify_ref(inputs["p3_v2_report"], label="P3 v2 report")
    index_path = _verify_ref(inputs["f06_development_index"], label="F06 index")
    f06_manifest_path = _verify_ref(
        inputs["f06_placement_manifest"], label="F06 placement manifest"
    )
    f06_mechanics_spec_path = _verify_ref(inputs["f06_mechanics_spec"], label="F06 mechanics spec")
    profile_path = _verify_ref(inputs["book_visibility_profile"], label="book visibility profile")
    _verify_f06_visibility_identity(
        manifest_path=f06_manifest_path,
        mechanics_spec_path=f06_mechanics_spec_path,
        profile_path=profile_path,
        profile_sha256=str(inputs["book_visibility_profile"]["sha256"]),
        visibility_seed=int(spec["source_clock_boundary"]["visibility_seed"]),
    )

    index = pd.read_csv(index_path, dtype={"day": str}).set_index("day")
    v2_report = json.loads(v2_report_path.read_text(encoding="utf-8"))
    frozen_inputs = {
        (str(row["kind"]), Path(str(row["path"])).name): str(row["sha256"])
        for row in v2_report["inputs"]
    }
    paths = spec["paths"]
    placement_root = resolve_portable_path(str(paths["placement_root"])).resolve()
    bbo_root = resolve_portable_path(str(paths["bbo_root"])).resolve()
    trade_root = resolve_portable_path(str(paths["trade_root"])).resolve()
    supplemental_next = spec.get("supplemental_next_trade_identities", {})
    models = DecisionCadenceOOFModels(
        v4_1_spec={
            "path": str(v4_1_path),
            "sha256": inputs["p3_v4_1_spec"]["sha256"],
        },
        v2_artifact={
            "path": str(v2_path),
            "sha256": inputs["p3_v2_artifact"]["sha256"],
        },
    )

    daily_parts: list[pd.DataFrame] = []
    campaign_parts: list[pd.DataFrame] = []
    calibration_parts: list[pd.DataFrame] = []
    context_rows: list[dict[str, Any]] = []
    input_rows: list[dict[str, Any]] = []
    monotonicity_violations = 0
    monotonicity_comparisons = 0

    source_clock = spec["source_clock_boundary"]
    for sequence, raw_day in enumerate(spec["days"], start=1):
        day = str(raw_day)
        next_day = (pd.Timestamp(day) + pd.Timedelta(days=1)).date().isoformat()
        if day not in index.index:
            raise ValueError(f"{day} is absent from the frozen F06 index")
        placement_ref = {
            "path": str(placement_root / f"day={day}" / "placement.parquet"),
            "sha256": str(index.loc[day, "panel_sha256"]),
        }
        bbo_name = f"BTCUSDC-bbo-{day}.parquet"
        bbo_ref = {
            "path": str(bbo_root / bbo_name),
            "sha256": frozen_inputs.get(("bbo", bbo_name), ""),
        }
        trade_refs: list[dict[str, Any]] = []
        for trade_day in (day, next_day):
            trade_name = f"BTCUSDC-aggTrades-{trade_day}.csv"
            expected = frozen_inputs.get(("trade", trade_name))
            if expected is None:
                supplemental = supplemental_next.get(trade_day)
                if not isinstance(supplemental, dict):
                    raise ValueError(f"{trade_day} next-day label tape lacks a frozen identity")
                expected = str(supplemental.get("sha256", ""))
            trade_refs.append({"path": str(trade_root / trade_name), "sha256": expected})

        placement_path = _verify_ref(placement_ref, label=f"{day} placement")
        bbo_path = _verify_ref(bbo_ref, label=f"{day} BBO")
        decisions = load_f06_baseline_eligible_decisions(
            placement_path,
            expected_sha256=str(placement_ref["sha256"]),
        )
        batch = extract_policy_visible_decision_context(
            decisions,
            source=FrozenPolicyVisibleBboSource(
                path=bbo_path,
                sha256=str(bbo_ref["sha256"]),
                source_identity=str(source_clock["bbo_source_identity"]),
                visibility_profile_path=profile_path,
                visibility_profile_sha256=str(inputs["book_visibility_profile"]["sha256"]),
                visibility_profile_id=str(source_clock["visibility_profile_id"]),
                visibility_seed=int(source_clock["visibility_seed"]),
            ),
        )
        trade_paths = [_verify_ref(ref, label=f"{day} label trade") for ref in trade_refs]
        trade_hashes = {str(path): _sha256(path) for path in trade_paths}
        trade_ts, trade_prices, buyer_maker = load_official_aggressive_trades(
            trade_paths,
            expected_sha256=trade_hashes,
        )
        scored = score_decision_day(
            batch,
            models=models,
            trade_ts_ms=trade_ts,
            trade_prices=trade_prices,
            buyer_maker=buyer_maker,
        )
        if scored.empty:
            raise RuntimeError(f"{day} has no supported policy-visible predictions")
        daily, campaigns = summarize_scored_day(
            scored,
            denominator=decisions,
            context_batch=batch,
        )
        daily_parts.append(daily)
        campaign_parts.append(campaigns)
        calibration_parts.append(calibration_summary(scored))
        violations, comparisons = _monotonicity(scored)
        monotonicity_violations += violations
        monotonicity_comparisons += comparisons
        context_rows.append(
            {
                "day": day,
                "fold_id": models.fold_id(day),
                "denominator_rows": int(len(decisions)),
                "context_supported_rows": int(batch.metadata["supported_rows"]),
                "context_unsupported_rows": int(batch.metadata["unsupported_rows"]),
                "context_coverage": float(batch.metadata["supported_rows"] / len(decisions)),
                "unsupported_reason_counts_json": json.dumps(
                    batch.metadata["unsupported_reason_counts"], sort_keys=True
                ),
                "visibility_delay_mean_ms": float(batch.metadata["visibility_delay_mean_ms"]),
                "visibility_delay_p90_ms": float(batch.metadata["visibility_delay_p90_ms"]),
                "visibility_delay_p99_ms": float(batch.metadata["visibility_delay_p99_ms"]),
                "aws_receive_time_transport_supported": False,
            }
        )
        input_rows.extend(
            [
                {"day": day, "kind": "placement", **placement_ref},
                {"day": day, "kind": "bbo", **bbo_ref},
                *({"day": day, "kind": "label_trade", **ref} for ref in trade_refs),
            ]
        )
        print(
            "P3 policy-visible transport: "
            f"{sequence}/{len(spec['days'])} {day} "
            f"context={batch.metadata['supported_rows']}/{len(decisions)}",
            flush=True,
        )
        del decisions, batch, scored, trade_ts, trade_prices, buyer_maker
        gc.collect()

    daily_metrics = pd.concat(daily_parts, ignore_index=True)
    campaign_metrics = pd.concat(campaign_parts, ignore_index=True)
    calibration = aggregate_calibration_summaries(calibration_parts)
    report = evaluate_transport(
        daily_metrics=daily_metrics,
        calibration=calibration,
        monotonicity_violations=monotonicity_violations,
        gates=TransportGates(**spec["gates"]),
    )
    report.update(
        {
            "schema_version": SCHEMA_VERSION,
            "identity": IDENTITY,
            "question": (
                "does frozen canonical-v4.1 transport to F06 policy-visible "
                "baseline decision cadence"
            ),
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "spec": _artifact(spec_path),
            "model_identities": models.identity,
            "historical_evidence_boundary": spec["historical_evidence_boundary"],
            "source_clock_boundary": source_clock,
            "monotonicity_comparisons": monotonicity_comparisons,
            "input_file_count": len(input_rows) + 3,
            "predecessor_transport": spec["predecessor_transport"],
        }
    )

    output_dir = resolve_portable_path(str(spec["output_directory"])).resolve()
    if output_dir.exists():
        raise FileExistsError(f"transport output already exists: {output_dir}")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.stage-", dir=output_dir.parent))
    try:
        daily_metrics.to_parquet(stage / "daily_metrics.parquet", index=False)
        campaign_metrics.to_parquet(stage / "campaign_metrics.parquet", index=False)
        calibration.to_parquet(stage / "calibration_by_bin.parquet", index=False)
        pd.DataFrame(context_rows).to_parquet(stage / "context_audit.parquet", index=False)
        _write_json(
            stage / "input_manifest.json",
            [
                *input_rows,
                {"day": None, "kind": "visibility_profile", **inputs["book_visibility_profile"]},
                {"day": None, "kind": "f06_manifest", **inputs["f06_placement_manifest"]},
                {"day": None, "kind": "f06_mechanics_spec", **inputs["f06_mechanics_spec"]},
            ],
        )
        _write_json(stage / "report.json", report)
        published = {
            path.name: {
                "path": str(output_dir / path.name),
                "sha256": _sha256(path),
                "size_bytes": path.stat().st_size,
            }
            for path in sorted(stage.iterdir())
            if path.is_file()
        }
        _write_json(
            stage / "manifest.json",
            {
                "identity": IDENTITY,
                "created_at_utc": report["created_at_utc"],
                "files": published,
                "permissions": report["permissions"],
            },
        )
        (stage / "COMPLETE").write_text("complete\n", encoding="ascii")
        os.replace(stage, output_dir)
    except Exception:
        shutil.rmtree(stage, ignore_errors=True)
        raise
    print(json.dumps({"decision": report["decision"], "output": str(output_dir)}))
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    args = parser.parse_args()
    run(args.spec)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
