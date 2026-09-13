#!/usr/bin/env python3
"""Freeze the outcome-blind F02 reach-time source request and day manifest."""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_paths import data_root  # noqa: E402
from research.families.f02_empirical_p3_touch.audit.p3_reach_time_source_manifest import (  # noqa: E402
    build_source_day_manifest,
    canonical_sha256,
    sha256_file,
)

EXPECTED_PANEL_COUNTS = {
    "fit_2025_provider": 93,
    "fit_2026_current": 69,
    "historical_2026_validation": 24,
    "historical_2026_test_diagnostic": 24,
}


def _atomic_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _eligible_overlap_days(
    quality_root: Path,
    *,
    native_days: set[str],
) -> list[str]:
    overlap: list[str] = []
    for path in sorted(quality_root.glob("BTCUSDC-2026-*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        day = str(payload.get("day", ""))
        if (
            payload.get("provider_normalized_replay_candidate") is True
            and day in native_days
        ):
            overlap.append(day)
    if overlap != sorted(set(overlap)):
        raise ValueError("provider/native overlap discovery is not unique")
    return overlap


def _panel_request(
    *,
    inherited_manifest_path: Path,
    provider_quality_root: Path,
    native_quality_csv: Path,
) -> dict[str, object]:
    inherited = json.loads(inherited_manifest_path.read_text(encoding="utf-8"))
    inherited_panels = inherited.get("panels", {})
    for name, expected in EXPECTED_PANEL_COUNTS.items():
        days = list(inherited_panels.get(name, ()))
        if len(days) != expected or days != sorted(set(days)):
            raise ValueError(f"inherited panel {name} no longer matches {expected} days")
    with native_quality_csv.open(newline="", encoding="utf-8") as handle:
        native_quality = {row["day"]: row for row in csv.DictReader(handle)}
    source_panels = (
        ("fit_2026_current", "fit_2026_native"),
        (
            "historical_2026_validation",
            "historical_2026_validation_diagnostic",
        ),
        (
            "historical_2026_test_diagnostic",
            "historical_2026_test_diagnostic",
        ),
    )
    selected_native: dict[str, list[str]] = {}
    exclusions: list[dict[str, object]] = []
    for inherited_name, successor_name in source_panels:
        selected_native[successor_name] = []
        for day in inherited_panels[inherited_name]:
            quality = native_quality.get(day)
            eligible = bool(
                quality
                and str(quality.get("coverage_99_valid", "")).strip().lower()
                in {"true", "1"}
            )
            if eligible:
                selected_native[successor_name].append(day)
            else:
                exclusions.append(
                    {
                        "day": day,
                        "inherited_panel": inherited_name,
                        "reason": "current_native_coverage_99_invalid_or_missing",
                        "bbo_coverage": (
                            quality.get("bbo_coverage") if quality else None
                        ),
                    }
                )
    native_days = {
        day for days in selected_native.values() for day in days
    }
    request: dict[str, object] = {
        "schema_version": "p3_reach_time_source_panel_request.v1",
        "identity": "p3_aggressive_reach_time_conditioned_hazard_v1",
        "last_materially_modified": "2026-08-04",
        "inherited_panel_identity": {
            "path": str(inherited_manifest_path),
            "sha256": sha256_file(inherited_manifest_path),
            "purpose": "chronological_panel_membership_only",
        },
        "panels": [
            {
                "name": "fit_2025_provider",
                "source": "provider",
                "dates": inherited_panels["fit_2025_provider"],
            },
            {
                "name": "fit_2026_native",
                "source": "native",
                "dates": selected_native["fit_2026_native"],
            },
            {
                "name": "historical_2026_validation_diagnostic",
                "source": "native",
                "dates": selected_native[
                    "historical_2026_validation_diagnostic"
                ],
            },
            {
                "name": "historical_2026_test_diagnostic",
                "source": "native",
                "dates": selected_native["historical_2026_test_diagnostic"],
            },
        ],
        "overlap_dates": _eligible_overlap_days(
            provider_quality_root,
            native_days=native_days,
        ),
        "historical_native_panels_previously_read": True,
        "current_quality_exclusions": exclusions,
        "inherited_day_count": sum(EXPECTED_PANEL_COUNTS.values()),
        "selected_day_count": 93 + len(native_days),
        "economic_outcomes_read": False,
        "sealed_holdout_read": False,
    }
    request["canonical_request_sha256"] = canonical_sha256(request)
    return request


def parse_args() -> argparse.Namespace:
    project_data_root = data_root(ROOT)
    docs = ROOT / "research/families/f02_empirical_p3_touch/docs"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--inherited-manifest",
        type=Path,
        default=docs / "p3_touch_source_aware_expanded_v3_day_manifest_20260803.json",
    )
    parser.add_argument(
        "--provider-quality-root",
        type=Path,
        default=project_data_root / "normalized_tardis_l2_100ms_v1/quality",
    )
    parser.add_argument(
        "--provider-bbo-root",
        type=Path,
        default=project_data_root / "normalized_tardis_l2_100ms_v1/bbo",
    )
    parser.add_argument(
        "--native-quality-csv",
        type=Path,
        default=project_data_root / "normalized_l2_100ms_v2/daily_quality.csv",
    )
    parser.add_argument(
        "--native-bbo-root",
        type=Path,
        default=project_data_root / "normalized_l2_100ms_v2/bbo",
    )
    parser.add_argument(
        "--aggtrades-root", type=Path, default=project_data_root / "raw"
    )
    parser.add_argument(
        "--request-output",
        type=Path,
        default=docs / "p3_reach_time_source_panel_request_v1_20260804.json",
    )
    parser.add_argument(
        "--manifest-output",
        type=Path,
        default=docs / "p3_reach_time_source_day_manifest_v1_20260804.json",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    inherited = args.inherited_manifest.expanduser().resolve()
    quality_root = args.provider_quality_root.expanduser().resolve()
    request_output = args.request_output.expanduser().resolve()
    manifest_output = args.manifest_output.expanduser().resolve()
    request = _panel_request(
        inherited_manifest_path=inherited,
        provider_quality_root=quality_root,
        native_quality_csv=args.native_quality_csv.expanduser().resolve(),
    )
    _atomic_json(request_output, request)
    manifest = build_source_day_manifest(
        provider_quality_root=quality_root,
        provider_bbo_root=args.provider_bbo_root,
        native_daily_quality_csv=args.native_quality_csv,
        native_bbo_root=args.native_bbo_root,
        official_aggtrades_root=args.aggtrades_root,
        panels=request["panels"],
        overlap_dates=request["overlap_dates"],
        panel_request_identity={
            "path": str(request_output),
            "sha256": sha256_file(request_output),
            "canonical_sha256": request["canonical_request_sha256"],
        },
    )
    _atomic_json(manifest_output, manifest)
    print(request_output)
    print(manifest_output)
    print(manifest["canonical_manifest_sha256"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
