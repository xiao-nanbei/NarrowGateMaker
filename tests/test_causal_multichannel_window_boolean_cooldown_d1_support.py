from __future__ import annotations

import csv
import hashlib
import json
import os
from datetime import date, timedelta
from pathlib import Path

import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_d1_support as audit,
)
from research.families.f10_live_replay_attribution.audit import (
    current_live_held_ber_replay_baseline_50d as baseline50,
)


pytestmark = pytest.mark.skipif(
    not os.environ.get("NARROWGATE_PRIVATE_RESEARCH_ROOT"),
    reason="private historical operational denominator is not configured",
)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _file(path: Path, payload: bytes = b"x") -> dict[str, object]:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return {"path": str(path), "sha256": _sha(path), "size_bytes": len(payload)}


def _fixture(tmp_path: Path) -> tuple[audit.AuditPaths, str, str]:
    days = (*audit.PREFIX40, *audit.ADDED10)
    all_source_days = sorted(
        {
            (date.fromisoformat(day) + timedelta(days=offset)).isoformat()
            for day in days
            for offset in (-1, 0, 1)
        }
    )
    raw_root = tmp_path / "cryptohft"
    trade_root = tmp_path / "trades"
    normalized = tmp_path / "normalized_l2_100ms_v2"
    features_root = tmp_path / "features"
    model_meta = tmp_path / "model" / "bundle_meta.json"

    for source_day in all_source_days:
        for hour in range(24):
            _file(
                raw_root
                / audit.EXCHANGE
                / source_day
                / f"{hour:02d}"
                / f"{audit.SYMBOL}_orderbook.parquet.zst"
            )
        trade = trade_root / f"{audit.SYMBOL}-trades-{source_day}.csv"
        trade.parent.mkdir(parents=True, exist_ok=True)
        trade.write_text(
            "id,price,qty,quote_qty,time,is_buyer_maker\n1,1,1,1,1,false\n",
            encoding="utf-8",
        )

    quality_rows: list[dict[str, object]] = []
    for source_day in all_source_days:
        bbo = _file(normalized / "bbo" / f"{audit.SYMBOL}-bbo-{source_day}.parquet")
        l2 = _file(normalized / "l2" / f"{audit.SYMBOL}-l2-{source_day}.parquet")
        quality_rows.append(
            {
                "day": source_day,
                "formal_eligible": True,
                "formal_exclusion_reason": "",
                "bbo_sha256": bbo["sha256"],
                "bbo_size_bytes": bbo["size_bytes"],
                "l2_sha256": l2["sha256"],
                "l2_size_bytes": l2["size_bytes"],
            }
        )
    quality_path = normalized / "daily_quality.csv"
    quality_path.parent.mkdir(parents=True, exist_ok=True)
    with quality_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(quality_rows[0]))
        writer.writeheader()
        writer.writerows(quality_rows)
    _json(
        normalized / "manifest.json",
        {
            "dataset_version": "normalized_l2_100ms_v2",
            "symbol": audit.SYMBOL,
            "daily_quality": {"sha256": _sha(quality_path)},
        },
    )

    daily_files = []
    for source_day in all_source_days:
        receipt = _file(features_root / f"features_{source_day}.parquet")
        daily_files.append(
            {
                "day": source_day,
                "file": Path(str(receipt["path"])).name,
                "sha256": receipt["sha256"],
                "size_bytes": receipt["size_bytes"],
            }
        )
    feature_manifest = features_root / "causal_feature_manifest.json"
    _json(
        feature_manifest,
        {
            "feature_semantics_version": 6,
            "feature_cutoff_semantics": "strict_exclusive_completed_bucket_end",
            "feature_ready_offset_ms": 10_000,
            "daily_files": daily_files,
        },
    )
    _json(model_meta, {"heads": 13})

    prefix_components = []
    added_overlays = []
    overlay_root = tmp_path / "overlays"
    for day in days:
        directory = overlay_root / day
        artifact = _file(directory / "model_overlay.npz")
        manifest = directory / "manifest.json"
        if day in audit.PREFIX40:
            _json(
                manifest,
                {
                    "identity": {
                        "utc_day": day,
                        "prior_feature_sha256": "a" * 64,
                        "target_feature_sha256": "b" * 64,
                    },
                    "files": {
                        "model_overlay.npz": {
                            "sha256": artifact["sha256"],
                            "size_bytes": artifact["size_bytes"],
                        }
                    },
                },
            )
            prefix_components.append(
                {
                    "utc_day": day,
                    "manifest_path": str(manifest),
                    "manifest_sha256": _sha(manifest),
                }
            )
        else:
            _json(
                manifest,
                {
                    "utc_day": day,
                    "prior_feature_sha256": "a" * 64,
                    "target_feature_sha256": "b" * 64,
                    "overlay_sha256": artifact["sha256"],
                },
            )
            added_overlays.append(
                {"utc_day": day, "overlay_sha256": artifact["sha256"]}
            )
    prefix_panel = tmp_path / "prefix-panel.json"
    _json(
        prefix_panel,
        {
            "identity_payload": {
                "ordered_utc_days": list(audit.PREFIX40),
                "components": prefix_components,
            }
        },
    )
    execution_plan = tmp_path / "execution" / "execution-plan.json"
    for day in audit.ADDED10:
        source = overlay_root / day
        destination = execution_plan.parent / "overlays" / day
        destination.mkdir(parents=True, exist_ok=True)
        (destination / "model_overlay.npz").write_bytes(
            (source / "model_overlay.npz").read_bytes()
        )
        manifest = json.loads((source / "manifest.json").read_text())
        _json(destination / "manifest.json", manifest)
    _json(
        execution_plan,
        {
            "ordered_utc_days": list(days),
            "added_overlays": added_overlays,
        },
    )

    panel_spec = tmp_path / "panel.json"
    _json(
        panel_spec,
        {
            "immutable_prefix": {"ordered_utc_days": list(audit.PREFIX40)},
            "added_panel": {"ordered_utc_days": list(audit.ADDED10)},
            "sources": {
                "feature_manifest_sha256": _sha(feature_manifest),
                "model_bundle_meta_sha256": _sha(model_meta),
                "prefix_control_overlay_panel_sha256": _sha(prefix_panel),
            },
        },
    )
    panel_sha = _sha(panel_spec)
    v2_spec = tmp_path / "v2.json"
    _json(
        v2_spec,
        {
            "identity": audit.IDENTITY,
            "source_separation": {
                "strict_native_2026": {"panel_spec_sha256": panel_sha}
            },
            "ordered_utc_days": {
                "prefix40": list(audit.PREFIX40),
                "added10": list(audit.ADDED10),
            },
        },
    )
    strict_spec = tmp_path / "strict.json"
    _json(
        strict_spec,
        {
            "exchange_truth": {
                "raw_root": str(raw_root),
                "exchange": audit.EXCHANGE,
                "symbol": audit.SYMBOL,
                "warmup_hours": 24,
            }
        },
    )
    binding_root = tmp_path / "dataset-binding"
    dataset_binding = baseline50._ensure_dataset_binding(
        binding_root,
        baseline50._spec(),
    )
    paths = audit.AuditPaths(
        v2_spec=v2_spec,
        panel_spec=panel_spec,
        strict_spec=strict_spec,
        raw_cryptohft_root=raw_root,
        normalized_root=normalized,
        individual_trade_root=trade_root,
        feature_manifest=feature_manifest,
        model_bundle_meta=model_meta,
        prefix_overlay_panel=prefix_panel,
        panel_execution_plan=execution_plan,
        dataset_binding=Path(dataset_binding["path"]),
    )
    return paths, _sha(v2_spec), panel_sha


def test_audit_reports_full_prefix_added_and_pooled_support(tmp_path: Path) -> None:
    paths, v2_sha, panel_sha = _fixture(tmp_path)
    report = audit.build_support_audit(
        paths=paths,
        expected_v2_spec_sha256=v2_sha,
        expected_panel_spec_sha256=panel_sha,
    )

    assert report["support"]["prefix40"]["full_source_support_day_count"] == 40
    assert report["support"]["added10"]["full_source_support_day_count"] == 10
    assert report["support"]["pooled50"]["full_source_support_day_count"] == 50
    assert report["support"]["pooled50"]["reduced_support_days"] == []
    assert report["permissions"] == {
        "economic_outcomes_read": False,
        "cooldown_labels_generated": False,
        "orders_simulated": False,
        "action_authorized": False,
        "live_authorized": False,
        "exact_historical_receive_time_authority": False,
    }
    assert all(row["raw_cryptohft_72h"]["present_hour_count"] == 72 for row in report["days"])


def test_missing_source_is_explicit_reduced_support_not_silent_drop(tmp_path: Path) -> None:
    paths, v2_sha, panel_sha = _fixture(tmp_path)
    missing_day = "2026-04-21"
    missing_trade = paths.individual_trade_root / f"{audit.SYMBOL}-trades-{missing_day}.csv"
    missing_trade.unlink()
    quality_path = paths.normalized_root / "daily_quality.csv"
    rows = list(csv.DictReader(quality_path.open(newline="", encoding="utf-8")))
    for row in rows:
        if row["day"] == missing_day:
            row["formal_eligible"] = "False"
            row["formal_exclusion_reason"] = "fixture_sequence_gap"
    with quality_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    normalized_manifest = paths.normalized_root / "manifest.json"
    payload = json.loads(normalized_manifest.read_text())
    payload["daily_quality"]["sha256"] = _sha(quality_path)
    _json(normalized_manifest, payload)

    report = audit.build_support_audit(
        paths=paths,
        expected_v2_spec_sha256=v2_sha,
        expected_panel_spec_sha256=panel_sha,
    )
    row = next(item for item in report["days"] if item["target_day"] == "2026-04-20")
    assert not row["full_source_support"]
    assert "official_individual_trades_dminus1_d_dplus1_incomplete" in row[
        "reduced_support_reasons"
    ]
    assert "normalized_dplus1_not_formal:fixture_sequence_gap" in row[
        "reduced_support_reasons"
    ]
    reduced = report["support"]["prefix40"]["reduced_support_days"]
    assert any(item["target_day"] == "2026-04-20" for item in reduced)
    assert report["support"]["prefix40"]["requested_day_count"] == 40


def test_duplicate_or_missing_frozen_days_fail_closed(tmp_path: Path) -> None:
    paths, _, panel_sha = _fixture(tmp_path)
    payload = json.loads(paths.v2_spec.read_text())
    payload["ordered_utc_days"]["prefix40"][-1] = audit.PREFIX40[0]
    _json(paths.v2_spec, payload)

    with pytest.raises(audit.SupportAuditError, match="duplicate days"):
        audit.build_support_audit(
            paths=paths,
            expected_v2_spec_sha256=_sha(paths.v2_spec),
            expected_panel_spec_sha256=panel_sha,
        )


def test_duplicate_trade_files_fail_closed(tmp_path: Path) -> None:
    paths, v2_sha, panel_sha = _fixture(tmp_path)
    day = audit.PREFIX40[0]
    duplicate = paths.individual_trade_root / f"{audit.SYMBOL}-trades-{day}.csv.gz"
    duplicate.write_bytes(b"duplicate")

    with pytest.raises(audit.SupportAuditError, match="duplicate individual-trade"):
        audit.build_support_audit(
            paths=paths,
            expected_v2_spec_sha256=v2_sha,
            expected_panel_spec_sha256=panel_sha,
        )


def test_overlay_reference_admission_is_hash_bound(tmp_path: Path) -> None:
    paths, v2_sha, panel_sha = _fixture(tmp_path)
    prefix = json.loads(paths.prefix_overlay_panel.read_text())
    component = prefix["identity_payload"]["components"][0]
    day = component["utc_day"]
    manifest_path = Path(component["manifest_path"])
    artifact = manifest_path.parent / "model_overlay.npz"
    reference = manifest_path.parent / "reference.json"
    _json(
        reference,
        {
            "identity": {"day": day},
            "data": {
                "path": str(artifact),
                "sha256": _sha(artifact),
                "size_bytes": artifact.stat().st_size,
            },
        },
    )
    _json(
        manifest_path,
        {
            "identity": {"utc_day": day},
            "files": {
                "reference.json": {
                    "sha256": _sha(reference),
                    "size_bytes": reference.stat().st_size,
                }
            },
        },
    )
    component["manifest_sha256"] = _sha(manifest_path)
    _json(paths.prefix_overlay_panel, prefix)
    panel = json.loads(paths.panel_spec.read_text())
    panel["sources"]["prefix_control_overlay_panel_sha256"] = _sha(
        paths.prefix_overlay_panel
    )
    _json(paths.panel_spec, panel)
    panel_sha = _sha(paths.panel_spec)
    v2 = json.loads(paths.v2_spec.read_text())
    v2["source_separation"]["strict_native_2026"]["panel_spec_sha256"] = panel_sha
    _json(paths.v2_spec, v2)

    report = audit.build_support_audit(
        paths=paths,
        expected_v2_spec_sha256=_sha(paths.v2_spec),
        expected_panel_spec_sha256=panel_sha,
    )
    assert report["support"]["pooled50"]["full_source_support_day_count"] == 50
