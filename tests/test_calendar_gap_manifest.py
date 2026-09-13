import json
from pathlib import Path

import pandas as pd

from data.quality.calendar_gap_manifest import (
    DAY_MS,
    build_calendar_continuity_manifest,
    load_anchor_identity,
    load_day_sources,
    sha256_file,
    validate_calendar_continuity_manifest,
)

ROOT = Path(__file__).resolve().parents[1]


def test_current_frozen_panel_has_40_targets_inside_expanded_envelope() -> None:
    identity = load_anchor_identity(
        ROOT
        / "research/families/f09_campaign_action_uplift/docs/"
        "causal_v12_ranked_toxicity_exposure_guard_carryover_safe_v2_mechanics_spec_20260803.json",
        day_field="panels.development_days",
    )

    assert identity["target_day_count"] == 40
    assert identity["target_days"][0] == "2026-04-17"
    assert identity["target_days"][-1] == "2026-06-26"
    assert "2026-06-01" not in identity["target_days"]


def test_manifest_keeps_missing_bridge_day_offline_without_dropping_calendar(
    tmp_path,
) -> None:
    day_a = "2026-01-01"
    day_f = "2026-01-02"
    l2 = tmp_path / "day-a.parquet"
    day_start = int(pd.Timestamp(day_a, tz="UTC").timestamp() * 1_000)
    pd.DataFrame(
        {
            "timestamp": [day_start + 100, day_start + DAY_MS - 100],
            "bid_price_1": [100.0, 101.0],
        }
    ).to_parquet(l2)
    quality = tmp_path / "quality.csv"
    pd.DataFrame(
        [
            {
                "day": day_a,
                "formal_eligible": True,
                "sequence_valid": True,
                "coverage_99_valid": True,
                "l2_source_path": str(l2),
                "l2_sha256": sha256_file(l2),
                "formal_exclusion_reason": "",
            }
        ]
    ).to_csv(quality, index=False)
    ledger = tmp_path / "ledger.csv"
    pd.DataFrame(
        [
            {"day": day_a, "quality_grade": "A", "quality_reasons": ""},
            {
                "day": day_f,
                "quality_grade": "F",
                "quality_reasons": "normalized_l2_not_built",
            },
        ]
    ).to_csv(ledger, index=False)
    anchor = tmp_path / "anchor.json"
    anchor.write_text(json.dumps({"panels": {"development_days": [day_a]}}))
    trade_root = tmp_path / "trades"
    (trade_root / "BTCUSDC").mkdir(parents=True)
    (trade_root / "BTCUSDC" / f"BTCUSDC-trades-{day_a}.csv").write_text("price\n100\n")

    identity = load_anchor_identity(anchor, day_field="panels.development_days")
    sources = load_day_sources(
        start_day=day_a,
        end_day=day_f,
        grade_ledger_path=ledger,
        source_quality_paths=[quality],
    )
    manifest = build_calendar_continuity_manifest(
        sources,
        start_day=day_a,
        end_day=day_f,
        anchor_identity=identity,
        official_trade_root=trade_root,
        maximum_contiguous_gap_ms=DAY_MS,
    )
    validate_calendar_continuity_manifest(manifest)

    assert manifest["calendar_day_count"] == 2
    assert manifest["anchor_target_days"] == [day_a]
    assert manifest["quality_grade_counts"]["F"] == 1
    assert manifest["data_readiness"]["missing_native_normalized_l2_days"] == [day_f]
    assert manifest["data_readiness"]["missing_any_source_normalized_l2_days"] == [day_f]
    assert manifest["data_readiness"]["missing_daily_mark_days"] == [day_f]
    assert not manifest["authority"][
        "tail_governance_causal_attribution_without_on_off_control"
    ]


def test_provider_normalized_day_bridges_calendar_without_native_authority(
    tmp_path,
) -> None:
    native_day = "2026-01-01"
    provider_day = "2026-01-02"
    l2 = tmp_path / "native.parquet"
    day_start = int(pd.Timestamp(native_day, tz="UTC").timestamp() * 1_000)
    pd.DataFrame(
        {
            "timestamp": [day_start + 100, day_start + DAY_MS - 100],
            "bid_price_1": [100.0, 101.0],
        }
    ).to_parquet(l2)
    quality = tmp_path / "native_quality.csv"
    pd.DataFrame(
        [
            {
                "day": native_day,
                "formal_eligible": True,
                "sequence_valid": True,
                "coverage_99_valid": True,
                "l2_source_path": str(l2),
                "l2_sha256": sha256_file(l2),
                "formal_exclusion_reason": "",
            }
        ]
    ).to_csv(quality, index=False)
    ledger = tmp_path / "ledger.csv"
    pd.DataFrame(
        [
            {"day": native_day, "quality_grade": "A", "quality_reasons": ""},
            {
                "day": provider_day,
                "quality_grade": "F",
                "quality_reasons": "native_l2_absent",
            },
        ]
    ).to_csv(ledger, index=False)
    anchor = tmp_path / "anchor.json"
    anchor.write_text(
        json.dumps({"panels": {"development_days": [native_day]}}),
        encoding="utf-8",
    )

    provider_root = tmp_path / "provider"
    provider_outputs = {}
    for name in ("bbo", "l2", "clock"):
        path = provider_root / name / f"BTCUSDC-{name}-{provider_day}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(f"{name}:{provider_day}".encode())
        provider_outputs[f"{name}_output"] = {
            "path": str(path),
            "sha256": sha256_file(path),
            "size_bytes": path.stat().st_size,
        }
    provider_quality = (
        provider_root / "quality" / f"BTCUSDC-{provider_day}.json"
    )
    provider_quality.parent.mkdir(parents=True, exist_ok=True)
    provider_end = (
        int(pd.Timestamp(provider_day, tz="UTC").timestamp() * 1_000)
        + DAY_MS
    )
    provider_quality.write_text(
        json.dumps(
            {
                "day": provider_day,
                "complete_day": True,
                "provider_normalized_replay_candidate": True,
                "last_timestamp_ms": provider_end - 100,
                **provider_outputs,
            }
        ),
        encoding="utf-8",
    )
    trade_root = tmp_path / "trades"
    native_trades = (
        trade_root / "BTCUSDC" / f"BTCUSDC-trades-{native_day}.csv"
    )
    native_trades.parent.mkdir(parents=True, exist_ok=True)
    native_trades.write_text("price\n100\n", encoding="utf-8")

    identity = load_anchor_identity(anchor, day_field="panels.development_days")
    sources = load_day_sources(
        start_day=native_day,
        end_day=provider_day,
        grade_ledger_path=ledger,
        source_quality_paths=[quality],
    )
    manifest = build_calendar_continuity_manifest(
        sources,
        start_day=native_day,
        end_day=provider_day,
        anchor_identity=identity,
        official_trade_root=trade_root,
        provider_normalized_root=provider_root,
        maximum_contiguous_gap_ms=DAY_MS,
    )

    provider_row = manifest["day_sources"][1]
    assert not provider_row["native_normalized_l2_file_available"]
    assert not provider_row["strategy_tape_usable"]
    assert provider_row["provider_normalized_tape_usable"]
    assert provider_row["daily_mark_source"] == "tardis_provider_normalized_bbo"
    assert manifest["data_readiness"][
        "calendar_all_any_source_normalized_l2_files_available"
    ]
    assert manifest["data_readiness"]["calendar_daily_mark_bridge_complete"]
    assert not manifest["authority"]["upgrade_non_a_to_grade_a"]
