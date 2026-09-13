from __future__ import annotations

import json
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from data_paths import data_root
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_daily_sources as daily,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_feature_generator as base,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_materialization_benchmark as benchmark,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_orico_source_spec as builder,
)

TARGET_DAY = "2025-08-02"
WARMUP_DAY = "2025-08-01"
NATIVE_TARGET_DAY = "2026-04-17"
NATIVE_WARMUP_DAY = "2026-04-16"
ORICO_ROOT = data_root()
COVERAGE_AUDIT = Path(
    "research/families/f03_causal_13_head/docs/"
    "causal_v12_1s_2026_native_source_coverage_v1_20260805.json"
)


def _exact_profile_paths(root: Path) -> tuple[Path, ...]:
    profile = builder.PROFILES[builder.PROVIDER_NORMALIZED_PROFILE]
    days = (WARMUP_DAY, TARGET_DAY)
    return (
        *(
            root / profile.local_trade_tempo_dir / f"BTCUSDC-trade-tempo-{day}.parquet"
            for day in days
        ),
        root / profile.local_manifest_path,
        *(root / profile.execution_l2_dir / f"BTCUSDC-l2-{day}.parquet" for day in days),
        *(root / profile.execution_l2_quality_dir / f"BTCUSDC-{day}.json" for day in days),
        *(root / profile.metrics_dir / f"BTCUSDC-metrics-{day}.csv" for day in days),
        *(root / profile.reference_bar_dir / f"BTCUSDT-1s-{day}.parquet" for day in days),
        *(root / profile.reference_bar_dir / f"BTCUSDT-1s-{day}.parquet.meta.json" for day in days),
    )


@pytest.fixture
def exact_layout(tmp_path: Path) -> Path:
    root = tmp_path / "MarketData" / "NarrowGate_BTCUSDC"
    root.mkdir(parents=True)
    for path in _exact_profile_paths(root):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}\n", encoding="utf-8")
    return root


def _eligible_probe(bundle: daily.DailySourceBundle) -> dict[str, object]:
    return {
        "schema_version": daily.SCHEMA_VERSION,
        "bundle_identity_sha256": bundle.identity_sha256(),
        "physical_materialization_eligible": True,
        "failure_reasons": [],
    }


def _registry_base_probe(bundle: daily.DailySourceBundle) -> dict[str, object]:
    coverage = {
        group: {"valid": True}
        for group in (
            "local_trade_tempo",
            "execution_l2",
            "execution_l2_quality",
            "metrics",
            "reference_bars",
            "reference_bar_manifest",
        )
    }
    return {
        "schema_version": daily.SCHEMA_VERSION,
        "bundle_identity_sha256": bundle.identity_sha256(),
        "physical_materialization_eligible": False,
        "failure_reasons": ["legacy per-day quality shape is intentionally unsupported"],
        "path_day_coverage": coverage,
        "local_source_authority": {"valid": True},
        "execution_l2_quality_authority": {"valid": False},
        "metrics_authority": {"valid": True},
        "reference_btcusdt_authority": {"valid": True},
        "bar_clock_authority": {"valid": True},
        "files": [
            {
                "group": (
                    "execution_l2_quality"
                    if path in bundle.execution_l2_quality_paths
                    else "physical_source"
                ),
                "path": str(path),
                "schema_supported": path not in bundle.execution_l2_quality_paths,
            }
            for path in bundle.all_paths()
        ],
    }


def _write_minimal_l2(path: Path) -> None:
    values: dict[str, pa.Array] = {"timestamp": pa.array([1], type=pa.int64())}
    for side in ("bid", "ask"):
        for kind in ("px", "qty"):
            for level in range(1, 21):
                values[f"{side}_{kind}_{level}"] = pa.array([1.0], type=pa.float64())
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(values), path)


@pytest.fixture
def native_exact_layout(tmp_path: Path) -> Path:
    root = tmp_path / "MarketData" / "NarrowGate_BTCUSDC"
    root.mkdir(parents=True)
    profile = builder.PROFILES[builder.NATIVE_HISTORICAL_MINIMAL141_PROFILE]
    days = (NATIVE_WARMUP_DAY, NATIVE_TARGET_DAY)
    for day in days:
        for path in (
            root / profile.local_trade_tempo_dir / f"BTCUSDC-trade-tempo-{day}.parquet",
            root / profile.metrics_dir / f"BTCUSDC-metrics-{day}.csv",
            root / profile.reference_bar_dir / f"BTCUSDT-1s-{day}.parquet",
            root / profile.reference_bar_dir / f"BTCUSDT-1s-{day}.parquet.meta.json",
        ):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("{}\n", encoding="utf-8")
        _write_minimal_l2(root / profile.execution_l2_dir / f"BTCUSDC-l2-{day}.parquet")
    local_manifest = root / profile.local_manifest_path
    local_manifest.parent.mkdir(parents=True, exist_ok=True)
    local_manifest.write_text("{}\n", encoding="utf-8")

    quality_path = root / str(profile.execution_l2_quality_path)
    quality_path.parent.mkdir(parents=True, exist_ok=True)
    header = (
        "day,rebuilt,sequence_valid,warmup_valid,target_source_valid,"
        "formal_eligible,formal_exclusion_reason,source_label,reconstruction_mode,"
        "source_formal_capable,cadence_schema_valid,l2_rows,l2_source_path,"
        "l2_sha256,l2_size_bytes\n"
    )
    rows: list[str] = []
    manifest_files: list[dict[str, object]] = []
    for day in days:
        l2_path = root / profile.execution_l2_dir / f"BTCUSDC-l2-{day}.parquet"
        sha = daily.sha256_file(l2_path)
        size = l2_path.stat().st_size
        rows.append(
            f"{day},True,True,True,True,True,,registry_20260727,"
            f"registry_snapshot_20260727,True,True,1,{l2_path},{sha},{size}\n"
        )
        manifest_files.append(
            {
                "day": day,
                "kind": "l2",
                "destination_relative_path": f"l2/BTCUSDC-l2-{day}.parquet",
                "source_identity": {"sha256": sha, "size_bytes": size},
            }
        )
    quality_path.write_text(header + "".join(rows), encoding="utf-8")
    manifest_path = root / str(profile.execution_l2_manifest_path)
    manifest_path.write_text(
        json.dumps(
            {
                "contract_version": 1,
                "dataset_version": "normalized_l2_100ms_v2",
                "cadence_policy": {"levels": 20},
                "daily_quality": {
                    "sha256": daily.sha256_file(quality_path),
                    "size_bytes": quality_path.stat().st_size,
                },
                "files": manifest_files,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return root


def test_builder_resolves_exact_d_minus_one_and_target_authorities(
    exact_layout: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(daily, "probe_source_bundle", _eligible_probe)

    built = builder.build_orico_daily_source_spec(
        target_day=TARGET_DAY,
        market_data_root=exact_layout,
    )

    assert [path.name for path in built.bundle.local_trade_tempo_paths] == [
        f"BTCUSDC-trade-tempo-{WARMUP_DAY}.parquet",
        f"BTCUSDC-trade-tempo-{TARGET_DAY}.parquet",
    ]
    assert [path.name for path in built.bundle.execution_l2_quality_paths] == [
        f"BTCUSDC-{WARMUP_DAY}.json",
        f"BTCUSDC-{TARGET_DAY}.json",
    ]
    assert [path.name for path in built.bundle.reference_bar_manifest_paths] == [
        f"BTCUSDT-1s-{WARMUP_DAY}.parquet.meta.json",
        f"BTCUSDT-1s-{TARGET_DAY}.parquet.meta.json",
    ]
    assert built.probe["physical_materialization_eligible"] is True


def test_builder_does_not_search_for_missing_quality_authority(
    exact_layout: Path,
) -> None:
    missing = next(
        path
        for path in _exact_profile_paths(exact_layout)
        if path.name == f"BTCUSDC-{WARMUP_DAY}.json"
    )
    missing.unlink()
    substitute = missing.with_name(f"alternate-BTCUSDC-{WARMUP_DAY}.json")
    substitute.write_text("{}\n", encoding="utf-8")

    with pytest.raises(base.FeatureContractError, match="fallback discovery is forbidden"):
        builder.resolve_orico_daily_source_bundle(
            target_day=TARGET_DAY,
            market_data_root=exact_layout,
        )


def test_native_historical_profile_resolves_exact_registry_authorities(
    native_exact_layout: Path,
) -> None:
    bundle = builder.resolve_orico_daily_source_bundle(
        target_day=NATIVE_TARGET_DAY,
        market_data_root=native_exact_layout,
        profile_id=builder.NATIVE_HISTORICAL_MINIMAL141_PROFILE,
    )

    assert [path.name for path in bundle.execution_l2_quality_paths] == [
        "manifest.json",
        "daily_quality.csv",
    ]
    assert [path.name for path in bundle.execution_l2_paths] == [
        f"BTCUSDC-l2-{NATIVE_WARMUP_DAY}.parquet",
        f"BTCUSDC-l2-{NATIVE_TARGET_DAY}.parquet",
    ]
    assert bundle.execution_l2_clock_identity == "cryptohft_transaction_time_100ms_grid"
    assert bundle.reference_source_identity == "binance_futures_reference_individual_trades_1s.v1"


def test_native_historical_profile_probe_binds_warmup_and_target_roles(
    native_exact_layout: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    probed: list[daily.DailySourceBundle] = []

    def probe(bundle: daily.DailySourceBundle) -> dict[str, object]:
        probed.append(bundle)
        return _registry_base_probe(bundle)

    monkeypatch.setattr(daily, "probe_source_bundle", probe)

    built = builder.build_orico_daily_source_spec(
        target_day=NATIVE_TARGET_DAY,
        market_data_root=native_exact_layout,
        profile_id=builder.NATIVE_HISTORICAL_MINIMAL141_PROFILE,
    )

    authority = built.probe["execution_l2_quality_authority"]
    assert built.probe["physical_materialization_eligible"] is True
    assert [(row["day"], row["role"], row["valid"]) for row in authority["bound_days"]] == [
        (NATIVE_WARMUP_DAY, "warmup", True),
        (NATIVE_TARGET_DAY, "target", True),
    ]
    assert built.probe["fallback_discovery_used"] is False
    assert built.probe["substitute_warmup_used"] is False
    assert built.probe["aggregate_reference_bars_used"] is False
    assert probed == [built.bundle]


def test_native_historical_profile_rejects_manifest_hash_drift(
    native_exact_layout: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(daily, "probe_source_bundle", _registry_base_probe)
    profile = builder.PROFILES[builder.NATIVE_HISTORICAL_MINIMAL141_PROFILE]
    quality_path = native_exact_layout / str(profile.execution_l2_quality_path)
    quality_path.write_text(quality_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    with pytest.raises(base.FeatureContractError, match="daily-quality SHA256 mismatch"):
        builder.build_orico_daily_source_spec(
            target_day=NATIVE_TARGET_DAY,
            market_data_root=native_exact_layout,
            profile_id=builder.NATIVE_HISTORICAL_MINIMAL141_PROFILE,
        )


def test_native_historical_profile_never_substitutes_missing_d_minus_one(
    native_exact_layout: Path,
) -> None:
    profile = builder.PROFILES[builder.NATIVE_HISTORICAL_MINIMAL141_PROFILE]
    missing = (
        native_exact_layout / profile.execution_l2_dir / f"BTCUSDC-l2-{NATIVE_WARMUP_DAY}.parquet"
    )
    missing.unlink()
    substitute = missing.with_name(f"alternate-BTCUSDC-l2-{NATIVE_WARMUP_DAY}.parquet")
    _write_minimal_l2(substitute)

    with pytest.raises(base.FeatureContractError, match="fallback discovery is forbidden"):
        builder.resolve_orico_daily_source_bundle(
            target_day=NATIVE_TARGET_DAY,
            market_data_root=native_exact_layout,
            profile_id=builder.NATIVE_HISTORICAL_MINIMAL141_PROFILE,
        )


def test_existing_profiles_keep_per_day_json_quality_semantics() -> None:
    for profile_id in (builder.PROVIDER_NORMALIZED_PROFILE, builder.NATIVE_NORMALIZED_PROFILE):
        profile = builder.PROFILES[profile_id]
        assert profile.execution_l2_quality_authority == builder.PER_DAY_JSON_QUALITY_AUTHORITY
        assert profile.execution_l2_manifest_path is None
        assert profile.execution_l2_quality_path is None


@pytest.mark.skipif(
    os.environ.get("NARROWGATE_RUN_ORICO_INTEGRATION") != "1",
    reason="set NARROWGATE_RUN_ORICO_INTEGRATION=1 for the read-only 40-day ORICO probe",
)
def test_native_historical_profile_resolves_and_probes_frozen_40_days() -> None:
    coverage = json.loads(COVERAGE_AUDIT.read_text(encoding="utf-8"))
    development = next(
        row
        for row in coverage["proposed_profile_panel_coverage"]
        if row["panel_id"] == "development_40"
    )

    assert development["complete"] is True
    assert len(development["accepted_days"]) == 40
    for day in development["accepted_days"]:
        built = builder.build_orico_daily_source_spec(
            target_day=day,
            market_data_root=ORICO_ROOT,
            profile_id=builder.NATIVE_HISTORICAL_MINIMAL141_PROFILE,
        )
        assert built.probe["physical_materialization_eligible"] is True, day
        assert built.probe["profile_id"] == builder.NATIVE_HISTORICAL_MINIMAL141_PROFILE


@pytest.mark.skipif(
    os.environ.get("NARROWGATE_RUN_ORICO_INTEGRATION") != "1",
    reason="set NARROWGATE_RUN_ORICO_INTEGRATION=1 for the read-only metrics repair probe",
)
def test_native_historical_profile_accepts_repaired_metrics_clock_days() -> None:
    for day in ("2026-07-19", "2026-07-20"):
        built = builder.build_orico_daily_source_spec(
            target_day=day,
            market_data_root=ORICO_ROOT,
            profile_id=builder.NATIVE_HISTORICAL_MINIMAL141_PROFILE,
        )
        assert built.probe["metrics_authority"]["valid"] is True, day
        assert built.probe["physical_materialization_eligible"] is True, day


def test_builder_rejects_resolved_bundle_when_authority_probe_fails(
    exact_layout: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        daily,
        "probe_source_bundle",
        lambda bundle: {
            "physical_materialization_eligible": False,
            "failure_reasons": ["quality authority failed"],
        },
    )

    with pytest.raises(base.FeatureContractError, match="quality authority failed"):
        builder.build_orico_daily_source_spec(
            target_day=TARGET_DAY,
            market_data_root=exact_layout,
        )


def test_cli_atomically_publishes_roundtrippable_source_spec(
    exact_layout: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(daily, "probe_source_bundle", _eligible_probe)
    output = tmp_path / "source-spec.json"
    probe = tmp_path / "source-probe.json"

    assert (
        builder.main(
            [
                "--target-day",
                TARGET_DAY,
                "--market-data-root",
                str(exact_layout),
                "--output",
                str(output),
                "--probe-output",
                str(probe),
            ]
        )
        == 0
    )

    roundtrip = daily.DailySourceBundle.from_json(output)
    assert roundtrip.utc_day == TARGET_DAY
    assert json.loads(probe.read_text(encoding="utf-8"))["failure_reasons"] == []
    assert not list(output.parent.glob(".*.tmp-*"))


def test_canonical_benchmark_sample_is_unique_and_target_day_bounded() -> None:
    cutoffs = benchmark.canonical_cutoff_sample(TARGET_DAY, 1_000)
    day_start = cutoffs[0]

    assert len(cutoffs) == 1_000
    assert len(set(cutoffs)) == 1_000
    assert cutoffs == tuple(sorted(cutoffs))
    assert all(value % 1_000 == 0 for value in cutoffs)
    assert cutoffs[0] == day_start
    assert cutoffs[-1] < day_start + 86_400_000


def test_full_day_extrapolation_keeps_fixed_cost_once() -> None:
    estimated = benchmark.extrapolated_full_day_wall_seconds(
        fixed_seconds=12.0,
        measured_row_seconds=5.0,
        row_count=100,
    )

    assert estimated == pytest.approx(12.0 + 5.0 * 864.0)
