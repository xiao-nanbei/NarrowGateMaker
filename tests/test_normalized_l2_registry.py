from __future__ import annotations

import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from data.normalized_l2_registry import (
    CadencePolicy,
    FormalEligibilityError,
    IncompleteSourceUnionError,
    SourceRootSpec,
    assemble_registry,
    require_formal_day,
    sha256_file,
)

SYMBOL = "BTCUSDC"
TEST_POLICY = CadencePolicy(
    min_coverage=0.0,
    min_valid_spread_ratio=1.0,
    max_p99_gap_s=0.5,
)


def _write_pair(
    root: Path,
    day: str,
    *,
    interval_ms: int = 100,
    rows: int = 20,
) -> None:
    start_ms = int(pd.Timestamp(day, tz="UTC").timestamp() * 1_000)
    timestamps = start_ms + np.arange(rows, dtype=np.int64) * interval_ms
    bbo = pd.DataFrame(
        {
            "timestamp": timestamps,
            "best_bid": np.full(rows, 100.0),
            "best_bid_qty": np.full(rows, 2.0),
            "best_ask": np.full(rows, 100.1),
            "best_ask_qty": np.full(rows, 3.0),
        }
    )
    l2_columns: dict[str, np.ndarray] = {"timestamp": timestamps}
    for level in range(1, 21):
        l2_columns[f"bid_px_{level}"] = np.full(
            rows, 100.0 - (level - 1) * 0.1
        )
        l2_columns[f"bid_qty_{level}"] = np.full(rows, 2.0)
        l2_columns[f"ask_px_{level}"] = np.full(
            rows, 100.1 + (level - 1) * 0.1
        )
        l2_columns[f"ask_qty_{level}"] = np.full(rows, 3.0)
    l2 = pd.DataFrame(l2_columns)

    (root / "bbo").mkdir(parents=True, exist_ok=True)
    (root / "l2").mkdir(parents=True, exist_ok=True)
    bbo.to_parquet(root / "bbo" / f"{SYMBOL}-bbo-{day}.parquet", index=False)
    l2.to_parquet(root / "l2" / f"{SYMBOL}-l2-{day}.parquet", index=False)


def _write_inputs(
    root: Path,
    days: list[str],
    *,
    strict_days: list[str] | None = None,
) -> dict[str, Path]:
    paths = {
        "good_days": root / "good_days.csv",
        "source_availability": root / "source_availability.csv",
        "sequence_audit": root / "sequence_audit.csv",
        "strict_days": root / "strict_days.csv",
    }
    pd.DataFrame({"day": days}).to_csv(paths["good_days"], index=False)
    pd.DataFrame(
        {
            "day": days,
            "target_complete": [True] * len(days),
            "warmup_complete": [True] * len(days),
        }
    ).to_csv(paths["source_availability"], index=False)
    pd.DataFrame(
        {
            "day": days,
            "eligible": [True] * len(days),
        }
    ).to_csv(paths["sequence_audit"], index=False)
    pd.DataFrame({"day": strict_days or days}).to_csv(
        paths["strict_days"], index=False
    )
    return paths


def _assemble(
    *,
    output: Path,
    inputs: dict[str, Path],
    sources: list[SourceRootSpec],
    dry_run: bool = False,
):
    return assemble_registry(
        output_root=output,
        good_days_path=inputs["good_days"],
        source_availability_path=inputs["source_availability"],
        sequence_audit_path=inputs["sequence_audit"],
        normalized_strict_days_path=inputs["strict_days"],
        source_roots=sources,
        cadence_policy=TEST_POLICY,
        dry_run=dry_run,
    )


def test_priority_union_hardlinks_and_denies_legacy_formal_status(
    tmp_path: Path,
) -> None:
    days = ["2026-07-18", "2026-07-19", "2026-07-20"]
    inputs = _write_inputs(tmp_path, days)
    strict = tmp_path / "strict"
    retained = tmp_path / "retained"
    mixed = tmp_path / "mixed"

    _write_pair(strict, days[0])
    _write_pair(retained, days[0])
    _write_pair(retained, days[1])
    _write_pair(mixed, days[2])
    source_hash_before = sha256_file(
        mixed / "l2" / f"{SYMBOL}-l2-{days[2]}.parquet"
    )
    output = tmp_path / "normalized_l2_100ms_v2"

    result = _assemble(
        output=output,
        inputs=inputs,
        sources=[
            SourceRootSpec(
                strict,
                "snapshot_24h_warmup",
                formal_capable=True,
                label="strict",
            ),
            SourceRootSpec(
                retained,
                "delta_converged_120s",
                label="retained",
            ),
            SourceRootSpec(
                mixed,
                "legacy_mixed_verified_100ms",
                label="legacy_mixed",
            ),
        ],
    )

    quality = result.quality.set_index("day")
    assert quality.loc[days[0], "source_label"] == "strict"
    assert quality.loc[days[1], "source_label"] == "retained"
    assert quality.loc[days[2], "source_label"] == "legacy_mixed"
    assert quality.loc[days[2], "reconstruction_mode"] == (
        "legacy_mixed_verified_100ms"
    )
    assert bool(quality.loc[days[0], "formal_eligible"]) is True
    assert bool(quality.loc[days[1], "formal_eligible"]) is False
    assert bool(quality.loc[days[2], "formal_eligible"]) is False

    strict_source = strict / "bbo" / f"{SYMBOL}-bbo-{days[0]}.parquet"
    strict_target = output / "bbo" / strict_source.name
    mixed_source = mixed / "l2" / f"{SYMBOL}-l2-{days[2]}.parquet"
    mixed_target = output / "l2" / mixed_source.name
    assert os.path.samefile(strict_source, strict_target)
    assert os.path.samefile(mixed_source, mixed_target)
    assert sha256_file(mixed_source) == source_hash_before

    manifest = json.loads(
        (output / "manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["day_count"] == 3
    assert manifest["formal_day_count"] == 1
    assert manifest["source_counts"] == {
        "legacy_mixed": 1,
        "retained": 1,
        "strict": 1,
    }
    for identity in manifest["inputs"].values():
        assert identity["path"]
        assert len(identity["sha256"]) == 64
    assert len(manifest["files"]) == 6
    legacy_identity = manifest["legacy_mixed_sources"]
    assert len(legacy_identity) == 1
    assert legacy_identity[0]["root"] == str(mixed.resolve())
    assert legacy_identity[0]["bbo_directory"] == str(mixed.resolve() / "bbo")
    assert legacy_identity[0]["l2_directory"] == str(mixed.resolve() / "l2")
    assert legacy_identity[0]["included_file_count"] == 2
    assert {
        item["relative_path"] for item in legacy_identity[0]["included_files"]
    } == {
        f"bbo/{SYMBOL}-bbo-{days[2]}.parquet",
        f"l2/{SYMBOL}-l2-{days[2]}.parquet",
    }
    assert len(legacy_identity[0]["identity_sha256"]) == 64

    accepted = require_formal_day(output, days[0], verify_hashes=True)
    assert accepted["day"] == days[0]
    with pytest.raises(FormalEligibilityError, match="not formal eligible"):
        require_formal_day(output, days[2])


def test_dry_run_freezes_plan_without_creating_output(tmp_path: Path) -> None:
    day = "2026-07-20"
    inputs = _write_inputs(tmp_path, [day])
    source = tmp_path / "source"
    _write_pair(source, day)
    output = tmp_path / "normalized_l2_100ms_v2"

    result = _assemble(
        output=output,
        inputs=inputs,
        sources=[
            SourceRootSpec(
                source,
                "snapshot_24h_warmup",
                formal_capable=True,
            )
        ],
        dry_run=True,
    )

    assert result.dry_run is True
    assert result.manifest["dry_run"] is True
    assert len(result.manifest["files"]) == 2
    assert not output.exists()


def test_incomplete_source_union_fails_before_any_output(tmp_path: Path) -> None:
    days = ["2026-07-19", "2026-07-20"]
    inputs = _write_inputs(tmp_path, days)
    source = tmp_path / "source"
    _write_pair(source, days[0])
    output = tmp_path / "normalized_l2_100ms_v2"

    with pytest.raises(IncompleteSourceUnionError, match="1/2 good days"):
        _assemble(
            output=output,
            inputs=inputs,
            sources=[SourceRootSpec(source, "delta_converged_120s")],
        )

    assert not output.exists()
    assert not list(tmp_path.glob(".normalized_l2_100ms_v2.staging-*"))


def test_invalid_legacy_cadence_cannot_complete_union(tmp_path: Path) -> None:
    day = "2026-07-20"
    inputs = _write_inputs(tmp_path, [day])
    mixed = tmp_path / "mixed"
    _write_pair(mixed, day, interval_ms=1_000)
    output = tmp_path / "normalized_l2_100ms_v2"

    with pytest.raises(
        IncompleteSourceUnionError,
        match="bbo_cadence",
    ):
        _assemble(
            output=output,
            inputs=inputs,
            sources=[
                SourceRootSpec(
                    mixed,
                    "legacy_mixed_verified_100ms",
                    label="legacy_mixed",
                )
            ],
        )

    assert not output.exists()


def test_incomplete_100ms_coverage_is_descriptive_not_formal(
    tmp_path: Path,
) -> None:
    day = "2026-07-20"
    inputs = _write_inputs(tmp_path, [day], strict_days=[])
    retained = tmp_path / "retained"
    _write_pair(retained, day, interval_ms=100, rows=5)
    output = tmp_path / "normalized_l2_100ms_v2"

    result = assemble_registry(
        output_root=output,
        good_days_path=inputs["good_days"],
        source_availability_path=inputs["source_availability"],
        sequence_audit_path=inputs["sequence_audit"],
        normalized_strict_days_path=inputs["strict_days"],
        source_roots=[
            SourceRootSpec(
                retained,
                "delta_converged_120s",
                formal_capable=False,
            )
        ],
        cadence_policy=CadencePolicy(
            min_coverage=0.0,
            min_valid_spread_ratio=1.0,
            max_p99_gap_s=0.5,
        ),
    )

    row = result.quality.iloc[0]
    assert bool(row["rebuilt"]) is True
    assert bool(row["coverage_99_valid"]) is False
    assert bool(row["formal_eligible"]) is False


def test_formal_hash_gate_detects_replaced_destination(tmp_path: Path) -> None:
    day = "2026-07-20"
    inputs = _write_inputs(tmp_path, [day])
    source = tmp_path / "strict"
    _write_pair(source, day)
    output = tmp_path / "normalized_l2_100ms_v2"
    _assemble(
        output=output,
        inputs=inputs,
        sources=[
            SourceRootSpec(
                source,
                "snapshot_24h_warmup",
                formal_capable=True,
            )
        ],
    )

    target = output / "bbo" / f"{SYMBOL}-bbo-{day}.parquet"
    target.unlink()
    target.write_bytes(b"not parquet but same registry path")

    with pytest.raises(FormalEligibilityError, match="size mismatch|SHA256 mismatch"):
        require_formal_day(output, day, verify_hashes=True)
