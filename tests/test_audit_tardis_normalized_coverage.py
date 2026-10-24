import hashlib
import json
from pathlib import Path

from data.audit_tardis_normalized_coverage import audit_coverage, main


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_manifest(tmp_path: Path, days: list[str]) -> Path:
    downloads = []
    for day in days:
        for dataset in ("book_ticker", "incremental_book_L2"):
            raw = tmp_path / "raw" / dataset / f"{day}.csv.zst"
            raw.parent.mkdir(parents=True, exist_ok=True)
            raw.write_bytes(f"{dataset}:{day}".encode())
            downloads.append(
                {
                    "day": day,
                    "dataset": dataset,
                    "exists": True,
                    "zstd_valid": True,
                    "path": str(raw),
                    "size_bytes": raw.stat().st_size,
                    "sha256": _sha256(raw),
                }
            )
    manifest = tmp_path / "download.json"
    manifest.write_text(
        json.dumps({"complete": True, "downloads": downloads}),
        encoding="utf-8",
    )
    return manifest


def _admit_day(root: Path, day: str) -> None:
    outputs = {}
    for name in ("bbo", "l2", "clock"):
        path = root / name / f"BTCUSDC-{name}-{day}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(f"{name}:{day}".encode())
        outputs[f"{name}_output"] = {
            "sha256": _sha256(path),
            "size_bytes": path.stat().st_size,
        }
    quality = root / "quality" / f"BTCUSDC-{day}.json"
    quality.parent.mkdir(parents=True, exist_ok=True)
    quality.write_text(
        json.dumps(
            {
                "day": day,
                "complete_day": True,
                "provider_normalized_replay_candidate": True,
                "policy_visible": False,
                "exact_queue_policy_eligible": False,
                "clock_source": "tardis_provider_local",
                **outputs,
            }
        ),
        encoding="utf-8",
    )


def test_coverage_requires_atomic_four_piece_admission(tmp_path: Path) -> None:
    days = ["2026-01-01", "2026-01-02"]
    manifest = _write_manifest(tmp_path, days)
    normalized = tmp_path / "normalized"
    _admit_day(normalized, days[0])

    report = audit_coverage(
        start_day=days[0],
        end_day=days[-1],
        normalized_root=normalized,
        download_manifests=[manifest],
        rehash_existing=True,
    )

    assert report["complete_day_count"] == 1
    assert report["target_missing_day_count"] == 1
    assert report["complete_target_days"] == [days[0]]
    assert report["provider_candidate_day_count"] == 1
    assert report["policy_visible_day_count"] == 0
    assert report["exact_queue_policy_eligible_day_count"] == 0
    assert report["clock_source_counts"] == {"tardis_provider_local": 1}
    assert report["runnable_missing_days"] == [days[1]]


def test_coverage_rejects_truncated_admitted_output_without_rehash(
    tmp_path: Path,
) -> None:
    day = "2026-01-01"
    manifest = _write_manifest(tmp_path, [day])
    normalized = tmp_path / "normalized"
    _admit_day(normalized, day)
    l2 = normalized / "l2" / f"BTCUSDC-l2-{day}.parquet"
    l2.write_bytes(b"truncated")

    report = audit_coverage(
        start_day=day,
        end_day=day,
        normalized_root=normalized,
        download_manifests=[manifest],
    )

    assert report["target_missing_day_count"] == 1
    assert report["rows"][0]["normalized_reason"] == "l2_size_mismatch"


def test_source_unavailable_day_can_be_excluded_without_claiming_it_complete(
    tmp_path: Path,
) -> None:
    available = "2026-01-01"
    unavailable = "2026-01-02"
    manifest = _write_manifest(tmp_path, [available])
    normalized = tmp_path / "normalized"
    _admit_day(normalized, available)

    report = audit_coverage(
        start_day=available,
        end_day=unavailable,
        normalized_root=normalized,
        download_manifests=[manifest],
        excluded_days=[unavailable],
        rehash_existing=True,
    )

    assert report["calendar_day_count"] == 2
    assert report["complete_day_count"] == 1
    assert report["missing_day_count"] == 1
    assert report["normalization_target_day_count"] == 1
    assert report["target_complete_day_count"] == 1
    assert report["target_missing_day_count"] == 0
    assert report["complete_target_days"] == [available]
    assert report["excluded_days"] == [unavailable]
    assert report["raw_blocked_day_count"] == 0
    excluded_row = next(row for row in report["rows"] if row["day"] == unavailable)
    assert not excluded_row["normalization_target"]
    assert excluded_row["exclusion_reason"] == "owner_excluded_source_unavailable"


def test_cli_does_not_export_excluded_day_as_a_raw_blocker(tmp_path: Path) -> None:
    available = "2026-01-01"
    excluded = "2026-01-02"
    manifest = _write_manifest(tmp_path, [available])
    normalized = tmp_path / "normalized"
    _admit_day(normalized, available)
    report = tmp_path / "report.json"
    complete = tmp_path / "complete.csv"
    runnable = tmp_path / "runnable.csv"
    blocked = tmp_path / "blocked.csv"

    assert (
        main(
            [
                "--start-day",
                available,
                "--end-day",
                excluded,
                "--normalized-root",
                str(normalized),
                "--download-manifest",
                str(manifest),
                "--exclude-day",
                excluded,
                "--report-json",
                str(report),
                "--complete-days-csv",
                str(complete),
                "--runnable-days-csv",
                str(runnable),
                "--blocked-days-csv",
                str(blocked),
            ]
        )
        == 0
    )
    assert blocked.read_text(encoding="utf-8") == "day\n"
