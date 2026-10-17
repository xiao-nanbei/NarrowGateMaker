from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_strict_label_panel_runner as runner,
)


def _fake_run_day_factory(*, fail_days: set[str] | None = None):
    failures = fail_days or set()

    def fake_run_day(
        day: str,
        *,
        feature_block: str,
        support_identity: str,
        max_opportunities: int | None,
        output: Path,
        cache_root: Path,
        native_cache: Path,
        native_cache_receipt: Path,
    ) -> dict[str, Any]:
        del cache_root, native_cache
        assert native_cache_receipt.is_file()
        assert feature_block == "M2"
        assert support_identity == runner.strict_labels.FULL_SUPPORT_IDENTITY
        assert max_opportunities is None
        if day in failures:
            raise RuntimeError(f"synthetic failure for {day}")
        manifest_path = runner._day_manifest_path(Path(output), day)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": runner.strict_labels.DAY_SCHEMA_VERSION,
            "target_day": day,
            "feature_block": feature_block,
            "source_support_identity": support_identity,
            "max_opportunities": max_opportunities,
        }
        if manifest_path.exists():
            return json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest_path.write_text(
            json.dumps(payload, sort_keys=True) + "\n",
            encoding="ascii",
        )
        return payload

    return fake_run_day


def _progress_path(output: Path, days: tuple[str, ...], formal: bool) -> Path:
    return runner._run_directory(output, days, formal) / "progress.json"


def _fake_segment_prebuild_factory(*, calls: list[str] | None = None):
    def fake_prebuild(
        segment: runner.SourceSegment,
        *,
        native_cache: Path,
    ) -> dict[str, Any]:
        if calls is not None:
            calls.append(segment.segment_id)
        return {
            "schema_version": (
                f"{runner.RUNNER_IDENTITY}.native_cache_segment_receipt.v3"
            ),
            "identity": runner.IDENTITY,
            "segment_id": segment.segment_id,
            "start_day": segment.start_day,
            "end_day": segment.end_day,
            "source_days": list(segment.source_days),
            "target_days": list(segment.target_days),
            "anchor_day": segment.target_days[0],
            "strict_start_day": segment.target_days[0],
            "hour_count": segment.hour_count,
            "native_cache_root": str(native_cache.resolve()),
            "cache_contract_sha256": "a" * 64,
            "hours": [
                {
                    "utc_hour": utc_hour,
                    "cache_identity_sha256": "b" * 64,
                    "manifest_sha256": "c" * 64,
                    "data_sha256": "d" * 64,
                }
                for utc_hour in runner._expected_segment_hours(segment)
            ],
            "source_scheduler_stats": {
                "consumed_events": segment.hour_count,
                "initialized": True,
            },
            "strict_counter_baseline": {
                field: 0
                for field in runner.strict_labels._STRICT_SOURCE_ZERO_FIELDS
            },
            "strict_zero_counters": {
                field: 0
                for field in runner.strict_labels._STRICT_SOURCE_ZERO_FIELDS
            },
            "target_start_states": [
                {
                    "target_day": day,
                    "target_start_ts_ns": 1,
                    "initialized": True,
                    "initialization_source": "snapshot",
                    "segment_id": 1,
                }
                for day in segment.target_days
            ],
            "materialization_cache_stats": {},
            "validation_cache_stats": {},
            "scheduler_replay_count": 1,
            "target_day_scheduler_replay_count": 0,
            "economic_outcomes_read": False,
            "arms_run": False,
            "nested_oof_run": False,
            "action_authorized": False,
            "live_authorized": False,
        }

    return fake_prebuild


def _install_fake_panel_prebuild(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runner,
        "_prebuild_native_segment",
        _fake_segment_prebuild_factory(),
    )


def test_formal_universe_is_41_days_and_excludes_reduced() -> None:
    spec = runner._load_json(Path(runner.V2_SPEC))
    formal, reduced, prefix40, added10 = runner._formal_day_universe(spec)

    assert len(formal) == 41
    assert len(reduced) == 9
    assert len(prefix40) == 40
    assert len(added10) == 10
    assert set(formal).isdisjoint(reduced)
    assert sum(day in prefix40 for day in formal) == 33
    assert sum(day in added10 for day in formal) == 8
    plan = runner._source_union_plan(formal, formal=True)
    assert len(plan.unique_source_days) == 57
    assert len(plan.segments) == 8
    assert plan.naive_target_hour_scans == 2_952
    assert plan.unique_source_hours == 1_368
    assert plan.hours_saved == 1_584
    assert [
        (segment.start_day, segment.end_day, len(segment.source_days))
        for segment in plan.segments
    ] == [
        ("2026-04-16", "2026-04-20", 5),
        ("2026-04-21", "2026-04-23", 3),
        ("2026-04-30", "2026-05-06", 7),
        ("2026-05-28", "2026-05-31", 4),
        ("2026-06-01", "2026-06-03", 3),
        ("2026-06-04", "2026-06-26", 23),
        ("2026-07-02", "2026-07-10", 9),
        ("2026-07-15", "2026-07-17", 3),
    ]


def test_formal_source_union_fails_closed_on_denominator_drift() -> None:
    spec = runner._load_json(Path(runner.V2_SPEC))
    formal, _, _, _ = runner._formal_day_universe(spec)

    with pytest.raises(runner.StrictLabelPanelRunnerError, match="not 41"):
        runner._source_union_plan(formal[:-1], formal=True)

    engineering = runner._source_union_plan(formal[:2], formal=False)
    assert len(engineering.target_days) == 2
    assert len(engineering.unique_source_days) == 4
    assert len(engineering.segments) == 1
    assert engineering.hours_saved == 48


def test_segment_prebuild_allows_recovered_warmup_gap_but_not_strict_gap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    segment = runner.SourceSegment(
        segment_id="segment-001-20260421-20260423",
        start_day="2026-04-21",
        end_day="2026-04-23",
        source_days=("2026-04-21", "2026-04-22", "2026-04-23"),
        target_days=("2026-04-22",),
    )

    class FakeTape:
        strict_gap_count = 0

        def __init__(self, **kwargs: Any) -> None:
            self.raw_root = Path(kwargs["raw_root"])
            self.day = str(kwargs["day"])
            self.symbol = str(kwargs["symbol"])
            self.tick_size = float(kwargs["tick_size"])
            self.exchange = str(kwargs["exchange"])
            self.warmup_hours = int(kwargs.get("warmup_hours", 24))
            self.continuation_hours = int(kwargs.get("continuation_hours", 0))
            self.cache_dir = Path(kwargs["cache_dir"])
            start = datetime.fromisoformat(self.day).replace(tzinfo=UTC)
            self.day_start_ns = int(start.timestamp() * 1_000_000_000)

        def materialize_cache(self, *, verify_sha256: bool) -> dict[str, Any]:
            assert verify_sha256
            start = datetime.fromisoformat(self.day).replace(tzinfo=UTC) - timedelta(
                hours=self.warmup_hours
            )
            hours = [
                {
                    "utc_hour": (
                        start + timedelta(hours=index)
                    ).strftime("%Y-%m-%dT%H:00:00Z")
                }
                for index in range(segment.hour_count)
            ]
            return {
                "expected_hour_count": segment.hour_count,
                "complete_hour_count": segment.hour_count,
                "canonical_identity_sha256": "a" * 64,
                "hours": hours,
            }

        def cache_stats(self) -> dict[str, int]:
            return {
                "hour_hits": segment.hour_count,
                "hour_failures_fallback_to_source": 0,
            }

    class FakeScheduler:
        def __init__(self, tape: FakeTape, **_: Any) -> None:
            self.tape = tape
            self.sequence = SimpleNamespace(
                initialized=True,
                initialization_source="snapshot",
            )
            self.segment_id = 1
            self.final = False

        def advance_to(self, value: int, *, inclusive: bool = True) -> None:
            del inclusive
            self.final = value == 2**63 - 1

        def stats_dict(self) -> dict[str, Any]:
            counters = {
                field: 0
                for field in runner.strict_labels._STRICT_SOURCE_ZERO_FIELDS
            }
            counters["sequence_gaps"] = 1 + (
                FakeTape.strict_gap_count if self.final else 0
            )
            return {
                **counters,
                "consumed_events": segment.hour_count if self.final else 1,
                "initialized": True,
            }

    monkeypatch.setattr(runner.strict_labels.strict_baseline, "_spec", lambda: {})
    monkeypatch.setattr(
        runner.strict_labels.strict_baseline,
        "_native_tape",
        lambda *_args, **_kwargs: FakeTape(
            raw_root=tmp_path,
            day="2026-04-22",
            symbol="BTCUSDC",
            tick_size=0.1,
            exchange="binance_futures",
            cache_dir=tmp_path / "cache",
        ),
    )
    monkeypatch.setattr(
        runner.strict_labels,
        "_clone_native_tape",
        lambda tape, **_kwargs: tape,
    )
    monkeypatch.setattr(
        runner.strict_labels,
        "HistoricalExchangeBookScheduler",
        FakeScheduler,
    )

    receipt = runner._prebuild_native_segment(
        segment,
        native_cache=tmp_path / "cache",
    )
    assert receipt["strict_counter_baseline"]["sequence_gaps"] == 1
    assert receipt["strict_zero_counters"]["sequence_gaps"] == 0
    assert receipt["target_start_states"][0]["initialization_source"] == "snapshot"

    FakeTape.strict_gap_count = 1
    with pytest.raises(
        runner.StrictLabelPanelRunnerError,
        match="strict source audit failed",
    ):
        runner._prebuild_native_segment(
            segment,
            native_cache=tmp_path / "cache",
        )


def test_reduced_day_cannot_enter_engineering_subset() -> None:
    spec = runner._load_json(Path(runner.V2_SPEC))
    formal, reduced, _, _ = runner._formal_day_universe(spec)

    with pytest.raises(runner.StrictLabelPanelRunnerError, match="reduced-support"):
        runner._selected_days(formal, reduced, [next(iter(reduced))])


def test_formal_run_emits_all_41_day_manifests(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_panel_prebuild(monkeypatch)
    monkeypatch.setattr(
        runner.strict_labels,
        "run_day",
        _fake_run_day_factory(),
    )

    manifest = runner.run_panel(
        output=tmp_path / "output",
        cache_root=tmp_path / "cache",
        native_cache=tmp_path / "native",
    )

    assert manifest["formal_full_support_run"] is True
    assert manifest["day_count"] == 41
    assert manifest["prefix40_full_support_count"] == 33
    assert manifest["added10_full_support_count"] == 8
    assert len(manifest["day_manifests"]) == 41
    assert [row["day"] for row in manifest["day_manifests"]] == manifest[
        "ordered_days"
    ]


@pytest.mark.parametrize("workers", [0, 3])
def test_day_worker_limit_is_hard_capped(workers: int) -> None:
    with pytest.raises(runner.StrictLabelPanelRunnerError, match="within"):
        runner._validate_workers(workers)


@pytest.mark.parametrize("workers", [0, 5])
def test_prebuild_worker_limit_is_independently_capped(workers: int) -> None:
    with pytest.raises(
        runner.StrictLabelPanelRunnerError,
        match="prebuild_workers must be within",
    ):
        runner._validate_workers(
            workers,
            name="prebuild_workers",
            cap=runner.MAX_PREBUILD_WORKERS_CAP,
        )

    assert runner._validate_workers(
        4,
        name="prebuild_workers",
        cap=runner.MAX_PREBUILD_WORKERS_CAP,
    ) == 4


def test_worker_defaults_and_cli_are_separate() -> None:
    defaults = runner._parser().parse_args([])
    assert defaults.max_workers == 1
    assert defaults.prebuild_workers == 2

    explicit = runner._parser().parse_args(
        ["--max-workers", "1", "--prebuild-workers", "4"]
    )
    assert explicit.max_workers == 1
    assert explicit.prebuild_workers == 4


def test_engineering_subset_completes_atomically(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_panel_prebuild(monkeypatch)
    spec = runner._load_json(Path(runner.V2_SPEC))
    formal, _, _, _ = runner._formal_day_universe(spec)
    days = formal[:2]
    monkeypatch.setattr(
        runner.strict_labels,
        "run_day",
        _fake_run_day_factory(),
    )

    manifest = runner.run_panel(
        output=tmp_path / "output",
        cache_root=tmp_path / "cache",
        native_cache=tmp_path / "native",
        max_workers=2,
        engineering_days=days,
    )

    assert manifest["ordered_days"] == list(days)
    assert manifest["feature_block"] == "M2"
    assert manifest["max_opportunities"] is None
    assert manifest["formal_full_support_run"] is False
    assert all(value is False for value in manifest["permissions"].values())
    progress_path = _progress_path(tmp_path / "output", days, False)
    progress = json.loads(progress_path.read_text(encoding="utf-8"))
    assert progress["state"] == "completed"
    assert all(progress["days"][day]["status"] == "completed" for day in days)
    assert not tuple(progress_path.parent.glob(".progress.json.*"))


def test_run_panel_passes_prebuild_workers_independently(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_panel_prebuild(monkeypatch)
    spec = runner._load_json(Path(runner.V2_SPEC))
    formal, _, _, _ = runner._formal_day_universe(spec)
    days = formal[:1]
    monkeypatch.setattr(
        runner.strict_labels,
        "run_day",
        _fake_run_day_factory(),
    )
    original_prebuild = runner.prebuild_native_panel_cache
    observed: list[int] = []

    def recording_prebuild(
        *,
        output: Path,
        native_cache: Path,
        prebuild_workers: int,
        engineering_days: tuple[str, ...] | None,
    ) -> dict[str, Any]:
        observed.append(prebuild_workers)
        return original_prebuild(
            output=output,
            native_cache=native_cache,
            prebuild_workers=prebuild_workers,
            engineering_days=engineering_days,
        )

    monkeypatch.setattr(runner, "prebuild_native_panel_cache", recording_prebuild)
    manifest = runner.run_panel(
        output=tmp_path / "output",
        cache_root=tmp_path / "cache",
        native_cache=tmp_path / "native",
        max_workers=1,
        prebuild_workers=2,
        engineering_days=days,
    )

    assert observed == [2]
    assert manifest["native_cache_prebuild"]["segment_count"] == 1


def test_resume_revalidates_completed_admission_and_rejects_hash_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_panel_prebuild(monkeypatch)
    spec = runner._load_json(Path(runner.V2_SPEC))
    formal, _, _, _ = runner._formal_day_universe(spec)
    days = formal[:1]
    output = tmp_path / "output"
    fake = _fake_run_day_factory()
    monkeypatch.setattr(runner.strict_labels, "run_day", fake)
    first = runner.run_panel(
        output=output,
        cache_root=tmp_path / "cache",
        native_cache=tmp_path / "native",
        engineering_days=days,
    )
    second = runner.run_panel(
        output=output,
        cache_root=tmp_path / "cache",
        native_cache=tmp_path / "native",
        engineering_days=days,
    )
    assert second == first

    progress_path = _progress_path(output, days, False)
    progress = json.loads(progress_path.read_text(encoding="utf-8"))
    progress["identity_binding"]["strict_label_code_sha256"] = "0" * 64
    progress_path.write_text(
        json.dumps(progress, indent=2, sort_keys=True) + "\n",
        encoding="ascii",
    )
    with pytest.raises(runner.StrictLabelPanelRunnerError, match="identity drift"):
        runner.run_panel(
            output=output,
            cache_root=tmp_path / "cache",
            native_cache=tmp_path / "native",
            engineering_days=days,
        )


def test_failed_day_is_durable_and_resume_can_complete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_panel_prebuild(monkeypatch)
    spec = runner._load_json(Path(runner.V2_SPEC))
    formal, _, _, _ = runner._formal_day_universe(spec)
    days = formal[:2]
    output = tmp_path / "output"
    monkeypatch.setattr(
        runner.strict_labels,
        "run_day",
        _fake_run_day_factory(fail_days={days[1]}),
    )
    with pytest.raises(runner.StrictLabelPanelRunnerError, match="failed days"):
        runner.run_panel(
            output=output,
            cache_root=tmp_path / "cache",
            native_cache=tmp_path / "native",
            max_workers=2,
            engineering_days=days,
        )

    progress_path = _progress_path(output, days, False)
    failed_progress = json.loads(progress_path.read_text(encoding="utf-8"))
    assert failed_progress["state"] == "failed"
    assert failed_progress["days"][days[0]]["status"] == "completed"
    assert failed_progress["days"][days[1]]["status"] == "failed"
    assert failed_progress["days"][days[1]]["failed_at"] is not None
    assert failed_progress["days"][days[1]]["elapsed_seconds"] is not None
    assert not tuple(progress_path.parent.glob(".progress.json.*"))

    monkeypatch.setattr(
        runner.strict_labels,
        "run_day",
        _fake_run_day_factory(),
    )
    manifest = runner.run_panel(
        output=output,
        cache_root=tmp_path / "cache",
        native_cache=tmp_path / "native",
        max_workers=1,
        engineering_days=days,
    )
    resumed = json.loads(progress_path.read_text(encoding="utf-8"))
    assert manifest["day_count"] == 2
    assert resumed["state"] == "completed"
    assert resumed["days"][days[0]]["attempts"] == 1
    assert resumed["days"][days[1]]["attempts"] == 2


def test_native_cache_prebuild_reads_no_economics_and_resumes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = runner._load_json(Path(runner.V2_SPEC))
    formal, _, _, _ = runner._formal_day_universe(spec)
    days = formal[:2]
    calls: list[str] = []
    monkeypatch.setattr(
        runner,
        "_prebuild_native_segment",
        _fake_segment_prebuild_factory(calls=calls),
    )
    output = tmp_path / "output"
    first = runner.prebuild_native_panel_cache(
        output=output,
        native_cache=tmp_path / "native",
        prebuild_workers=1,
        engineering_days=days,
    )
    second = runner.prebuild_native_panel_cache(
        output=output,
        native_cache=tmp_path / "native",
        prebuild_workers=1,
        engineering_days=days,
    )

    assert len(calls) == 1
    assert second == first
    assert first["day_count"] == 2
    assert first["unique_source_day_count"] == 4
    assert first["segment_count"] == 1
    assert first["unique_source_hours"] == 96
    assert first["hours_saved"] == 48
    assert first["segment_scheduler_replay_count"] == 1
    assert first["target_day_scheduler_replay_count"] == 0
    assert first["economic_outcomes_read"] is False
    assert first["arms_run"] is False
    assert first["nested_oof_run"] is False
    assert all(row["complete_hour_count"] == 72 for row in first["days"])
    assert all(row["scheduler_replay_count"] == 0 for row in first["days"])
    assert all(value == 0 for value in first["strict_zero_counters"].values())


def test_formal_prebuild_uses_eight_resumable_segment_workers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runner,
        "_prebuild_native_segment",
        _fake_segment_prebuild_factory(),
    )
    output = tmp_path / "output"
    manifest = runner.prebuild_native_panel_cache(
        output=output,
        native_cache=tmp_path / "native",
        prebuild_workers=2,
    )

    assert manifest["formal_full_support_run"] is True
    assert manifest["day_count"] == 41
    assert manifest["unique_source_day_count"] == 57
    assert manifest["segment_count"] == 8
    assert manifest["segment_worker_limit"] == 2
    assert manifest["segment_scheduler_replay_count"] == 8
    assert manifest["unique_source_hours"] == 1_368
    assert manifest["hours_saved"] == 1_584
    assert len(manifest["days"]) == 41
    assert all(row["complete_hour_count"] == 72 for row in manifest["days"])
    assert all(row["arms_run"] is False for row in manifest["segments"])
    assert all(
        row["economic_outcomes_read"] is False
        for row in manifest["segments"]
    )
    prebuild = runner._native_prebuild_directory(
        output,
        tuple(manifest["ordered_days"]),
        True,
    )
    assert len(tuple((prebuild / "segments").glob("*.json"))) == 8
    assert len(tuple((prebuild / "targets").glob("*.json"))) == 41
    assert not tuple((prebuild / "segment_results").glob("*.json"))

    resumed = runner.prebuild_native_panel_cache(
        output=output,
        native_cache=tmp_path / "native",
        prebuild_workers=2,
    )
    assert resumed == manifest


def test_v9_label_root_is_isolated_while_source_union_v3_is_reused(
    tmp_path: Path,
) -> None:
    days = ("2026-04-17",)
    label_root = runner._run_directory(tmp_path, days, True)
    source_root = runner._native_prebuild_directory(tmp_path, days, True)

    assert label_root.name == runner.FORMAL_RUN_DIRECTORY_NAME
    assert label_root.name != "formal_full_support_41d"
    assert source_root == (
        tmp_path
        / "panel_runner"
        / "formal_full_support_41d"
        / "native_cache_prebuild_union_v3"
    )
    day_manifest = runner._day_manifest_path(tmp_path, days[0])
    assert f"execution_identity={runner.strict_labels.FORMAL_EXECUTION_IDENTITY}" in (
        day_manifest.parts
    )


def test_progress_v1_is_rejected() -> None:
    payload = {
        "schema_version": f"{runner.RUNNER_IDENTITY}.progress.v1",
    }

    with pytest.raises(runner.StrictLabelPanelRunnerError, match="progress schema"):
        runner._validate_progress(
            payload,
            days=("2026-04-17",),
            formal=False,
            binding={},
            output=Path("/tmp/output"),
            cache_root=Path("/tmp/cache"),
            native_cache=Path("/tmp/native"),
            native_cache_prebuild={},
        )
def test_prebuild_resume_rejects_segment_receipt_hash_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = runner._load_json(Path(runner.V2_SPEC))
    formal, _, _, _ = runner._formal_day_universe(spec)
    days = formal[:1]
    output = tmp_path / "output"
    monkeypatch.setattr(
        runner,
        "_prebuild_native_segment",
        _fake_segment_prebuild_factory(),
    )
    manifest = runner.prebuild_native_panel_cache(
        output=output,
        native_cache=tmp_path / "native",
        engineering_days=days,
    )
    segment_path = Path(manifest["segments"][0]["receipt_path"])
    segment_path.write_text("{}\n", encoding="ascii")

    with pytest.raises(runner.StrictLabelPanelRunnerError, match="hash drifted"):
        runner.prebuild_native_panel_cache(
            output=output,
            native_cache=tmp_path / "native",
            engineering_days=days,
        )
