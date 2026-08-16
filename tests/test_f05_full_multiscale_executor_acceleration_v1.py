from __future__ import annotations

import json
import os
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pytest

from models.tick_data_types import HistoricalBBOData, HistoricalL2Data
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_nested_oof_v1 as nested,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_repeated_policy_backend_v1 as backend,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_replay_adapter_v1 as adapter,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_v1 as offline,
)

DAY = "2026-07-01"


def _sha(character: str) -> str:
    return character * 64


def _day_key(
    *,
    candidate: str = "1",
    side: str = "SELL",
    fold: str = "outer1",
    day: str = DAY,
    stage: str = "outer_oof",
) -> adapter.DayReplayCacheKey:
    return adapter.DayReplayCacheKey(
        adapter_artifact_sha256=_sha("a"),
        source_manifest_sha256=_sha("b"),
        panel_manifest_sha256=_sha("c"),
        fold_manifest_sha256=_sha("d"),
        execution_manifest_sha256=_sha("e"),
        exact_owner_policy_sha256=offline.ACTIVE_OWNER_POLICY_SHA256,
        candidate_policy_sha256=_sha(candidate),
        side=side,
        stage=stage,
        fold_id=fold,
        utc_day=day,
        day_input_sha256=_sha("f"),
    )


def _day_job(
    *,
    candidate: str = "1",
    side: str = "SELL",
    fold: str = "outer1",
    day: str = DAY,
    stage: str = "outer_oof",
) -> adapter._DayReplayJob:
    key = _day_key(
        candidate=candidate,
        side=side,
        fold=fold,
        day=day,
        stage=stage,
    )
    return adapter._DayReplayJob(
        kind="sequential",
        utc_day=day,
        cache_key=key,
        payload={},
    )


def _b0_key(
    *,
    side: str = "SELL",
    fold: str = "outer1",
    day_input: str = "1",
) -> adapter.B0ControlCacheKey:
    return adapter.B0ControlCacheKey(
        adapter_artifact_sha256=_sha("a"),
        source_manifest_sha256=_sha("b"),
        panel_manifest_sha256=_sha("c"),
        fold_manifest_sha256=_sha("d"),
        execution_manifest_sha256=_sha("e"),
        exact_owner_policy_sha256=offline.ACTIVE_OWNER_POLICY_SHA256,
        exact_owner_predicate_bundle_sha256=(offline.ACTIVE_PREDICATE_BUNDLE_SHA256),
        exact_owner_private_config_sha256=offline.ACTIVE_PRIVATE_CONFIG_SHA256,
        fixed_bridge_sha256=_sha("f"),
        replay_engine=adapter.REPLAY_ENGINE,
        queue_identity=adapter.QUEUE_IDENTITY,
        same_millisecond_ambiguity_policy=(adapter.SAME_MILLISECOND_AMBIGUITY_POLICY),
        side=side,
        stage="outer_oof",
        fold_id=fold,
        utc_day=DAY,
        day_input_sha256=_sha(day_input),
        canonical_day_input_binding_sha256=_sha("2"),
        market_window_identity_sha256=_sha("3"),
        model_overlay_identity_sha256=_sha("4"),
        latency_identity_sha256=_sha("5"),
        queue_random_identity_sha256=_sha("6"),
        replay_input_receipt_sha256=_sha("7"),
        target_day_semantics_sha256=_sha("8"),
    )


def _b0_path(value: float = 1.0) -> adapter.B0ControlPath:
    summary = {
        "engine": adapter.REPLAY_ENGINE,
        "python_authoritative": True,
        "repeated_policy_enabled": True,
        "metric_blockers": [],
        "terminal_mtm_pnl_usdc": value,
        "cooldown_duration_policy_audit": {
            "policy_sha256": offline.ACTIVE_OWNER_POLICY_SHA256,
            "predicate_bundle_sha256": offline.ACTIVE_PREDICATE_BUNDLE_SHA256,
        },
    }
    campaigns = pd.DataFrame({"campaign_id": ("campaign-1",), "terminal_value_usdc": (value,)})
    fills = pd.DataFrame({"fill_id": ("fill-1",), "price": (100.0,)})
    decisions = pd.DataFrame(
        {
            "snapshot_id": ("snapshot-1",),
            "policy_sha256": (offline.ACTIVE_OWNER_POLICY_SHA256,),
            "action_id": ("CONTROL_85N",),
            "support_valid": (True,),
        }
    )
    return adapter.B0ControlPath(
        summary=summary,
        campaigns=campaigns,
        fills=fills,
        decisions=decisions,
    )


def _concurrent_b0_writer(
    root: str,
    key_payload: dict[str, str],
    counter_path: str,
) -> tuple[str, bool]:
    cache = adapter.DayReplayCache(Path(root))
    key = adapter.B0ControlCacheKey(**key_payload)

    def compute() -> adapter.B0ControlPath:
        descriptor = os.open(
            counter_path,
            os.O_WRONLY | os.O_CREAT | os.O_APPEND,
            0o600,
        )
        try:
            os.write(descriptor, f"{os.getpid()}\n".encode("ascii"))
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        time.sleep(0.15)
        return _b0_path()

    _, evidence = cache.load_or_compute_b0_control(key, compute)
    return str(evidence["cache_receipt_sha256"]), bool(evidence["reused"])


def test_global_scheduler_executes_day_major_and_returns_contract_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    jobs = (
        _day_job(candidate="2", day="2026-07-02"),
        _day_job(candidate="1", day="2026-07-01"),
        _day_job(candidate="2", day="2026-07-01"),
        _day_job(candidate="1", day="2026-07-02"),
    )
    execution_order: list[tuple[str, str]] = []
    pool_workers: list[int] = []

    def execute(job: adapter._DayReplayJob) -> adapter._DayReplayJobResult:
        execution_order.append((job.utc_day, job.cache_key.candidate_policy_sha256))
        return adapter._DayReplayJobResult(
            utc_day=job.utc_day,
            cache_key_sha256=job.cache_key.cache_key_sha256,
            frames={"rows": pd.DataFrame({"utc_day": (job.utc_day,)})},
        )

    class FakePool:
        def __init__(self, *, max_workers: int, initializer: Any) -> None:
            pool_workers.append(max_workers)
            assert initializer is adapter._mark_global_policy_day_worker

        def __enter__(self) -> FakePool:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def map(self, function: Any, values: Any, *, chunksize: int) -> tuple[Any, ...]:
            assert chunksize == 1
            return tuple(function(value) for value in values)

    monkeypatch.setattr(adapter, "_execute_fixed_day_job", execute)
    monkeypatch.setattr(adapter, "ProcessPoolExecutor", FakePool)

    results = adapter.run_global_policy_day_jobs(jobs, total_worker_tokens=10)

    assert pool_workers == [adapter.GLOBAL_SEQUENTIAL_WORKER_TOKENS]
    assert execution_order == [
        ("2026-07-01", _sha("1")),
        ("2026-07-01", _sha("2")),
        ("2026-07-02", _sha("1")),
        ("2026-07-02", _sha("2")),
    ]
    result_jobs = {job.cache_key.cache_key_sha256: job for job in jobs}
    assert [
        (
            result_jobs[result.cache_key_sha256].cache_key.candidate_policy_sha256,
            result.utc_day,
        )
        for result in results
    ] == [
        (_sha("1"), "2026-07-01"),
        (_sha("1"), "2026-07-02"),
        (_sha("2"), "2026-07-01"),
        (_sha("2"), "2026-07-02"),
    ]


@pytest.mark.parametrize("tokens", (0, 11, True, "ten"))
def test_global_scheduler_rejects_invalid_total_tokens(tokens: Any) -> None:
    with pytest.raises(adapter.OfflineReplayAdapterError):
        adapter.run_global_policy_day_jobs((_day_job(),), total_worker_tokens=tokens)


def test_global_scheduler_rejects_nested_and_nonsequential_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(adapter._GLOBAL_POLICY_DAY_WORKER_ENV, "1")
    with pytest.raises(adapter.OfflineReplayAdapterError, match="nested"):
        adapter.run_global_policy_day_jobs((_day_job(),), total_worker_tokens=1)
    monkeypatch.delenv(adapter._GLOBAL_POLICY_DAY_WORKER_ENV)
    one_shot = replace(_day_job(), kind="one_shot")
    with pytest.raises(adapter.OfflineReplayAdapterError, match="sequential jobs only"):
        adapter.run_global_policy_day_jobs((one_shot,), total_worker_tokens=1)


def test_b0_key_is_candidate_independent_but_side_fold_and_input_safe() -> None:
    base = _b0_key()
    side = replace(base, side="BUY")
    fold = replace(base, fold_id="outer2")
    day_input = replace(base, day_input_sha256=_sha("9"))

    assert "candidate_policy_sha256" not in base.payload()
    assert (
        len(
            {
                base.cache_key_sha256,
                side.cache_key_sha256,
                fold.cache_key_sha256,
                day_input.cache_key_sha256,
            }
        )
        == 4
    )
    with pytest.raises(adapter.OfflineReplayAdapterError, match="owner policy"):
        replace(base, exact_owner_policy_sha256=_sha("0"))


def test_b0_cache_round_trip_resume_and_candidate_always_runs(tmp_path: Path) -> None:
    cache = adapter.DayReplayCache(tmp_path / "cache")
    key = _b0_key()
    calls = {"control": 0, "candidate": 0}

    def control() -> adapter.B0ControlPath:
        calls["control"] += 1
        return _b0_path()

    def candidate() -> str:
        calls["candidate"] += 1
        return f"candidate-{calls['candidate']}"

    first = adapter._run_candidate_with_b0_control_cache(
        cache=cache,
        b0_key=key,
        compute_control=control,
        compute_candidate=candidate,
    )
    second = adapter._run_candidate_with_b0_control_cache(
        cache=cache,
        b0_key=key,
        compute_control=control,
        compute_candidate=candidate,
    )

    assert calls == {"control": 1, "candidate": 2}
    assert first[1] == "candidate-1"
    assert second[1] == "candidate-2"
    assert first[2]["reused"] is False
    assert second[2]["reused"] is True
    assert first[2]["cache_receipt_sha256"] == second[2]["cache_receipt_sha256"]


def test_exact_b0_candidate_uses_control_artifact_and_preserves_decisions(
    tmp_path: Path,
) -> None:
    cache = adapter.DayReplayCache(tmp_path / "cache")
    candidate_calls = 0

    def forbidden_candidate() -> None:
        nonlocal candidate_calls
        candidate_calls += 1
        raise AssertionError("exact B0 candidate must not rerun")

    control, candidate, _ = adapter._run_candidate_with_b0_control_cache(
        cache=cache,
        b0_key=_b0_key(),
        compute_control=_b0_path,
        compute_candidate=forbidden_candidate,
        candidate_is_exact_b0=True,
    )

    assert candidate_calls == 0
    pd.testing.assert_frame_equal(candidate[3], control.decisions)
    assert candidate[3] is not control.decisions


def test_pre_materialized_b0_must_exist_and_never_recomputes(tmp_path: Path) -> None:
    cache = adapter.DayReplayCache(tmp_path / "cache")
    key = _b0_key()
    with pytest.raises(adapter.OfflineReplayAdapterError, match="pre-materialized"):
        adapter._run_candidate_with_b0_control_cache(
            cache=cache,
            b0_key=key,
            compute_control=lambda: (_ for _ in ()).throw(AssertionError()),
            compute_candidate=lambda: "candidate",
            control_pre_materialized=True,
        )
    cache.load_or_compute_b0_control(key, _b0_path)
    _, candidate, evidence = adapter._run_candidate_with_b0_control_cache(
        cache=cache,
        b0_key=key,
        compute_control=lambda: (_ for _ in ()).throw(AssertionError()),
        compute_candidate=lambda: "candidate",
        control_pre_materialized=True,
    )
    assert candidate == "candidate"
    assert evidence["pre_materialized"] is True


def test_b0_cache_fails_closed_on_manifest_and_payload_corruption(
    tmp_path: Path,
) -> None:
    cache = adapter.DayReplayCache(tmp_path / "cache")
    key = _b0_key()
    cache.load_or_compute_b0_control(key, _b0_path)
    entry = cache.b0_control_entries / key.cache_key_sha256
    campaigns = entry / "campaigns.parquet"
    campaigns.write_bytes(campaigns.read_bytes() + b"corrupt")
    with pytest.raises(adapter.OfflineReplayAdapterError, match="file hash drifted"):
        cache.load_b0_control(key)

    other_cache = adapter.DayReplayCache(tmp_path / "other-cache")
    other_cache.load_or_compute_b0_control(key, _b0_path)
    manifest_path = other_cache.b0_control_entries / key.cache_key_sha256 / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="ascii"))
    manifest["candidate_policy_bound"] = True
    manifest_path.write_text(json.dumps(manifest), encoding="ascii")
    with pytest.raises(adapter.OfflineReplayAdapterError, match="manifest drifted"):
        other_cache.load_b0_control(key)


def test_b0_and_candidate_day_caches_are_disjoint(tmp_path: Path) -> None:
    cache = adapter.DayReplayCache(tmp_path / "cache")
    b0_key = _b0_key()
    candidate_key = _day_key(candidate="9")
    candidate_rows = pd.DataFrame({"candidate": (1,)})
    cache.load_or_compute_b0_control(b0_key, _b0_path)
    cache.admit_sequential(candidate_key, candidate_rows)

    loaded_b0 = cache.load_b0_control(b0_key)
    loaded_candidate = cache.load_sequential(candidate_key)
    assert loaded_b0 is not None
    assert loaded_candidate is not None
    pd.testing.assert_frame_equal(loaded_candidate, candidate_rows)
    assert cache._b0_control_entry(b0_key).parent == cache.b0_control_entries
    assert cache._entry(candidate_key).parent == cache.entries


def test_b0_cache_concurrent_callers_compute_and_write_once(tmp_path: Path) -> None:
    root = tmp_path / "cache"
    counter = tmp_path / "compute-count.txt"
    result_root = tmp_path / "concurrent-results"
    result_root.mkdir()
    key = _b0_key()
    child_pids: list[int] = []
    for index in range(4):
        pid = os.fork()
        if pid == 0:
            try:
                receipt, reused = _concurrent_b0_writer(
                    str(root),
                    key.payload(),
                    str(counter),
                )
                (result_root / f"{index}.json").write_text(
                    json.dumps({"receipt": receipt, "reused": reused}),
                    encoding="ascii",
                )
            except BaseException as exc:
                (result_root / f"{index}.error").write_text(
                    repr(exc),
                    encoding="utf-8",
                )
                os._exit(1)
            os._exit(0)
        child_pids.append(pid)

    statuses = [os.waitpid(pid, 0)[1] for pid in child_pids]
    errors = tuple(result_root.glob("*.error"))
    assert errors == ()
    assert all(os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0 for status in statuses)
    payloads = tuple(
        json.loads((result_root / f"{index}.json").read_text(encoding="ascii"))
        for index in range(4)
    )
    results = tuple((str(payload["receipt"]), bool(payload["reused"])) for payload in payloads)

    assert len(counter.read_text(encoding="ascii").splitlines()) == 1
    assert len({receipt for receipt, _ in results}) == 1
    assert sum(not reused for _, reused in results) == 1
    partials = tuple((root / "b0_control_entries").glob("*.partial"))
    assert partials == ()


def test_paired_receipt_binds_candidate_and_b0_receipt() -> None:
    base = adapter._paired_replay_receipt_sha256(
        utc_day=DAY,
        target_side="SELL",
        candidate_policy_sha256=_sha("1"),
        market_window_identity_sha256=_sha("2"),
        model_overlay_identity_sha256=_sha("3"),
        b0_control_cache_key_sha256=_sha("4"),
        b0_control_cache_receipt_sha256=_sha("5"),
    )
    assert base != adapter._paired_replay_receipt_sha256(
        utc_day=DAY,
        target_side="SELL",
        candidate_policy_sha256=_sha("1"),
        market_window_identity_sha256=_sha("2"),
        model_overlay_identity_sha256=_sha("3"),
        b0_control_cache_key_sha256=_sha("4"),
        b0_control_cache_receipt_sha256=_sha("6"),
    )
    assert base != adapter._paired_replay_receipt_sha256(
        utc_day=DAY,
        target_side="SELL",
        candidate_policy_sha256=_sha("7"),
        market_window_identity_sha256=_sha("2"),
        model_overlay_identity_sha256=_sha("3"),
        b0_control_cache_key_sha256=_sha("4"),
        b0_control_cache_receipt_sha256=_sha("5"),
    )


def test_bulk_phase_deduplicates_b0_and_marks_candidates_pre_materialized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = adapter.DayReplayCache(tmp_path / "cache")
    first = _day_job(candidate="1")
    second = _day_job(candidate="2")
    key = _b0_key()
    options = adapter._ExecutionOptions(binding={}, cache=cache, workers=6)
    prepared = (
        adapter._PreparedSequentialReplay(
            request=object(),  # type: ignore[arg-type]
            options=options,
            receipt={"receipt_sha256": _sha("1")},
            cached_frames=(),
            jobs=(first,),
        ),
        adapter._PreparedSequentialReplay(
            request=object(),  # type: ignore[arg-type]
            options=options,
            receipt={"receipt_sha256": _sha("2")},
            cached_frames=(),
            jobs=(second,),
        ),
    )
    monkeypatch.setattr(adapter, "_prospective_b0_control_cache_key", lambda _job: key)

    b0_jobs, candidate_jobs = adapter._build_bulk_b0_and_candidate_phases(prepared)

    assert len(b0_jobs) == 1
    assert b0_jobs[0].kind == "b0_control"
    assert "candidate" not in b0_jobs[0].payload
    assert len(candidate_jobs) == 2
    assert all(job.payload["b0_control_pre_materialized"] is True for job in candidate_jobs)


def test_bulk_api_is_exposed_for_backend_evaluate_many(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replay_adapter = adapter._CanonicalOfflineReplayAdapter()
    observed: dict[str, Any] = {}

    def evaluate_many(items: Any, *, total_worker_tokens: int) -> tuple[str, ...]:
        observed["items"] = items
        observed["tokens"] = total_worker_tokens
        return ("done",)

    monkeypatch.setattr(replay_adapter, "evaluate_many", evaluate_many)
    item = adapter.SequentialPolicyDayBatchItem(
        request=object(),  # type: ignore[arg-type]
        replay_inputs=pd.DataFrame(),
    )

    assert replay_adapter.evaluate_repeated_policy_batch((item,), total_worker_tokens=10) == (
        "done",
    )
    assert observed == {"items": (item,), "tokens": 10}


def _governed_acceleration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> adapter.SequentialReplayAccelerationOptions:
    tmp_path.mkdir(parents=True, exist_ok=True)
    hot = tmp_path / "hot"
    cold = tmp_path / "cold"
    hot.mkdir()
    cold.mkdir()
    monkeypatch.setenv("NARROWGATE_CACHE_HOT_ROOT", str(hot))
    monkeypatch.setenv("NARROWGATE_CACHE_COLD_ROOT", str(cold))
    monkeypatch.setenv(
        "NARROWGATE_CACHE_LEDGER_PATH",
        str(hot / ".cache_tier_lru" / "access_ledger.sqlite3"),
    )
    root = cold / "replay_dag" / adapter.EXECUTOR_ACCELERATION_IDENTITY
    root.parent.mkdir()
    return adapter.SequentialReplayAccelerationOptions(day_input_cache_root=root)


def _mmap_source_replay() -> SimpleNamespace:
    clock = np.array([1_000, 2_000, 3_000, 4_000], dtype=np.int64)
    trades = pd.DataFrame(
        {
            "trade_id": np.arange(4, dtype=np.int64),
            "price": np.array([100.0, 100.1, 100.2, 100.1], dtype=np.float64),
            "quantity": np.full(4, 0.001, dtype=np.float64),
            "transact_time": clock,
            "is_buyer_maker": np.array([True, False, False, True]),
        }
    )
    bbo = HistoricalBBOData(
        ts_ms=clock,
        best_bid=np.array([99.9, 100.0, 100.1, 100.0]),
        best_ask=np.array([100.1, 100.2, 100.3, 100.2]),
        bid_qty=np.full(4, 1.0),
        ask_qty=np.full(4, 1.1),
        source="executor-acceleration-test",
    )
    l2 = HistoricalL2Data(
        ts_ms=clock,
        bid_px=np.column_stack((bbo.best_bid, bbo.best_bid - 0.1)),
        bid_qty=np.full((4, 2), 2.0),
        ask_px=np.column_stack((bbo.best_ask, bbo.best_ask + 0.1)),
        ask_qty=np.full((4, 2), 2.1),
        source="executor-acceleration-test",
    )
    return SimpleNamespace(
        utc_day=DAY,
        continuation_day="2026-07-02",
        trades=trades,
        var_ts_ms=clock,
        var_ssq=np.array([0.1, 0.2, 0.3, 0.4]),
        var_ti=np.array([1.0, 1.1, 1.2, 1.3]),
        var_retsq=np.array([0.0, 0.01, 0.02, 0.01]),
        bbo_data=bbo,
        l2_data=l2,
        ml_data=(
            clock,
            np.array([0.2, 0.3, 0.4, 0.5]),
            {"feature_a": np.array([1.0, 2.0, 3.0, 4.0])},
        ),
        params={"replay_event_clock": "exchange_time_merged", "rng_seed": 7},
        market_window_identity_sha256=_sha("2"),
        model_overlay_identity_sha256=_sha("3"),
        latency_identity_sha256=_sha("4"),
        queue_random_identity_sha256=_sha("5"),
        replay_input_receipt_sha256=_sha("6"),
    )


def _mmap_request() -> SimpleNamespace:
    source_receipts = {
        "source_manifest_canonical_sha256": _sha("1"),
        "target_day_receipt_sha256": _sha("2"),
        "context_source_receipts_sha256": _sha("3"),
        "continuation_source_day_receipt_sha256": _sha("4"),
        "context_book_receipts_sha256": _sha("5"),
        "bbo_sha256": _sha("6"),
        "continuation_bbo_sha256": _sha("7"),
        "l2_sha256": _sha("8"),
        "continuation_l2_sha256": _sha("9"),
        "context_feature_receipts_sha256": _sha("a"),
        "features_daily_manifest_sha256": _sha("b"),
        "features_day_file_sha256": _sha("c"),
        "continuation_features_day_file_sha256": _sha("d"),
        "feature_dag_sha256": _sha("e"),
    }
    return SimpleNamespace(
        utc_day=DAY,
        input_binding_sha256=_sha("f"),
        source_receipts=source_receipts,
    )


def _mmap_job(
    *,
    candidate: str,
    fold: str,
) -> adapter._DayReplayJob:
    projection = {
        "utc_day": DAY,
        "input_binding_sha256": _sha("f"),
        "projection_identity": "test-canonical-day-projection",
    }
    projection["projection_receipt_sha256"] = adapter._canonical_sha256(projection)
    replay = _mmap_source_replay()
    rows = pd.DataFrame(
        {
            "market_window_identity_sha256": (replay.market_window_identity_sha256,),
            "model_overlay_identity_sha256": (replay.model_overlay_identity_sha256,),
            "latency_identity_sha256": (replay.latency_identity_sha256,),
            "queue_random_identity_sha256": (replay.queue_random_identity_sha256,),
            "replay_input_receipt_sha256": (replay.replay_input_receipt_sha256,),
            "d_plus_1_utc_day": (replay.continuation_day,),
        }
    )
    return replace(
        _day_job(candidate=candidate, fold=fold),
        payload={
            "fixed_bridge": {},
            "portable_binding": {"day_projections": {DAY: projection}},
            "replay_inputs": rows,
            "candidate": object(),
            "target_side": "SELL",
        },
    )


def _mmap_prepared(
    *,
    cache: adapter.DayReplayCache,
) -> tuple[adapter._PreparedSequentialReplay, ...]:
    options = adapter._ExecutionOptions(binding={}, cache=cache, workers=6)
    return (
        adapter._PreparedSequentialReplay(
            request=object(),  # type: ignore[arg-type]
            options=options,
            receipt={"receipt_sha256": _sha("1")},
            cached_frames=(),
            jobs=(_mmap_job(candidate="1", fold="outer1"),),
        ),
        adapter._PreparedSequentialReplay(
            request=object(),  # type: ignore[arg-type]
            options=options,
            receipt={"receipt_sha256": _sha("2")},
            cached_frames=(),
            jobs=(_mmap_job(candidate="2", fold="outer2"),),
        ),
    )


def test_bulk_day_input_mmap_cold_warm_equivalence_and_worker_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    acceleration = _governed_acceleration(tmp_path, monkeypatch)
    cache = adapter.DayReplayCache(tmp_path / "replay-cache")
    prepared = _mmap_prepared(cache=cache)
    source_replay = _mmap_source_replay()
    request = _mmap_request()
    cold_materializations = 0

    def project(_job: adapter._DayReplayJob) -> tuple[SimpleNamespace, SimpleNamespace]:
        nonlocal cold_materializations
        cold_materializations += 1
        return request, source_replay

    monkeypatch.setattr(adapter, "_validate_fixed_bridge", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(adapter, "_canonical_day_projection", project)

    cold = adapter._bind_bulk_day_input_mmaps(
        prepared,
        acceleration=acceleration,
        total_worker_tokens=1,
    )
    warm_verifications = 0
    original_load = cache.load_day_input_mmap_binding

    def load_once_per_key(*args: object, **kwargs: object) -> adapter.DayInputMmapBinding | None:
        nonlocal warm_verifications
        warm_verifications += 1
        return original_load(*args, **kwargs)

    monkeypatch.setattr(cache, "load_day_input_mmap_binding", load_once_per_key)
    warm = adapter._bind_bulk_day_input_mmaps(
        prepared,
        acceleration=acceleration,
        total_worker_tokens=1,
    )

    assert cold_materializations == 1
    assert warm_verifications == 1
    cold_payloads = [item.jobs[0].payload["day_input_mmap_binding"] for item in cold]
    warm_payloads = [item.jobs[0].payload["day_input_mmap_binding"] for item in warm]
    assert cold_payloads[0] == cold_payloads[1] == warm_payloads[0] == warm_payloads[1]

    monkeypatch.setattr(adapter, "_canonical_day_request", lambda **_kwargs: request)
    with adapter._canonical_day_projection_context(cold[0].jobs[0]) as (
        opened_request,
        replay,
        evidence,
    ):
        assert opened_request is request
        assert evidence is not None and evidence["read_only_mmap"] is True
        np.testing.assert_array_equal(replay.bbo_data.best_bid, source_replay.bbo_data.best_bid)
        np.testing.assert_array_equal(replay.l2_data.bid_px, source_replay.l2_data.bid_px)
        assert replay.bbo_data.best_bid.flags.writeable is False
        assert replay.l2_data.bid_px.flags.writeable is False

    assert adapter.build_canonical_replay_adapter()._acceleration is None


def test_day_input_mmap_adapter_binding_hash_drift_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    acceleration = _governed_acceleration(tmp_path, monkeypatch)
    cache = adapter.DayReplayCache(tmp_path / "replay-cache")
    prepared = _mmap_prepared(cache=cache)[:1]
    monkeypatch.setattr(adapter, "_validate_fixed_bridge", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        adapter,
        "_canonical_day_projection",
        lambda _job: (_mmap_request(), _mmap_source_replay()),
    )
    rebound = adapter._bind_bulk_day_input_mmaps(
        prepared,
        acceleration=acceleration,
        total_worker_tokens=1,
    )
    contract = adapter._day_input_mmap_binding_from_payload(
        rebound[0].jobs[0].payload["day_input_mmap_binding"]
    )
    path = cache.day_input_mmap_bindings / f"{contract.materialization_key_sha256}.json"
    payload = json.loads(path.read_text(encoding="ascii"))
    payload["canonical_input_binding_sha256"] = _sha("0")
    path.write_text(json.dumps(payload), encoding="ascii")

    with pytest.raises(adapter.OfflineReplayAdapterError, match="receipt drifted"):
        cache.load_day_input_mmap_binding(
            contract.materialization_key_sha256,
            acceleration=acceleration,
        )


BACKEND_DAYS = ("2026-09-01", "2026-09-02")


def _backend_candidate(name: str, token: str) -> nested.FittedCandidate:
    return nested.FittedCandidate(
        ladder_name=name,
        side="SELL",
        policy=None,
        selected_profile=f"profile::{name}",
        training_days=("2026-08-20",),
        training_row_sha256=_sha(token),
        policy_payload={"kind": "synthetic", "name": name},
        policy_sha256=_sha(token.upper()),
        fit_audit={},
        feature_pool_audit=None,
    )


def _backend_request(candidate: nested.FittedCandidate) -> nested.EvaluationRequest:
    return nested.EvaluationRequest(
        candidate=candidate,
        side="SELL",
        days=BACKEND_DAYS,
        fold_id="outer1",
        stage="outer_oof",
        panel_role=offline.PANEL_ROLE,
    )


def _backend_valid_rows(request: nested.EvaluationRequest) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for day in request.days:
        rows.append(
            {
                "utc_day": day,
                "side": request.side,
                "panel_role": request.panel_role,
                "candidate_terminal_value_usdc": 1.0,
                "exact_owner_terminal_value_usdc": 0.5,
                "point_identified": True,
                "policy_assignment_count": 1,
                "nonbaseline_action_count": 1,
                "feature_ready_active_treatment_events": 1,
                "repeated_sequential_policy": True,
                "one_shot_effect_aggregation_used": False,
                "exact_current_owner_row_wise_baseline": True,
                "candidate_executed_policy_sha256": (
                    request.candidate.expected_executed_policy_sha256
                ),
                "exact_owner_executed_policy_sha256": offline.ACTIVE_OWNER_POLICY_SHA256,
                "paired_replay_receipt_sha256": adapter._canonical_sha256(
                    {"request_sha256": request.request_sha256, "utc_day": day}
                ),
                "candidate_target_side": request.side,
                "same_market_source": True,
                "common_random_source": True,
                "arm_local_state": True,
                "common_row_count": 1,
                "common_campaign_count": 1,
                "candidate_closed_campaign_value_usdc": 1.0,
                "exact_owner_closed_campaign_value_usdc": 0.5,
                "candidate_campaign_q10_usdc": -0.1,
                "exact_owner_campaign_q10_usdc": -0.1,
                "candidate_campaign_cvar10_usdc": -0.2,
                "exact_owner_campaign_cvar10_usdc": -0.2,
                "candidate_inventory_time_btc_s": 1.0,
                "exact_owner_inventory_time_btc_s": 1.0,
                "candidate_max_abs_inventory_btc": 0.001,
                "exact_owner_max_abs_inventory_btc": 0.001,
                "candidate_fill_count": 1,
                "exact_owner_fill_count": 1,
                "candidate_negative_terminal_rate": 0.0,
                "exact_owner_negative_terminal_rate": 0.0,
                "candidate_campaign_mae_usdc": 0.1,
                "exact_owner_campaign_mae_usdc": 0.1,
                "candidate_repair_event_rate": 0.0,
                "exact_owner_repair_event_rate": 0.0,
                "candidate_mean_repair_time_s": 1.0,
                "exact_owner_mean_repair_time_s": 1.0,
                "candidate_censoring_rate": 0.0,
                "exact_owner_censoring_rate": 0.0,
                "action_count::CONTROL_85N": 1,
                "role_count::add": 1,
                "consecutive_units_count::1": 1,
                "fallback_count::none": 1,
            }
        )
    return pd.DataFrame(rows)


def _backend_mechanics() -> backend.OutcomeBlindMechanics:
    bindings = backend.FormalExecutionBindings(
        execution_manifest_sha256=_sha("1"),
        source_manifest_sha256=_sha("2"),
        panel_manifest_sha256=_sha("3"),
        fold_manifest_sha256=_sha("4"),
        nested_fold_manifest_sha256=_sha("5"),
        exact_owner_policy_sha256=offline.ACTIVE_OWNER_POLICY_SHA256,
        exact_owner_predicate_bundle_sha256=offline.ACTIVE_PREDICATE_BUNDLE_SHA256,
        exact_owner_private_config_sha256=offline.ACTIVE_PRIVATE_CONFIG_SHA256,
    )
    return backend.OutcomeBlindMechanics(
        panel=None,  # type: ignore[arg-type]
        replay_inputs=pd.DataFrame(
            {
                "utc_day": BACKEND_DAYS,
                "side": ("SELL", "SELL"),
                "opportunity_id": ("s1", "s2"),
            }
        ),
        selected_days=BACKEND_DAYS,
        fold_manifest=None,  # type: ignore[arg-type]
        bindings=bindings,
        file_sha256={},
        mechanics_receipt_sha256=_sha("6"),
    )


def test_actual_backend_to_adapter_bulk_abi_uses_global_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    acceleration = _governed_acceleration(tmp_path / "bulk-abi", monkeypatch)
    replay_adapter = adapter._CanonicalOfflineReplayAdapter(
        acceleration=acceleration,
        global_worker_tokens=10,
    )
    prepare_calls: list[str] = []
    global_pool_calls: list[tuple[int, int]] = []
    phase_order: list[str] = []
    replay_cache = adapter.DayReplayCache(tmp_path / "replay-cache")

    def prepare(
        *,
        adapter_artifact_sha256: str,
        request: backend.CanonicalSequentialReplayRequest,
        replay_inputs: pd.DataFrame,
    ) -> adapter._PreparedSequentialReplay:
        assert backend._frame_sha256(replay_inputs) == request.replay_input_sha256
        prepare_calls.append(request.evaluation_request.request_sha256)
        receipt = backend.build_sequential_replay_receipt(
            request,
            adapter_identity=replay_adapter.identity,
            adapter_artifact_sha256=adapter_artifact_sha256,
        )
        evaluation = request.evaluation_request
        jobs = tuple(
            replace(
                _day_job(
                    candidate=evaluation.candidate.expected_executed_policy_sha256[0],
                    fold=evaluation.fold_id,
                    day=day,
                    stage=evaluation.stage,
                ),
                payload={"evaluation_request": evaluation},
            )
            for day in evaluation.days
        )
        return adapter._PreparedSequentialReplay(
            request=request,
            options=adapter._ExecutionOptions(
                binding={},
                cache=replay_cache,
                workers=6,
            ),
            receipt=receipt,
            cached_frames=(),
            jobs=jobs,
        )

    def forbidden_single(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("backend silently fell back to single-request replay")

    def build_phases(
        prepared: Any,
    ) -> tuple[tuple[adapter._DayReplayJob, ...], tuple[adapter._DayReplayJob, ...]]:
        phase_order.append("b0-build")
        return (), tuple(job for item in prepared for job in item.jobs)

    def bind_mmaps(
        prepared: Any,
        *,
        acceleration: adapter.SequentialReplayAccelerationOptions,
        total_worker_tokens: int,
    ) -> Any:
        assert acceleration is replay_adapter._acceleration
        assert total_worker_tokens == 10
        phase_order.append("mmap")
        return tuple(prepared)

    def global_pool(
        jobs: Any,
        *,
        total_worker_tokens: int,
    ) -> tuple[adapter._DayReplayJobResult, ...]:
        job_tuple = tuple(jobs)
        phase_order.append("candidate")
        global_pool_calls.append((len(job_tuple), total_worker_tokens))
        results: list[adapter._DayReplayJobResult] = []
        for job in job_tuple:
            evaluation = job.payload["evaluation_request"]
            rows = _backend_valid_rows(evaluation)
            rows = rows.loc[rows["utc_day"] == job.utc_day].copy()
            results.append(
                adapter._DayReplayJobResult(
                    utc_day=job.utc_day,
                    cache_key_sha256=job.cache_key.cache_key_sha256,
                    frames={"rows": rows},
                )
            )
        return tuple(results)

    monkeypatch.setattr(adapter, "_prepare_sequential_replay", prepare)
    monkeypatch.setattr(replay_adapter, "evaluate_repeated_policy", forbidden_single)
    monkeypatch.setattr(adapter, "_bind_bulk_day_input_mmaps", bind_mmaps)
    monkeypatch.setattr(adapter, "_build_bulk_b0_and_candidate_phases", build_phases)

    def b0_phase(*_args: object, **_kwargs: object) -> tuple[adapter._DayReplayJobResult, ...]:
        phase_order.append("b0")
        return ()

    monkeypatch.setattr(adapter, "_run_global_b0_control_jobs", b0_phase)
    monkeypatch.setattr(adapter, "_prospective_b0_control_cache_key", lambda _job: _b0_key())
    monkeypatch.setattr(
        adapter.DayReplayCache,
        "load_b0_control",
        lambda _self, _key: (_b0_path(), {"receipt_sha256": _sha("7")}),
    )
    monkeypatch.setattr(adapter, "run_global_policy_day_jobs", global_pool)
    evaluator = backend.CanonicalSequentialEvaluator(_backend_mechanics(), replay_adapter)
    requests = (
        _backend_request(_backend_candidate("E1_FULL_EMA_BANK", "8")),
        _backend_request(_backend_candidate("E2_DIRECTIONAL_EMA", "9")),
    )

    results = evaluator.evaluate_many(requests)

    assert prepare_calls == [request.request_sha256 for request in requests]
    assert global_pool_calls == [(4, 10)]
    assert phase_order == ["mmap", "b0-build", "b0", "candidate"]
    assert [result.request_sha256 for result in results] == [
        request.request_sha256 for request in requests
    ]
    assert len(evaluator.receipts) == 2


def test_backend_batch_expected_receipt_drift_fails_before_replay() -> None:
    replay_adapter = adapter._CanonicalOfflineReplayAdapter()
    evaluator = backend.CanonicalSequentialEvaluator(_backend_mechanics(), replay_adapter)
    request = _backend_request(_backend_candidate("E1_FULL_EMA_BANK", "8"))
    prepared = evaluator._prepare_request(request)
    wrong_request = replace(prepared, request_sha256=_sha("0"))
    with pytest.raises(adapter.OfflineReplayAdapterError, match="request identity drifted"):
        replay_adapter.evaluate_repeated_policies((wrong_request,))

    drifted = replace(prepared, expected_receipt={"receipt_sha256": _sha("0")})

    with pytest.raises(adapter.OfflineReplayAdapterError, match="expected receipt drifted"):
        replay_adapter.evaluate_repeated_policies((drifted,))
