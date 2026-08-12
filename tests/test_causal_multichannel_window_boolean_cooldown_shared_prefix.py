from __future__ import annotations

import fcntl
import hashlib
import json
import os
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

import pytest

from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_shared_prefix import (
    ARM_RESULT_SCHEMA_VERSION,
    OPPORTUNITY_MANIFEST_SCHEMA_VERSION,
    STRICT_COUNTER_FIELDS,
    PosixCooldownSharedPrefixExecutor,
    SharedPrefixExecutionError,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_strict_checkpoint import (
    ARM_DURATION_MS,
    BUY_ARMS,
    SELL_ARMS,
)

pytestmark = pytest.mark.skipif(not hasattr(os, "fork"), reason="POSIX fork required")

STRICT_QUEUE_SCOPE = "strategy_independent_native_snapshot_delta_exchange_time_v1"
TARGET_DAY = "2026-04-17"
TARGET_VISIBLE_MS = int(datetime(2026, 4, 17, 12, 0, 0, 123_000, tzinfo=UTC).timestamp() * 1_000)


def _identity_hashes() -> dict[str, str]:
    return {
        "baseline_identity_sha256": "1" * 64,
        "config_sha256": "2" * 64,
        "code_sha256": "3" * 64,
        "model_sha256": "4" * 64,
        "p3_sha256": "5" * 64,
        "feature_dag_sha256": "6" * 64,
        "execution_abi_sha256": "7" * 64,
    }


def _executor(output_root: Path) -> PosixCooldownSharedPrefixExecutor:
    return PosixCooldownSharedPrefixExecutor(
        output_root=output_root,
        target_day=TARGET_DAY,
        source_contract_sha256="8" * 64,
        execution_identity_hashes=_identity_hashes(),
        max_parallel_arms=2,
        require_strict_native=True,
    )


def _opportunity(
    side: str,
    *,
    ordinal: int = 17,
    strict_counter_baseline: Mapping[str, int] | None = None,
) -> dict[str, object]:
    baseline = {
        name: int((strict_counter_baseline or {}).get(name, 0))
        for name in STRICT_COUNTER_FIELDS
    }
    if strict_counter_baseline is None:
        baseline["exchange_book_events_consumed"] = 100
        baseline["exchange_book_events_accepted"] = 80
        baseline["exchange_book_events_rejected"] = 20
    return {
        "exposure_fill_ordinal": ordinal,
        "partial_fill_ordinal": 1,
        "fill_visible_ts_ms": TARGET_VISIBLE_MS + ordinal,
        "fill_exchange_ts_ms": TARGET_VISIBLE_MS + ordinal - 23,
        "side": side,
        "role_at_fill": "add",
        "order_id": 901 + ordinal,
        "campaign_id": 73 + ordinal,
        "fill_qty_btc": 0.001,
        "baseline_duration_ms": 255_000.0,
        "cooldown_v2_snapshot_id": f"snapshot-{side.lower()}-{ordinal}",
        "cooldown_v2_source_bundle_sha256": "9" * 64,
        "exchange_book_queue_mode": "strict",
        "exchange_book_queue_scope": STRICT_QUEUE_SCOPE,
        "strict_counter_baseline": baseline,
        "exchange_book_queue_missing_trace_cursor": baseline[
            "exchange_book_queue_missing_count"
        ],
        "exchange_book_queue_missing_count_at_assignment": baseline[
            "exchange_book_queue_missing_count"
        ],
    }


def _canonical_sha256(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _record_concurrency(counter_path: Path, delta: int) -> None:
    counter_path.parent.mkdir(parents=True, exist_ok=True)
    with counter_path.open("a+", encoding="ascii") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        handle.seek(0)
        text = handle.read().strip()
        state = json.loads(text) if text else {"active": 0, "maximum": 0}
        state["active"] = int(state["active"]) + delta
        if state["active"] < 0:
            raise AssertionError("concurrency counter became negative")
        state["maximum"] = max(int(state["maximum"]), int(state["active"]))
        handle.seek(0)
        handle.truncate()
        json.dump(state, handle, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _dispatch_synthetic_arms(
    executor: PosixCooldownSharedPrefixExecutor,
    opportunity: Mapping[str, object],
    *,
    concurrency_counter: Path | None = None,
    arm_delay_s: float = 0.08,
    treatment_events_consumed: int = 1,
    ambiguous_field: str | None = None,
    missing_queue_seed_count: int = 0,
    missing_queue_seed_arm_id: str | None = None,
    missing_trace_count_override: int | None = None,
    failing_arm_id: str | None = None,
    drain: bool = True,
) -> None:
    selection = executor.dispatch(opportunity)
    if selection is None:
        if drain:
            executor.audit()
        return
    if selection.arm_id == failing_arm_id:
        os._exit(91)
    if concurrency_counter is not None:
        _record_concurrency(concurrency_counter, 1)
        time.sleep(arm_delay_s)
        _record_concurrency(concurrency_counter, -1)
    baseline_counters = dict(selection.strict_counter_baseline)
    counters = {
        name: baseline_counters[name]
        + (
            treatment_events_consumed
            if name == "exchange_book_events_consumed"
            else 0
        )
        for name in STRICT_COUNTER_FIELDS
    }
    if ambiguous_field is not None:
        counters[ambiguous_field] += 1
    arm_missing_count = (
        int(missing_queue_seed_count)
        if missing_queue_seed_arm_id is None
        or selection.arm_id == missing_queue_seed_arm_id
        else 0
    )
    counters["exchange_book_queue_missing_count"] += arm_missing_count
    counters["exchange_book_queue_lookup_count"] += arm_missing_count
    missing_trace_count = (
        arm_missing_count
        if missing_trace_count_override is None
        else int(missing_trace_count_override)
    )
    executor.finalize_simulation_result(
        {
            "_cooldown_duration_fork_trace": {
                "action": selection.action,
                "synthetic_engineering_test_only": True,
                "assignment_to_washout_value_usdc": 123.45,
            },
            "exchange_book_queue_mode": "strict",
            "exchange_book_queue_scope": STRICT_QUEUE_SCOPE,
            "_exchange_book_queue_ambiguity_trace": (
                []
                if ambiguous_field is None
                else [
                    {
                        "reason": "same_ms_book_trade_ordering",
                        "ambiguous": True,
                        "event_ts_ms": TARGET_VISIBLE_MS,
                        "order_id": 952,
                        "side": "SELL",
                        "state": "ACTIVE",
                        "price_tick": 640_000,
                        "activate_ts_ms": TARGET_VISIBLE_MS - 1_000,
                        "cancel_ts_ms": -1,
                        "queue_seed_status": "exact",
                    }
                ]
            ),
            "_exchange_book_queue_missing_trace": [
                {
                    "order_id": 10_000 + index,
                    "side": "BUY",
                    "price": 64_000.1,
                    "price_tick": 640_001,
                    "activate_ts_ms": TARGET_VISIBLE_MS + index,
                    "status": "missing",
                    "reason": "outside_snapshot_range",
                    "asof_exchange_ts_ns": (TARGET_VISIBLE_MS + index) * 1_000_000,
                    "segment_id": 3,
                    "snapshot_min_tick": 639_900,
                    "snapshot_max_tick": 640_100,
                }
                for index in range(missing_trace_count)
            ],
            **counters,
        }
    )
    raise AssertionError("arm child must terminate in finalize_simulation_result")


def _one_admission(root: Path) -> Path:
    day_root = root / TARGET_DAY
    admissions = [path for path in day_root.iterdir() if path.is_dir()]
    assert len(admissions) == 1
    return admissions[0]


@pytest.mark.parametrize(
    ("side", "expected_arms"),
    (("BUY", BUY_ARMS), ("SELL", SELL_ARMS)),
)
def test_exactly_eight_ordered_side_arms_and_max_two_concurrent(
    tmp_path: Path,
    side: str,
    expected_arms: Sequence[str],
) -> None:
    output_root = tmp_path / side.lower()
    counter_path = tmp_path / f"{side.lower()}-concurrency.json"
    executor = _executor(output_root)

    _dispatch_synthetic_arms(
        executor,
        _opportunity(side),
        concurrency_counter=counter_path,
    )

    admission = _one_admission(output_root)
    manifest = json.loads((admission / "manifest.json").read_text(encoding="ascii"))
    assert manifest["arm_count"] == 8
    assert tuple(row["arm_id"] for row in manifest["arms"]) == tuple(expected_arms)
    assert manifest["max_parallel_arms"] == 2
    timing = manifest["execution_timing"]
    assert timing["supervisor_wall_time_s"] > 0
    assert timing["global_peak_concurrent_supervisors_observed"] == 1
    assert timing["global_peak_concurrent_arms_observed"] == 2
    assert set(timing["arm_wall_time_s_by_arm"]) == set(expected_arms)
    assert json.loads(counter_path.read_text(encoding="ascii")) == {
        "active": 0,
        "maximum": 2,
    }
    for row, arm_id in zip(manifest["arms"], expected_arms, strict=True):
        payload = json.loads((admission / row["path"]).read_text(encoding="ascii"))
        assert payload["arm_id"] == arm_id
        assert payload["action"] == (
            "CONTROL_85N" if arm_id == "CONTROL_85N" else "FIXED_DURATION_MS"
        )
        assert payload["fixed_duration_ms"] == float(ARM_DURATION_MS[arm_id] or 0)
        assert row["wall_time_s"] > 0


def test_atomic_admission_binds_manifest_arm_and_success_hashes(
    tmp_path: Path,
) -> None:
    output_root = tmp_path / "labels"
    executor = _executor(output_root)
    _dispatch_synthetic_arms(executor, _opportunity("BUY"))

    admission = _one_admission(output_root)
    assert not tuple((output_root / TARGET_DAY).glob(".*.staging.*"))
    assert {path.name for path in admission.iterdir()} == {
        "manifest.json",
        "_SUCCESS",
        *(f"arm-{arm_id}.json" for arm_id in BUY_ARMS),
    }
    manifest_path = admission / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="ascii"))
    success = json.loads((admission / "_SUCCESS").read_text(encoding="ascii"))
    assert manifest["schema_version"] == OPPORTUNITY_MANIFEST_SCHEMA_VERSION
    assert manifest["atomic_admission"] is True
    assert success["manifest_sha256"] == hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    for row in manifest["arms"]:
        arm_path = admission / row["path"]
        payload = json.loads(arm_path.read_text(encoding="ascii"))
        assert payload["schema_version"] == ARM_RESULT_SCHEMA_VERSION
        assert row["size_bytes"] == arm_path.stat().st_size
        assert row["sha256"] == hashlib.sha256(arm_path.read_bytes()).hexdigest()
        embedded_hash = payload.pop("canonical_result_sha256")
        assert embedded_hash == _canonical_sha256(payload)


def test_resume_validates_admission_without_rerunning_arms(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_root = tmp_path / "labels"
    opportunity = _opportunity("SELL")
    first = _executor(output_root)
    _dispatch_synthetic_arms(first, opportunity)
    admission = _one_admission(output_root)
    before = {
        path.name: (path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest())
        for path in admission.iterdir()
    }

    def unexpected_supervisor(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("resume must not rerun an arm supervisor")

    resumed = _executor(output_root)
    monkeypatch.setattr(resumed, "_run_supervisor", unexpected_supervisor)
    assert resumed.dispatch(opportunity) is None

    after = {
        path.name: (path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest())
        for path in admission.iterdir()
    }
    assert after == before
    audit = resumed.audit()
    assert audit.opportunities_dispatched == 0
    assert audit.opportunities_resumed == 1
    assert audit.arm_processes_completed == 0


def test_stale_staging_fails_closed_before_fork(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_root = tmp_path / "labels"
    opportunity = _opportunity("BUY")
    executor = _executor(output_root)
    identity_sha256, _ = executor._opportunity_identity(opportunity)
    stale = executor.output_root / TARGET_DAY / f".{identity_sha256}.staging.interrupted"
    stale.mkdir(parents=True)

    def unexpected_fork() -> int:
        raise AssertionError("stale staging must fail before fork")

    monkeypatch.setattr(os, "fork", unexpected_fork)
    with pytest.raises(SharedPrefixExecutionError, match="stale shared-prefix staging"):
        executor.dispatch(opportunity)
    assert stale.is_dir()
    assert not (output_root / TARGET_DAY / identity_sha256).exists()


def test_corrupted_manifest_fails_closed_on_resume(tmp_path: Path) -> None:
    output_root = tmp_path / "labels"
    opportunity = _opportunity("BUY")
    _dispatch_synthetic_arms(_executor(output_root), opportunity)
    admission = _one_admission(output_root)
    manifest_path = admission / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="ascii"))
    manifest["arms"][0], manifest["arms"][1] = (
        manifest["arms"][1],
        manifest["arms"][0],
    )
    manifest_path.write_text(json.dumps(manifest), encoding="ascii")

    with pytest.raises(
        SharedPrefixExecutionError,
        match="exactly eight ordered arms",
    ):
        _executor(output_root).dispatch(opportunity)


def test_corrupted_arm_fails_closed_on_resume(tmp_path: Path) -> None:
    output_root = tmp_path / "labels"
    opportunity = _opportunity("SELL")
    _dispatch_synthetic_arms(_executor(output_root), opportunity)
    admission = _one_admission(output_root)
    arm_path = admission / f"arm-{SELL_ARMS[-1]}.json"
    arm_path.write_bytes(arm_path.read_bytes() + b"\n")

    with pytest.raises(SharedPrefixExecutionError, match="arm SHA256 drifted"):
        _executor(output_root).dispatch(opportunity)


def test_embedded_arm_hash_fails_closed_even_if_outer_hashes_are_rewritten(
    tmp_path: Path,
) -> None:
    output_root = tmp_path / "labels"
    opportunity = _opportunity("BUY")
    _dispatch_synthetic_arms(_executor(output_root), opportunity)
    admission = _one_admission(output_root)
    manifest_path = admission / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="ascii"))
    arm_row = manifest["arms"][0]
    arm_path = admission / arm_row["path"]
    payload = json.loads(arm_path.read_text(encoding="ascii"))
    payload["fork_trace"]["tampered_after_admission"] = True
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    arm_path.write_bytes(encoded)
    arm_row["size_bytes"] = len(encoded)
    arm_row["sha256"] = hashlib.sha256(encoded).hexdigest()
    manifest_encoded = json.dumps(
        manifest,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    manifest_path.write_bytes(manifest_encoded)
    success_path = admission / "_SUCCESS"
    success = json.loads(success_path.read_text(encoding="ascii"))
    success["manifest_sha256"] = hashlib.sha256(manifest_encoded).hexdigest()
    success_path.write_text(
        json.dumps(success, sort_keys=True, separators=(",", ":")),
        encoding="ascii",
    )

    with pytest.raises(
        SharedPrefixExecutionError,
        match="canonical result SHA256 drifted",
    ):
        _executor(output_root).dispatch(opportunity)


def test_existing_opportunity_lock_fails_closed_before_fork(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_root = tmp_path / "labels"
    opportunity = _opportunity("SELL")
    executor = _executor(output_root)
    identity_sha256, _ = executor._opportunity_identity(opportunity)
    day_root = output_root / TARGET_DAY
    day_root.mkdir(parents=True)
    lock_path = day_root / f".{identity_sha256}.lock"
    lock_path.write_text("concurrent-owner", encoding="ascii")

    def unexpected_fork() -> int:
        raise AssertionError("an existing opportunity lock must fail before fork")

    monkeypatch.setattr(os, "fork", unexpected_fork)
    with pytest.raises(
        SharedPrefixExecutionError,
        match="opportunity lock exists",
    ):
        executor.dispatch(opportunity)
    assert lock_path.read_text(encoding="ascii") == "concurrent-owner"


def test_public_contract_never_claims_portable_restore(tmp_path: Path) -> None:
    output_root = tmp_path / "labels"
    executor = _executor(output_root)
    opportunity = _opportunity("BUY")
    _dispatch_synthetic_arms(executor, opportunity)
    admission = _one_admission(output_root)
    manifest = json.loads((admission / "manifest.json").read_text(encoding="ascii"))

    assert manifest["portable_restore_authority"] is False
    assert manifest["opportunity_contract"]["portable_restore_authority"] is False
    assert manifest["opportunity_contract"]["checkpoint_semantics"] == (
        "posix_fork_copy_on_write_at_fill_callback"
    )
    audit = executor.audit()
    assert audit.portable_restore_authority is False
    assert audit.simulator_checkpoint_semantics == ("posix_fork_copy_on_write_at_fill_callback")
    assert not hasattr(executor, "restore")


def test_parent_replay_continues_with_two_opportunities_and_global_arm_bound(
    tmp_path: Path,
) -> None:
    output_root = tmp_path / "labels"
    counter_path = tmp_path / "global-concurrency.json"
    executor = _executor(output_root)

    started = time.monotonic()
    _dispatch_synthetic_arms(
        executor,
        _opportunity("BUY", ordinal=21),
        concurrency_counter=counter_path,
        arm_delay_s=0.15,
        drain=False,
    )
    first_dispatch_wall_s = time.monotonic() - started
    assert first_dispatch_wall_s < 0.30
    assert not tuple((output_root / TARGET_DAY).glob("*/_SUCCESS"))

    _dispatch_synthetic_arms(
        executor,
        _opportunity("SELL", ordinal=22),
        concurrency_counter=counter_path,
        arm_delay_s=0.15,
        drain=False,
    )
    audit = executor.audit()

    assert audit.opportunities_dispatched == 2
    assert audit.supervisor_processes_completed == 2
    assert audit.arm_processes_completed == 16
    assert audit.peak_concurrent_supervisors == 2
    assert audit.peak_concurrent_arms == 2
    assert audit.pending_supervisors == 0
    assert audit.asynchronous_parent_replay is True
    assert audit.executor_wall_time_s > 0
    assert audit.supervisor_wall_time_s_total > 0
    assert audit.arm_wall_time_s_total > 0
    assert json.loads(counter_path.read_text(encoding="ascii")) == {
        "active": 0,
        "maximum": 2,
    }
    admissions = [
        path
        for path in (output_root / TARGET_DAY).iterdir()
        if path.is_dir()
    ]
    assert len(admissions) == 2
    for admission in admissions:
        manifest = json.loads(
            (admission / "manifest.json").read_text(encoding="ascii")
        )
        timing = manifest["execution_timing"]
        assert timing["global_peak_concurrent_supervisors_observed"] == 2
        assert timing["global_peak_concurrent_arms_observed"] == 2


def test_child_failure_is_deferred_but_final_audit_fails_closed(
    tmp_path: Path,
) -> None:
    output_root = tmp_path / "labels"
    executor = _executor(output_root)

    _dispatch_synthetic_arms(
        executor,
        _opportunity("BUY", ordinal=31),
        failing_arm_id=BUY_ARMS[0],
        drain=False,
    )
    with pytest.raises(
        SharedPrefixExecutionError,
        match=(
            "supervisor failed: SharedPrefixExecutionError: "
            f"shared-prefix arm {BUY_ARMS[0]} exited unsuccessfully"
        ),
    ):
        executor.audit()

    assert not tuple((output_root / TARGET_DAY).glob("*/_SUCCESS"))
    staging = tuple((output_root / TARGET_DAY).glob(".*.staging.*"))
    assert staging
    error = json.loads((staging[0] / "_ERROR.json").read_text(encoding="ascii"))
    assert error["error_type"] == "SharedPrefixExecutionError"
    assert error["error"] == (
        f"shared-prefix arm {BUY_ARMS[0]} exited unsuccessfully"
    )


def test_parent_never_parses_arm_economic_payloads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_root = tmp_path / "labels"
    executor = _executor(output_root)
    parent_pid = os.getpid()
    original_validator = executor._validate_completed_destination

    def reject_parent_validation(*args: object, **kwargs: object) -> dict[str, object]:
        if os.getpid() == parent_pid:
            raise AssertionError("baseline parent parsed an arm result")
        return original_validator(*args, **kwargs)

    monkeypatch.setattr(
        executor,
        "_validate_completed_destination",
        reject_parent_validation,
    )
    _dispatch_synthetic_arms(executor, _opportunity("SELL", ordinal=41))

    audit = executor.audit()
    assert audit.economic_outcomes_read_by_parent is False
    assert audit.opportunities_dispatched == 1


def test_same_ms_queue_ambiguity_remains_strict_label_unsupported(
    tmp_path: Path,
) -> None:
    output_root = tmp_path / "labels"
    executor = _executor(output_root)
    ambiguous_field = "exchange_book_queue_ambiguous_event_count"
    _dispatch_synthetic_arms(
        executor,
        _opportunity("SELL", ordinal=51),
        ambiguous_field=ambiguous_field,
    )

    admission = _one_admission(output_root)
    manifest = json.loads((admission / "manifest.json").read_text(encoding="ascii"))
    for row in manifest["arms"]:
        payload = json.loads((admission / row["path"]).read_text(encoding="ascii"))
        strict = payload["strict_execution_contract"]
        assert strict[ambiguous_field] == 1
        assert strict["strict_native_label_eligible"] is False
        assert strict["strict_native_label_unsupported_reasons"] == [
            ambiguous_field
        ]
        assert strict["exchange_book_queue_ambiguity_trace"] == [
            {
                "reason": "same_ms_book_trade_ordering",
                "ambiguous": True,
                "event_ts_ms": TARGET_VISIBLE_MS,
                "order_id": 952,
                "side": "SELL",
                "state": "ACTIVE",
                "price_tick": 640_000,
                "activate_ts_ms": TARGET_VISIBLE_MS - 1_000,
                "cancel_ts_ms": -1,
                "queue_seed_status": "exact",
            }
        ]


def test_treatment_queue_missing_seed_is_retained_as_arm_level_unsupported(
    tmp_path: Path,
) -> None:
    output_root = tmp_path / "labels"
    executor = _executor(output_root)
    _dispatch_synthetic_arms(
        executor,
        _opportunity("BUY", ordinal=57),
        missing_queue_seed_count=1,
    )

    admission = _one_admission(output_root)
    manifest = json.loads((admission / "manifest.json").read_text(encoding="ascii"))
    assert manifest["arm_count"] == 8
    for row in manifest["arms"]:
        payload = json.loads((admission / row["path"]).read_text(encoding="ascii"))
        strict = payload["strict_execution_contract"]
        assert strict["exchange_book_queue_missing_count"] == 1
        assert strict["strict_native_label_eligible"] is False
        assert strict["strict_native_label_unsupported_reasons"] == [
            "exchange_book_queue_missing_count"
        ]
        assert strict["economic_point_label_status"] == "unsupported_redacted"
        assert strict["exchange_book_queue_ambiguity_trace"] == []
        assert len(strict["exchange_book_queue_missing_trace"]) == 1
        assert payload["fork_trace"]["assignment_to_washout_value_usdc"] is None


def test_one_missing_arm_does_not_contaminate_other_arms_or_next_opportunity(
    tmp_path: Path,
) -> None:
    output_root = tmp_path / "labels"
    executor = _executor(output_root)
    _dispatch_synthetic_arms(
        executor,
        _opportunity("BUY", ordinal=58),
        missing_queue_seed_count=1,
        missing_queue_seed_arm_id="FIXED_79S",
    )
    _dispatch_synthetic_arms(executor, _opportunity("BUY", ordinal=59))

    admissions = sorted(
        path
        for path in (output_root / TARGET_DAY).iterdir()
        if path.is_dir()
    )
    assert len(admissions) == 2
    first_payloads = []
    second_payloads = []
    for admission in admissions:
        manifest = json.loads(
            (admission / "manifest.json").read_text(encoding="ascii")
        )
        target = (
            first_payloads
            if manifest["opportunity_contract"]["opportunity"][
                "exposure_fill_ordinal"
            ]
            == 58
            else second_payloads
        )
        target.extend(
            json.loads((admission / row["path"]).read_text(encoding="ascii"))
            for row in manifest["arms"]
        )
    assert len(first_payloads) == 8
    assert len(second_payloads) == 8
    for payload in first_payloads:
        strict = payload["strict_execution_contract"]
        expected_missing = 1 if payload["arm_id"] == "FIXED_79S" else 0
        assert strict["exchange_book_queue_missing_count"] == expected_missing
        assert len(strict["exchange_book_queue_missing_trace"]) == expected_missing
        assert strict["strict_native_label_eligible"] is (expected_missing == 0)
        assert strict["economic_point_label_status"] == (
            "unsupported_redacted" if expected_missing else "eligible"
        )
        assert payload["fork_trace"]["assignment_to_washout_value_usdc"] == (
            None if expected_missing else 123.45
        )
    assert all(
        payload["strict_execution_contract"]["strict_native_label_eligible"]
        for payload in second_payloads
    )


def test_common_prefix_queue_failure_rejects_formal_opportunity_before_fork(
    tmp_path: Path,
) -> None:
    output_root = tmp_path / "labels"
    prefix = {name: 0 for name in STRICT_COUNTER_FIELDS}
    prefix.update(
        {
            "exchange_book_queue_lookup_count": 7,
            "exchange_book_queue_exact_count": 4,
            "exchange_book_queue_known_zero_count": 1,
            "exchange_book_queue_missing_count": 2,
            "exchange_book_queue_invalidated_order_count": 3,
            "exchange_book_queue_ambiguous_event_count": 3,
            "exchange_book_cancel_trade_ambiguous_order_count": 1,
            "exchange_book_cancel_book_ambiguous_order_count": 2,
            "exchange_book_events_consumed": 120,
            "exchange_book_events_accepted": 100,
            "exchange_book_events_rejected": 20,
        }
    )
    with pytest.raises(
        SharedPrefixExecutionError,
        match="queue evidence is not exact before assignment",
    ):
        _executor(output_root).dispatch(
            _opportunity(
                "BUY",
                ordinal=61,
                strict_counter_baseline=prefix,
            )
        )


def test_truncated_treatment_missing_trace_fails_closed(tmp_path: Path) -> None:
    executor = _executor(tmp_path / "labels")
    with pytest.raises(
        SharedPrefixExecutionError,
        match="shared-prefix supervisor failed",
    ):
        _dispatch_synthetic_arms(
            executor,
            _opportunity("SELL", ordinal=60),
            missing_queue_seed_count=1,
            missing_queue_seed_arm_id="FIXED_166S",
            missing_trace_count_override=0,
        )


def test_missing_trace_duplicate_and_field_drift_fail_closed(tmp_path: Path) -> None:
    executor = _executor(tmp_path / "labels")
    row = {
        "order_id": 10001,
        "side": "SELL",
        "price": 64_001.2,
        "price_tick": 640_012,
        "activate_ts_ms": TARGET_VISIBLE_MS,
        "status": "missing",
        "reason": "outside_snapshot_range",
        "asof_exchange_ts_ns": TARGET_VISIBLE_MS * 1_000_000,
        "segment_id": 2,
        "snapshot_min_tick": 639_900,
        "snapshot_max_tick": 640_100,
    }
    with pytest.raises(SharedPrefixExecutionError, match="duplicates"):
        executor._validated_missing_trace(
            [row, dict(row)],
            field="test_trace",
        )
    malformed = dict(row)
    malformed.pop("segment_id")
    with pytest.raises(SharedPrefixExecutionError, match="schema drifted"):
        executor._validated_missing_trace([malformed], field="test_trace")


def test_zero_post_assignment_book_events_can_still_be_exact(
    tmp_path: Path,
) -> None:
    output_root = tmp_path / "labels"
    _dispatch_synthetic_arms(
        _executor(output_root),
        _opportunity("SELL", ordinal=63),
        treatment_events_consumed=0,
    )

    admission = _one_admission(output_root)
    manifest = json.loads((admission / "manifest.json").read_text(encoding="ascii"))
    for row in manifest["arms"]:
        payload = json.loads((admission / row["path"]).read_text(encoding="ascii"))
        strict = payload["strict_execution_contract"]
        assert strict["exchange_book_events_consumed"] == 0
        assert strict["strict_native_label_eligible"] is True


def test_common_prefix_source_gap_still_fails_closed_before_fork(
    tmp_path: Path,
) -> None:
    prefix = {name: 0 for name in STRICT_COUNTER_FIELDS}
    prefix["exchange_book_source_gap_events"] = 1
    executor = _executor(tmp_path / "labels")

    with pytest.raises(
        SharedPrefixExecutionError,
        match="source/clock hard-zero counters failed",
    ):
        executor.dispatch(
            _opportunity(
                "SELL",
                ordinal=62,
                strict_counter_baseline=prefix,
            )
        )
