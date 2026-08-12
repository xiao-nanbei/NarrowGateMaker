from __future__ import annotations

import copy
import gzip
import hashlib
import json
from pathlib import Path

import pytest

from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    BASE_WINDOW_WIDTH_NS,
    CHANNELS_BY_BLOCK,
    CausalMultichannelEmaState,
    CausalWindowObservation,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_prospective_transport_fixture_v1 import (
    DEFAULT_MANIFEST_NAME,
    IDENTITY,
    MANIFEST_SCHEMA_VERSION,
    REQUIRED_ROLES,
    ProspectiveTransportFixtureContract,
    audit_recorded_fixture_directory,
    canonical_sha256,
    main,
    produce_and_audit_prospective_snapshot,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_snapshot import (
    CONTROL_POLICY_ID,
    IDENTITY_HASH_FIELDS,
    PROSPECTIVE_RECEIVE_TIME_PROFILE,
)

BASE_NS = 1_800_000_000_000_000_000
RUNTIME_SHA = "9" * 64


def _identity_hashes() -> dict[str, str]:
    values = ("a", "b", "c", "d", "e", "f", "1")
    return {field: value * 64 for field, value in zip(IDENTITY_HASH_FIELDS, values, strict=True)}


def _m0(*, decision_ns: int) -> dict[str, object]:
    return {
        "assignment_ts_ns": decision_ns,
        "fill_visible_ts_ns": decision_ns,
        "side": "SELL",
        "role_at_fill": "opener",
        "inventory_before_fill_btc": 0.0,
        "inventory_after_fill_btc": -0.001,
        "fill_qty_btc": 0.001,
        "order_qty_btc": 0.001,
        "cumulative_filled_qty_before_btc": 0.0,
        "cumulative_filled_qty_after_btc": 0.001,
        "remaining_order_qty_after_btc": 0.0,
        "partial_fill_ordinal": 1,
        "fill_is_partial": False,
        "order_age_s": 1.25,
        "queue_ahead_before_fill_btc": 0.0,
        "queue_state_before_fill": "known_zero",
        "target_price_tick": 640_000,
        "target_price_displayed_qty_btc": 0.0,
        "target_price_displayed_qty_status": "known_zero",
        "target_price_displayed_qty_known": True,
        "target_price_displayed_qty_is_queue_ahead": False,
        "consecutive_units_after": 1.0,
        "baseline_duration_ms": 85_000.0,
        "campaign_age_s": 0.0,
        "campaign_add_count": 0,
        "campaign_mae_to_date_usdc": 0.0,
        "campaign_inventory_time_to_date_btc_s": 0.0,
        "last_same_side_fill_age_s": None,
        "last_opposite_side_fill_age_s": None,
        "cooldown_remaining_ms": 0.0,
        "cooldown_blocker_active": False,
        "cooldown_lineage_revision_before": 0,
        "cooldown_deadline_owner": "none",
    }


def _feature_and_m0() -> tuple[dict[str, object], dict[str, object], int]:
    state = CausalMultichannelEmaState(
        block="M2",
        warmup_admitted=True,
        warmup_identity="d-minus-1-prospective-fixture",
    )
    for index, level in enumerate((100.0, 102.0, 99.0), start=1):
        right = BASE_NS + index * BASE_WINDOW_WIDTH_NS
        values = {
            channel.name: level + channel_index / 100.0
            for channel_index, channel in enumerate(CHANNELS_BY_BLOCK["M2"])
        }
        state.update(
            CausalWindowObservation(
                left_ts_ns=right - BASE_WINDOW_WIDTH_NS,
                right_ts_ns=right,
                feature_ready_ts_ns=right + 1_000_000,
                market_generation=index,
                depth_generation=index,
                values=values,
                warmup_admitted=True,
            )
        )
    decision_ns = BASE_NS + 3 * BASE_WINDOW_WIDTH_NS + 4_000_000
    m0 = _m0(decision_ns=decision_ns)
    return (
        state.feature_row(
            side="SELL",
            decision_ts_ns=decision_ns,
            m0_context=m0,
        ),
        m0,
        decision_ns,
    )


def _source(
    name: str,
    *,
    generation: int,
    ready_ns: int,
    feature_sha: str,
) -> dict[str, object]:
    atomic_id = "book-atomic-3" if name in {"market", "depth"} else "trade-9"
    atomic_generation = 3 if name in {"market", "depth"} else generation
    cursor = f"{name}-cursor-{generation}"
    return {
        "source_name": name,
        "exchange_ts_ns": ready_ns - 2_000_000,
        "receive_ts_ns": ready_ns - 1_000_000,
        "feature_ready_ts_ns": ready_ns,
        "generation": generation,
        "cursor": cursor,
        "feature_generation": generation,
        "feature_cursor": cursor,
        "atomic_snapshot_id": atomic_id,
        "atomic_generation": atomic_generation,
        "feature_row_sha256": feature_sha,
        "runtime_identity_sha256": RUNTIME_SHA,
        "sequence_gap_count": 0,
        "source_gap": False,
        "recorder_drop_count": 0,
    }


def _bundle() -> dict[str, object]:
    feature_row, m0, decision_ns = _feature_and_m0()
    feature_ready_ns = int(feature_row["feature_ready_ts_ns"])
    feature_sha = canonical_sha256(feature_row)
    m0_sha = canonical_sha256(m0)
    identity_hashes = _identity_hashes()
    sources = {
        "market_row": _source(
            "market", generation=3, ready_ns=feature_ready_ns, feature_sha=feature_sha
        ),
        "depth_row": _source(
            "depth", generation=3, ready_ns=feature_ready_ns, feature_sha=feature_sha
        ),
        "trade_row": _source(
            "trade", generation=9, ready_ns=feature_ready_ns, feature_sha=feature_sha
        ),
    }
    fill_exchange_ns = decision_ns - 2_000_000
    fill_receive_ns = decision_ns - 1_000_000
    private_fill = {
        "snapshot_id": "prospective-snapshot-1",
        "assignment_id": "assignment-1",
        "fill_event_id": "fill-event-1",
        "client_order_id": "client-order-1",
        "lifecycle_id": "lifecycle-1",
        "lineage_id": "sell-lineage-1",
        "lineage_revision": 1,
        "partial_fill_ordinal": 1,
        "partial_fill_qty_btc": 0.001,
        "assignment_ts_ns": decision_ns,
        "fill_exchange_ts_ns": fill_exchange_ns,
        "fill_receive_ts_ns": fill_receive_ns,
        "fill_visible_ts_ns": decision_ns,
        "feature_ready_ts_ns": feature_ready_ns,
        "feature_row_sha256": feature_sha,
        "m0_context_sha256": m0_sha,
        "market_generation": 3,
        "depth_generation": 3,
        "trade_generation": 9,
        "runtime_identity_sha256": RUNTIME_SHA,
        "source_gap": False,
        "recorder_drop_count": 0,
        "writer_drop_count": 0,
    }
    lifecycle = {
        "event_id": "fill-event-1",
        "lifecycle_id": "lifecycle-1",
        "client_order_id": "client-order-1",
        "lifecycle_sequence": 3,
        "lifecycle_event": "full_fill",
        "event_visibility_ts_ns": decision_ns,
        "event_exchange_ts_ns": fill_exchange_ns,
        "event_exchange_clock_valid": True,
        "source_callback_received_ts_ns": fill_receive_ns,
        "source_callback_id": "user-stream-callback-1",
        "source_callback_event_ordinal": 1,
        "source_callback_event_count": 1,
        "remaining_quantity_before": 0.001,
        "remaining_quantity_after": 0.0,
        "runtime_identity_sha256": RUNTIME_SHA,
    }
    contract = ProspectiveTransportFixtureContract(
        expected_runtime_identity_sha256=RUNTIME_SHA,
        expected_identity_hashes=identity_hashes,
        max_visible_age_ns_by_source={
            "market": 10_000_000,
            "depth": 10_000_000,
            "trade": 10_000_000,
        },
        feature_block="M2",
    )
    return {
        "contract": contract,
        **sources,
        "private_fill_row": private_fill,
        "lifecycle_row": lifecycle,
        "identity_hashes": identity_hashes,
        "m0_context": m0,
        "feature_row": feature_row,
    }


def _run(bundle: dict[str, object]):
    return produce_and_audit_prospective_snapshot(**bundle)  # type: ignore[arg-type]


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _rewrite_manifest(root: Path, manifest: dict[str, object]) -> None:
    body = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    manifest["manifest_sha256"] = canonical_sha256(body)
    (root / DEFAULT_MANIFEST_NAME).write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_recorded_fixture_directory(
    root: Path,
    *,
    missing_roles: set[str] | None = None,
    freshness_contract_present: bool = True,
    health_overrides: dict[str, int] | None = None,
) -> tuple[dict[str, object], dict[str, list[dict[str, object]]]]:
    root.mkdir(parents=True, exist_ok=True)
    bundle = _bundle()
    feature_row = bundle["feature_row"]
    m0_context = bundle["m0_context"]
    private_fill = bundle["private_fill_row"]
    assert isinstance(feature_row, dict)
    assert isinstance(m0_context, dict)
    assert isinstance(private_fill, dict)
    role_rows: dict[str, list[dict[str, object]]] = {
        "market_source": [copy.deepcopy(bundle["market_row"])],  # type: ignore[list-item]
        "depth_source": [copy.deepcopy(bundle["depth_row"])],  # type: ignore[list-item]
        "trade_source": [copy.deepcopy(bundle["trade_row"])],  # type: ignore[list-item]
        "private_fill": [copy.deepcopy(private_fill)],
        "lifecycle": [copy.deepcopy(bundle["lifecycle_row"])],  # type: ignore[list-item]
        "feature_companion": [
            {
                "snapshot_id": private_fill["snapshot_id"],
                "feature_row_sha256": canonical_sha256(feature_row),
                "feature_row": copy.deepcopy(feature_row),
                "runtime_identity_sha256": RUNTIME_SHA,
            }
        ],
        "assignment_companion": [
            {
                "snapshot_id": private_fill["snapshot_id"],
                "m0_context_sha256": canonical_sha256(m0_context),
                "m0_context": copy.deepcopy(m0_context),
                "identity_hashes": _identity_hashes(),
                "runtime_identity_sha256": RUNTIME_SHA,
            }
        ],
    }
    omitted = missing_roles or set()
    artifacts: list[dict[str, object]] = []
    parts = root / "parts"
    parts.mkdir()
    for role in REQUIRED_ROLES:
        if role in omitted:
            continue
        path = parts / f"{role}.jsonl.gz"
        with gzip.open(path, "wt", encoding="utf-8") as handle:
            for row in role_rows[role]:
                handle.write(json.dumps(row, sort_keys=True) + "\n")
        artifacts.append(
            {
                "role": role,
                "path": path.relative_to(root).as_posix(),
                "format": "jsonl_gzip",
                "row_count": len(role_rows[role]),
                "sha256": _file_sha256(path),
            }
        )
    freshness_body = {
        "frozen_before_capture": True,
        "max_visible_age_ns_by_source": {
            "market": 10_000_000,
            "depth": 10_000_000,
            "trade": 10_000_000,
        },
    }
    health = {
        "market_tape_drop_count": 0,
        "private_fill_drop_count": 0,
        "lifecycle_writer_drop_count": 0,
        "feature_companion_drop_count": 0,
        "assignment_companion_drop_count": 0,
        "error_count": 0,
    }
    health.update(health_overrides or {})
    manifest: dict[str, object] = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "identity": IDENTITY,
        "capture_id": "synthetic-recorded-capture-1",
        "runtime_identity_sha256": RUNTIME_SHA,
        "identity_hashes": _identity_hashes(),
        "freshness_contract": (
            {
                **freshness_body,
                "contract_sha256": canonical_sha256(freshness_body),
            }
            if freshness_contract_present
            else None
        ),
        "required_roles": list(REQUIRED_ROLES),
        "artifacts": artifacts,
        "health": health,
        "manifest_sha256": "",
    }
    _rewrite_manifest(root, manifest)
    return manifest, role_rows


def _artifact(manifest: dict[str, object], role: str) -> dict[str, object]:
    artifacts = manifest["artifacts"]
    assert isinstance(artifacts, list)
    return next(row for row in artifacts if isinstance(row, dict) and row.get("role") == role)


def test_valid_fixture_produces_prospective_snapshot_without_authority() -> None:
    result = _run(_bundle())

    assert result.snapshot is not None
    assert result.snapshot.policy_input_valid is True
    assert result.snapshot.visibility_profile == PROSPECTIVE_RECEIVE_TIME_PROFILE
    assert result.fallback_policy_id is None
    assert result.fallback_reason is None
    assert result.audit["status"] == "fixture_passed_no_live_authority"
    assert all(result.audit["gates"].values())
    assert result.audit["permissions"] == {
        "fixture_transport_valid": True,
        "real_bounded_capture_authority": False,
        "research_supported": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    report = dict(result.audit)
    observed_sha = report.pop("canonical_report_sha256")
    assert observed_sha == canonical_sha256(report)


@pytest.mark.parametrize(
    ("field", "offset_ns", "failed_gate"),
    (
        ("exchange_ts_ns", 2_000_000, "source_clock_ordering"),
        ("receive_ts_ns", 2_000_000, "source_clock_ordering"),
        ("feature_ready_ts_ns", 10_000_000, "feature_ready_before_fill_cutoff"),
    ),
)
def test_source_clock_ordering_and_cutoff_fail_closed(
    field: str,
    offset_ns: int,
    failed_gate: str,
) -> None:
    bundle = _bundle()
    market = bundle["market_row"]
    assert isinstance(market, dict)
    if field == "exchange_ts_ns":
        market[field] = int(market["receive_ts_ns"]) + offset_ns
    elif field == "receive_ts_ns":
        market[field] = int(market["feature_ready_ts_ns"]) + offset_ns
    else:
        fill = bundle["private_fill_row"]
        assert isinstance(fill, dict)
        market[field] = int(fill["fill_visible_ts_ns"]) + offset_ns

    result = _run(bundle)

    assert result.audit["status"] == "failed_closed"
    assert result.audit["gates"][failed_gate] is False
    assert result.fallback_policy_id == CONTROL_POLICY_ID
    assert result.snapshot is not None
    assert result.snapshot.policy_input_valid is False


def test_fill_clock_inversion_cannot_construct_a_snapshot() -> None:
    bundle = _bundle()
    fill = bundle["private_fill_row"]
    assert isinstance(fill, dict)
    fill["fill_exchange_ts_ns"] = int(fill["fill_receive_ts_ns"]) + 1

    result = _run(bundle)

    assert result.audit["status"] == "failed_closed"
    assert result.audit["gates"]["fill_clock_ordering"] is False
    assert result.snapshot is None
    assert result.fallback_policy_id == CONTROL_POLICY_ID


def test_market_depth_atomic_generation_mismatch_falls_back() -> None:
    bundle = _bundle()
    depth = bundle["depth_row"]
    assert isinstance(depth, dict)
    depth["atomic_generation"] = 4

    result = _run(bundle)

    assert result.audit["gates"]["atomic_market_depth_generation"] is False
    assert result.snapshot is not None
    assert result.snapshot.policy_input_valid is False
    assert "atomic_market_depth_generation" in str(result.fallback_reason)


def test_stale_source_fails_closed_without_inventing_a_freshness_limit() -> None:
    bundle = _bundle()
    identity_hashes = _identity_hashes()
    bundle["contract"] = ProspectiveTransportFixtureContract(
        expected_runtime_identity_sha256=RUNTIME_SHA,
        expected_identity_hashes=identity_hashes,
        max_visible_age_ns_by_source={"market": 1, "depth": 10_000_000, "trade": 10_000_000},
        feature_block="M2",
    )

    result = _run(bundle)

    assert result.audit["gates"]["source_freshness"] is False
    assert result.snapshot is not None
    assert result.snapshot.fallback_policy_id == CONTROL_POLICY_ID


@pytest.mark.parametrize(
    ("target", "field", "value", "gate"),
    (
        ("depth_row", "sequence_gap_count", 1, "zero_sequence_gap"),
        ("trade_row", "source_gap", True, "zero_sequence_gap"),
        ("market_row", "recorder_drop_count", 1, "zero_recorder_writer_drop"),
        ("private_fill_row", "writer_drop_count", 1, "zero_recorder_writer_drop"),
    ),
)
def test_gap_and_drop_inputs_fail_closed(
    target: str,
    field: str,
    value: object,
    gate: str,
) -> None:
    bundle = _bundle()
    row = bundle[target]
    assert isinstance(row, dict)
    row[field] = value

    result = _run(bundle)

    assert result.audit["gates"][gate] is False
    assert result.snapshot is not None
    assert result.snapshot.policy_input_valid is False


def test_identity_hash_mismatch_is_not_exposed_as_policy_input() -> None:
    bundle = _bundle()
    hashes = bundle["identity_hashes"]
    assert isinstance(hashes, dict)
    hashes["config_sha256"] = "2" * 64

    result = _run(bundle)

    assert result.audit["gates"]["identity_hashes_match"] is False
    assert result.snapshot is not None
    assert result.snapshot.policy_input_valid is False
    assert result.snapshot.fallback_policy_id == CONTROL_POLICY_ID


def test_runtime_hash_mismatch_is_not_exposed_as_policy_input() -> None:
    bundle = _bundle()
    trade = bundle["trade_row"]
    assert isinstance(trade, dict)
    trade["runtime_identity_sha256"] = "8" * 64

    result = _run(bundle)

    assert result.audit["gates"]["runtime_identity_match"] is False
    assert result.snapshot is not None
    assert result.snapshot.policy_input_valid is False


def test_feature_hash_mismatch_is_not_exposed_as_policy_input() -> None:
    bundle = _bundle()
    depth = bundle["depth_row"]
    assert isinstance(depth, dict)
    depth["feature_row_sha256"] = "7" * 64

    result = _run(bundle)

    assert result.audit["gates"]["feature_source_binding"] is False
    assert result.snapshot is not None
    assert result.snapshot.policy_input_valid is False


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("event_id", "different-fill-event"),
        ("client_order_id", "different-client-order"),
        ("remaining_quantity_before", 0.002),
    ),
)
def test_lifecycle_join_mismatch_fails_closed(field: str, value: object) -> None:
    bundle = _bundle()
    lifecycle = bundle["lifecycle_row"]
    assert isinstance(lifecycle, dict)
    lifecycle[field] = value

    result = _run(bundle)

    assert result.audit["gates"]["lifecycle_join"] is False
    assert result.snapshot is not None
    assert result.snapshot.policy_input_valid is False


def test_economic_outcome_column_is_rejected_before_snapshot_construction() -> None:
    bundle = _bundle()
    market = bundle["market_row"]
    assert isinstance(market, dict)
    market["terminal_pnl_usdc"] = 1.0

    result = _run(bundle)

    assert result.snapshot is None
    assert result.audit["gates"]["economic_fields_absent"] is False
    assert result.audit["failure_reasons"] == ["economic_field_forbidden:market.terminal_pnl_usdc"]


def test_input_rows_are_not_mutated() -> None:
    bundle = _bundle()
    contract = bundle["contract"]
    assert isinstance(contract, ProspectiveTransportFixtureContract)
    contract_before = contract.to_dict()
    before = {key: copy.deepcopy(value) for key, value in bundle.items() if key != "contract"}

    _run(bundle)

    assert contract.to_dict() == contract_before
    assert {key: value for key, value in bundle.items() if key != "contract"} == before


def test_complete_recorded_gzip_directory_converts_and_audits(tmp_path: Path) -> None:
    _write_recorded_fixture_directory(tmp_path)

    report = audit_recorded_fixture_directory(tmp_path)

    assert report["status"] == "fixture_directory_passed_no_live_authority"
    assert report["missing_roles"] == []
    assert report["fill_join_count"] == 1
    assert all(report["gates"].values())
    assert report["permissions"] == {
        "fixture_directory_valid": True,
        "real_bounded_capture_authority": False,
        "research_supported": False,
        "action_authorized": False,
        "live_authorized": False,
    }


def test_read_only_cli_emits_the_directory_audit(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_recorded_fixture_directory(tmp_path)

    assert main(["--root", str(tmp_path)]) == 0
    report = json.loads(capsys.readouterr().out)

    assert report["status"] == "fixture_directory_passed_no_live_authority"
    assert report["fill_join_count"] == 1


def test_missing_required_role_is_structured_and_cannot_pass(tmp_path: Path) -> None:
    _write_recorded_fixture_directory(
        tmp_path,
        missing_roles={"assignment_companion"},
    )

    report = audit_recorded_fixture_directory(tmp_path)

    assert report["status"] == "failed_closed"
    assert report["missing_roles"] == ["assignment_companion"]
    assert report["gates"]["required_roles_complete"] is False
    assert "required_roles_missing" in report["blockers"]


def test_duplicate_private_fill_join_fails_closed(tmp_path: Path) -> None:
    manifest, role_rows = _write_recorded_fixture_directory(tmp_path)
    artifact = _artifact(manifest, "private_fill")
    path = tmp_path / str(artifact["path"])
    rows = [*role_rows["private_fill"], copy.deepcopy(role_rows["private_fill"][0])]
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    artifact["row_count"] = len(rows)
    artifact["sha256"] = _file_sha256(path)
    _rewrite_manifest(tmp_path, manifest)

    report = audit_recorded_fixture_directory(tmp_path)

    assert report["status"] == "failed_closed"
    assert report["gates"]["fill_joins_unique"] is False
    assert any("duplicate private fill join" in row for row in report["blockers"])


def test_bad_gzip_is_detected_even_when_manifest_hash_matches(tmp_path: Path) -> None:
    manifest, _ = _write_recorded_fixture_directory(tmp_path)
    artifact = _artifact(manifest, "depth_source")
    path = tmp_path / str(artifact["path"])
    path.write_bytes(b"not-a-gzip-stream")
    artifact["sha256"] = _file_sha256(path)
    _rewrite_manifest(tmp_path, manifest)

    report = audit_recorded_fixture_directory(tmp_path)

    assert report["status"] == "failed_closed"
    assert report["gates"]["file_integrity"] is False
    assert "depth_source" in report["missing_roles"]
    assert any("BadGzipFile" in row for row in report["blockers"])


def test_artifact_hash_mismatch_fails_before_row_admission(tmp_path: Path) -> None:
    manifest, _ = _write_recorded_fixture_directory(tmp_path)
    artifact = _artifact(manifest, "trade_source")
    path = tmp_path / str(artifact["path"])
    path.write_bytes(path.read_bytes() + b"drift")

    report = audit_recorded_fixture_directory(tmp_path)

    assert report["status"] == "failed_closed"
    assert report["gates"]["file_integrity"] is False
    assert any("SHA256 mismatch" in row for row in report["blockers"])


def test_artifact_row_count_mismatch_fails_closed(tmp_path: Path) -> None:
    manifest, _ = _write_recorded_fixture_directory(tmp_path)
    artifact = _artifact(manifest, "lifecycle")
    artifact["row_count"] = int(artifact["row_count"]) + 1
    _rewrite_manifest(tmp_path, manifest)

    report = audit_recorded_fixture_directory(tmp_path)

    assert report["status"] == "failed_closed"
    assert report["gates"]["row_counts_match"] is False
    assert any("row count mismatch" in row for row in report["blockers"])


def test_nonzero_drop_health_blocks_an_otherwise_complete_directory(
    tmp_path: Path,
) -> None:
    _write_recorded_fixture_directory(
        tmp_path,
        health_overrides={"feature_companion_drop_count": 1},
    )

    report = audit_recorded_fixture_directory(tmp_path)

    assert report["status"] == "failed_closed"
    assert report["gates"]["zero_drop_health"] is False
    assert "nonzero_drop_or_error_health" in report["blockers"]


def test_missing_frozen_freshness_contract_is_an_explicit_blocker(
    tmp_path: Path,
) -> None:
    _write_recorded_fixture_directory(
        tmp_path,
        freshness_contract_present=False,
    )

    report = audit_recorded_fixture_directory(tmp_path)

    assert report["status"] == "failed_closed"
    assert report["gates"]["freshness_contract_frozen"] is False
    assert "freshness_contract_missing" in report["blockers"]


def test_existing_bounded_seven_file_summary_reports_unadapted_missing_roles(
    tmp_path: Path,
) -> None:
    files = [
        {
            "path": "logs/market_tape/binance.jsonl.gz",
            "event_counts": {"book": 10, "trade": 10},
        },
        *[
            {
                "path": f"logs/external_venues/source-{index}.jsonl.gz",
                "event_counts": {"book": 10},
            }
            for index in range(6)
        ],
    ]
    (tmp_path / "summary.json").write_text(
        json.dumps({"file_count": 7, "files": files}),
        encoding="utf-8",
    )

    report = audit_recorded_fixture_directory(tmp_path)

    assert report["status"] == "failed_closed"
    assert report["missing_roles"] == sorted(REQUIRED_ROLES)
    assert "bounded_seven_file_integrity_summary" in report["available_unadapted_roles"]
    assert "raw_local_book_receive_time" in report["available_unadapted_roles"]
    assert "raw_local_trade_receive_time" in report["available_unadapted_roles"]
    assert "raw_local_depth_receive_time" not in report["available_unadapted_roles"]
