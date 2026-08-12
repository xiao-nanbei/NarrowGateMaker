from __future__ import annotations

import copy

import pytest

from models.replay.baseline_epoch_manifest import (
    REQUIRED_IDENTITY_FIELDS,
    SCHEMA_VERSION,
    build_manifest_from_baseline_identities,
    canonical_sha256,
    epoch_identity_sha256,
    extract_epoch_identity,
    finalize_manifest,
    utc_timestamp_ns,
    validate_baseline_epoch_manifest,
)


def _identity(seed: str = "a") -> dict[str, str]:
    return {name: canonical_sha256([seed, name]) for name in REQUIRED_IDENTITY_FIELDS}


def _epoch(start: int, end: int, *, seed: str = "a") -> dict[str, object]:
    identity = _identity(seed)
    return {
        "epoch_id": f"epoch-{seed}",
        "start_ts_ns": start,
        "end_ts_ns": end,
        "start_reason": "scope_start" if start == 0 else "config_deployment",
        "boundary_status": "first_decision_bound",
        "identity": identity,
        "identity_sha256": epoch_identity_sha256(identity),
        "binding_status": "fully_bound",
        "initial_economic_state_complete": True,
        "lifecycle_estimation_authorized": True,
        "continuous_economic_estimation_authorized": True,
        "pooling_authorized": False,
    }


def _manifest() -> dict[str, object]:
    return finalize_manifest(
        {
            "schema_version": SCHEMA_VERSION,
            "manifest_id": "test",
            "source_clock": "utc_ns",
            "scope_start_ts_ns": 0,
            "scope_end_ts_ns": 20,
            "utc_midnight_splits_epoch": False,
            "pooled_estimation_authorized": False,
            "required_identity_fields": list(REQUIRED_IDENTITY_FIELDS),
            "epochs": [_epoch(0, 10), _epoch(10, 20, seed="b")],
            "unbound_intervals": [],
        }
    )


def _rehash(manifest: dict[str, object]) -> None:
    manifest.pop("canonical_manifest_sha256", None)
    manifest["canonical_manifest_sha256"] = canonical_sha256(manifest)


def test_manifest_accepts_identity_change_without_utc_midnight_split() -> None:
    manifest = _manifest()
    validate_baseline_epoch_manifest(manifest)
    assert manifest["utc_midnight_splits_epoch"] is False


def test_manifest_rejects_partial_epoch_authority() -> None:
    manifest = _manifest()
    epoch = manifest["epochs"][0]
    epoch["identity"]["feature_dag_sha256"] = None
    epoch["identity_sha256"] = epoch_identity_sha256(epoch["identity"])
    epoch["binding_status"] = "partially_bound"
    _rehash(manifest)
    with pytest.raises(ValueError, match="cannot authorize lifecycle"):
        validate_baseline_epoch_manifest(manifest)


def test_manifest_requires_explicit_unbound_gap() -> None:
    manifest = _manifest()
    manifest["epochs"][1]["start_ts_ns"] = 12
    _rehash(manifest)
    with pytest.raises(ValueError, match="coverage gap"):
        validate_baseline_epoch_manifest(manifest)


def test_manifest_accepts_explicit_unbound_gap() -> None:
    payload = _manifest()
    payload["epochs"][1]["start_ts_ns"] = 12
    payload["unbound_intervals"] = [
        {"start_ts_ns": 10, "end_ts_ns": 12, "reason": "restart evidence absent"}
    ]
    _rehash(payload)
    validate_baseline_epoch_manifest(payload)


def test_manifest_rejects_overlap() -> None:
    manifest = _manifest()
    manifest["epochs"][1]["start_ts_ns"] = 9
    _rehash(manifest)
    with pytest.raises(ValueError, match="overlapping epochs"):
        validate_baseline_epoch_manifest(manifest)


def test_manifest_rejects_economic_fields() -> None:
    manifest = copy.deepcopy(_manifest())
    manifest["pnl_usdc"] = 1.0
    _rehash(manifest)
    with pytest.raises(ValueError, match="economic keys"):
        validate_baseline_epoch_manifest(manifest)


def test_manifest_rejects_hash_mutation() -> None:
    manifest = _manifest()
    manifest["epochs"][0]["identity"]["config_sha256"] = "0" * 64
    _rehash(manifest)
    with pytest.raises(ValueError, match="identity_sha256 mismatch"):
        validate_baseline_epoch_manifest(manifest)


def test_manifest_never_authorizes_epoch_pooling() -> None:
    manifest = _manifest()
    manifest["epochs"][0]["pooling_authorized"] = True
    _rehash(manifest)
    with pytest.raises(ValueError, match="forbids pre-estimation pooling"):
        validate_baseline_epoch_manifest(manifest)


def test_operational_identity_extraction_keeps_missing_contracts_null() -> None:
    payload = {
        "schema_version": "baseline.v1",
        "baseline_id": "b1",
        "effective_at_utc": "2026-08-01T00:00:00Z",
        "runtime_code": {"live/main.py": "0" * 64},
        "config": {"sha256": "1" * 64, "ml_enabled": True},
        "model": {"bundle_meta_sha256": "2" * 64},
        "p3": {"sha256": "3" * 64},
    }
    identity, evidence = extract_epoch_identity(payload)
    assert identity["runtime_code_sha256"] == canonical_sha256(payload["runtime_code"])
    assert identity["feature_dag_sha256"] is None
    assert identity["initial_runtime_state_sha256"] is None
    assert "feature_dag_sha256" in evidence["missing_fields"]


def test_identity_chain_builder_is_fail_closed_without_restart_audit(tmp_path) -> None:
    first = {
        "schema_version": "baseline.v1",
        "baseline_id": "b1",
        "effective_at_utc": "2026-08-01T00:00:00Z",
        "runtime_code": {"live/main.py": "0" * 64},
        "config": {"sha256": "1" * 64, "action_enabled": False},
        "model": {"bundle_meta_sha256": "2" * 64, "feature_dag_sha256": "3" * 64},
        "p3": {"sha256": "4" * 64},
        "runtime_profile": {"profile": "native"},
        "data_identity": {"source": "native"},
    }
    second = copy.deepcopy(first)
    second["baseline_id"] = "b2"
    second["effective_at_utc"] = "2026-08-02T00:00:00Z"
    second["config"]["sha256"] = "5" * 64
    paths = []
    for index, payload in enumerate((first, second), start=1):
        path = tmp_path / f"identity-{index}.json"
        path.write_text(__import__("json").dumps(payload), encoding="utf-8")
        paths.append(path)
    overrides = {
        baseline_id: {
            "initial_runtime_state_sha256": canonical_sha256([baseline_id, "state"]),
            "clock_semantics_sha256": canonical_sha256([baseline_id, "clock"]),
        }
        for baseline_id in ("b1", "b2")
    }
    manifest = build_manifest_from_baseline_identities(
        paths,
        manifest_id="chain",
        scope_start_ts_ns=utc_timestamp_ns("2026-07-31T00:00:00Z"),
        scope_end_ts_ns=utc_timestamp_ns("2026-08-03T00:00:00Z"),
        overrides_by_baseline_id=overrides,
        restart_audit_complete=False,
    )
    assert len(manifest["epochs"]) == 2
    assert manifest["epochs"][1]["start_reason"] == "config_deployment"
    assert all(
        epoch["lifecycle_estimation_authorized"] is False
        for epoch in manifest["epochs"]
    )


def test_identity_chain_builder_splits_observed_unrestored_restart(tmp_path) -> None:
    payload = {
        "schema_version": "baseline.v1",
        "baseline_id": "b1",
        "effective_at_utc": "2026-08-01T00:00:00Z",
        "runtime_code": {"live/main.py": "0" * 64},
        "config": {"sha256": "1" * 64, "action_enabled": False},
        "model": {"bundle_meta_sha256": "2" * 64, "feature_dag_sha256": "3" * 64},
        "p3": {"sha256": "4" * 64},
        "runtime_profile": {"profile": "native"},
        "data_identity": {"source": "native"},
    }
    path = tmp_path / "identity.json"
    path.write_text(__import__("json").dumps(payload), encoding="utf-8")
    overrides = {
        "b1": {
            "initial_runtime_state_sha256": canonical_sha256(["b1", "state"]),
            "clock_semantics_sha256": canonical_sha256(["b1", "clock"]),
        }
    }
    restart_ts = utc_timestamp_ns("2026-08-01T12:00:00Z")
    manifest = build_manifest_from_baseline_identities(
        [path],
        manifest_id="restart-chain",
        scope_start_ts_ns=utc_timestamp_ns("2026-08-01T00:00:00Z"),
        scope_end_ts_ns=utc_timestamp_ns("2026-08-02T00:00:00Z"),
        overrides_by_baseline_id=overrides,
        boundary_events=[
            {
                "start_ts_ns": restart_ts,
                "boundary_reason": "unrestored_process_restart",
                "initial_runtime_state_sha256": None,
                "identity_updates": {},
                "source_evidence": "maker.log startup",
            }
        ],
        restart_audit_complete=False,
    )
    assert [row["start_ts_ns"] for row in manifest["epochs"]] == [
        utc_timestamp_ns("2026-08-01T00:00:00Z"),
        restart_ts,
    ]
    assert manifest["epochs"][1]["start_reason"] == "unrestored_process_restart"
    assert manifest["epochs"][1]["identity"]["initial_runtime_state_sha256"] is None
