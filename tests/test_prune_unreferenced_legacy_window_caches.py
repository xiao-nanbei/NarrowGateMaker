from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest

from models.replay_cache_audit import audit_json, audit_legacy_window_caches
from scripts.prune_unreferenced_legacy_window_caches import (
    CANDIDATE_STATUS,
    DRY_RUN_SCHEMA,
    PruneValidationError,
    build_dry_run_receipt,
    execute_prune,
    main,
)


def _legacy(cache_root: Path, day: str, version: int, digest: str, content: bytes) -> Path:
    path = cache_root / f"btcusdc_{day}_tick_window_v{version}_{digest}.pkl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def _freeze_audit(
    tmp_path: Path,
    cache_root: Path,
    *,
    reference_roots: tuple[Path, ...] = (),
    mutate: object | None = None,
) -> tuple[Path, str, dict[str, object]]:
    payload = audit_legacy_window_caches(cache_root, reference_roots=reference_roots)
    if mutate is not None:
        mutate(payload)
    raw = audit_json(payload).encode()
    audit_path = tmp_path / "frozen_audit.json"
    audit_path.write_bytes(raw)
    return audit_path, hashlib.sha256(raw).hexdigest(), payload


def _reference_root(tmp_path: Path, text: str = '{"identity":"clean"}\n') -> Path:
    root = tmp_path / "frozen_references"
    root.mkdir()
    (root / "manifest.json").write_text(text)
    return root


def _plan(
    audit_path: Path,
    audit_sha: str,
    cache_root: Path,
    reference_root: Path,
    *,
    fresh_roots: list[Path] | None = None,
    include_versions: set[int] | None = None,
) -> dict[str, object]:
    return build_dry_run_receipt(
        audit_path=audit_path,
        audit_sha256=audit_sha,
        cache_root=cache_root,
        frozen_reference_roots=[reference_root],
        fresh_reference_roots=fresh_roots,
        include_versions=include_versions,
    )


def test_dry_run_hashes_candidates_and_does_not_delete(tmp_path: Path) -> None:
    cache_root = tmp_path / "window_cache"
    candidate = _legacy(cache_root, "2026-01-01", 12, "a" * 16, b"candidate")
    audit_path, audit_sha, _ = _freeze_audit(tmp_path, cache_root)
    references = _reference_root(tmp_path)

    plan = _plan(audit_path, audit_sha, cache_root, references)

    assert plan["schema_version"] == DRY_RUN_SCHEMA
    assert plan["execution_eligible"] is True
    assert candidate.exists()
    computed = plan["computed_candidates"][0]
    assert computed["payload_sha256"] == hashlib.sha256(b"candidate").hexdigest()
    assert plan["frozen_reference_scan"]["roots"][0]["scanned_file_count"] == 1
    assert plan["authorization"]["owner_approval_token"].startswith("OWNER-APPROVE-")


def test_version_filter_is_bound_and_preserves_newer_candidate(tmp_path: Path) -> None:
    cache_root = tmp_path / "window_cache"
    old = _legacy(cache_root, "2026-01-01", 12, "a" * 16, b"old")
    current = _legacy(cache_root, "2026-01-02", 13, "b" * 16, b"current")
    audit_path, audit_sha, _ = _freeze_audit(tmp_path, cache_root)
    references = _reference_root(tmp_path)

    plan = _plan(
        audit_path,
        audit_sha,
        cache_root,
        references,
        include_versions={10, 11, 12},
    )

    assert plan["candidate_version_filter"] == [10, 11, 12]
    assert [row["basename"] for row in plan["eligible_candidates"]] == [old.name]
    receipt = execute_prune(
        plan,
        owner_approval_token=plan["authorization"]["owner_approval_token"],
        receipt_path=tmp_path / "receipt.json",
    )
    assert receipt["deleted_count"] == 1
    assert not old.exists()
    assert current.exists()


@pytest.mark.parametrize(
    ("reference_kind", "reference_value", "expected_match_type"),
    [
        ("basename", "{basename}", "exact_basename"),
        ("prefix", "{prefix}", "exact_cache_key_prefix"),
        ("payload", "{payload_sha256}", "exact_payload_sha256"),
    ],
)
def test_identity_only_reference_blocks_the_entire_plan(
    tmp_path: Path,
    reference_kind: str,
    reference_value: str,
    expected_match_type: str,
) -> None:
    del reference_kind
    cache_root = tmp_path / "window_cache"
    candidate = _legacy(cache_root, "2026-01-01", 12, "b" * 16, b"hash-only")
    audit_path, audit_sha, payload = _freeze_audit(tmp_path, cache_root)
    record = payload["files"][0]
    values = {
        "basename": record["basename"],
        "prefix": record["cache_key_prefix"],
        "payload_sha256": hashlib.sha256(candidate.read_bytes()).hexdigest(),
    }
    references = _reference_root(tmp_path, reference_value.format(**values))

    plan = _plan(audit_path, audit_sha, cache_root, references)

    assert plan["execution_eligible"] is False
    assert plan["requires_reaudit"] is True
    assert plan["eligible_candidates"] == []
    assert plan["authorization"]["owner_approval_token"] is None
    assert plan["frozen_reference_scan"]["hits"][0]["match_type"] == expected_match_type
    assert candidate.exists()


def test_execute_requires_exact_token_and_writes_atomic_receipt(tmp_path: Path) -> None:
    cache_root = tmp_path / "window_cache"
    candidate = _legacy(cache_root, "2026-01-01", 12, "c" * 16, b"delete-in-test")
    audit_path, audit_sha, _ = _freeze_audit(tmp_path, cache_root)
    references = _reference_root(tmp_path)
    plan = _plan(audit_path, audit_sha, cache_root, references)
    receipt_path = tmp_path / "receipt.json"

    with pytest.raises(PruneValidationError, match="approval token"):
        execute_prune(plan, owner_approval_token="wrong", receipt_path=receipt_path)
    assert candidate.exists()

    receipt = execute_prune(
        plan,
        owner_approval_token=plan["authorization"]["owner_approval_token"],
        receipt_path=receipt_path,
    )

    assert not candidate.exists()
    assert receipt_path.exists()
    assert receipt["status"] == "complete"
    assert receipt["deleted_count"] == 1
    assert json.loads(receipt_path.read_text())["receipt_sha256"] == receipt["receipt_sha256"]
    assert list(tmp_path.glob(f".{receipt_path.name}.*.tmp")) == []


def test_execute_rehashes_and_rejects_post_plan_payload_drift(tmp_path: Path) -> None:
    cache_root = tmp_path / "window_cache"
    candidate = _legacy(cache_root, "2026-01-01", 12, "d" * 16, b"first")
    audit_path, audit_sha, _ = _freeze_audit(tmp_path, cache_root)
    references = _reference_root(tmp_path)
    plan = _plan(audit_path, audit_sha, cache_root, references)
    original = candidate.stat()
    candidate.write_bytes(b"other")
    os.utime(candidate, ns=(original.st_atime_ns, original.st_mtime_ns))

    with pytest.raises(PruneValidationError, match="payload SHA drift"):
        execute_prune(
            plan,
            owner_approval_token=plan["authorization"]["owner_approval_token"],
            receipt_path=tmp_path / "receipt.json",
        )
    assert candidate.exists()


def test_partial_unlink_failure_leaves_atomic_failure_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_root = tmp_path / "window_cache"
    first = _legacy(cache_root, "2026-01-01", 12, "8" * 16, b"first")
    second = _legacy(cache_root, "2026-01-02", 12, "9" * 16, b"second")
    audit_path, audit_sha, _ = _freeze_audit(tmp_path, cache_root)
    references = _reference_root(tmp_path)
    plan = _plan(audit_path, audit_sha, cache_root, references)
    receipt_path = tmp_path / "receipt.json"
    original_unlink = os.unlink
    calls = 0

    def fail_second_unlink(path: str | bytes | int, *, dir_fd: int | None = None) -> None:
        nonlocal calls
        if dir_fd is not None:
            calls += 1
            if calls == 2:
                raise OSError("synthetic unlink failure")
        original_unlink(path, dir_fd=dir_fd)

    monkeypatch.setattr(os, "unlink", fail_second_unlink)
    with pytest.raises(OSError, match="synthetic unlink failure"):
        execute_prune(
            plan,
            owner_approval_token=plan["authorization"]["owner_approval_token"],
            receipt_path=receipt_path,
        )

    receipt = json.loads(receipt_path.read_text())
    assert receipt["status"] == "failed_partial"
    assert receipt["deleted_count"] == 1
    assert not first.exists()
    assert second.exists()


@pytest.mark.parametrize("drift", ["size", "mtime"])
def test_audit_metadata_drift_fails_before_plan(tmp_path: Path, drift: str) -> None:
    cache_root = tmp_path / "window_cache"
    candidate = _legacy(cache_root, "2026-01-01", 12, "e" * 16, b"stable")
    audit_path, audit_sha, _ = _freeze_audit(tmp_path, cache_root)
    references = _reference_root(tmp_path)
    if drift == "size":
        candidate.write_bytes(b"different-size")
    else:
        current = candidate.stat()
        os.utime(candidate, ns=(current.st_atime_ns, current.st_mtime_ns + 1_000_000))

    with pytest.raises(PruneValidationError, match=f"candidate {drift} drift"):
        _plan(audit_path, audit_sha, cache_root, references)
    assert candidate.exists()


def test_audit_sha_is_mandatory_and_exact(tmp_path: Path) -> None:
    cache_root = tmp_path / "window_cache"
    candidate = _legacy(cache_root, "2026-01-01", 12, "f" * 16, b"stable")
    audit_path, _, _ = _freeze_audit(tmp_path, cache_root)
    references = _reference_root(tmp_path)

    with pytest.raises(PruneValidationError, match="audit SHA256 mismatch"):
        _plan(audit_path, "0" * 64, cache_root, references)
    assert candidate.exists()


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("schema_version", "unknown", "unsupported audit schema"),
        ("mode", "mutable", "unsafe audit mode"),
        ("cache_root", "/tmp/not-the-root", "cache_root"),
    ],
)
def test_schema_mode_and_root_are_frozen(
    tmp_path: Path,
    field: str,
    value: str,
    message: str,
) -> None:
    cache_root = tmp_path / "window_cache"
    candidate = _legacy(cache_root, "2026-01-01", 12, "1" * 16, b"stable")

    def mutate(payload: dict[str, object]) -> None:
        payload[field] = value

    audit_path, audit_sha, _ = _freeze_audit(tmp_path, cache_root, mutate=mutate)
    references = _reference_root(tmp_path)
    with pytest.raises((FileNotFoundError, PruneValidationError), match=message):
        _plan(audit_path, audit_sha, cache_root, references)
    assert candidate.exists()


def test_unknown_governance_status_and_path_escape_fail_closed(tmp_path: Path) -> None:
    cache_root = tmp_path / "window_cache"
    candidate = _legacy(cache_root, "2026-01-01", 12, "2" * 16, b"stable")
    references = _reference_root(tmp_path)

    def unknown_status(payload: dict[str, object]) -> None:
        payload["files"][0]["governance_status"] = "looks_unreferenced"

    audit_path, audit_sha, _ = _freeze_audit(tmp_path, cache_root, mutate=unknown_status)
    with pytest.raises(PruneValidationError, match="unknown governance_status"):
        _plan(audit_path, audit_sha, cache_root, references)

    def escape(payload: dict[str, object]) -> None:
        payload["files"][0]["path"] = str(tmp_path / payload["files"][0]["basename"])

    audit_path, audit_sha, _ = _freeze_audit(tmp_path, cache_root, mutate=escape)
    with pytest.raises(PruneValidationError, match="escapes"):
        _plan(audit_path, audit_sha, cache_root, references)
    assert candidate.exists()


def test_fresh_audit_candidate_mismatch_blocks_execution(tmp_path: Path) -> None:
    cache_root = tmp_path / "window_cache"
    first = _legacy(cache_root, "2026-01-01", 12, "3" * 16, b"first")
    audit_path, audit_sha, _ = _freeze_audit(tmp_path, cache_root)
    second = _legacy(cache_root, "2026-01-02", 12, "4" * 16, b"second")
    references = _reference_root(tmp_path)

    plan = _plan(
        audit_path,
        audit_sha,
        cache_root,
        references,
        fresh_roots=[references],
    )

    assert plan["fresh_reference_audit"]["candidate_set_matches"] is False
    assert plan["fresh_reference_audit"]["added_candidates"] == [second.name]
    assert plan["execution_eligible"] is False
    assert plan["authorization"]["owner_approval_token"] is None
    assert first.exists() and second.exists()


def test_fresh_basename_reference_removes_candidate_and_reports_hit(tmp_path: Path) -> None:
    cache_root = tmp_path / "window_cache"
    candidate = _legacy(cache_root, "2026-01-01", 12, "5" * 16, b"candidate")
    audit_path, audit_sha, _ = _freeze_audit(tmp_path, cache_root)
    references = _reference_root(tmp_path, candidate.name)

    plan = _plan(
        audit_path,
        audit_sha,
        cache_root,
        references,
        fresh_roots=[references],
    )

    # The legacy fresh audit only promotes basename references under its own
    # frozen/evidence path classes. The stronger identity scan blocks this root
    # independently, even when the legacy candidate set remains unchanged.
    assert plan["fresh_reference_audit"]["candidate_set_matches"] is True
    assert plan["frozen_reference_scan"]["hits"][0]["match_type"] == "exact_basename"
    assert plan["execution_eligible"] is False
    assert candidate.exists()


def test_cli_defaults_to_dry_run_and_never_unlinks(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    cache_root = tmp_path / "window_cache"
    candidate = _legacy(cache_root, "2026-01-01", 12, "6" * 16, b"candidate")
    audit_path, audit_sha, _ = _freeze_audit(tmp_path, cache_root)
    references = _reference_root(tmp_path)

    result = main(
        [
            "--audit-json",
            str(audit_path),
            "--audit-sha256",
            audit_sha,
            "--cache-root",
            str(cache_root),
            "--frozen-reference-root",
            str(references),
        ]
    )

    output = json.loads(capsys.readouterr().out)
    assert result == 0
    assert output["mode"] == "dry_run_no_delete"
    assert candidate.exists()


def test_execute_rejects_receipt_inside_frozen_roots(tmp_path: Path) -> None:
    cache_root = tmp_path / "window_cache"
    candidate = _legacy(cache_root, "2026-01-01", 12, "7" * 16, b"candidate")
    audit_path, audit_sha, _ = _freeze_audit(tmp_path, cache_root)
    references = _reference_root(tmp_path)
    plan = _plan(audit_path, audit_sha, cache_root, references)

    with pytest.raises(PruneValidationError, match="outside cache and frozen"):
        execute_prune(
            plan,
            owner_approval_token=plan["authorization"]["owner_approval_token"],
            receipt_path=references / "receipt.json",
        )
    assert candidate.exists()


def test_candidate_status_is_exact_constant() -> None:
    assert CANDIDATE_STATUS == "unreferenced_candidate_requires_user_approval"
