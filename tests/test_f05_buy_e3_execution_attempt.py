from __future__ import annotations

import json
import os
import stat
import subprocess
from pathlib import Path

import pytest

from scripts import f05_buy_e3_execution_attempt as subject
from scripts import f05_buy_e3_stability_receipts as stability


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ("git", *args),
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _write(path: Path, text: str) -> None:
    path.write_text(text, encoding="ascii")


def _commit(root: Path, message: str) -> str:
    _git(root, "add", ".")
    _git(root, "commit", "-m", message)
    return _git(root, "rev-parse", "HEAD")


def _private_receipt(
    path: Path,
    role: str,
    *,
    status: str | None = None,
) -> Path:
    expected = subject._DIRECT_RECEIPT_METADATA_BY_ROLE.get(role)  # noqa: SLF001
    if expected is None:
        schema, identity, expected_status = (
            f"narrowgate.test.{role}.v1",
            f"narrowgate.test.{role}",
            "passed",
        )
    else:
        schema, identity, expected_status = expected
    payload = {
        "schema_version": schema,
        "identity": identity,
        "status": expected_status if status is None else status,
    }
    payload["canonical_receipt_sha256"] = subject._document_sha256(  # noqa: SLF001
        payload, "canonical_receipt_sha256"
    )
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="ascii")
    path.chmod(0o600)
    return path


def _owner_wrapper(
    path: Path,
    role: str,
    *,
    status: str = "passed",
) -> Path:
    source = _private_receipt(
        path.with_name(f"{path.stem}.source.json"),
        f"{role}_source",
    )
    source_binding, _source_ids = subject._receipt_binding(  # noqa: SLF001
        source,
        f"{role}_source",
        require_owner_wrapper=False,
    )
    payload = {
        "schema_version": subject.PRE_ADMISSION_RECEIPT_WRAPPER_SCHEMA,
        "identity": subject.OWNER_IDENTITY,
        "role": role,
        "status": status,
        "source_receipt": source_binding,
        "evidence_boundary": dict(subject.PRE_ADMISSION_EVIDENCE_BOUNDARY),
        "permissions": dict(subject.PRE_ADMISSION_PERMISSIONS),
    }
    payload["canonical_receipt_sha256"] = subject._document_sha256(  # noqa: SLF001
        payload, "canonical_receipt_sha256"
    )
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="ascii")
    path.chmod(0o600)
    return path


def _final_composition_fixture(
    evidence_root: Path,
    attempt_manifest: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    schema: str | None = None,
    identity: str | None = None,
    status: str | None = None,
    filename: str = "final_composition.json",
    validator_calls: list[tuple[Path, Path]] | None = None,
) -> Path:
    attempt = json.loads(attempt_manifest.read_text(encoding="ascii"))
    receipt = evidence_root / "receipts" / filename
    receipt.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": schema or subject.FINAL_COMPOSITION_SCHEMA,
        "identity": identity or subject.FINAL_COMPOSITION_IDENTITY,
        "status": status or subject.FINAL_COMPOSITION_STATUS,
        "exact_artifact": {"artifact_sha256": attempt["artifact"]["artifact_sha256"]},
        "compatible_execution_attempt": subject._compatible_execution_attempt_identity(  # noqa: SLF001
            attempt
        ),
        "evidence_boundary": {
            "validation_read": False,
            "sealed_holdout_read": False,
            "economic_values_exposed": False,
        },
        "permissions": {
            "research_authorized": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    payload[subject.FINAL_COMPOSITION_CANONICAL_FIELD] = subject._document_sha256(  # noqa: SLF001
        payload,
        subject.FINAL_COMPOSITION_CANONICAL_FIELD,
    )
    receipt.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="ascii")
    receipt.chmod(0o600)

    def validate_fixture(*, evidence_root: Path, receipt_path: Path) -> dict:
        if validator_calls is not None:
            validator_calls.append((evidence_root, receipt_path))
        assert evidence_root == receipt.parents[1]
        assert receipt_path == receipt
        observed = json.loads(receipt_path.read_text(encoding="ascii"))
        if (
            observed.get("schema_version") != subject.FINAL_COMPOSITION_SCHEMA
            or observed.get("identity") != subject.FINAL_COMPOSITION_IDENTITY
            or observed.get("status") != subject.FINAL_COMPOSITION_STATUS
            or observed.get(subject.FINAL_COMPOSITION_CANONICAL_FIELD)
            != subject._document_sha256(  # noqa: SLF001
                observed,
                subject.FINAL_COMPOSITION_CANONICAL_FIELD,
            )
        ):
            raise subject.final_composition.FinalCompositionError(
                "fixture final composition identity drifted"
            )
        return observed

    monkeypatch.setattr(
        subject.final_composition,
        "validate_final_composition",
        validate_fixture,
    )
    return receipt


def _rewrite_receipt(path: Path, payload: dict) -> None:
    payload["canonical_receipt_sha256"] = subject._document_sha256(  # noqa: SLF001
        payload, "canonical_receipt_sha256"
    )
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="ascii")
    path.chmod(0o600)


def _forge_substantive_source(wrapper_path: Path, role: str) -> None:
    wrapper = json.loads(wrapper_path.read_text(encoding="ascii"))
    source = Path(wrapper["source_receipt"]["path"])
    source_payload = json.loads(source.read_text(encoding="ascii"))
    source_payload["forged_substantive_receipt"] = True
    _rewrite_receipt(source, source_payload)
    source_binding, _source_ids = subject._receipt_binding(  # noqa: SLF001
        source,
        f"{role}_source",
        require_owner_wrapper=False,
    )
    wrapper["source_receipt"] = source_binding
    _rewrite_receipt(wrapper_path, wrapper)


def _rewrite_manifest_canonical(path: Path, payload: dict) -> None:
    payload["canonical_execution_attempt_sha256"] = subject._document_sha256(  # noqa: SLF001
        payload,
        "canonical_execution_attempt_sha256",
    )
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="ascii")
    path.chmod(0o600)


def _stability_receipts(root: Path) -> dict[str, Path]:
    return {
        role: _owner_wrapper(root / f"{role}.json", role)
        for role in subject.PRE_ADMISSION_RECEIPT_ROLES
    }


def _stability_context_arguments(tagged_repo: dict[str, object]) -> dict[str, Path]:
    contract = tagged_repo["layer4_contract"]
    day_receipts = tagged_repo["layer4_day_receipts"]
    assert isinstance(contract, Path)
    assert isinstance(day_receipts, Path)
    return {
        "layer4_contract_path": contract,
        "layer4_day_receipt_dir": day_receipts,
    }


@pytest.fixture
def tagged_repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    root = tmp_path / "repo"
    root.mkdir()
    _git(root, "init")
    _git(root, "config", "user.name", "NarrowGate Test")
    _git(root, "config", "user.email", "narrowgate@example.invalid")
    source = root / "attempt.py"
    _write(source, "PRODUCER = True\n")
    producer_commit = _commit(root, "producer")
    producer_tree = _git(root, "rev-parse", f"{producer_commit}^{{tree}}")
    _git(root, "tag", "-a", "producer", "-m", "producer")
    producer_tag_object = _git(root, "rev-parse", "refs/tags/producer")

    _write(source, "PRODUCER = True\nRUNTIME_REPAIRED = True\n")
    runtime_commit = _commit(root, "runtime repair")
    _git(root, "tag", "-a", "attempt3", "-m", "attempt3")

    monkeypatch.setattr(subject, "PRODUCER_COMMIT", producer_commit)
    monkeypatch.setattr(subject, "PRODUCER_TREE", producer_tree)
    monkeypatch.setattr(subject, "PRODUCER_TAG", "producer")
    monkeypatch.setattr(subject, "PRODUCER_TAG_OBJECT", producer_tag_object)
    monkeypatch.setattr(subject, "RUNTIME_SOURCE_PATHS", {"attempt_tool": "attempt.py"})

    receipts = _stability_receipts(tmp_path)
    layer4_contract = tmp_path / "layer4_contract.json"
    layer4_contract.write_text('{"contract":true}\n', encoding="ascii")
    layer4_contract.chmod(0o600)
    layer4_day_receipts = tmp_path / "layer4_days"
    layer4_day_receipts.mkdir()
    stability_calls: list[tuple[dict[str, Path], stability.StabilityContext]] = []

    def validate_stability_wrappers(
        *,
        wrappers: dict[str, Path],
        context: stability.StabilityContext,
    ) -> dict[str, dict]:
        normalized = {str(role): Path(path) for role, path in wrappers.items()}
        stability_calls.append((normalized, context))
        if set(normalized) != set(subject.PRE_ADMISSION_RECEIPT_ROLES):
            raise stability.StabilityReceiptError("fixture role set drifted")
        validated: dict[str, dict] = {}
        for role in subject.PRE_ADMISSION_RECEIPT_ROLES:
            wrapper = json.loads(normalized[role].read_text(encoding="ascii"))
            source = Path(wrapper["source_receipt"]["path"])
            source_payload = json.loads(source.read_text(encoding="ascii"))
            if source_payload.get("forged_substantive_receipt") is True:
                raise stability.StabilityReceiptError(
                    f"forged substantive receipt rejected: {role}"
                )
            validated[role] = wrapper
        return validated

    monkeypatch.setattr(
        stability,
        "validate_stability_wrappers",
        validate_stability_wrappers,
    )

    artifact = {
        "artifact_sha256": subject.ARTIFACT_SHA256,
        "files": {
            role: {
                "path": str(tmp_path / f"{role}.json"),
                "file_sha256": digest,
                "size_bytes": 1,
                "device": 1,
                "inode": index + 1,
            }
            for index, (role, digest) in enumerate(subject.ARTIFACT_FILE_SHA256.items())
        },
        "formal_manifest": {
            "path": str(tmp_path / "formal.json"),
            "file_sha256": "f" * 64,
            "size_bytes": 1,
            "device": 1,
            "inode": 99,
            "canonical_sha256": subject.FORMAL_MANIFEST_CANONICAL_SHA256,
        },
    }
    monkeypatch.setattr(subject, "_artifact_binding", lambda **_: artifact)
    return {
        "root": root,
        "runtime_commit": runtime_commit,
        "artifact": artifact,
        "placeholder": tmp_path / "unused.json",
        "receipts": receipts,
        "layer4_contract": layer4_contract,
        "layer4_day_receipts": layer4_day_receipts,
        "stability_calls": stability_calls,
    }


def test_git_ancestry_distinguishes_descendant_and_sibling(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    _git(root, "init")
    _git(root, "config", "user.name", "NarrowGate Test")
    _git(root, "config", "user.email", "narrowgate@example.invalid")
    _write(root / "source", "base\n")
    base = _commit(root, "base")
    _git(root, "switch", "-c", "descendant")
    _write(root / "source", "descendant\n")
    descendant = _commit(root, "descendant")
    _git(root, "switch", "--detach", base)
    _git(root, "switch", "-c", "sibling")
    _write(root / "source", "sibling\n")
    sibling = _commit(root, "sibling")

    assert subject._git_is_ancestor(root, base, descendant)
    assert not subject._git_is_ancestor(root, descendant, sibling)


def test_private_json_requires_mode_owner_single_link_and_no_symlink(tmp_path: Path) -> None:
    source = tmp_path / "private.json"
    _write(source, '{"ok": true}\n')
    source.chmod(0o600)
    assert subject._read_private_json(source, "fixture") == {"ok": True}

    source.chmod(0o644)
    with pytest.raises(subject.ExecutionAttemptError, match="private single-link"):
        subject._read_private_json(source, "fixture")
    source.chmod(0o600)

    hardlink = tmp_path / "hardlink.json"
    os.link(source, hardlink)
    with pytest.raises(subject.ExecutionAttemptError, match="private single-link"):
        subject._read_private_json(source, "fixture")
    hardlink.unlink()

    symlink = tmp_path / "symlink.json"
    symlink.symlink_to(source)
    with pytest.raises(subject.ExecutionAttemptError, match="symbolic link"):
        subject._read_private_json(symlink, "fixture")


def test_stable_file_read_rejects_pathname_inode_swap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.json"
    displaced = tmp_path / "source.displaced.json"
    replacement = tmp_path / "replacement.json"
    source.write_bytes(b'{"version":1}\n')
    replacement.write_bytes(b'{"version":1}\n')
    real_read = subject.os.read
    swapped = False

    def swapping_read(descriptor: int, size: int) -> bytes:
        nonlocal swapped
        chunk = real_read(descriptor, size)
        if chunk and not swapped:
            swapped = True
            source.rename(displaced)
            replacement.rename(source)
        return chunk

    monkeypatch.setattr(subject.os, "read", swapping_read)
    with pytest.raises(subject.ExecutionAttemptError, match="inode or bytes changed"):
        subject._read_stable_file_record(  # noqa: SLF001
            source,
            "artifact/source fixture",
            require_private=False,
        )

    assert swapped is True
    assert source.read_bytes() == b'{"version":1}\n'
    assert displaced.read_bytes() == b'{"version":1}\n'


def test_build_write_and_validate_compatible_attempt(tagged_repo: dict[str, object]) -> None:
    root = tagged_repo["root"]
    placeholder = tagged_repo["placeholder"]
    assert isinstance(root, Path)
    assert isinstance(placeholder, Path)
    receipts = tagged_repo["receipts"]
    assert isinstance(receipts, dict)
    payload = subject.build_manifest(
        repository_root=root,
        attempt_id="attempt-20260823-a",
        annotated_tag="attempt3",
        manifest_path=placeholder,
        policy_path=placeholder,
        predicate_bundle_path=placeholder,
        formal_manifest_path=placeholder,
        pre_admission_receipt_paths=receipts,
        **_stability_context_arguments(tagged_repo),
    )

    assert payload["research_contract"]["changed"] is False
    assert payload["research_contract"]["ordinary_bugfix_attempt"] is True
    assert payload["runtime_execution"]["execution_commit"] == tagged_repo["runtime_commit"]
    source_binding = payload["runtime_sources"]["files"]["attempt_tool"]
    assert source_binding["size_bytes"] > 0
    assert source_binding["device"] > 0
    assert source_binding["inode"] > 0
    assert (
        payload["artifact_producer_execution"]["execution_commit"]
        != payload["runtime_execution"]["execution_commit"]
    )
    assert payload["permissions"] == {
        "research": False,
        "action": False,
        "live": False,
    }
    assert all(value is False for value in payload["evidence_boundary"].values())
    assert set(payload["pre_admission_evidence"]) == set(subject.PRE_ADMISSION_RECEIPT_ROLES)
    calls = tagged_repo["stability_calls"]
    assert isinstance(calls, list)
    assert len(calls) == 1
    assert set(calls[0][0]) == set(subject.PRE_ADMISSION_RECEIPT_ROLES)
    assert calls[0][1].execution_commit == tagged_repo["runtime_commit"]

    manifest = root.parent / "execution_attempt.json"
    file_hash = subject.atomic_write(manifest, payload)
    assert file_hash == subject.file_sha256(manifest)
    assert manifest.stat().st_mode & 0o777 == 0o600
    assert subject.validate_manifest(manifest, repository_root=root) == payload
    assert len(calls) == 2


@pytest.mark.parametrize(
    "attempt_id",
    (
        "",
        " attempt-20260823-a",
        "attempt-20260823-a ",
        "bad/id",
        "attempt3",
        "attempt-formal-v25",
        3,
    ),
)
def test_build_rejects_attempt_id_outside_exact_contract(
    tagged_repo: dict[str, object],
    attempt_id: object,
) -> None:
    root = tagged_repo["root"]
    placeholder = tagged_repo["placeholder"]
    receipts = tagged_repo["receipts"]
    assert isinstance(root, Path)
    assert isinstance(placeholder, Path)
    assert isinstance(receipts, dict)

    with pytest.raises(subject.ExecutionAttemptError, match="attempt id"):
        subject.build_manifest(
            repository_root=root,
            attempt_id=attempt_id,  # type: ignore[arg-type]
            annotated_tag="attempt3",
            manifest_path=placeholder,
            policy_path=placeholder,
            predicate_bundle_path=placeholder,
            formal_manifest_path=placeholder,
            pre_admission_receipt_paths=receipts,
            **_stability_context_arguments(tagged_repo),
        )


def test_execution_manifest_writer_refuses_existing_target(tmp_path: Path) -> None:
    target = tmp_path / "attempt.json"
    target.write_bytes(b"owner-controlled-existing-bytes\n")
    target.chmod(0o600)

    with pytest.raises(subject.ExecutionAttemptError, match="already exists"):
        subject.atomic_write(target, {"new": "manifest"})

    assert target.read_bytes() == b"owner-controlled-existing-bytes\n"


def test_execution_manifest_writer_refuses_concurrent_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "attempt.json"
    competitor = b'{"competitor":true}\n'
    real_open = subject.os.open
    raced = False

    def racing_open(path, flags, mode=0o777):
        nonlocal raced
        if Path(path) == target and flags & os.O_EXCL and not raced:
            raced = True
            descriptor = real_open(
                target,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
            try:
                os.write(descriptor, competitor)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        return real_open(path, flags, mode)

    monkeypatch.setattr(subject.os, "open", racing_open)
    with pytest.raises(subject.ExecutionAttemptError, match="already exists"):
        subject.atomic_write(target, {"new": "manifest"})

    assert raced is True
    assert target.read_bytes() == competitor


def test_execution_manifest_writer_refuses_post_write_swap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "attempt.json"
    displaced = tmp_path / "attempt.displaced.json"
    real_read = subject._read_private_json_record  # noqa: SLF001
    swapped = False

    def swapping_read(path: Path, label: str):
        nonlocal swapped
        if Path(path) == target and label == "execution attempt manifest" and not swapped:
            swapped = True
            target.rename(displaced)
            target.write_text('{"competitor": true}\n', encoding="ascii")
            target.chmod(0o600)
        return real_read(path, label)

    monkeypatch.setattr(subject, "_read_private_json_record", swapping_read)
    with pytest.raises(subject.ExecutionAttemptError, match="bytes drifted"):
        subject.atomic_write(target, {"new": "manifest"})

    assert swapped is True
    assert not target.exists()
    assert displaced.exists()


def test_execution_manifest_writer_fsyncs_file_and_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "attempt.json"
    real_fsync = subject.os.fsync
    observed: list[str] = []

    def recording_fsync(descriptor: int) -> None:
        mode = os.fstat(descriptor).st_mode
        observed.append("directory" if stat.S_ISDIR(mode) else "file")
        real_fsync(descriptor)

    monkeypatch.setattr(subject.os, "fsync", recording_fsync)
    subject.atomic_write(target, {"new": "manifest"})

    assert "file" in observed
    assert "directory" in observed


def test_freeze_rejects_untracked_worktree(tagged_repo: dict[str, object]) -> None:
    root = tagged_repo["root"]
    placeholder = tagged_repo["placeholder"]
    assert isinstance(root, Path)
    assert isinstance(placeholder, Path)
    receipts = tagged_repo["receipts"]
    assert isinstance(receipts, dict)
    _write(root / "untracked", "not admitted\n")

    with pytest.raises(subject.ExecutionAttemptError, match="completely clean"):
        subject.build_manifest(
            repository_root=root,
            attempt_id="attempt-20260823-a",
            annotated_tag="attempt3",
            manifest_path=placeholder,
            policy_path=placeholder,
            predicate_bundle_path=placeholder,
            formal_manifest_path=placeholder,
            pre_admission_receipt_paths=receipts,
            **_stability_context_arguments(tagged_repo),
        )


def test_validation_rejects_manifest_tamper(tagged_repo: dict[str, object]) -> None:
    root = tagged_repo["root"]
    placeholder = tagged_repo["placeholder"]
    assert isinstance(root, Path)
    assert isinstance(placeholder, Path)
    receipts = tagged_repo["receipts"]
    assert isinstance(receipts, dict)
    payload = subject.build_manifest(
        repository_root=root,
        attempt_id="attempt-20260823-a",
        annotated_tag="attempt3",
        manifest_path=placeholder,
        policy_path=placeholder,
        predicate_bundle_path=placeholder,
        formal_manifest_path=placeholder,
        pre_admission_receipt_paths=receipts,
        **_stability_context_arguments(tagged_repo),
    )
    payload["permissions"]["live"] = True
    manifest = root.parent / "tampered.json"
    manifest.write_text(json.dumps(payload), encoding="ascii")
    manifest.chmod(0o600)

    with pytest.raises(subject.ExecutionAttemptError, match="canonical hash drifted"):
        subject.validate_manifest(manifest, repository_root=root)


def _freeze_attempt_fixture(tagged_repo: dict[str, object]) -> tuple[Path, Path]:
    root = tagged_repo["root"]
    placeholder = tagged_repo["placeholder"]
    receipts = tagged_repo["receipts"]
    assert isinstance(root, Path)
    assert isinstance(placeholder, Path)
    assert isinstance(receipts, dict)
    payload = subject.build_manifest(
        repository_root=root,
        attempt_id="attempt-20260823-a",
        annotated_tag="attempt3",
        manifest_path=placeholder,
        policy_path=placeholder,
        predicate_bundle_path=placeholder,
        formal_manifest_path=placeholder,
        pre_admission_receipt_paths=receipts,
        **_stability_context_arguments(tagged_repo),
    )
    manifest = root.parent / "execution_attempt.json"
    subject.atomic_write(manifest, payload)
    return root, manifest


@pytest.mark.parametrize(
    "case",
    (
        "attempt_id_whitespace",
        "research_changed",
        "unchanged_fields_reordered",
        "ordinary_bugfix_false",
        "research_contract_extra",
        "research_permission_true",
        "permissions_extra",
        "validation_read_true",
        "evidence_boundary_extra",
    ),
)
def test_validate_manifest_rejects_recanonicalized_semantic_drift(
    tagged_repo: dict[str, object],
    case: str,
) -> None:
    root, manifest = _freeze_attempt_fixture(tagged_repo)
    payload = json.loads(manifest.read_text(encoding="ascii"))
    if case == "attempt_id_whitespace":
        payload["attempt_id"] = " attempt-20260823-a"
    elif case == "research_changed":
        payload["research_contract"]["changed"] = True
    elif case == "unchanged_fields_reordered":
        payload["research_contract"]["unchanged_fields"] = list(
            reversed(payload["research_contract"]["unchanged_fields"])
        )
    elif case == "ordinary_bugfix_false":
        payload["research_contract"]["ordinary_bugfix_attempt"] = False
    elif case == "research_contract_extra":
        payload["research_contract"]["extra"] = False
    elif case == "research_permission_true":
        payload["permissions"]["research"] = True
    elif case == "permissions_extra":
        payload["permissions"]["extra"] = False
    elif case == "validation_read_true":
        payload["evidence_boundary"]["validation_read"] = True
    elif case == "evidence_boundary_extra":
        payload["evidence_boundary"]["extra"] = False
    else:  # pragma: no cover - parameter list and mutation dispatch are frozen together.
        raise AssertionError(case)
    _rewrite_manifest_canonical(manifest, payload)

    with pytest.raises(subject.ExecutionAttemptError):
        subject.validate_manifest(manifest, repository_root=root)


def test_formal_manifest_requires_embedded_recomputed_and_fixed_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    formal = {
        "schema_version": "owner.execution.v1",
        "identity": subject.OWNER_IDENTITY,
        "status": "pre_refit_owner_execution_bound",
    }
    formal["canonical_execution_manifest_sha256"] = subject._document_sha256(  # noqa: SLF001
        formal, "canonical_execution_manifest_sha256"
    )
    monkeypatch.setattr(
        subject,
        "FORMAL_MANIFEST_CANONICAL_SHA256",
        formal["canonical_execution_manifest_sha256"],
    )
    assert (
        subject._validate_formal_manifest_identity(formal)
        == (  # noqa: SLF001
            formal["canonical_execution_manifest_sha256"]
        )
    )

    formal["status"] = "body_tampered_without_updating_embedded_identity"
    with pytest.raises(subject.ExecutionAttemptError, match="identities differ"):
        subject._validate_formal_manifest_identity(formal)  # noqa: SLF001


def test_freeze_requires_every_named_stability_receipt(
    tagged_repo: dict[str, object],
) -> None:
    root = tagged_repo["root"]
    placeholder = tagged_repo["placeholder"]
    receipts = dict(tagged_repo["receipts"])
    receipts.pop("parity_layer4")
    assert isinstance(root, Path)
    assert isinstance(placeholder, Path)

    with pytest.raises(subject.ExecutionAttemptError, match="role set is incomplete"):
        subject.build_manifest(
            repository_root=root,
            attempt_id="attempt-20260823-a",
            annotated_tag="attempt3",
            manifest_path=placeholder,
            policy_path=placeholder,
            predicate_bundle_path=placeholder,
            formal_manifest_path=placeholder,
            pre_admission_receipt_paths=receipts,
            **_stability_context_arguments(tagged_repo),
        )


@pytest.mark.parametrize(
    "role",
    (
        "parity_layer4",
        "sell54",
        "regression",
        "durability_concurrency_cache",
    ),
)
def test_freeze_rejects_substantively_forged_stability_source(
    tagged_repo: dict[str, object],
    role: str,
) -> None:
    root = tagged_repo["root"]
    placeholder = tagged_repo["placeholder"]
    receipts = tagged_repo["receipts"]
    assert isinstance(root, Path)
    assert isinstance(placeholder, Path)
    assert isinstance(receipts, dict)
    _forge_substantive_source(Path(receipts[role]), role)

    with pytest.raises(
        subject.ExecutionAttemptError,
        match=f"substantive stability wrapper validation failed.*{role}",
    ):
        subject.build_manifest(
            repository_root=root,
            attempt_id="attempt-20260823-a",
            annotated_tag="attempt3",
            manifest_path=placeholder,
            policy_path=placeholder,
            predicate_bundle_path=placeholder,
            formal_manifest_path=placeholder,
            pre_admission_receipt_paths=receipts,
            **_stability_context_arguments(tagged_repo),
        )


@pytest.mark.parametrize(
    ("section", "field"),
    [
        ("evidence_boundary", "economic_values_exposed"),
        ("evidence_boundary", "economic_values_used_for_selection"),
        ("evidence_boundary", "validation_read"),
        ("evidence_boundary", "sealed_holdout_read"),
        ("permissions", "research"),
        ("permissions", "action"),
        ("permissions", "live"),
    ],
)
def test_pre_admission_wrapper_rejects_missing_governance_field(
    tmp_path: Path,
    section: str,
    field: str,
) -> None:
    wrapper = _owner_wrapper(tmp_path / "single_day.json", "single_day")
    payload = json.loads(wrapper.read_text(encoding="ascii"))
    payload[section].pop(field)
    _rewrite_receipt(wrapper, payload)

    with pytest.raises(subject.ExecutionAttemptError, match="incomplete or drifted"):
        subject._receipt_binding(  # noqa: SLF001
            wrapper,
            "single_day",
            require_owner_wrapper=True,
        )


@pytest.mark.parametrize(
    ("section", "field"),
    [
        ("evidence_boundary", "economic_values_exposed"),
        ("evidence_boundary", "economic_values_used_for_selection"),
        ("evidence_boundary", "validation_read"),
        ("evidence_boundary", "sealed_holdout_read"),
        ("permissions", "research"),
        ("permissions", "action"),
        ("permissions", "live"),
    ],
)
def test_pre_admission_wrapper_rejects_true_governance_field(
    tmp_path: Path,
    section: str,
    field: str,
) -> None:
    wrapper = _owner_wrapper(tmp_path / "single_day.json", "single_day")
    payload = json.loads(wrapper.read_text(encoding="ascii"))
    payload[section][field] = True
    _rewrite_receipt(wrapper, payload)

    with pytest.raises(subject.ExecutionAttemptError, match="incomplete or drifted"):
        subject._receipt_binding(  # noqa: SLF001
            wrapper,
            "single_day",
            require_owner_wrapper=True,
        )


def test_pre_admission_wrapper_rejects_extra_boundary_and_wrong_owner(
    tmp_path: Path,
) -> None:
    wrapper = _owner_wrapper(tmp_path / "single_day.json", "single_day")
    payload = json.loads(wrapper.read_text(encoding="ascii"))
    payload["evidence_boundary"]["unknown_boundary"] = False
    _rewrite_receipt(wrapper, payload)
    with pytest.raises(subject.ExecutionAttemptError, match="evidence boundary"):
        subject._receipt_binding(  # noqa: SLF001
            wrapper,
            "single_day",
            require_owner_wrapper=True,
        )

    wrapper = _owner_wrapper(tmp_path / "single_day_owner.json", "single_day")
    payload = json.loads(wrapper.read_text(encoding="ascii"))
    payload["identity"] = "not-the-owner"
    _rewrite_receipt(wrapper, payload)
    with pytest.raises(subject.ExecutionAttemptError, match="owner wrapper identity"):
        subject._receipt_binding(  # noqa: SLF001
            wrapper,
            "single_day",
            require_owner_wrapper=True,
        )


def test_pre_admission_rejects_unwrapped_or_malformed_governance(
    tmp_path: Path,
) -> None:
    historical = _private_receipt(tmp_path / "historical.json", "historical")
    with pytest.raises(subject.ExecutionAttemptError, match="owner wrapper fields"):
        subject._receipt_binding(  # noqa: SLF001
            historical,
            "single_day",
            require_owner_wrapper=True,
        )

    wrapper = _owner_wrapper(tmp_path / "single_day.json", "single_day")
    payload = json.loads(wrapper.read_text(encoding="ascii"))
    payload["permissions"] = ["research=false", "action=false", "live=false"]
    _rewrite_receipt(wrapper, payload)
    with pytest.raises(subject.ExecutionAttemptError, match="permissions are incomplete"):
        subject._receipt_binding(  # noqa: SLF001
            wrapper,
            "single_day",
            require_owner_wrapper=True,
        )


def test_pre_admission_wrapper_revalidates_nested_source_binding(
    tmp_path: Path,
) -> None:
    wrapper = _owner_wrapper(tmp_path / "single_day.json", "single_day")
    payload = json.loads(wrapper.read_text(encoding="ascii"))
    source = Path(payload["source_receipt"]["path"])
    source_payload = json.loads(source.read_text(encoding="ascii"))
    source_payload["status"] = "verified"
    _rewrite_receipt(source, source_payload)

    with pytest.raises(subject.ExecutionAttemptError, match="exact status drifted"):
        subject._receipt_binding(  # noqa: SLF001
            wrapper,
            "single_day",
            require_owner_wrapper=True,
        )


def test_validation_rejects_stability_receipt_byte_and_canonical_drift(
    tagged_repo: dict[str, object],
) -> None:
    root, manifest = _freeze_attempt_fixture(tagged_repo)
    receipt = Path(tagged_repo["receipts"]["regression"])
    payload = json.loads(receipt.read_text(encoding="ascii"))
    source = Path(payload["source_receipt"]["path"])
    source_payload = json.loads(source.read_text(encoding="ascii"))
    source_payload["metadata_only_nonce"] = "changed"
    _rewrite_receipt(source, source_payload)
    source_binding, _source_ids = subject._receipt_binding(  # noqa: SLF001
        source,
        "regression_source",
        require_owner_wrapper=False,
    )
    payload["source_receipt"] = source_binding
    _rewrite_receipt(receipt, payload)

    with pytest.raises(subject.ExecutionAttemptError, match="receipt bytes or canonical"):
        subject.validate_manifest(manifest, repository_root=root)


def test_finalize_is_private_no_replace_and_validates_exact_composition(
    tagged_repo: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, manifest = _freeze_attempt_fixture(tagged_repo)
    evidence_root = root.parent / "evidence"
    validator_calls: list[tuple[Path, Path]] = []
    result = _final_composition_fixture(
        evidence_root,
        manifest,
        monkeypatch,
        validator_calls=validator_calls,
    )
    final_path = root.parent / "final_receipt.json"

    payload, file_hash = subject.finalize_attempt(
        repository_root=root,
        attempt_manifest_path=manifest,
        result_receipt_paths={subject.FINAL_RESULT_RECEIPT_ROLE: result},
        composition_evidence_root=evidence_root,
        output_path=final_path,
    )

    assert file_hash == subject.file_sha256(final_path)
    assert final_path.stat().st_mode & 0o777 == 0o600
    assert final_path.stat().st_nlink == 1
    assert set(payload["result_receipts"]) == {subject.FINAL_RESULT_RECEIPT_ROLE}
    assert (
        payload["result_receipts"][subject.FINAL_RESULT_RECEIPT_ROLE]["schema_version"]
        == subject.FINAL_COMPOSITION_SCHEMA
    )
    assert (
        payload["result_receipts"][subject.FINAL_RESULT_RECEIPT_ROLE]["canonical_sha256"]
        == json.loads(result.read_text(encoding="ascii"))[subject.FINAL_COMPOSITION_CANONICAL_FIELD]
    )
    assert (
        payload["attempt_manifest"]["canonical_sha256"]
        == json.loads(manifest.read_text(encoding="ascii"))["canonical_execution_attempt_sha256"]
    )
    assert subject.validate_final_receipt(final_path, repository_root=root) == payload
    assert validator_calls == [
        (evidence_root.resolve(), result),
        (evidence_root.resolve(), result),
    ]
    with pytest.raises(subject.ExecutionAttemptError, match="already exists"):
        subject.finalize_attempt(
            repository_root=root,
            attempt_manifest_path=manifest,
            result_receipt_paths={subject.FINAL_RESULT_RECEIPT_ROLE: result},
            composition_evidence_root=evidence_root,
            output_path=final_path,
        )


@pytest.mark.parametrize(
    "roles",
    [
        {},
        {"final_composition": "valid", "nested_report": "extra"},
        {"formal_result": "wrong"},
    ],
)
def test_finalize_requires_exact_final_composition_role_set(
    tagged_repo: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    roles: dict[str, str],
) -> None:
    root, manifest = _freeze_attempt_fixture(tagged_repo)
    evidence_root = root.parent / "role-evidence"
    result = _final_composition_fixture(
        evidence_root,
        manifest,
        monkeypatch,
    )
    paths = {role: result for role in roles}

    with pytest.raises(subject.ExecutionAttemptError, match="role set is incomplete"):
        subject.build_final_receipt(
            repository_root=root,
            attempt_manifest_path=manifest,
            result_receipt_paths=paths,
            composition_evidence_root=evidence_root,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema", "wrong.final_composition.v9"),
        ("schema", f"{subject.OWNER_IDENTITY}.final_composition_receipt.v1"),
        ("identity", "wrong.final_composition"),
        ("status", "not_passed"),
        ("status", "bypassed"),
        ("status", "incomplete_waiting_for_gate"),
    ],
)
def test_finalize_rejects_wrong_composition_identity_or_exact_status(
    tagged_repo: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: str,
) -> None:
    root, manifest = _freeze_attempt_fixture(tagged_repo)
    evidence_root = root.parent / f"bad-{field}-{value}"
    kwargs = {field: value}
    result = _final_composition_fixture(
        evidence_root,
        manifest,
        monkeypatch,
        **kwargs,
    )

    with pytest.raises(subject.ExecutionAttemptError, match="exact status drifted"):
        subject.build_final_receipt(
            repository_root=root,
            attempt_manifest_path=manifest,
            result_receipt_paths={subject.FINAL_RESULT_RECEIPT_ROLE: result},
            composition_evidence_root=evidence_root,
        )


@pytest.mark.parametrize(
    "case",
    (
        "attempt_canonical",
        "execution_commit",
        "execution_tree",
        "annotated_tag",
        "annotated_tag_object",
        "wrapper_identity",
        "wrapper_missing",
        "wrapper_extra",
        "compatible_extra",
    ),
)
def test_finalize_rejects_composition_compatible_attempt_drift(
    tagged_repo: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    root, manifest = _freeze_attempt_fixture(tagged_repo)
    evidence_root = root.parent / f"compatible-drift-{case}"
    result = _final_composition_fixture(
        evidence_root,
        manifest,
        monkeypatch,
    )
    payload = json.loads(result.read_text(encoding="ascii"))
    compatible = payload["compatible_execution_attempt"]
    wrappers = compatible["pre_admission_wrapper_canonical_sha256"]
    if case == "attempt_canonical":
        compatible["canonical_execution_attempt_sha256"] = "0" * 64
    elif case == "execution_commit":
        compatible["execution_commit"] = "0" * 40
    elif case == "execution_tree":
        compatible["execution_tree"] = "1" * 40
    elif case == "annotated_tag":
        compatible["annotated_tag"] = "swapped-attempt-tag"
    elif case == "annotated_tag_object":
        compatible["annotated_tag_object"] = "2" * 40
    elif case == "wrapper_identity":
        wrappers["parity_layer4"] = "3" * 64
    elif case == "wrapper_missing":
        wrappers.pop("sell54")
    elif case == "wrapper_extra":
        wrappers["extra"] = "4" * 64
    elif case == "compatible_extra":
        compatible["extra"] = False
    else:  # pragma: no cover - parameter list and mutation dispatch are frozen together.
        raise AssertionError(case)
    payload[subject.FINAL_COMPOSITION_CANONICAL_FIELD] = subject._document_sha256(  # noqa: SLF001
        payload,
        subject.FINAL_COMPOSITION_CANONICAL_FIELD,
    )
    result.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="ascii")
    result.chmod(0o600)

    with pytest.raises(
        subject.ExecutionAttemptError,
        match="compatible execution attempt identity drifted",
    ):
        subject.build_final_receipt(
            repository_root=root,
            attempt_manifest_path=manifest,
            result_receipt_paths={subject.FINAL_RESULT_RECEIPT_ROLE: result},
            composition_evidence_root=evidence_root,
        )


def test_final_validation_rejects_composition_tamper(
    tagged_repo: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, manifest = _freeze_attempt_fixture(tagged_repo)
    evidence_root = root.parent / "tamper-evidence"
    result = _final_composition_fixture(
        evidence_root,
        manifest,
        monkeypatch,
    )
    final_path = root.parent / "final_result_tamper.json"
    subject.finalize_attempt(
        repository_root=root,
        attempt_manifest_path=manifest,
        result_receipt_paths={subject.FINAL_RESULT_RECEIPT_ROLE: result},
        composition_evidence_root=evidence_root,
        output_path=final_path,
    )

    payload = json.loads(result.read_text(encoding="ascii"))
    payload["metadata_only_note"] = "tampered"
    payload[subject.FINAL_COMPOSITION_CANONICAL_FIELD] = subject._document_sha256(  # noqa: SLF001
        payload,
        subject.FINAL_COMPOSITION_CANONICAL_FIELD,
    )
    result.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="ascii")
    result.chmod(0o600)

    with pytest.raises(subject.ExecutionAttemptError, match="bytes, path, or canonical"):
        subject.validate_final_receipt(final_path, repository_root=root)


def test_final_validation_rejects_attempt_manifest_swap(
    tagged_repo: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, manifest = _freeze_attempt_fixture(tagged_repo)
    evidence_root = root.parent / "attempt-swap-evidence"
    result = _final_composition_fixture(
        evidence_root,
        manifest,
        monkeypatch,
    )
    final_path = root.parent / "final_manifest_swap.json"
    subject.finalize_attempt(
        repository_root=root,
        attempt_manifest_path=manifest,
        result_receipt_paths={subject.FINAL_RESULT_RECEIPT_ROLE: result},
        composition_evidence_root=evidence_root,
        output_path=final_path,
    )

    swapped = json.loads(manifest.read_text(encoding="ascii"))
    swapped["generated_utc"] = "2099-01-01T00:00:00Z"
    swapped["canonical_execution_attempt_sha256"] = subject._document_sha256(  # noqa: SLF001
        swapped, "canonical_execution_attempt_sha256"
    )
    manifest.write_text(json.dumps(swapped, sort_keys=True) + "\n", encoding="ascii")
    manifest.chmod(0o600)

    with pytest.raises(subject.ExecutionAttemptError, match="attempt manifest binding drifted"):
        subject.validate_final_receipt(final_path, repository_root=root)


def test_final_validation_rejects_composition_path_swap(
    tagged_repo: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, manifest = _freeze_attempt_fixture(tagged_repo)
    evidence_root = root.parent / "path-swap-evidence"
    result = _final_composition_fixture(
        evidence_root,
        manifest,
        monkeypatch,
    )
    final_path = root.parent / "final_path_swap.json"
    subject.finalize_attempt(
        repository_root=root,
        attempt_manifest_path=manifest,
        result_receipt_paths={subject.FINAL_RESULT_RECEIPT_ROLE: result},
        composition_evidence_root=evidence_root,
        output_path=final_path,
    )
    payload = json.loads(final_path.read_text(encoding="ascii"))
    payload["result_receipts"][subject.FINAL_RESULT_RECEIPT_ROLE]["path"] = str(
        evidence_root / "receipts" / "swapped.json"
    )
    final_path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="ascii")
    final_path.chmod(0o600)

    with pytest.raises(subject.ExecutionAttemptError, match="canonical hash drifted"):
        subject.validate_final_receipt(final_path, repository_root=root)


def test_final_validation_rejects_final_receipt_tamper(
    tagged_repo: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, manifest = _freeze_attempt_fixture(tagged_repo)
    evidence_root = root.parent / "final-tamper-evidence"
    result = _final_composition_fixture(
        evidence_root,
        manifest,
        monkeypatch,
    )
    final_path = root.parent / "final_tamper.json"
    subject.finalize_attempt(
        repository_root=root,
        attempt_manifest_path=manifest,
        result_receipt_paths={subject.FINAL_RESULT_RECEIPT_ROLE: result},
        composition_evidence_root=evidence_root,
        output_path=final_path,
    )
    payload = json.loads(final_path.read_text(encoding="ascii"))
    payload["status"] = "tampered"
    final_path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="ascii")
    final_path.chmod(0o600)

    with pytest.raises(subject.ExecutionAttemptError, match="canonical hash drifted"):
        subject.validate_final_receipt(final_path, repository_root=root)
