from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

import pytest

import models.cache_tier_lru as cache_lru
from models.cache_tier_lru import (
    CacheTierAuthorizationError,
    CacheTierConfig,
    CacheTierValidationError,
    apply_cache_tier_plan,
    build_cache_tier_plan,
    cleanup_stale_partials,
    list_artifacts,
    owner_token_for_plan,
    record_cache_access,
    register_cache_write,
    scan_cache_tiers,
    validate_cache_tiers,
    validate_plan,
)
from scripts.manage_cache_tiers import main


def _config(
    tmp_path: Path,
    *,
    reserve: int = 100,
    target: int = 200,
    ttl_days: int = 30,
    allow_unknown_migration: bool = True,
) -> CacheTierConfig:
    hot = tmp_path / "hot"
    cold = tmp_path / "ORICO" / "cache"
    hot.mkdir(parents=True)
    cold.mkdir(parents=True)
    return CacheTierConfig(
        hot_root=hot,
        cold_root=cold,
        ledger_path=hot / ".cache_tier_lru" / "access.sqlite3",
        hot_safety_reserve_bytes=reserve,
        hot_target_free_bytes=target,
        cold_ttl_days=ttl_days,
        allowed_cache_roots=("window_cache", "replay_dag"),
        allow_unknown_migration=allow_unknown_migration,
        lock_timeout_s=0.2,
    )


def _artifact(
    root: Path,
    relative: str,
    *,
    content: bytes = b"cache",
    mtime_ns: int | None = None,
) -> Path:
    path = root / relative
    path.mkdir(parents=True)
    (path / "payload.bin").write_bytes(content)
    if mtime_ns is not None:
        os.utime(path, ns=(mtime_ns, mtime_ns))
    return path


def _env(monkeypatch: pytest.MonkeyPatch, config: CacheTierConfig) -> None:
    monkeypatch.setenv("NARROWGATE_CACHE_HOT_ROOT", str(config.hot_root))
    monkeypatch.setenv("NARROWGATE_CACHE_COLD_ROOT", str(config.cold_root))
    monkeypatch.setenv("NARROWGATE_CACHE_LEDGER_PATH", str(config.ledger_path))


def _migration_plan(config: CacheTierConfig) -> dict[str, object]:
    return build_cache_tier_plan(
        config,
        include_deletions=False,
        hot_free_bytes=50,
        cold_free_bytes=10_000,
    )


def test_lru_plan_uses_last_access_then_access_count_and_reaches_target(tmp_path: Path) -> None:
    config = _config(tmp_path, reserve=100, target=250)
    _artifact(config.hot_root, "window_cache/old", content=b"a" * 100, mtime_ns=10)
    _artifact(config.hot_root, "window_cache/middle", content=b"b" * 100, mtime_ns=20)
    _artifact(config.hot_root, "window_cache/new", content=b"c" * 100, mtime_ns=30)
    scan_cache_tiers(config)

    plan = build_cache_tier_plan(
        config,
        include_deletions=False,
        hot_free_bytes=50,
        cold_free_bytes=10_000,
    )

    assert [row["relative_path"] for row in plan["migrations"]] == [
        "window_cache/old",
        "window_cache/middle",
    ]
    assert plan["filesystem_snapshot"]["hot_bytes_to_reclaim"] == 200
    assert plan["dry_run"] is True


def test_referenced_and_frozen_may_migrate_but_unknown_needs_explicit_permission(
    tmp_path: Path,
) -> None:
    config = _config(
        tmp_path,
        reserve=100,
        target=250,
        allow_unknown_migration=False,
    )
    _artifact(config.hot_root, "window_cache/frozen", content=b"f" * 100, mtime_ns=1)
    _artifact(config.hot_root, "window_cache/referenced", content=b"r" * 100, mtime_ns=2)
    _artifact(config.hot_root, "window_cache/unknown", content=b"u" * 100, mtime_ns=3)
    scan_cache_tiers(
        config,
        reference_classes={
            "window_cache/frozen": "frozen",
            "window_cache/referenced": "referenced",
        },
    )

    plan = _migration_plan(config)

    assert [(row["relative_path"], row["reference_class"]) for row in plan["migrations"]] == [
        ("window_cache/frozen", "frozen"),
        ("window_cache/referenced", "referenced"),
    ]


def test_apply_is_dry_run_by_default_and_requires_operation_owner_token(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, reserve=100, target=150)
    artifact = _artifact(config.hot_root, "window_cache/a", content=b"x" * 100)
    scan_cache_tiers(config)
    plan = _migration_plan(config)

    receipt = apply_cache_tier_plan(plan, config=config, operation="migrate")
    assert receipt["status"] == "dry_run"
    assert artifact.is_dir()

    with pytest.raises(CacheTierAuthorizationError, match="exact owner token"):
        apply_cache_tier_plan(
            plan,
            config=config,
            operation="migrate",
            owner_token=owner_token_for_plan(plan, "delete"),
            execute=True,
        )
    assert artifact.is_dir()

    receipt = apply_cache_tier_plan(
        plan,
        config=config,
        operation="migrate",
        owner_token=owner_token_for_plan(plan, "migrate"),
        execute=True,
    )
    assert receipt["status"] == "complete"
    assert artifact.is_symlink()


def test_migration_preserves_relative_path_and_symlink_access_hits_cold_tier(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path, reserve=100, target=150)
    hot = _artifact(config.hot_root, "window_cache/a", content=b"payload" * 20)
    scan_cache_tiers(config)
    plan = _migration_plan(config)
    apply_cache_tier_plan(
        plan,
        config=config,
        operation="migrate",
        owner_token=owner_token_for_plan(plan, "migrate"),
        execute=True,
    )
    _env(monkeypatch, config)

    record = record_cache_access(hot, cache_root=config.hot_root, strict=True)

    assert record is not None
    assert record.relative_path == "window_cache/a"
    assert record.tier == "cold"
    assert Path(record.physical_path) == config.cold_root / "window_cache/a"
    assert Path(record.hot_link_path or "") == hot
    assert hot.readlink() == Path(
        os.path.relpath(config.cold_root / "window_cache/a", start=hot.parent)
    )


def test_atomic_symlink_failure_restores_original_hot_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path, reserve=100, target=150)
    hot = _artifact(config.hot_root, "window_cache/a", content=b"original" * 20)
    scan_cache_tiers(config)
    plan = _migration_plan(config)

    def fail_after_hot_backup(*args: object, **kwargs: object) -> str:
        del args, kwargs
        raise OSError("synthetic symlink publication failure")

    monkeypatch.setattr(cache_lru, "_symlink_target", fail_after_hot_backup)
    receipt = apply_cache_tier_plan(
        plan,
        config=config,
        operation="migrate",
        owner_token=owner_token_for_plan(plan, "migrate"),
        execute=True,
    )

    assert receipt["status"] == "failed"
    assert receipt["errors"][0]["type"] == "OSError"
    assert hot.is_dir()
    assert not hot.is_symlink()
    assert (hot / "payload.bin").read_bytes() == b"original" * 20
    assert list(hot.parent.glob(".*.cache-tier-lru.hot-backup.*")) == []


def test_cold_delete_requires_unreferenced_and_expired_ttl(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path, reserve=100, target=150, ttl_days=30)
    stale_ns = 1_000_000_000
    hot = _artifact(
        config.hot_root,
        "window_cache/stale",
        content=b"old" * 40,
        mtime_ns=stale_ns,
    )
    _artifact(
        config.hot_root,
        "window_cache/referenced",
        content=b"keep" * 30,
        mtime_ns=stale_ns,
    )
    scan_cache_tiers(
        config,
        reference_classes={
            "window_cache/stale": "unreferenced",
            "window_cache/referenced": "referenced",
        },
    )
    _env(monkeypatch, config)
    assert record_cache_access(hot, cache_root=config.hot_root, strict=True) is not None
    migration = build_cache_tier_plan(
        config,
        include_deletions=False,
        hot_free_bytes=0,
        cold_free_bytes=10_000,
    )
    apply_cache_tier_plan(
        migration,
        config=config,
        operation="migrate",
        owner_token=owner_token_for_plan(migration, "migrate"),
        execute=True,
    )
    now_ns = time.time_ns() + config.cold_ttl_ns + 1

    deletion = build_cache_tier_plan(
        config,
        include_migrations=False,
        include_deletions=True,
        now_ns=now_ns,
    )

    assert [row["relative_path"] for row in deletion["deletions"]] == [
        "window_cache/stale"
    ]
    apply_cache_tier_plan(
        deletion,
        config=config,
        operation="delete",
        owner_token=owner_token_for_plan(deletion, "delete"),
        execute=True,
    )
    assert not os.path.lexists(hot)
    assert not (config.cold_root / "window_cache/stale").exists()
    assert (config.hot_root / "window_cache/referenced").exists()


def test_plan_drift_after_cache_access_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path, reserve=100, target=150)
    hot = _artifact(config.hot_root, "window_cache/a", content=b"x" * 100)
    scan_cache_tiers(config)
    plan = _migration_plan(config)
    _env(monkeypatch, config)
    assert record_cache_access(hot, cache_root=config.hot_root, strict=True) is not None

    receipt = apply_cache_tier_plan(
        plan,
        config=config,
        operation="migrate",
        owner_token=owner_token_for_plan(plan, "migrate"),
        execute=True,
    )
    assert receipt["status"] == "failed"
    assert "ledger field access_count changed" in receipt["errors"][0]["detail"]
    assert hot.is_dir()


def test_plan_payload_mutation_is_rejected(tmp_path: Path) -> None:
    config = _config(tmp_path, reserve=100, target=150)
    _artifact(config.hot_root, "window_cache/a", content=b"x" * 100)
    scan_cache_tiers(config)
    plan = _migration_plan(config)
    plan["migrations"][0]["size_bytes"] += 1

    with pytest.raises(CacheTierValidationError, match="plan SHA256 drift"):
        validate_plan(plan, config)


def test_orico_missing_fails_scan_plan_and_validate(tmp_path: Path) -> None:
    hot = tmp_path / "hot"
    hot.mkdir()
    config = CacheTierConfig(
        hot_root=hot,
        cold_root=tmp_path / "missing-orico" / "cache",
        ledger_path=hot / ".state" / "ledger.sqlite3",
        hot_safety_reserve_bytes=100,
        hot_target_free_bytes=200,
        allowed_cache_roots=("window_cache",),
    )

    with pytest.raises(CacheTierValidationError, match="cold cache root is missing"):
        scan_cache_tiers(config)
    with pytest.raises(CacheTierValidationError, match="cold cache root is missing"):
        build_cache_tier_plan(config)
    with pytest.raises(CacheTierValidationError, match="cold cache root is missing"):
        validate_cache_tiers(config)


def test_forbidden_raw_report_and_frozen_roots_are_rejected(tmp_path: Path) -> None:
    config = _config(tmp_path)
    for forbidden in ("raw", "reports", "frozen_artifacts"):
        invalid = CacheTierConfig(
            hot_root=config.hot_root,
            cold_root=config.cold_root,
            ledger_path=config.ledger_path,
            allowed_cache_roots=(forbidden,),
        )
        with pytest.raises(CacheTierValidationError, match="forbidden cache root"):
            invalid.validate()


def test_invalid_manifest_fails_plan_before_any_copy(tmp_path: Path) -> None:
    config = _config(tmp_path, reserve=100, target=150)
    hot = _artifact(config.hot_root, "window_cache/a", content=b"x" * 100)
    (hot / "manifest.json").write_text("{not-json")
    scan_cache_tiers(config)

    with pytest.raises(CacheTierValidationError, match="invalid cache manifest"):
        _migration_plan(config)
    assert hot.is_dir()
    assert not (config.cold_root / "window_cache/a").exists()


def test_invalid_lru_candidate_is_bound_as_exclusion_when_valid_capacity_remains(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, reserve=100, target=150)
    invalid = _artifact(
        config.hot_root,
        "window_cache/invalid",
        content=b"x" * 100,
        mtime_ns=1,
    )
    (invalid / "manifest.json").write_text(
        json.dumps({"files": [{"path": "missing.parquet", "sha256": "a" * 64}]})
    )
    os.utime(invalid, ns=(1, 1))
    _artifact(
        config.hot_root,
        "window_cache/valid",
        content=b"y" * 200,
        mtime_ns=2,
    )
    scan_cache_tiers(config)

    plan = _migration_plan(config)

    assert [row["relative_path"] for row in plan["migrations"]] == [
        "window_cache/valid"
    ]
    assert plan["migration_exclusions"][0]["relative_path"] == "window_cache/invalid"
    assert "missing or escaping file" in plan["migration_exclusions"][0]["detail"]


def test_access_ledger_failure_is_best_effort_unless_strict(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    hot = _artifact(config.hot_root, "window_cache/a")
    bad_ledger = config.hot_root / "ledger-is-a-directory"
    bad_ledger.mkdir()
    monkeypatch.setenv("NARROWGATE_CACHE_HOT_ROOT", str(config.hot_root))
    monkeypatch.setenv("NARROWGATE_CACHE_COLD_ROOT", str(config.cold_root))
    monkeypatch.setenv("NARROWGATE_CACHE_LEDGER_PATH", str(bad_ledger))

    assert record_cache_access(hot, cache_root=config.hot_root) is None
    assert (hot / "payload.bin").read_bytes() == b"cache"
    with pytest.raises(Exception, match="database|directory"):
        register_cache_write(hot, cache_root=config.hot_root, strict=True)


def test_register_write_records_identity_reference_and_size(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    hot = _artifact(config.hot_root, "window_cache/a", content=b"payload")
    _env(monkeypatch, config)
    identity = "a" * 64

    record = register_cache_write(
        hot,
        cache_root=config.hot_root / "window_cache",
        identity_sha256=identity,
        reference_class="referenced",
        strict=True,
    )

    assert record is not None
    assert record.logical_id == f"sha256:{identity}"
    assert record.relative_path == "window_cache/a"
    assert record.reference_class == "referenced"
    assert record.size_bytes == len(b"payload")


def test_cli_scan_plan_and_validate_are_non_destructive(tmp_path: Path, capsys: Any) -> None:
    config = _config(tmp_path)
    hot = _artifact(config.hot_root, "window_cache/a")
    common = [
        "--hot-root",
        str(config.hot_root),
        "--cold-root",
        str(config.cold_root),
        "--ledger",
        str(config.ledger_path),
        "--allow-root",
        "window_cache",
        "--hot-safety-reserve-gib",
        "1",
        "--hot-target-free-gib",
        "1",
    ]
    assert main(["scan", *common]) == 0
    capsys.readouterr()
    plan_path = tmp_path / "plan.json"
    assert main(["plan", *common, "--operation", "delete", "--output", str(plan_path)]) == 0
    capsys.readouterr()
    assert main(["validate", *common, "--plan", str(plan_path)]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["valid"] is True
    assert payload["plan"]["valid"] is True
    assert hot.is_dir()
    assert not hot.is_symlink()


def test_validate_detects_broken_cold_symlink(tmp_path: Path) -> None:
    config = _config(tmp_path, reserve=100, target=150)
    _artifact(config.hot_root, "window_cache/a", content=b"x" * 100)
    scan_cache_tiers(config)
    plan = _migration_plan(config)
    apply_cache_tier_plan(
        plan,
        config=config,
        operation="migrate",
        owner_token=owner_token_for_plan(plan, "migrate"),
        execute=True,
    )
    cold = config.cold_root / "window_cache/a"
    os.rename(cold, cold.with_name("moved"))

    result = validate_cache_tiers(config)

    assert result["valid"] is False
    assert result["issues"][0]["relative_path"] == "window_cache/a"
    assert list_artifacts(config)[0].tier == "cold"


def test_validate_detects_missing_managed_hot_symlink(tmp_path: Path) -> None:
    config = _config(tmp_path, reserve=100, target=150)
    hot = _artifact(config.hot_root, "window_cache/a", content=b"x" * 100)
    scan_cache_tiers(config)
    plan = _migration_plan(config)
    apply_cache_tier_plan(
        plan,
        config=config,
        operation="migrate",
        owner_token=owner_token_for_plan(plan, "migrate"),
        execute=True,
    )
    hot.unlink()

    result = validate_cache_tiers(config)

    assert result["valid"] is False
    assert "missing its hot compatibility symlink" in result["issues"][0]["detail"]


def test_stale_cleanup_preserves_transaction_recovery_artifacts(tmp_path: Path) -> None:
    config = _config(tmp_path)
    hot_parent = config.hot_root / "window_cache"
    cold_parent = config.cold_root / "window_cache"
    hot_parent.mkdir()
    cold_parent.mkdir()
    safe_partial = cold_parent / ".a.cache-tier-lru.partial.dead"
    safe_link = hot_parent / ".a.cache-tier-lru.link.dead"
    hot_backup = hot_parent / ".a.cache-tier-lru.hot-backup.dead"
    delete_tombstone = cold_parent / ".a.cache-tier-lru.delete.dead"
    link_backup = hot_parent / ".a.cache-tier-lru.link-backup.dead"
    for path in (
        safe_partial,
        safe_link,
        hot_backup,
        delete_tombstone,
        link_backup,
    ):
        path.write_text("recovery")
        os.utime(path, ns=(1, 1))

    removed = cleanup_stale_partials(config, older_than_s=0)

    assert removed == sorted((str(safe_link), str(safe_partial)))
    assert not safe_partial.exists()
    assert not safe_link.exists()
    assert hot_backup.exists()
    assert delete_tombstone.exists()
    assert link_backup.exists()


def test_scan_rejects_material_hot_and_cold_duplicates(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _artifact(config.hot_root, "window_cache/a", content=b"hot")
    _artifact(config.cold_root, "window_cache/a", content=b"cold")

    with pytest.raises(
        CacheTierValidationError,
        match="duplicate cache artifact exists in both tiers",
    ):
        scan_cache_tiers(config)


def test_fresh_cold_residence_blocks_immediate_delete_even_with_ancient_mtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path, reserve=100, target=150, ttl_days=30)
    hot = _artifact(
        config.hot_root,
        "window_cache/ancient",
        content=b"ancient" * 20,
        mtime_ns=1,
    )
    scan_cache_tiers(
        config,
        reference_classes={"window_cache/ancient": "unreferenced"},
    )
    _env(monkeypatch, config)
    assert record_cache_access(hot, cache_root=config.hot_root, strict=True) is not None
    migration = _migration_plan(config)
    apply_cache_tier_plan(
        migration,
        config=config,
        operation="migrate",
        owner_token=owner_token_for_plan(migration, "migrate"),
        execute=True,
    )
    record = list_artifacts(config)[0]

    deletion = build_cache_tier_plan(
        config,
        include_migrations=False,
        now_ns=int(record.cold_admitted_ns or 0) + 24 * 60 * 60 * 1_000_000_000,
    )

    assert deletion["deletions"] == []
    assert record.managed_by_lru is True
    assert record.cold_admitted_ns is not None


def test_preexisting_orico_cache_is_never_granted_delete_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path, ttl_days=1)
    cold = _artifact(
        config.cold_root,
        "window_cache/preexisting",
        content=b"cold",
        mtime_ns=1,
    )
    scan_cache_tiers(
        config,
        reference_classes={"window_cache/preexisting": "unreferenced"},
    )
    _env(monkeypatch, config)
    assert record_cache_access(cold, cache_root=config.cold_root, strict=True) is not None

    plan = build_cache_tier_plan(
        config,
        include_migrations=False,
        now_ns=time.time_ns() + 10 * config.cold_ttl_ns,
    )

    assert plan["deletions"] == []
    assert list_artifacts(config)[0].managed_by_lru is False


def test_managed_cold_cache_with_no_post_migration_hit_expires_after_ttl(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, reserve=100, target=150, ttl_days=1)
    _artifact(config.hot_root, "window_cache/unused", content=b"x" * 100)
    scan_cache_tiers(
        config,
        reference_classes={"window_cache/unused": "unreferenced"},
    )
    migration = _migration_plan(config)
    apply_cache_tier_plan(
        migration,
        config=config,
        operation="migrate",
        owner_token=owner_token_for_plan(migration, "migrate"),
        execute=True,
    )
    record = list_artifacts(config)[0]
    assert record.last_explicit_access_ns is None

    deletion = build_cache_tier_plan(
        config,
        include_migrations=False,
        now_ns=int(record.cold_admitted_ns or 0) + config.cold_ttl_ns + 1,
    )

    assert [row["relative_path"] for row in deletion["deletions"]] == [
        "window_cache/unused"
    ]


def test_reference_audit_is_hash_bound_and_manual_override_cannot_downgrade(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, allow_unknown_migration=False)
    frozen = _artifact(config.hot_root, "window_cache/frozen", content=b"f")
    referenced = _artifact(config.hot_root, "window_cache/referenced", content=b"r")
    unreferenced = _artifact(config.hot_root, "window_cache/unreferenced", content=b"u")
    audit = tmp_path / "reference.csv"
    audit.write_text(
        "classification,path\n"
        f"frozen_historical_referenced,{frozen}\n"
        f"currently_referenced,{referenced}\n"
        f"safely_unreferenced_deletion_candidate,{unreferenced}\n"
    )

    scan = scan_cache_tiers(
        config,
        reference_audit_csv=audit,
        reference_classes={
            "window_cache/frozen": "unreferenced",
            "window_cache/referenced": "unreferenced",
        },
    )
    classes = {record.relative_path: record.reference_class for record in list_artifacts(config)}
    plan = build_cache_tier_plan(
        config,
        include_deletions=False,
        hot_free_bytes=config.hot_safety_reserve_bytes,
    )

    assert classes == {
        "window_cache/frozen": "frozen",
        "window_cache/referenced": "referenced",
        "window_cache/unreferenced": "unreferenced",
    }
    assert scan["reference_audit"]["sha256"] == plan["reference_provenance"]["sha256"]

    audit.write_text(audit.read_text() + "unknown_manual_review,/tmp/not-in-hot\n")
    with pytest.raises(CacheTierValidationError, match="drifted since scan"):
        build_cache_tier_plan(config, include_migrations=False)


def test_project_wide_reference_audit_ignores_paths_outside_hot_tier(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    hot = _artifact(config.hot_root, "window_cache/a", content=b"cache")
    outside = tmp_path / "repo" / ".pytest_cache"
    outside.mkdir(parents=True)
    audit = tmp_path / "reference.csv"
    audit.write_text(
        "classification,path\n"
        f"currently_referenced,{hot}\n"
        f"safely_unreferenced_deletion_candidate,{outside}\n"
    )

    result = scan_cache_tiers(config, reference_audit_csv=audit)

    assert list_artifacts(config)[0].reference_class == "referenced"
    assert result["reference_audit"]["normalized_path_count"] == 1
    assert result["reference_audit"]["ignored_out_of_scope_path_count"] == 1


def test_nested_unreferenced_audit_never_marks_parent_unreferenced(tmp_path: Path) -> None:
    config = _config(tmp_path)
    parent = _artifact(config.hot_root, "replay_dag/namespace", content=b"parent")
    nested = parent / "child"
    nested.mkdir()
    audit = tmp_path / "reference.csv"
    audit.write_text(
        "classification,path\n"
        f"safely_unreferenced_deletion_candidate,{nested}\n"
    )

    scan_cache_tiers(config, reference_audit_csv=audit)

    assert list_artifacts(config)[0].reference_class == "unknown"


def test_loader_child_access_coalesces_to_scanned_parent_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    parent = _artifact(config.hot_root, "replay_dag/namespace", content=b"parent")
    scan_cache_tiers(config)
    child = _artifact(parent, "day/identity", content=b"child")
    _env(monkeypatch, config)
    record = record_cache_access(child, cache_root=config.hot_root, strict=True)

    assert record is not None
    assert record.relative_path == "replay_dag/namespace"
    assert record.access_count == 1
    assert [row.relative_path for row in list_artifacts(config)] == [
        "replay_dag/namespace"
    ]
    build_cache_tier_plan(config)


def test_scan_preserves_narrower_managed_descendant_instead_of_parent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    child = _artifact(
        config.hot_root,
        "window_cache/components_v2/market_context/btcusdc/2026-04-18/hash",
        content=b"child",
    )
    _env(monkeypatch, config)
    record_cache_access(child, cache_root=config.hot_root, strict=True)

    result = scan_cache_tiers(config)

    assert [row.relative_path for row in list_artifacts(config)] == [
        "window_cache/components_v2/market_context/btcusdc/2026-04-18/hash"
    ]
    assert result["skipped_managed_parent_artifacts"] == {
        "window_cache/components_v2": [
            "window_cache/components_v2/market_context/btcusdc/2026-04-18/hash"
        ]
    }
    build_cache_tier_plan(config)


def test_scan_discovers_nested_manifest_artifact_boundary(tmp_path: Path) -> None:
    config = _config(tmp_path)
    parent = _artifact(config.hot_root, "window_cache/components_v2", content=b"parent")
    child = _artifact(parent, "market_context/day/hash", content=b"child")
    (child / "manifest.json").write_text(json.dumps({"identity_sha256": "a" * 64}))

    scan_cache_tiers(config)

    record = list_artifacts(config)[0]
    assert record.relative_path == "window_cache/components_v2/market_context/day/hash"
    assert record.identity_sha256 == "a" * 64


def test_scan_keeps_hot_copy_when_tiers_share_relative_path_and_identity(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    relative = "replay_dag/native_exchange_book_hour_v1/exchange/symbol/day/00"
    hot = _artifact(config.hot_root, relative, content=b"same-payload")
    cold = _artifact(config.cold_root, relative, content=b"same-payload")
    identity = "a" * 64
    (hot / "manifest.json").write_text(
        json.dumps({"identity_sha256": identity, "data_path": str(hot / "payload.bin")})
    )
    (cold / "manifest.json").write_text(
        json.dumps({"identity_sha256": identity, "data_path": str(cold / "payload.bin")})
    )

    result = scan_cache_tiers(config)

    record = list_artifacts(config)[0]
    assert record.relative_path == relative
    assert record.tier == "hot"
    assert result["identical_identity_tier_duplicates"] == {
        relative: [str(hot), str(cold)]
    }


def test_plan_excludes_existing_cold_destination_with_different_fingerprint(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, reserve=100, target=150)
    duplicate_relative = "replay_dag/native_exchange_book_hour_v1/exchange/symbol/day/00"
    hot = _artifact(
        config.hot_root,
        duplicate_relative,
        content=b"same-payload",
        mtime_ns=1,
    )
    cold = _artifact(config.cold_root, duplicate_relative, content=b"same-payload")
    identity = "a" * 64
    (hot / "manifest.json").write_text(
        json.dumps({"identity_sha256": identity, "data_path": str(hot / "payload.bin")})
    )
    (cold / "manifest.json").write_text(
        json.dumps({"identity_sha256": identity, "data_path": str(cold / "payload.bin")})
    )
    os.utime(hot, ns=(1, 1))
    _artifact(
        config.hot_root,
        "window_cache/migratable",
        content=b"x" * 100,
        mtime_ns=2,
    )
    scan_cache_tiers(config)

    plan = build_cache_tier_plan(
        config,
        include_deletions=False,
        hot_free_bytes=50,
        cold_free_bytes=10_000,
    )

    assert [row["relative_path"] for row in plan["migrations"]] == [
        "window_cache/migratable"
    ]
    assert plan["migration_exclusions"] == [
        {
            "relative_path": duplicate_relative,
            "reason": "cold_destination_content_conflict",
            "detail": (
                "cold destination already exists with a different physical fingerprint: "
                f"{cold}"
            ),
        }
    ]


def test_scan_preserves_published_parent_symlink_as_artifact_boundary(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    relative = "window_cache/paired_action_resolution_mechanics_v1"
    cold_parent = config.cold_root / relative
    child = _artifact(cold_parent, "day/03/hash", content=b"child")
    (child / "manifest.json").write_text(
        json.dumps({"identity_sha256": "a" * 64})
    )
    hot_parent = config.hot_root / relative
    hot_parent.parent.mkdir(parents=True, exist_ok=True)
    hot_parent.symlink_to(cold_parent)

    result = scan_cache_tiers(config)

    records = list_artifacts(config)
    assert [record.relative_path for record in records] == [relative]
    assert records[0].tier == "cold"
    assert result["identical_identity_tier_duplicates"] == {}


def test_recursive_managed_root_and_nested_reports_fail_scan_closed(tmp_path: Path) -> None:
    config = _config(tmp_path)
    artifact = _artifact(config.hot_root, "window_cache/a", content=b"cache")
    reports = artifact / "reports"
    reports.mkdir()
    (reports / "result.json").write_text("{}")

    with pytest.raises(CacheTierValidationError, match="forbidden or recursive"):
        scan_cache_tiers(config)


def test_first_row_db_commit_failure_restores_hot_and_ledger(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path, reserve=100, target=150)
    hot = _artifact(config.hot_root, "window_cache/a", content=b"x" * 100)
    scan_cache_tiers(config)
    plan = _migration_plan(config)

    def fail_commit(connection: object) -> None:
        del connection
        raise OSError("synthetic DB commit failure")

    monkeypatch.setattr(cache_lru, "_commit_connection", fail_commit)
    receipt = apply_cache_tier_plan(
        plan,
        config=config,
        operation="migrate",
        owner_token=owner_token_for_plan(plan, "migrate"),
        execute=True,
        receipt_path=tmp_path / "failure-receipt.json",
    )

    assert receipt["status"] == "failed"
    assert receipt["completed_count"] == 0
    assert hot.is_dir() and not hot.is_symlink()
    assert not (config.cold_root / "window_cache/a").exists()
    assert list_artifacts(config)[0].tier == "hot"


def test_later_row_db_failure_yields_reconcilable_partial_without_divergence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path, reserve=100, target=250)
    first = _artifact(config.hot_root, "window_cache/first", content=b"1" * 100, mtime_ns=1)
    second = _artifact(config.hot_root, "window_cache/second", content=b"2" * 100, mtime_ns=2)
    scan_cache_tiers(config)
    plan = _migration_plan(config)
    original_commit = cache_lru._commit_connection
    calls = 0

    def fail_second_commit(connection: object) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("synthetic second DB commit failure")
        original_commit(connection)

    monkeypatch.setattr(cache_lru, "_commit_connection", fail_second_commit)
    receipt_path = tmp_path / "partial-receipt.json"
    receipt = apply_cache_tier_plan(
        plan,
        config=config,
        operation="migrate",
        owner_token=owner_token_for_plan(plan, "migrate"),
        execute=True,
        receipt_path=receipt_path,
    )
    rows = {record.relative_path: record for record in list_artifacts(config)}

    assert receipt["status"] == "partial"
    assert receipt["reconcilable_partial_state"] is True
    assert receipt["completed_count"] == 1
    assert json.loads(receipt_path.read_text())["status"] == "partial"
    assert first.is_symlink()
    assert rows["window_cache/first"].tier == "cold"
    assert second.is_dir() and not second.is_symlink()
    assert rows["window_cache/second"].tier == "hot"
    assert not (config.cold_root / "window_cache/second").exists()


def test_delete_db_commit_failure_restores_cold_data_and_hot_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path, reserve=100, target=150, ttl_days=1)
    hot = _artifact(config.hot_root, "window_cache/a", content=b"x" * 100)
    scan_cache_tiers(config, reference_classes={"window_cache/a": "unreferenced"})
    _env(monkeypatch, config)
    assert record_cache_access(hot, cache_root=config.hot_root, strict=True) is not None
    migration = _migration_plan(config)
    apply_cache_tier_plan(
        migration,
        config=config,
        operation="migrate",
        owner_token=owner_token_for_plan(migration, "migrate"),
        execute=True,
    )
    record = list_artifacts(config)[0]
    deletion = build_cache_tier_plan(
        config,
        include_migrations=False,
        now_ns=max(
            int(record.cold_admitted_ns or 0),
            int(record.last_explicit_access_ns or 0),
        )
        + config.cold_ttl_ns
        + 1,
    )

    def fail_commit(connection: object) -> None:
        del connection
        raise OSError("synthetic delete commit failure")

    monkeypatch.setattr(cache_lru, "_commit_connection", fail_commit)
    receipt = apply_cache_tier_plan(
        deletion,
        config=config,
        operation="delete",
        owner_token=owner_token_for_plan(deletion, "delete"),
        execute=True,
    )

    assert receipt["status"] == "failed"
    assert hot.is_symlink()
    assert (config.cold_root / "window_cache/a" / "payload.bin").read_bytes() == b"x" * 100
    assert list_artifacts(config)[0].tier == "cold"
