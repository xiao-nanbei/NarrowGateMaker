from __future__ import annotations

from pathlib import Path

import pytest

import live.config as live_config
from execution.order_lifecycle_journal_storage_v2 import BOUNDED_REMOTE_SPOOL
from live.config import Config, LifecycleJournalV2Config, _validate_config

EXAMPLE_REMOTE_REPO = Path("/srv/operator/NarrowGate_BTCUSDC")
EXAMPLE_REMOTE_SPOOL = EXAMPLE_REMOTE_REPO / "formal_collection"


def test_lifecycle_journal_v2_is_disabled_by_default() -> None:
    cfg = Config()
    assert cfg.lifecycle_journal_v2.enabled is False
    assert cfg.lifecycle_journal_v2.storage_profile == "local_orico_replay_admission"
    _validate_config(cfg)


def test_lifecycle_journal_v2_rejects_bad_queue_even_when_disabled() -> None:
    cfg = Config(
        lifecycle_journal_v2=LifecycleJournalV2Config(queue_size=0),
    )
    try:
        _validate_config(cfg)
    except ValueError as exc:
        assert "queue_size must be positive" in str(exc)
    else:
        raise AssertionError("invalid queue size was accepted")


def test_enabled_lifecycle_journal_requires_configured_mount(monkeypatch) -> None:
    monkeypatch.setattr(live_config.os.path, "ismount", lambda _path: False)
    cfg = Config(
        lifecycle_journal_v2=LifecycleJournalV2Config(
            enabled=True,
            baseline_identity_path="baseline.json",
            baseline_identity_sha256="a" * 64,
        )
    )
    with pytest.raises(ValueError, match="requires its storage mount"):
        _validate_config(cfg)


def test_enabled_lifecycle_journal_requires_hash_and_configured_output_roots(
    monkeypatch,
) -> None:
    monkeypatch.setattr(Path, "exists", lambda _path: True)
    monkeypatch.setattr(live_config.os.path, "ismount", lambda _path: True)
    cfg = Config(
        lifecycle_journal_v2=LifecycleJournalV2Config(
            enabled=True,
            root=str(Path("/var/tmp") / "not-admission-root"),
            baseline_identity_path="baseline.json",
            baseline_identity_sha256="a" * 64,
        )
    )
    with pytest.raises(ValueError, match="root must be inside required_mount"):
        _validate_config(cfg)

    cfg.lifecycle_journal_v2.root = LifecycleJournalV2Config().root
    cfg.lifecycle_journal_v2.baseline_identity_sha256 = ""
    with pytest.raises(ValueError, match="requires baseline_identity_sha256"):
        _validate_config(cfg)


def test_enabled_bounded_remote_spool_does_not_require_local_mount(
    monkeypatch,
) -> None:
    monkeypatch.setattr(live_config.os.path, "ismount", lambda _path: False)
    monkeypatch.setattr(Path, "exists", lambda _path: True)
    monkeypatch.setattr(Path, "is_dir", lambda _path: True)
    monkeypatch.setattr(Path, "is_symlink", lambda _path: False)
    settings = LifecycleJournalV2Config(
        enabled=True,
        storage_profile=BOUNDED_REMOTE_SPOOL,
        baseline_identity_path="baseline.json",
        baseline_identity_sha256="a" * 64,
    )
    remote_root = Path(settings.remote_spool_allowlisted_roots[0])
    settings.root = str(remote_root / "order_lifecycle_journal_v2")
    settings.prospective_epoch_root = str(remote_root / "prospective_baseline_epochs")
    cfg = Config(lifecycle_journal_v2=settings)
    _validate_config(cfg)


@pytest.mark.parametrize(
    ("root", "epoch_root", "allowlist", "match"),
    [
        (
            "relative/journal",
            str(EXAMPLE_REMOTE_SPOOL / "epochs"),
            [str(EXAMPLE_REMOTE_SPOOL)],
            "root must be absolute",
        ),
        (
            "/etc/formal_collection/journal",
            "/etc/formal_collection/epochs",
            ["/etc/formal_collection"],
            "sensitive directory",
        ),
        (
            "/srv/operator/journal",
            "/srv/operator/epochs",
            ["/srv/operator"],
            "too broad or sensitive",
        ),
        (
            str(EXAMPLE_REMOTE_REPO / "outside/journal"),
            str(EXAMPLE_REMOTE_SPOOL / "epochs"),
            [str(EXAMPLE_REMOTE_SPOOL)],
            "root must be a strict child",
        ),
        (
            str(EXAMPLE_REMOTE_SPOOL / "journal"),
            str(EXAMPLE_REMOTE_SPOOL / "journal/epochs"),
            [str(EXAMPLE_REMOTE_SPOOL)],
            "distinct siblings",
        ),
    ],
)
def test_bounded_remote_spool_rejects_unsafe_paths(
    root: str,
    epoch_root: str,
    allowlist: list[str],
    match: str,
) -> None:
    cfg = Config(
        lifecycle_journal_v2=LifecycleJournalV2Config(
            storage_profile=BOUNDED_REMOTE_SPOOL,
            root=root,
            prospective_epoch_root=epoch_root,
            remote_spool_allowlisted_roots=allowlist,
        )
    )
    with pytest.raises(ValueError, match=match):
        _validate_config(cfg)


def test_bounded_remote_spool_requires_finite_session_bounds() -> None:
    settings = LifecycleJournalV2Config(
        storage_profile=BOUNDED_REMOTE_SPOOL,
        root=str(EXAMPLE_REMOTE_SPOOL / "journal"),
        prospective_epoch_root=str(EXAMPLE_REMOTE_SPOOL / "epochs"),
        remote_spool_allowlisted_roots=[str(EXAMPLE_REMOTE_SPOOL)],
        remote_session_max_duration_s=0.0,
    )
    with pytest.raises(ValueError, match="max_duration_s"):
        _validate_config(Config(lifecycle_journal_v2=settings))
