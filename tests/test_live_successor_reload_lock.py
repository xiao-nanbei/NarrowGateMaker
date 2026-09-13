from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from live import config as live_config


def test_successor_reload_rejects_candidate_sha_toctou_before_engine_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "successor.yaml"
    startup_bytes = b"strategy: {}\n"
    config_path.write_bytes(startup_bytes)
    startup_sha256 = hashlib.sha256(startup_bytes).hexdigest()
    previous = SimpleNamespace(identity="startup")
    engine = Mock()
    candidate = SimpleNamespace(_source_file_sha256="f" * 64)

    monkeypatch.setattr(live_config, "_cfg", previous)
    monkeypatch.setattr(live_config, "_cfg_path", config_path)
    monkeypatch.setattr(live_config, "_engine_ref", engine)
    monkeypatch.setattr(live_config, "_load_config_candidate", lambda _path: candidate)
    live_config.set_restart_only_config_sha256(startup_sha256)
    try:
        live_config.reload_config()
        assert live_config._cfg is previous
        engine.on_config_reload.assert_not_called()
    finally:
        live_config.set_restart_only_config_sha256(None)


@pytest.mark.parametrize(
    "changed_bytes",
    (
        b"spread_cap_mode: compress\n",
        b"strategy: {boolean_cooldown_evidence_route: revised_annotation}\n",
        b"strategy: {buy_e3_cooldown_evidence_route: revised_annotation}\n",
    ),
)
def test_successor_reload_allows_only_the_exact_startup_file_hash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    changed_bytes: bytes,
) -> None:
    config_path = tmp_path / "successor.yaml"
    config_path.write_bytes(b"startup")
    previous = SimpleNamespace(identity="startup")
    engine = Mock()
    monkeypatch.setattr(live_config, "_cfg", previous)
    monkeypatch.setattr(live_config, "_cfg_path", config_path)
    monkeypatch.setattr(live_config, "_engine_ref", engine)
    live_config.set_restart_only_config_sha256(hashlib.sha256(b"startup").hexdigest())
    config_path.write_bytes(changed_bytes)
    try:
        live_config.reload_config()
        assert live_config._cfg is previous
        engine.on_config_reload.assert_not_called()
    finally:
        live_config.set_restart_only_config_sha256(None)
