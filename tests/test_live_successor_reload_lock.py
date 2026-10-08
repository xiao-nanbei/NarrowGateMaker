from pathlib import Path
from unittest.mock import Mock

import pytest

from live import config as live_config


def test_reload_accepts_comment_only_replacement(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "config.yaml"
    path.write_text("multi_market: {}\n")
    previous = live_config._load_config_candidate(path)
    engine = Mock()
    monkeypatch.setattr(live_config, "_cfg", previous)
    monkeypatch.setattr(live_config, "_cfg_path", path)
    monkeypatch.setattr(live_config, "_engine_ref", engine)
    path.write_text("# edited documentation\nmulti_market: {}\n")
    live_config.reload_config()
    engine.on_config_reload.assert_called_once_with(live_config._cfg)
    assert live_config._cfg is not previous


@pytest.mark.parametrize("field", ["global_flow_shadow_enabled", "global_reference_shadow_enabled"])
def test_reload_rejects_restart_only_semantic_change(tmp_path: Path, monkeypatch, field) -> None:
    path = tmp_path / "config.yaml"
    path.write_text("multi_market: {}\n")
    previous = live_config._load_config_candidate(path)
    engine = Mock()
    monkeypatch.setattr(live_config, "_cfg", previous)
    monkeypatch.setattr(live_config, "_cfg_path", path)
    monkeypatch.setattr(live_config, "_engine_ref", engine)
    path.write_text(f"multi_market:\n  {field}: true\n")
    live_config.reload_config()
    assert live_config._cfg is previous
    engine.on_config_reload.assert_not_called()
