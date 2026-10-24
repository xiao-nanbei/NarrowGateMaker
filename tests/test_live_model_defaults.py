import json

import pytest
import yaml

from live import config
from live.main import _configured_model_dir


@pytest.fixture
def registered_candidate(tmp_path, monkeypatch):
    pointer = tmp_path / "current.json"
    selected = tmp_path / "candidate.yaml"
    bundle = tmp_path / "models"
    bundle.mkdir()
    (bundle / "public_input_model.json").write_text("{}")
    selected.write_text(yaml.safe_dump({"ml": {
        "model_dir": str(bundle), "feature_protocol": "execution_v1",
    }}))
    pointer.write_text(json.dumps({"config": selected.name}))
    monkeypatch.setattr(config, "LOCAL_LIVE_CANDIDATE", pointer)
    return pointer, selected, bundle


def load(tmp_path, value):
    path = tmp_path / "run.yaml"
    path.write_text(yaml.safe_dump(value))
    return config._load_config_candidate(path)


@pytest.mark.parametrize("raw", [{}, {"ml": {"enabled": True, "vol_blend": 0.3}}])
def test_live_omitted_model_uses_registered_bundle_and_protocol(
    tmp_path, registered_candidate, raw,
):
    _, _, bundle = registered_candidate
    cfg = load(tmp_path, raw)
    assert cfg.ml.model_dir == str(bundle)
    assert cfg.ml.feature_protocol == "execution_v1"
    assert _configured_model_dir(cfg) == bundle
    assert cfg._source_parsed_values == raw
    assert cfg.ml.vol_blend == raw.get("ml", {}).get("vol_blend", 0.5)


@pytest.mark.parametrize("model_dir", ["models/explicit-bundle", ""])
def test_explicit_model_never_reads_candidate(tmp_path, registered_candidate, model_dir):
    pointer, _, _ = registered_candidate
    pointer.write_text("not JSON")
    cfg = load(tmp_path, {"ml": {"model_dir": model_dir}})
    assert cfg.ml.model_dir == model_dir
    assert cfg.ml.feature_protocol == "legacy_v12"


def test_ml_off_does_not_read_candidate(tmp_path, registered_candidate):
    pointer, _, _ = registered_candidate
    pointer.write_text("not JSON")
    cfg = load(tmp_path, {"ml": {"enabled": False}})
    assert cfg.ml.model_dir == ""


def test_missing_local_registration_cannot_restore_retired_default(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "LOCAL_LIVE_CANDIDATE", tmp_path / "absent.json")
    cfg = load(tmp_path, {})
    assert cfg.ml.model_dir == ""
    with pytest.raises(ValueError, match="ml.model_dir must identify"):
        _configured_model_dir(cfg)


def test_explicit_protocol_is_not_silently_overwritten(tmp_path, registered_candidate):
    cfg = load(tmp_path, {"ml": {"feature_protocol": "legacy_v12"}})
    assert cfg.ml.feature_protocol == "legacy_v12"
    with pytest.raises(ValueError, match="bundle and live feature protocol must match"):
        _configured_model_dir(cfg)


@pytest.mark.parametrize("model", [
    {"feature_protocol": "execution_v1"},
    {"model_dir": "models/current"},
])
def test_invalid_registered_candidate_is_not_an_old_model_fallback(
    tmp_path, registered_candidate, model,
):
    _, selected, _ = registered_candidate
    selected.write_text(yaml.safe_dump({"ml": model}))
    with pytest.raises(ValueError, match="registered live candidate requires"):
        load(tmp_path, {})


def test_shared_replay_parser_never_inherits_local_live_default(registered_candidate):
    pointer, _, _ = registered_candidate
    pointer.write_text("not JSON")
    assert config._parse({}).ml.model_dir == ""


def test_candidate_selection_changes_only_newly_loaded_config(tmp_path, registered_candidate):
    _, selected, bundle = registered_candidate
    first = load(tmp_path, {})
    selected.write_text(yaml.safe_dump({"ml": {
        "model_dir": "models/next-explicit-candidate", "feature_protocol": "execution_v1",
    }}))
    second = load(tmp_path, {})
    assert first.ml.model_dir == str(bundle)
    assert second.ml.model_dir == "models/next-explicit-candidate"
