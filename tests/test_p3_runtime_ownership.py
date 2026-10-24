"""P3 runtime is independent of calibration and retains the research API."""

import json
import os
import pickle
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest

from narrowgate.prediction.touch_probability import TouchProbabilityModel

ROOT = Path(__file__).resolve().parents[1]
PUBLIC_P3 = ROOT / "examples/public_dry_run_model_bundle/touch_probability.json"


def test_runtime_requires_explicit_artifact_paths():
    with pytest.raises(TypeError):
        TouchProbabilityModel.load()
    with pytest.raises(TypeError):
        TouchProbabilityModel().save()


def test_research_facade_preserves_defaults_bytes_and_trusted_pickle(tmp_path, monkeypatch):
    from research.families.f02_empirical_p3_touch import touch_probability as research

    monkeypatch.setattr(research, "MODEL_DIR", tmp_path)
    model = research.TouchProbabilityModel.from_bytes(PUBLIC_P3.read_bytes())
    assert isinstance(model, TouchProbabilityModel)
    assert research.TouchProbabilityModel.prob is TouchProbabilityModel.prob
    assert research.TouchProbabilityModel.from_bytes.__func__ is TouchProbabilityModel.from_bytes.__func__
    model.save()
    loaded = research.TouchProbabilityModel.load()
    runtime = TouchProbabilityModel.load(tmp_path / "touch_probability.json")
    runtime.save(tmp_path / "explicit.json")
    assert (tmp_path / "explicit.json").read_bytes() == (tmp_path / "touch_probability.json").read_bytes()
    grid = np.linspace(-1, 250, 5001)
    np.testing.assert_array_equal(loaded.prob(grid), runtime.prob(grid))
    restored = pickle.loads(pickle.dumps(model))  # Own in-memory fixture, never external state.
    assert type(restored) is research.TouchProbabilityModel
    np.testing.assert_array_equal(restored.prob(grid), runtime.prob(grid))
    assert loaded.semantic_identity() == runtime.semantic_identity()


def test_runtime_import_and_inference_without_research_from_foreign_cwd(tmp_path):
    code = """
import importlib.abc
import json
import sys
class NoResearch(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'research' or fullname.startswith('research.'):
            raise AssertionError('runtime imported research: ' + fullname)
sys.meta_path.insert(0, NoResearch())
from narrowgate.prediction.touch_probability import TouchProbabilityModel
model = TouchProbabilityModel.load(sys.argv[1])
print(json.dumps(dict(prob=model.prob([0., 1., 10.]).tolist(),
                     identity=model.semantic_identity(),
                     pandas_loaded='pandas' in sys.modules)))
"""
    completed = subprocess.run(
        [sys.executable, "-B", "-c", code, str(PUBLIC_P3)],
        cwd=tmp_path, env={**os.environ, "PYTHONPATH": str(ROOT)},
        capture_output=True, text=True, check=True,
    )
    output = json.loads(completed.stdout)
    assert output["identity"] == TouchProbabilityModel.load(PUBLIC_P3).semantic_identity()
    assert output["pandas_loaded"] is False
    np.testing.assert_array_equal(
        output["prob"], TouchProbabilityModel.load(PUBLIC_P3).prob([0., 1., 10.]),
    )


def test_current_live_and_replay_loaders_do_not_import_p3_research(tmp_path):
    code = """
import importlib.abc
import os
import sys
class NoP3Research(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith('research.families.f02_empirical_p3_touch'):
            raise AssertionError('P3 research imported by runtime: ' + fullname)
sys.meta_path.insert(0, NoP3Research())
from pathlib import Path
from models.backtest_config import add_touch_probability_params
from strategy.maker_engine import _get_fill_model
params = add_touch_probability_params({}, model_path=Path(sys.argv[1]))
assert params['touch_probability_calibrated'] is True
model = _get_fill_model(Path(sys.argv[1]).parent)
assert model is not None
assert model.artifact_sha256 == params['touch_probability_artifact_sha256']
assert type(model).__module__ == 'narrowgate.prediction.touch_probability'
os.environ['MM_MODEL_DIR'] = str(Path(sys.argv[1]).parent)
default_model = _get_fill_model()
assert default_model.artifact_sha256 == model.artifact_sha256
"""
    subprocess.run(
        [sys.executable, "-B", "-c", code, str(PUBLIC_P3)],
        cwd=tmp_path, env={**os.environ, "PYTHONPATH": str(ROOT)},
        capture_output=True, text=True, check=True,
    )
