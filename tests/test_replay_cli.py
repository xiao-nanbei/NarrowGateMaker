"""The installed replay command does not dispatch historical input paths."""

import json

import pytest

from models.replay.cli import main


@pytest.mark.parametrize("arguments", [[], ["--day", "2025-08-01"],
    ["--data-bundle", "bundle", "--bbo-dir", "old"],
    ["--data-bundle", "bundle", "--sweep"]])
def test_retired_replay_options_are_not_accepted(arguments):
    with pytest.raises(SystemExit) as exc:
        main(arguments)
    assert exc.value.code == 2


def test_explicit_bundle_reaches_single_production_entry(tmp_path, monkeypatch):
    import models.backtest_tick as replay

    calls = []
    def simulate(root, params, *, signal_engine):
        calls.append((root, params, signal_engine))
        return {"net_pnl": -2.0, "public_input_contract": {"schema": "test"}}

    monkeypatch.setattr(replay, "simulate_public_inputs", simulate)
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"params": {"ml_enabled": False}}))
    output = tmp_path / "result.json"
    bundle = tmp_path / "bundle"
    assert main(["--data-bundle", str(bundle), "--data-replay-config", str(config),
                 "--summary-json", str(output)]) == 0
    assert calls == [(bundle, {"ml_enabled": False}, None)]
    assert json.loads(output.read_text())["net_pnl"] == -2.0


def test_old_replay_dispatcher_is_absent():
    import models.backtest_tick as replay

    assert not hasattr(replay, "run_cli")
