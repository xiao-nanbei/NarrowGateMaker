"""Real small-account plan/run/pause/restore tests; no profitability claim."""
from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys

import pytest

from narrowgate.cli import main
from narrowgate.replay import account, plan, runs
from tests.test_optional_update_branch import synthetic_parent  # noqa: F401
from tests.test_defense_urgency_execution import options
from tests.test_tick_runtime_checkpoint import assert_same
from execution.chunked_parquet_journal import iter_chunked_parquet_journal


@pytest.fixture
def run_plan(synthetic_parent, tmp_path):  # noqa: F811
    prepared, base = synthetic_parent
    cfg = options(prepared, base, tmp_path / "run", "B0")
    funding = tmp_path / "funding.json"
    funding.write_text(json.dumps(cfg["funding"]))
    params = cfg["common_params"]
    params["ml_enabled"] = False
    value = dict(schema=plan.SCHEMA, run_id="synthetic-engineering",
                 identity={k: cfg["spec"][k] for k in
                           ("experiment_id", "baseline_id", "arm_id",
                            "account_contract", "parent_id")},
                 bundle=str(prepared.inputs["bundle"].root), params=params,
                 model_dir=None, funding=str(funding),
                 accounting=dict(initial_capital=cfg["initial_capital"],
                                 max_mark_age_ns=cfg["max_mark_age_ns"]),
                 output=str(tmp_path / "run"))
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(value))
    return path, json.loads(path.read_text())


def test_cli_plan_complete_real_account_and_full_report(run_plan, capsys):
    path, value = run_plan
    assert main(["replay", "plan", "--config", str(path)]) == 0
    resolved = json.loads(capsys.readouterr().out)
    assert not Path(value["output"]).exists()
    assert resolved["binding"]["params"] == value["params"]
    assert resolved["profile"] == "consumer_bundle_json_single_account"
    assert main(["replay", "run", "--plan", str(path)]) == 0
    capsys.readouterr()
    root = Path(value["output"])
    assert main(["runs", "report", str(root)]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "COMPLETE"
    assert report["economic_complete"] and not report["economic_admission"]
    assert {"result.json", "accounting.json", "daily-ledger.json", "diagnostics.json"} <= report["artifacts"].keys()
    assert "pnl_per_btc" in report["metrics"] and "pnl_bps" in report["metrics"]
    assert runs.run(path)["status"] == "COMPLETE"
    assert runs.resume(root)["status"] == "COMPLETE"
    # A short terminal day remains one ledger slice, not an empty ledger.
    ledger = json.loads((root / "daily-ledger.json").read_text())
    assert len(ledger["rows"]) == 1
    assert ledger["rows"][0]["end_ns"] == resolved["binding"]["account_end_ns"]
    assert runs.request_pause(root)["status"] == "COMPLETION_PRESENT"
    (root / "metrics.json").write_text("{}")
    assert main(["runs", "report", str(root)]) == 2
    assert "payload changed" in capsys.readouterr().out


@pytest.mark.parametrize("change, message", [
    ({"unexpected": True}, "expected fields"),
    ({"schema": "older"}, "unsupported"),
    ({"model_dir": "absent-models"}, "directory missing"),
    ({"run_id": "../other"}, "letters"),
])
def test_plan_rejects_unknown_or_inconsistent_fields(run_plan, change, message):
    path, value = run_plan
    path.write_text(json.dumps(dict(value, **change)))
    with pytest.raises(ValueError, match=message):
        plan.resolve(path)
    assert not Path(value["output"]).exists()


def test_no_funding_default_or_implicit_live_params(run_plan):
    path, value = run_plan
    broken = deepcopy(value)
    del broken["params"]["rng_seed"]
    path.write_text(json.dumps(broken))
    with pytest.raises(ValueError, match="rng_seed"):
        plan.resolve(path)
    path.write_text(json.dumps(value))
    Path(value["funding"]).write_text("{}")
    with pytest.raises((ValueError, KeyError)):
        plan.resolve(path)
    assert not Path(value["output"]).exists()

def test_existing_parameter_file_and_explicit_overrides(run_plan):
    path, value = run_plan
    base = path.with_name("baseline-params.json")
    base.write_text(json.dumps(value["params"]))
    original = plan.resolve(path)
    value["params"] = dict(base_file=base.name, overrides={})
    path.write_text(json.dumps(value))
    linked = plan.resolve(path)
    assert linked["economic_identity"] == original["economic_identity"]
    assert linked["parameter_sources"]["tick_size"].endswith("baseline-params.json#/tick_size")
    value["params"]["overrides"]["unknown_parameter"] = 1
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="unknown base keys"):
        plan.resolve(path)


def test_requested_pause_forces_durable_cut_and_new_process_resume(run_plan, monkeypatch):
    path, value = run_plan
    root = Path(value["output"])
    # Compressed operational clock only, matching existing engine fixtures.
    monkeypatch.setattr(account, "HOUR_MS", 1000)
    monkeypatch.setattr(account, "DAY_MS", 4000)
    original = account.simulate_slice
    calls = []

    def slice_with_control(*args, **kwargs):
        answer = original(*args, **kwargs)
        calls.append(answer)
        if len(calls) == 2:
            assert runs.request_pause(root)["status"] == "PAUSE_REQUESTED"
        return answer

    monkeypatch.setattr(account, "simulate_slice", slice_with_control)
    assert runs.run(path)["status"] == "PAUSED"
    assert not runs.active(root)
    state = runs.status(root)
    assert state["status"] == "PAUSED"
    assert state["progress"]["memory_cut_ms"] == state["progress"]["durable_cut_ms"]
    assert runs.report(root)["metrics"] is None
    with pytest.raises(ValueError, match="use replay resume"):
        runs.run(path)
    from models.replay.runtime_checkpoint_io import load_trusted_runtime_checkpoint
    cp = load_trusted_runtime_checkpoint(root / "latest.checkpoint")
    assert cp["cut_ts_ms"] == state["progress"]["durable_cut_ms"]
    list(iter_chunked_parquet_journal(cp["l2_prefix"]["manifest"], verify_checksum=True))

    code = """from narrowgate.replay import account
account.HOUR_MS = 1000
account.DAY_MS = 4000
from narrowgate.cli import main
import sys
raise SystemExit(main(['replay', 'resume', '--run', sys.argv[1]]))
"""
    proc = subprocess.run([sys.executable, "-B", "-c", code, str(root)],
                          text=True, capture_output=True, timeout=120)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert runs.report(root)["economic_complete"]
    monkeypatch.setattr(account, "simulate_slice", original)
    value["output"] = str(root.with_name("whole"))
    path.write_text(json.dumps(value))
    assert runs.run(path)["status"] == "COMPLETE"
    whole = Path(value["output"])
    for name in ("accounting.json", "metrics.json", "daily-ledger.json", "diagnostics.json"):
        assert json.loads((root / name).read_text()) == json.loads((whole / name).read_text())
    resumed, expected = [json.loads((p / "result.json").read_text()) for p in (root, whole)]
    ja, jb = resumed.pop("_l2_journal"), expected.pop("_l2_journal")
    assert_same(resumed, expected)
    assert list(iter_chunked_parquet_journal(ja["manifest"])) == list(iter_chunked_parquet_journal(jb["manifest"]))


def test_missing_checkpoint_never_restarts_recorded_account(run_plan, monkeypatch):
    path, value = run_plan
    root = Path(value["output"])
    monkeypatch.setattr(account, "HOUR_MS", 1000)
    monkeypatch.setattr(account, "DAY_MS", 4000)
    monkeypatch.setattr(runs, "active", lambda _: True)
    original = account.simulate_slice
    def stop(*args, **kwargs):
        runs.request_pause(root)
        return original(*args, **kwargs)
    monkeypatch.setattr(account, "simulate_slice", stop)
    assert runs.run(path)["status"] == "PAUSED"
    (root / "latest.checkpoint").unlink()
    with pytest.raises(ValueError, match="refusing fresh start"):
        runs.resume(root)


def test_resolved_plan_or_funding_change_rejected_before_restore(run_plan, monkeypatch):
    path, value = run_plan
    root = Path(value["output"])
    monkeypatch.setattr(account, "HOUR_MS", 1000)
    monkeypatch.setattr(account, "DAY_MS", 4000)
    original = account.simulate_slice
    def stop(*args, **kwargs):
        runs.request_pause(root)
        return original(*args, **kwargs)
    monkeypatch.setattr(account, "simulate_slice", stop)
    assert runs.run(path)["status"] == "PAUSED"
    value["params"]["maker_fee"] += .0001
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="changed since admission"):
        runs.resume(root)


def test_help_and_unsupported_poolrun_do_not_execute(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["replay", "run", "--help"])
    assert exc.value.code == 0
    assert "--executor {local}" in capsys.readouterr().out
    with pytest.raises(SystemExit) as exc:
        main(["replay", "run", "--plan", "does-not-exist", "--executor", "poolrun"])
    assert exc.value.code == 2
