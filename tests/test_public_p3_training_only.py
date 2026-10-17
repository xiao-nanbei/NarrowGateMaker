import json

import pandas as pd
import pytest

from research.families.f02_empirical_p3_touch.audit.public_input_calibration import fit, validate_plan
from research.families.f03_causal_13_head.time_weighted_evaluation import TRAIN_DAYS


def plan():
    return {"source_profile": "tardis_only", "symbol": "BTCUSDC", "fit_days": list(TRAIN_DAYS),
            "observation_profile": {"profile_id": "synthetic_test", "clock_policy": "source_timestamp_proxy",
                "market_delay_ns": 0, "processing_ns": 0, "allowed_lateness_ns": 0,
                "max_book_age_ns": 1_000_000_000, "trade_coverage": "observed"}}


def test_rejects_heldout_or_missing_training_days():
    value = plan()
    validate_plan(value)
    value["fit_days"][1] = "2025-08-02"
    with pytest.raises(ValueError, match="predeclared"):
        validate_plan(value)


def test_fit_reads_only_declared_days_and_refuses_replacement(tmp_path, monkeypatch):
    value = plan()
    value["facts_root"] = str(tmp_path)
    source = tmp_path/"source"
    source.mkdir()
    (source/"manifest.json").write_text("{}")
    from data.facts import _digest
    sources = [{"day": "source", "manifest_sha256": _digest(source/"manifest.json")}]
    monkeypatch.setattr("research.families.f02_empirical_p3_touch.audit.public_input_calibration.bundle_paths",
                        lambda *a, **kw: [source])
    for day in TRAIN_DAYS:
        start = int(pd.Timestamp(day, tz="UTC").value)
        (tmp_path / (day + ".json")).write_text(json.dumps({
            "schema": "p3.public_input_day.v1", "day": day, "plan": value,
            "profile": value["observation_profile"], "source_bundles": sources,
            "BUY": [2.0, None], "SELL": [3.0, None],
            "decision_ns": [start, start+10_000_000_000],
            "actual_outcome_end_ns": [start+10_000_000_000, start+20_000_000_000]}))
    (tmp_path / "2025-08-02.json").write_text("not read")
    target = tmp_path / "model.json"
    metadata = fit(value, tmp_path, target)
    assert metadata["fit_days"] == list(TRAIN_DAYS)
    assert metadata["validation_days"] == metadata["test_days"] == []
    assert len(metadata["daily_inputs"]) == 100
    with pytest.raises(FileExistsError):
        fit(value, tmp_path, target)
    first = tmp_path / (TRAIN_DAYS[0]+".json")
    row = json.loads(first.read_text())
    row["source_bundles"][0]["manifest_sha256"] = "wrong-source"
    first.write_text(json.dumps(row))
    with pytest.raises(ValueError, match="source binding"):
        fit(value, tmp_path, tmp_path / "rejected-source.json")
    row["source_bundles"] = sources
    row["profile"]["market_delay_ns"] = 100
    first.write_text(json.dumps(row))
    with pytest.raises(ValueError, match="delivery profile"):
        fit(value, tmp_path, tmp_path / "rejected-profile.json")
    row["profile"] = value["observation_profile"]
    row["actual_outcome_end_ns"][0] += 86_400_000_000_000
    first.write_text(json.dumps(row))
    with pytest.raises(ValueError, match="outcome crosses"):
        fit(value, tmp_path, tmp_path / "rejected.json")
