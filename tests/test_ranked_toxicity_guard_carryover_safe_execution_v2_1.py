from __future__ import annotations

import json
import pickle
from pathlib import Path
from unittest.mock import patch

import pytest
import numpy as np
import pandas as pd

from research.families.f09_campaign_action_uplift.audit import (
    ranked_toxicity_guard_carryover_safe_execution_v2_1 as successor,
)


ROOT = Path(__file__).resolve().parents[1]
SPEC = (
    ROOT
    / "research/families/f09_campaign_action_uplift/docs/"
    "causal_v12_ranked_toxicity_exposure_guard_carryover_safe_v2_1_execution_spec_20260803.json"
)


def test_successor_binds_current_read_only_cache_contract() -> None:
    with pytest.raises(ValueError, match="authoritative_tick_replay SHA256 mismatch"):
        successor.load_spec(SPEC)
    spec = json.loads(SPEC.read_text())
    predecessor = successor._load_predecessor(spec)
    audit = successor._validate_cache_contract(spec)
    assert predecessor["family_id"].endswith("carryover_safe_v2")
    assert audit["legacy_window_cache_version"] == 13
    assert audit["window_data_materialization"] == "ephemeral"
    assert audit["action_dependent_replay_state_materialization"] == "forbidden"


def test_successor_forces_all_window_cache_writes_off() -> None:
    with patch.object(
        successor.legacy,
        "_configure_params",
        return_value={
            "window_cache_write_enabled": True,
            "legacy_monolithic_window_cache_write_enabled": True,
            "legacy_component_v1_write_enabled": True,
        },
    ):
        params = successor._configure_read_only_params({}, "2026-04-17")
    assert params["window_cache_write_enabled"] is False
    assert params["legacy_monolithic_window_cache_write_enabled"] is False
    assert params["legacy_component_v1_write_enabled"] is False


def test_v2_smoke_parity_is_exact_and_fail_closed() -> None:
    expected = json.loads(SPEC.read_text())["smoke_contract"]["expected_v2_counts"]
    successor.assert_v2_smoke_parity(expected, expected)
    changed = json.loads(json.dumps(expected))
    changed["SELL"]["carryover_transitions"] += 1
    with pytest.raises(successor.SmokeParityContractViolation):
        successor.assert_v2_smoke_parity(changed, expected)


def test_cache_tree_identity_detects_new_entry(tmp_path: Path) -> None:
    before = successor._identity(tmp_path)
    (tmp_path / "unexpected-cache.bin").write_bytes(b"x")
    after = successor._identity(tmp_path)
    assert before != after


def test_frozen_v2_files_remain_predecessor_artifacts() -> None:
    spec = json.loads(SPEC.read_text())
    predecessor_path = ROOT / spec["predecessor_v2"]["path"]
    implementation_path = (
        ROOT
        / spec["artifact_identities"]["frozen_v2_implementation_record"]["path"]
    )
    assert successor.legacy.sha256_file(predecessor_path) == spec["predecessor_v2"][
        "sha256"
    ]
    assert successor.legacy.sha256_file(implementation_path) == spec[
        "artifact_identities"
    ]["frozen_v2_implementation_record"]["sha256"]


def test_current_loader_reads_legacy_v13_windowdata_without_publishing(
    tmp_path: Path,
) -> None:
    from models import data_windows

    path = tmp_path / "btcusdc_2026-04-17_tick_window_v13_test.pkl"
    expected = data_windows.WindowData(
        trades=pd.DataFrame({"price": [1.0]}),
        var_ts_ms=np.asarray([1], dtype=np.int64),
        var_ssq=np.asarray([1.0]),
        var_ti=None,
        var_retsq=None,
        bbo_data=None,
        l2_data=None,
    )
    with path.open("wb") as handle:
        pickle.dump(expected, handle, protocol=pickle.HIGHEST_PROTOCOL)
    before = successor._identity(tmp_path)
    observed = data_windows._load_cached_window(path)
    after = successor._identity(tmp_path)
    assert isinstance(observed, data_windows.WindowData)
    assert observed.trades.equals(expected.trades)
    assert before == after
