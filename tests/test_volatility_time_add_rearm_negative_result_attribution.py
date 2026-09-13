from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from research.families.f09_campaign_action_uplift.audit import (
    volatility_time_add_rearm_negative_result_attribution as attribution,
)
from strategy.fill_cooldown import (
    LINEAGE_CANDIDATE_ACTION,
    LINEAGE_CONTROL_ACTION,
)


def _diagnostic_spec() -> dict:
    return {
        "schema_version": attribution.SCHEMA_VERSION,
        "mode": "diagnostic_only_post_result",
        "pre_registered": False,
        "bootstrap": {"draws": 100, "seed": 7},
        "permissions": {
            "validation_read": False,
            "sealed_holdout_read": False,
            "ranking_or_selection_authorized": False,
            "action_experiment_authorized": False,
            "live_deployment_authorized": False,
        },
    }


def test_post_result_spec_cannot_grant_authority(tmp_path: Path) -> None:
    spec = _diagnostic_spec()
    spec["permissions"]["validation_read"] = True
    path = tmp_path / "spec.json"
    path.write_text(json.dumps(spec), encoding="utf-8")

    with pytest.raises(ValueError, match="exceeds authority"):
        attribution.load_spec(path)


def test_storage_probe_accepts_a_not_yet_created_output_tree(tmp_path: Path) -> None:
    missing = tmp_path / "new_family" / "development"

    assert attribution.existing_storage_probe(missing) == tmp_path


def test_value_bridge_contrasts_preserve_pre_assignment_component() -> None:
    rows = []
    for day_index, day in enumerate(("2026-01-01", "2026-01-02")):
        for action, shift in (
            (LINEAGE_CONTROL_ACTION, 0.0),
            (LINEAGE_CANDIDATE_ACTION, 1.0),
        ):
            reward = 2.0 * shift
            pre = 3.0 * shift
            post = 5.0 * shift
            rows.append(
                {
                    "day": day,
                    "side": "BUY",
                    "action": action,
                    "behavior_propensity": 0.5,
                    "reward": reward,
                    "terminal_campaign_pnl": pre + reward + post,
                    "pre_assignment_campaign_pnl": pre,
                    "post_lineage_continuation_value": post,
                    "decision_to_campaign_terminal_value": reward + post,
                    "fill_value": reward,
                    "campaign_cost_avoidance": 0.0,
                    "queue_cost_avoidance": 0.0,
                    "inventory_layer": "1",
                    "variance_regime": "<0.5x",
                    "day_index": day_index,
                }
            )
    panel = pd.DataFrame(rows)

    bridge, strata = attribution.build_contrasts(panel, _diagnostic_spec())
    lookup = bridge.set_index(["side", "metric"])["uplift"]

    assert lookup[("BUY", "pre_assignment_campaign_pnl")] == pytest.approx(3.0)
    assert lookup[("BUY", "reward")] == pytest.approx(2.0)
    assert lookup[("BUY", "post_lineage_continuation_value")] == pytest.approx(5.0)
    assert lookup[("BUY", "terminal_campaign_pnl")] == pytest.approx(10.0)
    assert lookup[("BUY", "decision_to_campaign_terminal_value")] == pytest.approx(7.0)
    assert not strata.empty
    assert strata["pointwise_interval_only"].all()


def test_baseline_mechanics_match_is_never_transport_authority(
    tmp_path: Path,
) -> None:
    panel = pd.DataFrame(
        {
            "day": ["2026-01-01", "2026-01-01"],
            "side": ["BUY", "SELL"],
            "decision_ts_ms": [1000, 2000],
        }
    )
    mechanics = pd.DataFrame(
        {
            "day": ["2026-01-01"],
            "side": ["BUY"],
            "lineage_fill_ts_ms": [1000],
        }
    )
    path = tmp_path / "mechanics.parquet"
    mechanics.to_parquet(path, index=False)
    spec = {
        "input_identity": {
            "full_path_mechanical_panel": {"path": str(path), "sha256": "unused"}
        }
    }

    result = attribution.exact_path_match_coverage(panel, spec)

    assert result["matched_rows"] == 1
    assert result["match_rate"] == pytest.approx(0.5)
    assert result["transport_allowed"] is False
