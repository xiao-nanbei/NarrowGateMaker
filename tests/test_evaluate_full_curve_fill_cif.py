from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

from research.families.f06_placement_fill_cif.audit.evaluate_full_curve_fill_cif import evaluate


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_curve_evaluation_uses_empirical_points_without_unlocking_panels(
    tmp_path: Path,
) -> None:
    rows = []
    probabilities = {
        "closer_1tick": (0.10, 0.20),
        "current": (0.08, 0.16),
        "farther_1tick": (0.06, 0.12),
    }
    for action, values in probabilities.items():
        for horizon, probability in zip((500, 1000), values, strict=True):
            rows.append(
                {
                    "action_lifecycle_id": f"d0:{action}",
                    "cohort_id": "d0",
                    "day": "2026-01-01",
                    "side": "BUY",
                    "inventory_role": "opener",
                    "action": action,
                    "horizon_ms": horizon,
                    "probability": probability,
                    "target": int(action == "closer_1tick" and horizon == 1000),
                    "baseline_probability": 0.05,
                    "fold": 0,
                }
            )
    oof_path = tmp_path / "oof.parquet"
    pd.DataFrame(rows).to_parquet(oof_path, index=False)
    report_path = tmp_path / "report.json"
    report_path.write_text(
        json.dumps(
            {
                "family_id": "synthetic_full_curve",
                "duration_contract": {
                    "report_quantiles": {"p25": 500, "p50": 1000}
                },
                "outputs": {
                    "oof_predictions": {
                        "path": str(oof_path),
                        "sha256": _sha256(oof_path),
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    result = evaluate(
        report_path,
        bootstrap_samples=50,
        bootstrap_seed=7,
        distance_tolerance=1e-8,
    )

    assert result["empirical_horizons_ms"] == [500, 1000]
    assert result["horizon_cell_prediction_gate"] is False
    assert result["validation_read"] is False
    assert result["sealed_holdout_read"] is False
    assert result["action_or_live_authorization"] is False
    assert result["monotonicity"]["time_violations"] == 0
    assert result["monotonicity"]["distance_violations"] == 0
