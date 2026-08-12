from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from models.audit.experiment_scorecard import score_profile_contract
from research.families.f07_active_order_continuation.audit.queue_value_competing_risk import (
    ALL_HAZARDS,
    CompetingRiskBundle,
    fit_competing_risk_bundle,
    fit_feature_normalizer,
)
from research.families.f07_active_order_continuation.audit.queue_value_models import (
    NATIVE_EXCHANGE_EVENT_COLUMNS,
    fit_queue_reactive_hawkes,
)


def _panel() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    causes = (
        "favorable_fill",
        "adverse_fill",
        "cancel",
        "adverse_price_jump",
        "campaign_repair",
        "adverse_price_jump",
    )
    days = [f"2026-01-{day:02d}" for day in range(1, 21)]
    sequence = 0
    for day_index, day in enumerate(days):
        for local_index in range(48):
            sequence += 1
            side = "BUY" if local_index % 2 == 0 else "SELL"
            pressure = (local_index % 12 - 5.5) / 5.5
            cause = causes[(local_index + day_index) % len(causes)]
            markout = (
                1.0 + 0.2 * pressure
                if cause == "favorable_fill"
                else -1.5 + 0.2 * pressure
                if cause == "adverse_fill"
                else np.nan
            )
            rows.append(
                {
                    "day": day,
                    "decision_id": f"d-{sequence}",
                    "side": side,
                    "event_time_ms": 100.0 + 20.0 * (local_index % 5),
                    "interval_ms": 100.0 + 20.0 * (local_index % 5),
                    "interval_end_ts_ns": sequence * 200_000_000,
                    "first_event": cause,
                    "fill_value_markout_bps": markout,
                    "order_price": 100_000.0,
                    "price_jump_ticks": 1.0 + (local_index % 3),
                    "exchange_book_refill_count": float(
                        local_index % 4 == 0
                    ),
                    "exchange_book_cancel_count": float(
                        local_index % 5 == 0
                    ),
                    "adverse_market_order_count": float(
                        cause == "adverse_fill"
                    ),
                    "spread_ticks": 1.0 + local_index % 3,
                    "book_imbalance": pressure,
                    "queue_fraction_left": (local_index % 10) / 10.0,
                    "quote_distance_ticks": 10.0 + local_index % 20,
                    "order_age_ms": 50.0 + 10.0 * local_index,
                    "campaign_age_s": 30.0 + local_index,
                    "inventory_ratio": 0.1 + 0.02 * (local_index % 8),
                    "campaign_pnl_so_far": pressure,
                    "campaign_mae_so_far": -abs(pressure),
                    "campaign_add_count_so_far": local_index % 4,
                    "microprice_shift_bps": 0.2 * pressure,
                    "l2_book_cancel_ratio": (local_index % 5) / 5.0,
                    "l2_book_refresh_ratio": (local_index % 7) / 7.0,
                    "l2_quote_flip_rate": (local_index % 3) / 3.0,
                    "toxicity": (pressure + 1.0) / 2.0,
                    "markout_ema": -0.3 * pressure,
                }
            )
    return pd.DataFrame(rows)


def test_competing_risk_bundle_round_trip_and_candidate_budget(
    tmp_path: Path,
) -> None:
    panel = _panel()
    source_path = tmp_path / "panel.parquet"
    base_path = tmp_path / "base.json"
    split_path = tmp_path / "split.json"
    panel.to_parquet(source_path, index=False)
    queue = fit_queue_reactive_hawkes(
        panel,
        event_columns=NATIVE_EXCHANGE_EVENT_COLUMNS,
    )
    base_payload = {
        "fit_days": [f"2026-01-{day:02d}" for day in range(1, 13)],
        "internal_embargo_days": ["2026-01-13"],
        "calibration_days": [f"2026-01-{day:02d}" for day in range(14, 19)],
        "sides": {
            side: {"queue_artifact": queue.to_payload()}
            for side in ("BUY", "SELL")
        },
    }
    base_path.write_text(json.dumps(base_payload), encoding="utf-8")
    split_path.write_text('{"family_id":"test"}\n', encoding="utf-8")

    bundle, predictions, report = fit_competing_risk_bundle(
        panel,
        base_queue_bundle_payload=base_payload,
        fit_days=base_payload["fit_days"],
        internal_embargo_days=base_payload["internal_embargo_days"],
        calibration_days=base_payload["calibration_days"],
        source_panel_path=source_path,
        base_queue_bundle_path=base_path,
        evidence_split_path=split_path,
        score_profile_contract=score_profile_contract(
            "action_execution_selective_v1"
        ),
    )
    output = tmp_path / "bundle.json"
    bundle.save(output)
    loaded = CompetingRiskBundle.load(output)

    assert set(loaded.sides) == {"BUY", "SELL"}
    assert set(loaded.sides["BUY"].hazards) == set(ALL_HAZARDS)
    assert predictions["cancel_advantage_bps"].notna().all()
    for side in ("BUY", "SELL"):
        rate = loaded.sides[side].state_config.calibration_candidate_rate
        assert 0.05 <= rate <= 0.30
        assert report["sides"][side]["candidate_rate_passed"]
        row = panel[panel["side"] == side].iloc[0].to_dict()
        before = bundle.side_artifact(side).predict(row)
        after = loaded.side_artifact(side).predict(row)
        assert after.cancel_advantage_bps == pytest.approx(
            before.cancel_advantage_bps
        )


def test_competing_risk_normalizer_rejects_future_features() -> None:
    panel = _panel()
    panel["future_mid"] = 100_001.0
    with pytest.raises(ValueError, match="decision-time safe"):
        fit_feature_normalizer(panel, feature_names=("future_mid",))
