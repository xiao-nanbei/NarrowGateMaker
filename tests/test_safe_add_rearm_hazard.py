from __future__ import annotations

import numpy as np
import pandas as pd

from research.families.f09_campaign_action_uplift.audit.safe_add_rearm_hazard import (
    fit_walk_forward_scores,
    normalize_probe_frame,
    paired_uplift,
    summarize_probes,
)


def _probe_rows(days: int = 24, episodes_per_day: int = 12) -> pd.DataFrame:
    rng = np.random.default_rng(17)
    rows = []
    for day_index in range(days):
        day = f"2026-01-{day_index + 1:02d}"
        for episode in range(episodes_per_day):
            side = "BUY" if episode % 2 == 0 else "SELL"
            episode_ts = day_index * 1_000_000 + episode * 1_000
            signal = rng.normal()
            for elapsed_s, baseline_end in ((20, 0), (85, 1)):
                value = signal + (0.8 if elapsed_s == 20 and signal > 0 else 0.0)
                rows.append(
                    {
                        "day": day,
                        "side": side,
                        "episode_fill_ts_ms": episode_ts,
                        "scheduled_elapsed_ms": elapsed_s * 1000,
                        "baseline_cooldown_end": baseline_end,
                        "actionable": 1,
                        "outcome": "would_fill" if value != 0.0 else "no_fill_expiry",
                        "markout_30s_bps": value,
                        "opportunity_value_30s_bps": value,
                        "actual_elapsed_ms": elapsed_s * 1000,
                        "cooldown_total_ms": 85_000,
                        "cooldown_remaining_ms": max(0, 85_000 - elapsed_s * 1000),
                        "consecutive_units": 1.0,
                        "inventory": 0.003 if side == "BUY" else -0.003,
                        "inventory_ratio": 0.1 if side == "BUY" else -0.1,
                        "campaign_age_s": elapsed_s,
                        "campaign_max_abs_qty_so_far": 0.003,
                        "campaign_pnl_so_far": signal,
                        "campaign_adverse_excursion_so_far": min(signal, 0.0),
                        "campaign_exposure_increasing_fills_so_far": 1,
                        "campaign_reducing_fills_so_far": 0,
                        "distance_to_mid": 10.0,
                        "quote_delta_to_bbo": 9.9,
                        "toxicity": -signal,
                        "markout_ema": signal,
                        "microprice_shift_bps": signal,
                        "l2_quote_flip_rate": 0.0,
                        "l2_book_refresh_ratio": max(signal, 0.0),
                        "l2_book_cancel_ratio": max(-signal, 0.0),
                        "l2_near_depth_total": 2.0,
                        "repair_probability": 0.5 + 0.1 * np.tanh(signal),
                        "global_flow_pressure": -signal,
                        "submit_local_rank": 0.5,
                    }
                )
    return pd.DataFrame(rows)


def test_probe_summary_keeps_no_fill_in_opportunity_denominator() -> None:
    frame = normalize_probe_frame(
        pd.DataFrame(
            [
                {
                    "day": "2026-01-01",
                    "side": "BUY",
                    "episode_fill_ts_ms": 1,
                    "scheduled_elapsed_ms": 5000,
                    "baseline_cooldown_end": 0,
                    "actionable": 1,
                    "outcome": "would_fill",
                    "markout_30s_bps": 2.0,
                    "opportunity_value_30s_bps": 2.0,
                },
                {
                    "day": "2026-01-01",
                    "side": "BUY",
                    "episode_fill_ts_ms": 2,
                    "scheduled_elapsed_ms": 5000,
                    "baseline_cooldown_end": 0,
                    "actionable": 1,
                    "outcome": "no_fill_expiry",
                    "opportunity_value_30s_bps": 0.0,
                },
            ]
        )
    )
    summary = summarize_probes(frame).iloc[0]
    assert summary["would_fill_rate"] == 0.5
    assert summary["conditional_fill_markout_30s_mean_bps"] == 2.0
    assert summary["opportunity_value_30s_mean_bps"] == 1.0


def test_paired_uplift_matches_episodes_not_pooled_bins() -> None:
    frame = normalize_probe_frame(_probe_rows(days=2, episodes_per_day=4))
    paired = paired_uplift(frame)
    assert set(paired["scheduled_elapsed_s"]) == {20.0}
    assert paired["paired_episodes"].sum() == 8
    assert (paired["paired_uplift_30s_mean_bps"] >= 0.0).all()


def test_walk_forward_score_uses_future_days_only_for_evaluation() -> None:
    frame = normalize_probe_frame(_probe_rows())
    scored, metrics = fit_walk_forward_scores(
        frame,
        min_train_days=10,
        test_days=4,
        embargo_days=1,
        late_days=4,
        blocked_folds=3,
        min_train_rows=50,
        min_test_rows=20,
    )
    ok = metrics[metrics["status"] == "ok"]
    assert not scored.empty
    assert {
        "chronological",
        "blocked_day_crossfit",
        "late_holdout",
    }.issubset(set(ok["panel"]))
    chronological = ok[ok["panel"].isin({"chronological", "late_holdout"})]
    assert chronological["test_start_day"].min() > "2026-01-10"
    assert ok["high_minus_all_value_30s_bps"].median() > 0.0
