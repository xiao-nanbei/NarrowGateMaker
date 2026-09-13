from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from execution.chunked_parquet_journal import iter_chunked_parquet_journal
from research.families.f09_campaign_action_uplift.audit.causal_v12_ranked_toxicity_exposure_guard_full_path_adapter import (
    AdapterContractViolation,
)
from research.families.f09_campaign_action_uplift.audit.ranked_toxicity_guard_authoritative_replay_v1_5 import (
    RankedToxicityBaselineShadowCaptureV15,
    RankedToxicityGuardAuthoritativeReplayV15,
    RankedToxicityThresholdUnreadyReplayV15,
    baseline_opportunities_from_manifests,
    build_past_only_threshold_schedule_v15,
)

DAY = "2026-08-03"


def _day_start_ms(day: str = DAY) -> int:
    return int(
        datetime.fromisoformat(day)
        .replace(tzinfo=timezone.utc)
        .timestamp()
        * 1000
    )


def _prediction_kwargs(day: str = DAY) -> dict[str, float | int]:
    bucket = _day_start_ms(day) + 10_000
    return {
        "prediction_bucket_ts_ms": bucket,
        "feature_ready_ts_ms": bucket + 100,
        "observation_ts_ms": bucket + 200,
        "tox_bid": 0.9,
        "tox_ask": 0.1,
    }


def _quote_kwargs(day: str = DAY) -> dict[str, object]:
    prediction = _prediction_kwargs(day)
    return {
        "decision_id": f"BTCUSDC:{day}:decision-1:BUY",
        "decision_ts_ns": int(prediction["observation_ts_ms"]) * 1_000_000,
        "side": "BUY",
        "role": "opener",
        "baseline_eligible": True,
        "exposure_increasing": True,
        "can_post": True,
        "allow_exposure_increase": True,
        "active_exposure_order_id": "",
        "quote_price": 100.0,
        "quote_quantity": 0.001,
        "blocker_reasons": (),
        "policy_fingerprint": "c" * 64,
        "untreated_lineage_ordinal": 1,
    }


def _write_baseline_tape(tmp_path, *, day: str = DAY):
    baseline_dir = tmp_path / f"baseline-{day}"
    capture = RankedToxicityBaselineShadowCaptureV15(
        output_dir=baseline_dir,
        lineage_namespace=f"{day}|panel-1",
        sides=("BUY",),
        chunk_rows=1,
    )
    capture.on_prediction_bucket(**_prediction_kwargs(day))
    quote = _quote_kwargs(day)
    capture.on_quote_decision(**quote)
    capture.on_final_quote_action(
        decision_id=str(quote["decision_id"]),
        side="BUY",
        role="opener",
        exposure_increasing=True,
        candidate_action="place",
        candidate_price=100.0,
        candidate_quantity=0.001,
        candidate_order_id="",
        event_ts_ns=int(quote["decision_ts_ns"]),
    )
    capture.finish_replay(event_ts_ns=int(quote["decision_ts_ns"]))
    return baseline_dir / "manifest.json", quote


def test_v15_baseline_tape_binds_held_prediction_to_exact_opportunity(tmp_path) -> None:
    manifest, _ = _write_baseline_tape(tmp_path)

    rows = list(iter_chunked_parquet_journal(manifest))
    held = rows[0]["record"]["held_prediction"]
    assert held["toxicity_score"] == pytest.approx(0.9)
    assert held["prediction_bucket_ts_ms"] == _day_start_ms() + 10_000

    opportunities = baseline_opportunities_from_manifests({DAY: manifest})
    assert opportunities.to_dict("records") == [
        {
            "day": DAY,
            "side": "BUY",
            "role": "opener",
            "decision_id": f"BTCUSDC:{DAY}:decision-1:BUY",
            "decision_ts_ns": (_day_start_ms() + 10_200) * 1_000_000,
            "prediction_bucket_ts_ms": _day_start_ms() + 10_000,
            "feature_ready_ts_ms": _day_start_ms() + 10_100,
            "toxicity_score": 0.9,
        }
    ]


def test_v15_prediction_warmup_is_exact_no_treatment_passthrough(tmp_path) -> None:
    baseline_dir = tmp_path / "baseline-warmup"
    capture = RankedToxicityBaselineShadowCaptureV15(
        output_dir=baseline_dir,
        lineage_namespace=f"{DAY}|panel-1",
        sides=("BUY",),
        chunk_rows=1,
    )
    quote = {
        **_quote_kwargs(),
        "decision_id": f"BTCUSDC:{DAY}:preprediction:BUY",
        "decision_ts_ns": (_day_start_ms() + 100) * 1_000_000,
    }
    capture.on_quote_decision(**quote)
    capture.on_final_quote_action(
        decision_id=str(quote["decision_id"]),
        side="BUY",
        role="opener",
        exposure_increasing=True,
        candidate_action="place",
        candidate_price=100.0,
        candidate_quantity=0.001,
        candidate_order_id="",
        event_ts_ns=int(quote["decision_ts_ns"]),
    )
    capture.finish_replay(event_ts_ns=int(quote["decision_ts_ns"]))
    manifest = baseline_dir / "manifest.json"
    rows = list(iter_chunked_parquet_journal(manifest))
    assert rows[0]["record"]["held_prediction"] is None
    assert rows[0]["prediction_unready"] is True

    replay = RankedToxicityGuardAuthoritativeReplayV15(
        baseline_manifest_path=manifest,
        output_root=tmp_path / "candidate-warmup",
        frozen_model_sha256="a" * 64,
        threshold_schedule={"BUY": {DAY: (0.5, "b" * 64)}},
        sides=("BUY",),
        chunk_rows=1,
    )
    replay.validate_replay_start(
        params={"dynamic_fill_hazard_action_enabled": False},
        ml_data=([], [], [], [], [], []),
    )
    directive = replay.on_quote_decision(**quote)
    assert directive.allow_exposure_submission is True
    assert directive.request_cancel_once is False
    replay.on_final_quote_action(
        decision_id=str(quote["decision_id"]),
        side="BUY",
        role="opener",
        exposure_increasing=True,
        candidate_action="place",
        candidate_price=100.0,
        candidate_quantity=0.001,
        candidate_order_id="",
        event_ts_ns=int(quote["decision_ts_ns"]),
    )
    audit = replay.finish_replay(event_ts_ns=int(quote["decision_ts_ns"]))
    assert audit["preprediction_passthrough_decision_count"] == 1
    assert audit["baseline_shadow"]["complete"] is True


def test_v15_threshold_schedule_is_strictly_past_only_and_side_specific() -> None:
    start = datetime(2026, 4, 1, tzinfo=timezone.utc)
    days = [(start + timedelta(days=index)).date().isoformat() for index in range(7)]
    rows = []
    for day_index, day in enumerate(days):
        for side, offset in (("BUY", 0.0), ("SELL", 0.25)):
            for bucket_index in range(100):
                rows.append(
                    {
                        "day": day,
                        "side": side,
                        "role": "opener",
                        "decision_id": f"{day}:{side}:{bucket_index}",
                        "decision_ts_ns": day_index * 1_000_000 + bucket_index,
                        "prediction_bucket_ts_ms": day_index * 1_000 + bucket_index,
                        "feature_ready_ts_ms": day_index * 1_000 + bucket_index,
                        "toxicity_score": offset + day_index / 100.0 + bucket_index / 1000.0,
                    }
                )
    opportunities = pd.DataFrame(rows)

    schedules, report = build_past_only_threshold_schedule_v15(
        opportunities,
        development_days=days,
    )

    assert days[4] not in schedules["BUY"]
    assert days[5] in schedules["BUY"]
    assert days[5] in schedules["SELL"]
    buy_prior = opportunities[
        opportunities["side"].eq("BUY") & opportunities["day"].lt(days[5])
    ]["toxicity_score"]
    sell_prior = opportunities[
        opportunities["side"].eq("SELL") & opportunities["day"].lt(days[5])
    ]["toxicity_score"]
    assert schedules["BUY"][days[5]][0] == pytest.approx(
        buy_prior.quantile(0.9, interpolation="higher")
    )
    assert schedules["SELL"][days[5]][0] == pytest.approx(
        sell_prior.quantile(0.9, interpolation="higher")
    )
    assert report.loc[
        report["day"].eq(days[5]) & report["side"].eq("BUY"),
        "prior_buckets",
    ].item() == 500


def test_v15_unready_day_consumes_exact_denominator_without_action(tmp_path) -> None:
    manifest, quote = _write_baseline_tape(tmp_path)
    replay = RankedToxicityThresholdUnreadyReplayV15(
        baseline_manifest_path=manifest
    )
    replay.validate_replay_start(
        params={"dynamic_fill_hazard_action_enabled": False},
        ml_data=([], [], [], [], [], []),
    )
    replay.on_prediction_bucket(**_prediction_kwargs())
    directive = replay.on_quote_decision(**quote)
    assert directive.allow_exposure_submission is True
    assert directive.request_cancel_once is False
    replay.on_final_quote_action(
        decision_id=str(quote["decision_id"]),
        side="BUY",
        role="opener",
        exposure_increasing=True,
        candidate_action="place",
        candidate_price=100.0,
        candidate_quantity=0.001,
        candidate_order_id="",
        event_ts_ns=int(quote["decision_ts_ns"]),
    )
    audit = replay.finish_replay(event_ts_ns=int(quote["decision_ts_ns"]))
    assert audit["baseline_shadow"]["complete"] is True
    assert audit["assignment_count"] == 0
    assert audit["treatment_event_count"] == 0


def test_v15_unready_day_fails_on_baseline_shadow_drift(tmp_path) -> None:
    manifest, quote = _write_baseline_tape(tmp_path)
    replay = RankedToxicityThresholdUnreadyReplayV15(
        baseline_manifest_path=manifest
    )
    replay.on_prediction_bucket(**_prediction_kwargs())
    with pytest.raises(AdapterContractViolation, match="diverged"):
        replay.on_quote_decision(
            **{**quote, "blocker_reasons": ("unexpected_blocker",)}
        )
