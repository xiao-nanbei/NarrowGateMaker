from __future__ import annotations

import hashlib
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from research.families.f02_empirical_p3_touch.audit.p3_touch_decision_cadence_context import (
    DECISION_CONTEXT_FIELDS,
    OVERLAPPING_LABEL_CLUSTER_CONTRACT,
    DecisionCadenceContextError,
    FrozenCausalBboSource,
    extract_decision_cadence_context,
    load_f06_baseline_eligible_decisions,
)
from research.families.f02_empirical_p3_touch.audit.p3_touch_volatility_conditioned import (
    build_model_matrix,
)

DAY = "2026-06-08"
DECISION_MS = int(pd.Timestamp(f"{DAY}T12:00:00.123Z").timestamp() * 1_000)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def _bbo_frame(*, history_seconds: int = 70, future_price: float | None = None):
    offsets = np.arange(-history_seconds, 1, dtype=np.int64)
    steps = np.arange(len(offsets), dtype=np.int64)
    bid_ticks = 1_000 + steps + steps % 3
    frame = pd.DataFrame(
        {
            "timestamp": DECISION_MS + offsets * 1_000,
            "best_bid": bid_ticks.astype(np.float64) * 0.1,
            "best_ask": (bid_ticks + 1).astype(np.float64) * 0.1,
        }
    )
    if future_price is not None:
        frame.loc[len(frame)] = [DECISION_MS + 1, future_price, future_price + 0.1]
    return frame


def _placement_frame(*, best_bid: float, best_ask: float) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "decision_id": ["BTCUSDC:decision:BUY"],
            "day": [DAY],
            "side": ["BUY"],
            "inventory_role": ["opener"],
            "campaign_id": [7],
            "submit_ts_ns": [DECISION_MS * 1_000_000],
            "feature_ready_ts_ns": [DECISION_MS * 1_000_000],
            "best_bid": [best_bid],
            "best_ask": [best_ask],
            "baseline_price_tick": [995],
            "baseline_action": ["place"],
            "allow_post": [1],
            # These forbidden economic/lifecycle fields may exist upstream,
            # but the loader must never admit them to the research context.
            "terminal_pnl_usdc": [-12.0],
            "current__fill_qty": [0.001],
            "markout_10s_bps": [-1.0],
        }
    )


def _write_inputs(tmp_path: Path):
    bbo = _bbo_frame()
    current = bbo.iloc[-1]
    bbo_path = tmp_path / "bbo.parquet"
    placement_path = tmp_path / "placement.parquet"
    bbo.to_parquet(bbo_path, index=False)
    _placement_frame(
        best_bid=float(current.best_bid),
        best_ask=float(current.best_ask),
    ).to_parquet(placement_path, index=False)
    decisions = load_f06_baseline_eligible_decisions(
        placement_path,
        expected_sha256=_sha256(placement_path),
    )
    source = FrozenCausalBboSource(
        path=bbo_path,
        sha256=_sha256(bbo_path),
        source_identity="synthetic_native_ready_bbo.v1",
    )
    return bbo, decisions, source


def test_extracts_arbitrary_decision_with_v4_feature_semantics(tmp_path: Path):
    bbo, decisions, source = _write_inputs(tmp_path)

    batch = extract_decision_cadence_context(decisions, source=source)

    assert batch.frame["supported"].tolist() == [True]
    row = batch.frame.iloc[0]
    history_mid = 0.5 * (bbo.tail(61)["best_bid"].to_numpy() + bbo.tail(61)["best_ask"].to_numpy())
    differences = np.diff(history_mid)
    assert row["decision_ts_ms"] % 10_000 != 0
    assert row["feature_ready_ts_ms"] == DECISION_MS
    assert row["feature_ready_ts_ms"] <= row["decision_ts_ms"]
    assert row["fast_variance"] == pytest.approx(max(np.var(differences[-10:], ddof=1), 1e-6))
    assert row["slow_variance"] == pytest.approx(max(np.var(differences, ddof=1), 1e-6))
    assert tuple(batch.model_context()) == DECISION_CONTEXT_FIELDS
    matrix = build_model_matrix(
        batch.model_context(),
        side="BUY",
        distances=np.asarray([1.0]),
    )
    assert matrix.shape == (1, 10)
    assert batch.metadata["decision_cadence_transport_supported"] is False
    assert batch.metadata["permissions"]["action_authority"] is False
    assert batch.metadata["permissions"]["economic_outcomes_read"] is False


def test_future_bbo_cannot_change_decision_context(tmp_path: Path):
    bbo, decisions, source = _write_inputs(tmp_path)
    original = extract_decision_cadence_context(decisions, source=source)

    future_path = tmp_path / "bbo_with_future.parquet"
    _bbo_frame(future_price=9_999.0).to_parquet(future_path, index=False)
    future_source = FrozenCausalBboSource(
        path=future_path,
        sha256=_sha256(future_path),
        source_identity=source.source_identity,
    )
    extended = extract_decision_cadence_context(decisions, source=future_source)

    for field in DECISION_CONTEXT_FIELDS:
        np.testing.assert_allclose(
            original.supported[field].to_numpy(),
            extended.supported[field].to_numpy(),
        )


def test_same_timestamp_bbo_mismatch_fails_closed(tmp_path: Path):
    _, decisions, source = _write_inputs(tmp_path)
    decisions.loc[0, "best_bid"] -= 0.1

    batch = extract_decision_cadence_context(decisions, source=source)

    assert batch.frame["supported"].tolist() == [False]
    assert batch.frame["unsupported_reason"].tolist() == ["decision_bbo_source_tick_mismatch"]
    assert batch.model_context()["start_ts_ms"].size == 0


def test_incomplete_history_and_stale_current_fail_closed(tmp_path: Path):
    bbo, decisions, _ = _write_inputs(tmp_path)
    short_path = tmp_path / "short_bbo.parquet"
    bbo.tail(31).to_parquet(short_path, index=False)
    short_source = FrozenCausalBboSource(
        path=short_path,
        sha256=_sha256(short_path),
        source_identity="short_history",
    )
    short = extract_decision_cadence_context(decisions, source=short_source)
    assert short.frame["unsupported_reason"].tolist() == ["causal_60s_bbo_history_incomplete"]

    stale_decisions = decisions.copy()
    stale_decisions["submit_ts_ns"] += 6_000 * 1_000_000
    stale_decisions["feature_ready_ts_ns"] += 6_000 * 1_000_000
    stale_decisions["decision_ts_ms"] += 6_000
    stale = extract_decision_cadence_context(stale_decisions, source=short_source)
    assert stale.frame["unsupported_reason"].tolist() == ["current_bbo_unavailable_or_stale"]


def test_frozen_source_hash_and_clock_semantics_are_mandatory(tmp_path: Path):
    _, decisions, source = _write_inputs(tmp_path)
    bad_hash = FrozenCausalBboSource(
        path=source.path,
        sha256="0" * 64,
        source_identity=source.source_identity,
    )
    with pytest.raises(DecisionCadenceContextError, match="hash mismatch"):
        extract_decision_cadence_context(decisions, source=bad_hash)

    wrong_clock = FrozenCausalBboSource(
        path=source.path,
        sha256=source.sha256,
        source_identity=source.source_identity,
        timestamp_semantics="exchange_time_without_ready_latency",
    )
    with pytest.raises(DecisionCadenceContextError, match="causal observation clock"):
        extract_decision_cadence_context(decisions, source=wrong_clock)


def test_loader_rejects_noneligible_or_noncausal_denominator(tmp_path: Path):
    bbo = _bbo_frame()
    current = bbo.iloc[-1]
    path = tmp_path / "bad_placement.parquet"
    frame = _placement_frame(
        best_bid=float(current.best_bid),
        best_ask=float(current.best_ask),
    )
    frame.loc[0, "baseline_action"] = "cancel"
    frame.to_parquet(path, index=False)
    with pytest.raises(DecisionCadenceContextError, match="non-posting"):
        load_f06_baseline_eligible_decisions(path, expected_sha256=_sha256(path))

    frame.loc[0, "baseline_action"] = "place"
    frame.loc[0, "feature_ready_ts_ns"] = frame.loc[0, "submit_ts_ns"] + 1
    frame.to_parquet(path, index=False)
    with pytest.raises(DecisionCadenceContextError, match="feature-ready clock"):
        load_f06_baseline_eligible_decisions(path, expected_sha256=_sha256(path))


def test_extractor_cannot_bypass_baseline_eligibility_validation(tmp_path: Path):
    _, decisions, source = _write_inputs(tmp_path)
    decisions.loc[0, "allow_post"] = 0

    with pytest.raises(DecisionCadenceContextError, match="baseline-ineligible"):
        extract_decision_cadence_context(decisions, source=source)


def test_loader_does_not_admit_economic_or_lifecycle_outcomes(tmp_path: Path):
    _, decisions, _ = _write_inputs(tmp_path)

    assert "terminal_pnl_usdc" not in decisions
    assert "current__fill_qty" not in decisions
    assert "markout_10s_bps" not in decisions
    assert set(OVERLAPPING_LABEL_CLUSTER_CONTRACT["minimum_cluster_keys"]) == {
        "day",
        "campaign_id",
    }
    assert OVERLAPPING_LABEL_CLUSTER_CONTRACT["labels_extracted_by_this_module"] is False
