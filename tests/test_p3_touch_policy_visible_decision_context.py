from __future__ import annotations

import hashlib
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from models import backtest_tick as bt
from research.families.f02_empirical_p3_touch.audit.p3_touch_decision_cadence_context import (
    DECISION_CONTEXT_FIELDS,
    DecisionCadenceContextError,
    load_f06_baseline_eligible_decisions,
)
from research.families.f02_empirical_p3_touch.audit.p3_touch_policy_visible_decision_context import (
    FrozenPolicyVisibleBboSource,
    extract_policy_visible_decision_context,
    visibility_delay_ms,
)

DAY = "2026-06-08"
DECISION_MS = int(pd.Timestamp(f"{DAY}T12:00:00.123Z").timestamp() * 1_000)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_profile(tmp_path: Path, age_ms: float) -> Path:
    path = tmp_path / "visibility.csv"
    pd.DataFrame(
        {
            "event": ["requote", "ignored"],
            "status": ["ok", "ok"],
            "depth_age_s": [age_ms / 1_000.0, 99.0],
        }
    ).to_csv(path, index=False)
    return path


def _write_inputs(tmp_path: Path, *, age_ms: int = 1_000):
    offsets = np.arange(-75, 1, dtype=np.int64)
    source_ts = DECISION_MS + offsets * 1_000
    ticks = 1_000 + np.arange(len(offsets), dtype=np.int64)
    bbo = pd.DataFrame(
        {
            "timestamp": source_ts,
            "best_bid": ticks * 0.1,
            "best_ask": (ticks + 1) * 0.1,
        }
    )
    bbo_path = tmp_path / "bbo.parquet"
    bbo.to_parquet(bbo_path, index=False)
    profile_path = _write_profile(tmp_path, float(age_ms))

    visible_row = bbo.loc[bbo["timestamp"].eq(DECISION_MS - age_ms)].iloc[0]
    placement_path = tmp_path / "placement.parquet"
    pd.DataFrame(
        {
            "decision_id": ["BTCUSDC:decision:BUY"],
            "day": [DAY],
            "side": ["BUY"],
            "inventory_role": ["opener"],
            "campaign_id": [7],
            "submit_ts_ns": [DECISION_MS * 1_000_000],
            "feature_ready_ts_ns": [DECISION_MS * 1_000_000],
            "best_bid": [float(visible_row.best_bid)],
            "best_ask": [float(visible_row.best_ask)],
            "baseline_price_tick": [990],
            "baseline_action": ["place"],
            "allow_post": [1],
            "terminal_pnl_usdc": [-10.0],
        }
    ).to_parquet(placement_path, index=False)
    decisions = load_f06_baseline_eligible_decisions(
        placement_path,
        expected_sha256=_sha256(placement_path),
    )
    source = FrozenPolicyVisibleBboSource(
        path=bbo_path,
        sha256=_sha256(bbo_path),
        source_identity="synthetic_normalized_bbo.v1",
        visibility_profile_path=profile_path,
        visibility_profile_sha256=_sha256(profile_path),
        visibility_profile_id="synthetic_aws_profile",
        visibility_seed=20260718,
    )
    return bbo, decisions, source


def test_visibility_sampler_matches_authoritative_replay_kernel():
    timestamps = np.asarray(
        [1, 1_780_012_800_530, 1_780_012_920_594, 2**62],
        dtype=np.int64,
    )
    samples = np.asarray([0.4, 17.5, 82.4, 1_621.6], dtype=np.float64)

    observed = visibility_delay_ms(
        timestamps,
        samples_ms=samples,
        seed=20260718,
    )
    expected = np.asarray(
        [
            bt._exec_book_visibility_delay_ms(
                int(timestamp),
                mean_ms=0.0,
                jitter_ms=0.0,
                seed=20260718,
                samples_ms=samples,
            )
            for timestamp in timestamps
        ],
        dtype=np.int64,
    )

    np.testing.assert_array_equal(observed, expected)


def test_policy_visible_context_matches_delayed_placement_bbo(tmp_path: Path):
    bbo, decisions, source = _write_inputs(tmp_path, age_ms=1_000)

    batch = extract_policy_visible_decision_context(decisions, source=source)

    assert batch.frame["supported"].tolist() == [True]
    row = batch.frame.iloc[0]
    assert row["visibility_delay_ms"] == 1_000
    assert row["visible_bbo_cutoff_ts_ms"] == DECISION_MS - 1_000
    assert row["feature_ready_ts_ms"] == DECISION_MS
    assert row["book_age_ms"] == 1_000
    history = bbo.loc[
        bbo["timestamp"].between(
            DECISION_MS - 61_000,
            DECISION_MS - 1_000,
        )
    ]
    history_mid = 0.5 * (history["best_bid"].to_numpy() + history["best_ask"].to_numpy())
    differences = np.diff(history_mid)
    assert row["fast_variance"] == pytest.approx(max(np.var(differences[-10:], ddof=1), 1e-6))
    assert row["slow_variance"] == pytest.approx(max(np.var(differences, ddof=1), 1e-6))
    assert tuple(batch.model_context()) == DECISION_CONTEXT_FIELDS
    assert batch.metadata["aws_receive_time_transport_supported"] is False
    assert batch.metadata["permissions"]["economic_outcomes_read"] is False


def test_raw_current_bbo_is_not_substituted_for_policy_visible_bbo(tmp_path: Path):
    _, decisions, source = _write_inputs(tmp_path, age_ms=1_000)
    decisions.loc[0, "best_bid"] += 0.1
    decisions.loc[0, "best_ask"] += 0.1

    batch = extract_policy_visible_decision_context(decisions, source=source)

    assert batch.frame["supported"].tolist() == [False]
    assert batch.frame["unsupported_reason"].tolist() == [
        "policy_visible_decision_bbo_tick_mismatch"
    ]


def test_profile_hash_and_sample_support_fail_closed(tmp_path: Path):
    _, decisions, source = _write_inputs(tmp_path)
    bad_hash = FrozenPolicyVisibleBboSource(
        **{
            **source.__dict__,
            "visibility_profile_sha256": "0" * 64,
        }
    )
    with pytest.raises(DecisionCadenceContextError, match="hash mismatch"):
        extract_policy_visible_decision_context(decisions, source=bad_hash)

    empty_path = tmp_path / "empty_profile.csv"
    pd.DataFrame({"event": ["requote"], "status": ["ok"], "depth_age_s": [np.nan]}).to_csv(
        empty_path, index=False
    )
    empty = FrozenPolicyVisibleBboSource(
        **{
            **source.__dict__,
            "visibility_profile_path": empty_path,
            "visibility_profile_sha256": _sha256(empty_path),
        }
    )
    with pytest.raises(DecisionCadenceContextError, match="no finite age"):
        extract_policy_visible_decision_context(decisions, source=empty)


def test_future_bbo_cannot_change_policy_visible_context(tmp_path: Path):
    bbo, decisions, source = _write_inputs(tmp_path)
    original = extract_policy_visible_decision_context(decisions, source=source)
    future = pd.concat(
        [
            bbo,
            pd.DataFrame(
                {
                    "timestamp": [DECISION_MS + 1],
                    "best_bid": [9_999.0],
                    "best_ask": [9_999.1],
                }
            ),
        ],
        ignore_index=True,
    )
    future_path = tmp_path / "future.parquet"
    future.to_parquet(future_path, index=False)
    extended_source = FrozenPolicyVisibleBboSource(
        **{
            **source.__dict__,
            "path": future_path,
            "sha256": _sha256(future_path),
        }
    )

    extended = extract_policy_visible_decision_context(decisions, source=extended_source)

    for field in DECISION_CONTEXT_FIELDS:
        np.testing.assert_allclose(
            original.supported[field].to_numpy(),
            extended.supported[field].to_numpy(),
        )
