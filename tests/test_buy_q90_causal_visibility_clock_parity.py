from __future__ import annotations

from research.families.f10_live_replay_attribution.audit import (
    buy_q90_causal_visibility_clock_parity as parity,
)


def test_mechanics_projection_never_persists_economics() -> None:
    projected = parity._mechanics_only(
        {
            "dynamic_fill_hazard_eval_count": 100,
            "dynamic_fill_hazard_valid_eval_count": 80,
            "dynamic_fill_hazard_cancel_request_count": 4,
            "pnl": 123.0,
            "avg_markout": -9.0,
            "campaigns": 7,
        },
        2.0,
    )

    assert "pnl" not in projected
    assert "avg_markout" not in projected
    assert "campaigns" not in projected
    assert projected["rates"]["evaluations_per_hour"] == 50.0
    assert projected["rates"]["valid_fraction"] == 0.8


def test_role_total_variation_uses_normalized_denominators() -> None:
    assert parity._distribution_tv(
        {"opener": 90, "add": 10},
        {"opener": 45, "add": 5},
        ("opener", "add"),
    ) == 0.0
    assert parity._distribution_tv(
        {"opener": 100, "add": 0},
        {"opener": 0, "add": 100},
        ("opener", "add"),
    ) == 1.0


def test_shadow_truth_invariance_fails_on_fill_or_clock_truth_drift() -> None:
    base = {key: 0 for key in parity.TRUTH_INVARIANCE_KEYS}
    base["dynamic_fill_hazard_truth_state_fingerprint"] = "truth"
    modes = {
        "legacy_shadow": {"mechanics": dict(base)},
        "provider_receive_shadow": {"mechanics": dict(base)},
        "aws_profile_shadow": {"mechanics": dict(base)},
    }
    assert parity._truth_invariance(modes)["passed"] is True

    modes["aws_profile_shadow"]["mechanics"]["fills_bid"] = 1
    result = parity._truth_invariance(modes)
    assert result["passed"] is False
    assert "aws_profile_shadow:fills_bid" in result["mismatches"]


def test_identity_has_four_predeclared_mechanics_modes() -> None:
    assert parity.MODE_IDS == (
        "legacy_shadow",
        "provider_receive_shadow",
        "aws_profile_shadow",
        "aws_profile_apply",
    )
    assert not any(
        "pnl" in key.lower() or "markout" in key.lower() or "campaign" in key.lower()
        for key in parity.MECHANICS_RESULT_KEYS
    )
