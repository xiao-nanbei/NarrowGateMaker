from models.audit.panel_promotion_controller import control_panel_promotion


def _scorecard(*, panel: str, screening_only: bool = False, passed: bool = True) -> dict:
    return {
        "family_id": "family_v1",
        "panel_role": panel,
        "profile": {
            "profile_id": "paired_screen_v2" if screening_only else "action_alpha_v1",
            "screening_only": screening_only,
            "frozen_before_outcome": True,
        },
        "validity": {"passed": passed},
        "support": {"passed": passed},
        "hard_gates": {"passed": passed},
        "ranking_eligible": passed,
        "scorecard_sha256": "a" * 64,
    }


def test_screening_scorecard_cannot_unlock_a_panel() -> None:
    decision = control_panel_promotion(
        _scorecard(panel="screening", screening_only=True),
        panel_identity_frozen=True,
    )

    assert decision["decision"] == "screening_only_no_panel_unlock"
    assert not decision["unlock_allowed"]
    assert decision["next_stage"] is None
    assert not decision["live_promotion_allowed"]


def test_development_pass_only_unlocks_validation() -> None:
    decision = control_panel_promotion(
        _scorecard(panel="development"),
        panel_identity_frozen=True,
    )

    assert decision["decision"] == "development_validation_unlocked"
    assert decision["unlock_allowed"]
    assert decision["next_stage"] == "validation"
    assert not decision["live_promotion_allowed"]


def test_validation_requires_the_recorded_development_decision() -> None:
    blocked = control_panel_promotion(
        _scorecard(panel="validation"),
        panel_identity_frozen=True,
    )
    allowed = control_panel_promotion(
        _scorecard(panel="validation"),
        panel_identity_frozen=True,
        prior_panel_decision="development_validation_unlocked",
    )

    assert blocked["decision"] == "panel_unlock_blocked"
    assert "prior_panel_decision_required:development_validation_unlocked" in blocked[
        "failures"
    ]
    assert allowed["decision"] == "validation_holdout_unlocked"
    assert allowed["next_stage"] == "sealed_holdout"


def test_failed_action_panel_closes_family_without_live_permission() -> None:
    decision = control_panel_promotion(
        _scorecard(panel="development", passed=False),
        panel_identity_frozen=True,
    )

    assert decision["decision"] == "close_family_on_development"
    assert decision["close_family"]
    assert not decision["live_promotion_allowed"]
