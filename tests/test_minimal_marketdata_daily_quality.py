from models.audit.minimal_marketdata_daily_quality import classify_day


def test_formal_day_is_grade_a() -> None:
    assert classify_day(
        official_complete=True,
        cryptohft_complete=True,
        sequence_eligible=True,
        normalized_present=True,
        normalized_formal=True,
        normalized_coverage=0.999,
        normalized_max_internal_gap_s=2.0,
    ) == ("A", "formal_training_and_replay", True)


def test_sequence_valid_pending_rebuild_is_grade_c() -> None:
    assert classify_day(
        official_complete=True,
        cryptohft_complete=True,
        sequence_eligible=True,
        normalized_present=False,
        normalized_formal=False,
        normalized_coverage=0.0,
        normalized_max_internal_gap_s=None,
    ) == ("C", "source_valid_normalization_pending", False)


def test_missing_official_source_is_grade_f() -> None:
    assert classify_day(
        official_complete=False,
        cryptohft_complete=True,
        sequence_eligible=True,
        normalized_present=True,
        normalized_formal=True,
        normalized_coverage=1.0,
        normalized_max_internal_gap_s=1.0,
    ) == ("F", "excluded_missing_required_raw", False)


def test_sequence_failure_cannot_enter_formal_replay() -> None:
    assert classify_day(
        official_complete=True,
        cryptohft_complete=True,
        sequence_eligible=False,
        normalized_present=True,
        normalized_formal=True,
        normalized_coverage=1.0,
        normalized_max_internal_gap_s=1.0,
    ) == ("D", "official_trade_bar_only_no_exact_l2", False)


def test_long_internal_gap_requires_censoring() -> None:
    assert classify_day(
        official_complete=True,
        cryptohft_complete=True,
        sequence_eligible=True,
        normalized_present=True,
        normalized_formal=True,
        normalized_coverage=0.999,
        normalized_max_internal_gap_s=12.0,
    ) == ("B", "gap_censored_l2_replay_only", False)
