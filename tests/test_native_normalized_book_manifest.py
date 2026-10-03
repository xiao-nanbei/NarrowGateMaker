import pytest

from models.audit.native_normalized_book_manifest import (
    audit_normalized_book_universe,
    normalized_summary_is_strict,
    select_normalized_source,
)


def _summary(root: str, **overrides):
    values = {
        "root": root,
        "bbo_readable": True,
        "l2_readable": True,
        "tmp_exists": False,
        "l2_schema_complete": True,
        "bbo_coverage": 1.0,
        "l2_coverage": 1.0,
        "bbo_p99_gap_s": 0.1,
        "l2_p99_gap_s": 0.1,
        "l2_valid_spread_ratio": 1.0,
    }
    values.update(overrides)
    return values


def test_normalized_source_requires_complete_top20_and_coverage() -> None:
    eligible, reasons = normalized_summary_is_strict(
        _summary(
            "bad",
            l2_schema_complete=False,
            l2_coverage=0.8,
        ),
        min_coverage=0.99,
        min_valid_spread_ratio=0.999,
        max_p99_gap_s=0.5,
    )

    assert not eligible
    assert reasons == ["missing_top20_schema", "l2_coverage"]


def test_normalized_source_uses_first_strict_priority_root() -> None:
    selected, attempts = select_normalized_source(
        [
            _summary("incomplete", bbo_coverage=0.5),
            _summary("strict"),
            _summary("later"),
        ],
        min_coverage=0.99,
        min_valid_spread_ratio=0.999,
        max_p99_gap_s=0.5,
    )

    assert selected["root"] == "strict"
    assert attempts == [
        {
            "root": "incomplete",
            "eligible": False,
            "reasons": ["bbo_coverage"],
        },
        {"root": "strict", "eligible": True, "reasons": []},
    ]


def test_normalized_source_fails_when_every_root_is_invalid() -> None:
    with pytest.raises(ValueError, match="no strict normalized source"):
        select_normalized_source(
            [_summary("bad", tmp_exists=True)],
            min_coverage=0.99,
            min_valid_spread_ratio=0.999,
            max_p99_gap_s=0.5,
        )


def test_normalized_source_rejects_one_second_cadence() -> None:
    eligible, reasons = normalized_summary_is_strict(
        _summary(
            "slow",
            bbo_p99_gap_s=1.0,
            l2_p99_gap_s=1.0,
        ),
        min_coverage=0.99,
        min_valid_spread_ratio=0.999,
        max_p99_gap_s=0.5,
    )

    assert not eligible
    assert reasons == ["bbo_cadence", "l2_cadence"]


def test_normalized_audit_keeps_only_days_that_pass_frozen_gates(
    tmp_path,
) -> None:
    candidates = tmp_path / "candidates.csv"
    audit = tmp_path / "audit.csv"
    strict = tmp_path / "strict.csv"
    root = tmp_path / "root"
    candidates.write_text(
        "day\n2026-01-01\n2026-01-02\n",
        encoding="utf-8",
    )

    def load_summary(path, symbol, day_start, freshness_ms, *, levels):
        del symbol, freshness_ms, levels
        coverage = 1.0 if day_start.day == 1 else 0.98
        return _summary(str(path), bbo_coverage=coverage, l2_coverage=coverage)

    payload = audit_normalized_book_universe(
        candidate_days_path=candidates,
        source_roots=[root],
        audit_output_path=audit,
        strict_days_output_path=strict,
        summary_loader=load_summary,
    )

    assert payload["candidate_days_count"] == 2
    assert payload["strict_days_count"] == 1
    assert payload["excluded_days_count"] == 1
    assert strict.read_text(encoding="utf-8") == "day\n2026-01-01\n"
    assert audit.with_suffix(".manifest.json").is_file()
