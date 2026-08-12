from __future__ import annotations

import csv
import json
from datetime import date, timedelta
from pathlib import Path

import pytest

from research.families.f05_fill_quality_quote_ev.audit import fill_selection_score as score


def _days(count: int) -> list[str]:
    start = date(2026, 1, 1)
    return [(start + timedelta(days=offset)).isoformat() for offset in range(count)]


def _write_panel(path: Path, days: list[str], *, include_day: bool = True) -> None:
    fieldnames = [
        *(["day"] if include_day else []),
        "utc",
        "side",
        "filled",
        "markout_20s_bps",
        "markout_30s_bps",
        "quote_distance_bps",
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for index, day in enumerate(days):
            row = {
                "utc": f"{day}T00:00:00Z",
                "side": "BUY",
                "filled": "1",
                "markout_20s_bps": "1.0" if index % 2 else "-1.0",
                "markout_30s_bps": "1.0" if index % 2 else "-3.0",
                "quote_distance_bps": str(index + 1),
            }
            if include_day:
                row["day"] = day
            writer.writerow(row)


def test_blocked_day_mode_preserves_historical_fold_contract() -> None:
    days = _days(8)

    plan = score.build_split_plan(
        days,
        split_mode="blocked_day",
        blocked_folds=3,
    )

    assert plan.partition.development_days == tuple(days)
    assert plan.partition.validation_days == ()
    assert plan.partition.holdout_days == ()
    assert sorted(day for fold in plan.folds for day in fold.test_days) == days
    for fold in plan.folds:
        expected_test = tuple(
            day for day in days if score._fold_for_day(day, len(plan.folds)) == fold.fold
        )
        assert fold.test_days == expected_test
        assert fold.embargo_days == ()
        assert set(fold.train_days) == set(days) - set(expected_test)


def test_walk_forward_uses_expanding_past_only_folds_and_freezes_tail_roles() -> None:
    days = _days(15)

    plan = score.build_split_plan(
        days,
        split_mode="walk_forward",
        min_train_days=3,
        embargo_days=1,
        test_days=2,
        validation_days=2,
        holdout_days=2,
    )

    assert plan.partition.development_days == tuple(days[:9])
    assert plan.partition.embargo_before_validation_days == (days[9],)
    assert plan.partition.validation_days == tuple(days[10:12])
    assert plan.partition.embargo_before_holdout_days == (days[12],)
    assert plan.partition.holdout_days == tuple(days[13:15])
    assert len(plan.folds) == 2
    assert plan.folds[0].train_days == tuple(days[:3])
    assert plan.folds[0].embargo_days == (days[3],)
    assert plan.folds[0].test_days == tuple(days[4:6])
    assert plan.folds[1].train_days == tuple(days[:6])
    assert plan.folds[1].embargo_days == (days[6],)
    assert plan.folds[1].test_days == tuple(days[7:9])
    assert len(plan.folds[1].train_days) > len(plan.folds[0].train_days)
    for fold in plan.folds:
        assert max(fold.train_days) < min(fold.test_days)
        assert not (
            set(fold.train_days)
            & (
                set(fold.test_days)
                | set(plan.partition.validation_days)
                | set(plan.partition.holdout_days)
            )
        )


def test_walk_forward_freezes_before_validation_and_keeps_holdout_unread(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    days = _days(15)
    panel = tmp_path / "orders.csv"
    _write_panel(panel, days)
    out_prefix = tmp_path / "walk_forward"
    validation_days = set(days[10:12])
    unread_days = {days[9], days[12], *days[13:15]}
    original_label = score.fill_selection_label
    original_float = score._float
    original_fit = score.fit_model
    original_select_threshold = score._select_score_threshold
    label_days: list[str] = []
    state = {
        "threshold_selected": False,
        "frozen_fit_complete": False,
    }

    def guarded_label(
        row: dict[str, str],
        cfg: score.LabelConfig,
    ) -> int | None:
        day = row["day"]
        if day in unread_days:
            raise AssertionError(f"sealed outcome was read for {day}")
        if day in validation_days and not state["frozen_fit_complete"]:
            raise AssertionError(f"validation outcome was read before freeze for {day}")
        label_days.append(day)
        return original_label(row, cfg)

    def guarded_float(
        row: dict[str, str],
        key: str,
        default: float = float("nan"),
    ) -> float:
        day = row.get("day")
        if day in unread_days:
            raise AssertionError(f"sealed feature/outcome {key!r} was read for {day}")
        if day in validation_days and not state["frozen_fit_complete"]:
            raise AssertionError(f"validation feature/outcome {key!r} was read before freeze")
        return original_float(row, key, default)

    def guarded_select_threshold(
        development_oof_scores: list[float],
        *,
        explicit_threshold: float | None,
        quantile: float,
    ) -> tuple[float, str]:
        result = original_select_threshold(
            development_oof_scores,
            explicit_threshold=explicit_threshold,
            quantile=quantile,
        )
        state["threshold_selected"] = True
        return result

    def guarded_fit(*args: object, **kwargs: object):
        train_days = set(args[1])
        result = original_fit(*args, **kwargs)
        if train_days == set(days[:9]):
            assert state["threshold_selected"]
            state["frozen_fit_complete"] = True
        return result

    monkeypatch.setattr(score, "fill_selection_label", guarded_label)
    monkeypatch.setattr(score, "_float", guarded_float)
    monkeypatch.setattr(score, "_select_score_threshold", guarded_select_threshold)
    monkeypatch.setattr(score, "fit_model", guarded_fit)
    args = score.parse_args(
        [
            "--order-level-csv",
            str(panel),
            "--out-prefix",
            str(out_prefix),
            "--side",
            "BUY",
            "--split-mode",
            "walk_forward",
            "--min-train-days",
            "3",
            "--embargo-days",
            "1",
            "--test-days",
            "2",
            "--validation-days",
            "2",
            "--holdout-days",
            "2",
        ]
    )

    summary = score.run(args)

    assert label_days
    assert set(label_days) <= set(days[:12])
    assert validation_days <= set(label_days)
    assert not (set(days[13:15]) & set(label_days))
    assert summary["selection_scope"]["threshold_selection_role"] == ("development_oof")
    assert summary["selection_scope"]["threshold_selection_days"] == [
        *days[4:6],
        *days[7:9],
    ]
    assert summary["selection_scope"]["threshold_source"] == ("development_oof_score_quantile")
    assert summary["selection_scope"]["frozen_train_max_day"] == days[8]
    assert summary["selection_scope"]["validation_outcomes_read"] is True
    assert summary["selection_scope"]["holdout_gate"] == "closed_by_default"
    assert summary["selection_scope"]["holdout_outcomes_read"] is False
    model = json.loads(out_prefix.with_suffix(".fill_selection_model.json").read_text())
    assert model["day_partition"]["validation_days"] == days[10:12]
    assert model["day_partition"]["holdout_days"] == days[13:15]
    assert all(max(fold["train_days"]) < min(fold["test_days"]) for fold in model["folds"])
    frozen = json.loads(out_prefix.with_suffix(".fill_selection_frozen_model.json").read_text())
    assert frozen["train_days"] == days[:9]
    assert frozen["train_max_day"] == days[8]
    assert frozen["threshold"]["source"] == "development_oof_score_quantile"
    assert frozen["feature_identity"].startswith("sha256:")
    assert frozen["model_identity"].startswith("sha256:")

    with out_prefix.with_suffix(".fill_selection_daily.csv").open(newline="") as f:
        daily_days = {row["day"] for row in csv.DictReader(f)}
    assert daily_days == {*days[4:6], *days[7:9]}

    with out_prefix.with_suffix(".fill_selection_frozen_scores.csv").open(newline="") as f:
        frozen_scores = list(csv.DictReader(f))
    assert {row["day_role"] for row in frozen_scores} == {"validation"}
    assert {row["day"] for row in frozen_scores} == validation_days
    assert {row["threshold_source"] for row in frozen_scores} == {"development_oof_score_quantile"}
    assert {row["train_max_day"] for row in frozen_scores} == {days[8]}
    assert {row["feature_identity"] for row in frozen_scores} == {frozen["feature_identity"]}
    assert {row["model_identity"] for row in frozen_scores} == {frozen["model_identity"]}

    with out_prefix.with_suffix(".fill_selection_frozen_metrics.csv").open(newline="") as f:
        frozen_metrics = list(csv.DictReader(f))
    assert frozen_metrics
    assert {row["day_role"] for row in frozen_metrics} == {"validation"}

    with out_prefix.with_suffix(".fill_selection_day_roles.csv").open(newline="") as f:
        roles = {row["day"]: row for row in csv.DictReader(f)}
    assert roles[days[10]]["role"] == "validation"
    assert roles[days[10]]["outcome_status"] == "frozen_model_evaluated"
    assert roles[days[13]]["role"] == "holdout"
    assert roles[days[13]]["outcome_status"] == "frozen_unread"
    assert roles[days[13]]["development_oof_threshold_eligible"] == "0"


def test_acc_weights_markout_by_fill_qty_and_terminal_outcome_by_campaign() -> None:
    acc = score.Acc()
    repeated_campaign = {
        "day": "2026-07-01",
        "campaign_id": "7",
        "terminal_campaign_label": "loss_tail",
        "terminal_campaign_bad": "1",
        "terminal_campaign_tail_loss": "1",
        "terminal_final_total_pnl_delta": "-4",
        "terminal_early_drawdown_20m": "5",
    }
    acc.add(
        {
            **repeated_campaign,
            "filled": "1",
            "filled_qty": "0.001",
            "markout_20s_bps": "2",
            "markout_30s_bps": "4",
        },
        0.7,
        1,
    )
    acc.add(
        {
            **repeated_campaign,
            "filled": "1",
            "filled_qty": "0.003",
            "markout_20s_bps": "-2",
            "markout_30s_bps": "0",
        },
        0.3,
        0,
    )
    acc.add(
        {
            "day": "2026-07-01",
            "campaign_id": "8",
            "terminal_campaign_label": "positive_flat",
            "terminal_campaign_bad": "0",
            "terminal_campaign_tail_loss": "0",
            "terminal_final_total_pnl_delta": "2",
            "terminal_early_drawdown_20m": "1",
            "filled": "0",
        },
        0.5,
        None,
    )

    row = acc.as_row()

    assert row["terminal_labeled_orders"] == "3"
    assert row["terminal_labeled_campaigns"] == "2"
    assert float(row["terminal_bad_rate"]) == pytest.approx(0.5)
    assert float(row["terminal_tail_rate"]) == pytest.approx(0.5)
    assert float(row["avg_terminal_pnl"]) == pytest.approx(-1.0)
    assert float(row["avg_early_20m_drawdown"]) == pytest.approx(3.0)
    assert float(row["avg_markout_20s_bps_filled"]) == pytest.approx(-1.0)
    assert float(row["avg_markout_30s_bps_filled"]) == pytest.approx(1.0)


def test_walk_forward_reads_holdout_only_with_explicit_gate(tmp_path: Path) -> None:
    days = _days(15)
    panel = tmp_path / "orders.csv"
    _write_panel(panel, days)
    out_prefix = tmp_path / "opened_holdout"
    args = score.parse_args(
        [
            "--order-level-csv",
            str(panel),
            "--out-prefix",
            str(out_prefix),
            "--side",
            "BUY",
            "--split-mode",
            "walk_forward",
            "--min-train-days",
            "3",
            "--embargo-days",
            "1",
            "--test-days",
            "2",
            "--validation-days",
            "2",
            "--holdout-days",
            "2",
            "--score-threshold",
            "0.6",
            "--evaluate-sealed-holdout",
        ]
    )

    summary = score.run(args)

    assert summary["selection_scope"]["threshold_source"] == "pre_registered_cli"
    assert summary["selection_scope"]["holdout_gate"] == "explicitly_open"
    assert summary["selection_scope"]["holdout_outcomes_read"] is True
    assert summary["frozen_evaluation"]["holdout_score_rows"] == 2
    frozen = json.loads(out_prefix.with_suffix(".fill_selection_frozen_model.json").read_text())
    assert frozen["threshold"]["development_oof_quantile"] is None
    with out_prefix.with_suffix(".fill_selection_frozen_scores.csv").open(newline="") as f:
        rows = list(csv.DictReader(f))
    assert {row["day_role"] for row in rows} == {"validation", "holdout"}
    assert {row["day"] for row in rows if row["day_role"] == "holdout"} == set(days[13:15])
    with out_prefix.with_suffix(".fill_selection_day_roles.csv").open(newline="") as f:
        roles = {row["day"]: row for row in csv.DictReader(f)}
    assert roles[days[13]]["outcome_status"] == "sealed_holdout_evaluated"


def test_walk_forward_freezes_unread_holdout_from_day_universe_only(tmp_path: Path) -> None:
    days = _days(15)
    panel = tmp_path / "orders.csv"
    _write_panel(panel, days[:12])
    universe = tmp_path / "day_universe.csv"
    with universe.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["day"])
        writer.writeheader()
        writer.writerows({"day": day} for day in days)
    out_prefix = tmp_path / "unread_holdout_without_rows"

    args = score.parse_args(
        [
            "--order-level-csv",
            str(panel),
            "--day-universe-file",
            str(universe),
            "--out-prefix",
            str(out_prefix),
            "--side",
            "BUY",
            "--split-mode",
            "walk_forward",
            "--min-train-days",
            "3",
            "--embargo-days",
            "1",
            "--test-days",
            "2",
            "--validation-days",
            "2",
            "--holdout-days",
            "2",
        ]
    )

    summary = score.run(args)

    assert summary["available_input_days"] == days[:12]
    assert summary["days"] == days
    assert summary["day_partition"]["holdout_days"] == days[13:15]
    assert summary["selection_scope"]["holdout_outcomes_read"] is False
    with out_prefix.with_suffix(".fill_selection_day_roles.csv").open(newline="") as f:
        roles = {row["day"]: row for row in csv.DictReader(f)}
    assert roles[days[13]]["outcome_status"] == "frozen_unread"


def test_run_fails_fast_when_explicit_day_identity_is_missing(
    tmp_path: Path,
) -> None:
    panel = tmp_path / "missing_day.csv"
    _write_panel(panel, _days(3), include_day=False)
    args = score.parse_args(
        [
            "--order-level-csv",
            str(panel),
            "--out-prefix",
            str(tmp_path / "blocked"),
        ]
    )

    with pytest.raises(ValueError, match="missing required non-empty 'day' identity"):
        score.run(args)
