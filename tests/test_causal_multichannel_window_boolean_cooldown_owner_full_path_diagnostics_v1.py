from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_full_path_diagnostics_v1 as diagnostics,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_full_path_v1 as owner,
)


def _daily(day: str, arm: str, *, supported: bool, terminal: float) -> dict:
    return {
        "day": day,
        "arm": arm,
        "terminal_mtm_pnl_usdc": terminal,
        "closed_campaign_value_usdc": terminal - 0.1,
        "fills_total": 2 if arm == owner.CONTROL_ARM else 1,
        "candidate_supported_day": supported if arm == owner.CANDIDATE_ARM else False,
        "candidate_fallback_reason": (
            "" if supported and arm == owner.CANDIDATE_ARM else "control_or_fallback"
        ),
    }


def _campaigns(day: str) -> pd.DataFrame:
    rows = []
    for arm, shift in ((owner.CONTROL_ARM, 0.0), (owner.CANDIDATE_ARM, 0.2)):
        rows.extend(
            [
                {
                    "day": day,
                    "arm": arm,
                    "inventory_side": "LONG",
                    "closed": True,
                    "terminal_value_usdc": -0.5 + shift,
                    "multi_level": True,
                },
                {
                    "day": day,
                    "arm": arm,
                    "inventory_side": "SHORT",
                    "closed": True,
                    "terminal_value_usdc": 0.1 + shift,
                    "multi_level": False,
                },
            ]
        )
    return pd.DataFrame(rows)


def _fills(day: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "day": day,
                "arm": owner.CONTROL_ARM,
                "side": "SELL",
                "fill_qty": 0.001,
                "quote_px": 100.0,
                "ev_30s": -1.0,
                "toxic_30s": True,
                "inventory_role_at_submit": "opener",
            },
            {
                "day": day,
                "arm": owner.CONTROL_ARM,
                "side": "SELL",
                "fill_qty": 0.001,
                "quote_px": 100.0,
                "ev_30s": -2.0,
                "toxic_30s": True,
                "inventory_role_at_submit": "add",
            },
            {
                "day": day,
                "arm": owner.CANDIDATE_ARM,
                "side": "SELL",
                "fill_qty": 0.001,
                "quote_px": 100.0,
                "ev_30s": 1.0,
                "toxic_30s": False,
                "inventory_role_at_submit": "opener",
            },
        ]
    )


def _decisions(day: str) -> pd.DataFrame:
    base = {
        "day": day,
        "fill_visible_ts_ms": 1,
        "side": "SELL",
        "campaign_id": 1,
        "order_id": 1,
        "policy_sha256": "a" * 64,
        "predicate_bundle_sha256": "b" * 64,
        "support_valid": True,
    }
    return pd.DataFrame(
        [
            {
                **base,
                "exposure_fill_ordinal": 1,
                "role_at_fill": "opener",
                "baseline_duration_ms": 85_000.0,
                "action_id": "FIXED_211S",
                "duration_ms": 211_000.0,
                "fallback_reason": None,
                "matched_rule_index": 2,
                "snapshot_id": "snapshot-1",
            },
            {
                **base,
                "exposure_fill_ordinal": 2,
                "role_at_fill": "add",
                "baseline_duration_ms": 170_000.0,
                "action_id": "CONTROL_85N",
                "duration_ms": 170_000.0,
                "fallback_reason": "no_rule_match",
                "matched_rule_index": None,
                "snapshot_id": "snapshot-2",
            },
        ]
    )


def _write_day(root: Path, day: str, *, supported: bool = True) -> None:
    target = root / "days" / day
    target.mkdir(parents=True)
    summary_path = target / "summary.json"
    campaigns_path = target / "campaigns.parquet"
    fills_path = target / "fills.parquet"
    decisions_path = target / "candidate_decisions.parquet"
    summary = {
        "identity": owner.IDENTITY,
        "day": day,
        "arms": [
            _daily(day, owner.CONTROL_ARM, supported=False, terminal=-1.0),
            _daily(day, owner.CANDIDATE_ARM, supported=supported, terminal=-0.5),
        ],
        "research_supported": False,
        "action_authorized": False,
        "live_authorized": False,
        "strict_queue_authority": False,
        "receive_time_transport_authority": False,
    }
    summary_path.write_text(json.dumps(summary, sort_keys=True) + "\n", encoding="ascii")
    _campaigns(day).to_parquet(campaigns_path, index=False)
    _fills(day).to_parquet(fills_path, index=False)
    decisions = _decisions(day) if supported else pd.DataFrame(columns=owner.DECISION_COLUMNS)
    decisions.to_parquet(decisions_path, index=False)
    manifest = {
        "identity": owner.IDENTITY,
        "day": day,
        "candidate_support": {
            "supported": supported,
            "reason": "admitted" if supported else "missing_cache",
        },
        "summary": {"path": str(summary_path), "sha256": owner._sha256_file(summary_path)},
        "campaigns": {
            "path": str(campaigns_path),
            "sha256": owner._sha256_file(campaigns_path),
        },
        "fills": {"path": str(fills_path), "sha256": owner._sha256_file(fills_path)},
        "candidate_decisions": {
            "path": str(decisions_path),
            "sha256": owner._sha256_file(decisions_path),
        },
        "permissions": {
            "research_supported": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    manifest_path = target / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True) + "\n", encoding="ascii")
    (target / owner.DAY_SUCCESS).write_text(
        owner._sha256_file(manifest_path) + "\n", encoding="ascii"
    )


def _write_panel(root: Path, day: str) -> None:
    panel = root / "panel"
    panel.mkdir(parents=True)
    daily = pd.DataFrame(
        [
            _daily(day, owner.CONTROL_ARM, supported=False, terminal=-1.0),
            _daily(day, owner.CANDIDATE_ARM, supported=True, terminal=-0.5),
        ]
    )
    files = {
        "report.json": {
            "identity": owner.IDENTITY,
            "panel": {"daily_fresh_start": True, "continuous_replay": False},
            "permissions": {
                "research_supported": False,
                "action_authorized": False,
                "live_authorized": False,
            },
        },
        "daily_arms.parquet": daily,
        "campaigns.parquet": _campaigns(day),
        "fills.parquet": _fills(day),
        "candidate_decisions.parquet": _decisions(day),
    }
    manifest_files = []
    for name, value in files.items():
        path = panel / name
        if name.endswith(".json"):
            path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="ascii")
        else:
            value.to_parquet(path, index=False)
        manifest_files.append(
            {
                "relative_path": name,
                "sha256": owner._sha256_file(path),
                "bytes": path.stat().st_size,
            }
        )
    manifest = {
        "identity": owner.IDENTITY,
        "files": manifest_files,
        "permissions": {
            "research_supported": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    manifest_path = panel / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True) + "\n", encoding="ascii")
    (panel / owner.PANEL_SUCCESS).write_text(
        owner._sha256_file(manifest_path) + "\n", encoding="ascii"
    )


def test_partial_diagnostics_use_only_atomically_admitted_days(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    days = ["2026-01-01", "2026-01-02"]
    monkeypatch.setattr(diagnostics, "_frozen_days", lambda: days)
    _write_day(tmp_path, days[0], supported=True)
    (tmp_path / "days" / days[1]).mkdir(parents=True)
    progress = tmp_path / "progress"
    progress.mkdir()
    (progress / f"{days[1]}.json").write_text(
        json.dumps({"day": days[1], "state": "running"}) + "\n", encoding="ascii"
    )

    report = diagnostics.diagnose(tmp_path)

    assert report["status"]["input_mode"] == "partial_admitted_days"
    assert report["status"]["admitted_days"] == 1
    assert report["status"]["missing_days"] == [days[1]]
    assert report["status"]["incomplete_day_directories"] == [days[1]]
    assert report["status"]["progress_state_counts"] == {"running": 1}
    mechanics = report["mechanics"]
    assert mechanics["decision_count"] == 2
    assert mechanics["nonbaseline_count"] == 1
    assert mechanics["nonbaseline_rate"] == pytest.approx(0.5)
    assert mechanics["fallback_reasons"] == {"no_rule_match": 1}
    assert any(
        row["side"] == "SELL"
        and row["role_at_fill"] == "opener"
        and row["action_id"] == "FIXED_211S"
        for row in mechanics["side_role_action_duration"]
    )
    assert report["permissions"]["live_authorized"] is False
    assert report["evidence_scope"]["queue_semantics"] == "modeled_queue"
    assert report["evidence_scope"]["daily_fresh_start"] is True


@pytest.mark.parametrize("value", [None, "", "None", "none", "NULL", "nan"])
def test_serialized_null_fallback_reason_is_not_an_explicit_fallback(value: object) -> None:
    assert diagnostics._normalise_fallback_reason(value) is None


def test_buy_contract_control_is_separate_from_invalid_fallback() -> None:
    inputs = diagnostics.DiagnosticInputs(
        source_root=Path("/tmp"),
        input_mode="unit",
        expected_days=("2026-01-01",),
        admitted_days=("2026-01-01",),
        missing_days=(),
        incomplete_day_directories=(),
        unexpected_day_directories=(),
        progress_rows=(),
        bindings=(),
        daily=pd.DataFrame(),
        campaigns=pd.DataFrame(),
        fills=pd.DataFrame(),
        decisions=pd.DataFrame(
            [
                {
                    "day": "2026-01-01",
                    "side": "BUY",
                    "role_at_fill": "opener",
                    "campaign_id": 1,
                    "action_id": "CONTROL_85N",
                    "duration_ms": 85_000,
                    "baseline_duration_ms": 85_000,
                    "support_valid": True,
                    "fallback_reason": "buy_control_by_contract",
                },
                {
                    "day": "2026-01-01",
                    "side": "SELL",
                    "role_at_fill": "opener",
                    "campaign_id": 2,
                    "action_id": "FIXED_211S",
                    "duration_ms": 211_000,
                    "baseline_duration_ms": 85_000,
                    "support_valid": True,
                    "fallback_reason": "None",
                },
            ]
        ),
    )

    mechanics = diagnostics._mechanics(inputs)

    assert mechanics["contract_control_count"] == 1
    assert mechanics["fallback_reasons"] == {}


def test_fill_and_campaign_control_candidate_decomposition(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    day = "2026-01-01"
    monkeypatch.setattr(diagnostics, "_frozen_days", lambda: [day])
    _write_day(tmp_path, day)

    report = diagnostics.diagnose(tmp_path)

    fill_overall = report["fills_and_maker_value"]["overall"]
    assert fill_overall["control"]["fills"] == 2
    assert fill_overall["candidate"]["fills"] == 1
    assert fill_overall["fills_retention"] == pytest.approx(0.5)
    assert fill_overall["control"]["maker_value_30s_usdc"] == pytest.approx(-0.003)
    assert fill_overall["candidate"]["maker_value_30s_usdc"] == pytest.approx(0.001)
    sell_add = next(
        row
        for row in report["fills_and_maker_value"]["side_role"]
        if row["side"] == "SELL" and row["role"] == "add"
    )
    assert sell_add["control"]["fills"] == 1
    assert sell_add["candidate"]["fills"] == 0

    campaigns = report["campaigns_and_terminal_tail"]
    long_multi = next(
        row
        for row in campaigns["inventory_side_level"]
        if row["inventory_side"] == "LONG"
        and row["inventory_level"] == "MULTI"
    )
    assert long_multi["control"]["terminal_value_usdc"] == pytest.approx(-0.5)
    assert long_multi["candidate"]["terminal_value_usdc"] == pytest.approx(-0.3)
    assert campaigns["overall"]["candidate"]["q10_usdc"] > campaigns["overall"][
        "control"
    ]["q10_usdc"]
    assert report["daily_economics"]["paired_delta"][
        "positive_terminal_delta_days"
    ] == 1


def test_final_panel_input_is_hash_validated(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    day = "2026-01-01"
    monkeypatch.setattr(diagnostics, "_frozen_days", lambda: [day])
    _write_panel(tmp_path, day)

    report = diagnostics.diagnose(tmp_path / "panel")

    assert report["status"]["input_mode"] == "final_panel"
    assert report["status"]["admitted_days"] == 1
    assert report["mechanics"]["nonbaseline_count"] == 1

    with (tmp_path / "panel" / "fills.parquet").open("ab") as handle:
        handle.write(b"drift")
    with pytest.raises(diagnostics.OwnerFullPathDiagnosticsError, match="drifted"):
        diagnostics.diagnose(tmp_path / "panel")


def test_incomplete_final_panel_fails_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(diagnostics, "_frozen_days", lambda: ["2026-01-01"])
    panel = tmp_path / "panel"
    panel.mkdir()
    (panel / "manifest.json").write_text("{}\n", encoding="ascii")

    with pytest.raises(
        diagnostics.OwnerFullPathDiagnosticsError,
        match="final panel admission is incomplete",
    ):
        diagnostics.status(tmp_path)
