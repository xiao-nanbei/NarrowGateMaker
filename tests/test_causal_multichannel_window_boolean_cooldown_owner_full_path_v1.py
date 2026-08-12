from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_full_path_v1 as study,
)


def _policy(path: Path) -> tuple[Path, str]:
    payload = {
        "identity": "causal_multichannel_window_boolean_cooldown_owner_policy_v1",
        "selection": {
            "BUY": "CONTROL_85N",
            "SELL": "M2_boolean_small_profile_full_common33_refit",
        },
        "permissions": {
            "research_supported": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="ascii")
    return path, study._sha256_file(path)


def _summary(day: str, arm: str, *, supported: bool, terminal: float = -1.0) -> dict:
    return {
        "day": day,
        "arm": arm,
        "pnl_usdc": terminal,
        "terminal_mtm_pnl_usdc": terminal,
        "closed_campaign_value_usdc": terminal - 0.1,
        "fills_bid": 2,
        "fills_ask": 3,
        "fills_total": 5,
        "abs_inventory_time_btc_s": 10.0,
        "max_inventory_btc": 0.002,
        "final_inventory_btc": 0.0,
        "campaign_mae_usdc": -0.2,
        "candidate_supported_day": supported if arm == study.CANDIDATE_ARM else False,
        "candidate_fallback_reason": (
            "" if supported and arm == study.CANDIDATE_ARM else "fallback"
        ),
    }


def _campaigns(day: str, arm: str, terminal: float = -1.0) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "day": [day],
            "arm": [arm],
            "terminal_value_usdc": [terminal],
        }
    )


def _fills(day: str, arm: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "day": [day],
            "arm": [arm],
            "fill_ts": [1],
            "side": ["BUY"],
            "fill_qty": [0.001],
        }
    )


def _fake_spec() -> dict:
    days = [f"2026-01-{index:02d}" for index in range(1, 32)] + [
        f"2026-02-{index:02d}" for index in range(1, 20)
    ]
    assert len(days) == 50
    return {
        "baseline": {"config_sha256": "a" * 64},
        "immutable_prefix": {
            "ordered_utc_days": days[:40],
            "baseline_sha256": "b" * 64,
        },
        "added_panel": {"ordered_utc_days": days[40:]},
        "sources": {"model_bundle_meta_sha256": "c" * 64},
    }


def test_frozen_owner_runner_uses_exact_40_10_50_denominator() -> None:
    spec = study.baseline50._spec()
    days = study.baseline50.ordered_days(spec)

    assert len(days) == 50
    assert days[:40] == spec["immutable_prefix"]["ordered_utc_days"]
    assert days[40:] == spec["added_panel"]["ordered_utc_days"]
    assert study._selected_days(spec, None) == days
    assert study._selected_days(spec, [days[49], days[0]]) == [days[0], days[49]]


def test_missing_m2_cache_is_unsupported_without_becoming_invalid(tmp_path: Path) -> None:
    cache, binding = study._open_candidate_cache(tmp_path / "observations", "2026-01-01")

    assert cache is None
    assert binding["supported"] is False
    assert binding["reason"] == "daily_raw_m2_observation_cache_missing"


def test_partial_m2_cache_fails_closed_instead_of_falling_back(tmp_path: Path) -> None:
    day_root = tmp_path / "observations" / "2026-01-01"
    day_root.mkdir(parents=True)

    with pytest.raises(study.OwnerFullPathError, match="not validly admitted"):
        study._open_candidate_cache(tmp_path / "observations", "2026-01-01")


@dataclass(frozen=True)
class _Decision:
    action_id: str = "CONTROL_85N"
    duration_ms: float = 85_000.0
    fallback_reason: str = ""
    matched_rule_index: int | None = 0
    policy_sha256: str = study.DEFAULT_POLICY_SHA256
    predicate_bundle_sha256: str = "d" * 64
    snapshot_id: str = "snapshot-1"
    support_valid: bool = True


class _Evaluator:
    def evaluate(self, _snapshot, *, baseline_duration_ms: float) -> _Decision:
        assert baseline_duration_ms > 0
        return _Decision(duration_ms=baseline_duration_ms)

    def audit(self) -> dict[str, int]:
        return {"evaluations": 1}


class _Emitter:
    def capture_exposure_fill(self, **_kwargs):
        return SimpleNamespace(snapshot_id="snapshot-1")

    def audit(self) -> dict[str, int]:
        return {"snapshots_emitted": 1, "fallback_snapshots": 0}


def test_python_arm_binds_repeated_policy_to_every_exposure_fill(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured: dict = {}

    def simulate(engine, *_args, **kwargs):
        captured["engine"] = engine
        params = _args[3]
        captured["params"] = params
        assert kwargs["campaign_repair_model"] is not None
        return {
            "_campaign_repair_trace": [],
            "_cooldown_duration_policy_decisions": [
                {
                    "exposure_fill_ordinal": 1,
                    "fill_visible_ts_ms": 1,
                    "side": "SELL",
                    "role_at_fill": "add",
                    "campaign_id": 1,
                    "order_id": 1,
                    "baseline_duration_ms": 170_000.0,
                    "action_id": "FIXED_166S",
                    "duration_ms": 166_000.0,
                    "fallback_reason": "",
                    "matched_rule_index": 1,
                    "policy_sha256": study.DEFAULT_POLICY_SHA256,
                    "predicate_bundle_sha256": "d" * 64,
                    "snapshot_id": "snapshot-1",
                    "support_valid": True,
                }
            ],
            "_cooldown_v2_snapshot_receipts": [{"snapshot_id": "snapshot-1"}],
            "_cooldown_v2_snapshot_emitter_audit": {
                "snapshots_emitted": 1,
                "fallback_snapshots": 0,
            },
            "_cooldown_duration_policy_audit": {"evaluations": 1},
            "exchange_book_queue_mode": "disabled",
            "exchange_book_queue_scope": "disabled",
        }

    monkeypatch.setattr(study.bt, "_simulate_tick_with_engine", simulate)
    monkeypatch.setattr(
        study.native_runner,
        "_project_arm",
        lambda **kwargs: (
            _summary("2026-01-01", kwargs["arm"], supported=True),
            _campaigns("2026-01-01", kwargs["arm"]),
            _fills("2026-01-01", kwargs["arm"]),
        ),
    )
    window = SimpleNamespace(
        trades=pd.DataFrame(),
        var_ts_ms=[],
        var_ssq=[],
        bbo_data=None,
        l2_data=None,
        var_ti=None,
        var_retsq=None,
    )

    summary, _, _, decisions = study._simulate_python_arm(
        day="2026-01-01",
        arm=study.CANDIDATE_ARM,
        window=window,
        ml_data=(),
        base={"order_size": 0.001},
        progress_path=tmp_path / "progress.json",
        progress_interval_events=10,
        emitter=_Emitter(),
        evaluator=_Evaluator(),
    )

    assert captured["engine"] == "python"
    assert captured["params"]["cooldown_v2_snapshot_emitter"].audit()[
        "snapshots_emitted"
    ] == 1
    assert "cooldown_duration_policy_evaluator" in captured["params"]
    assert len(decisions) == 1
    assert summary["repeated_policy_decision_count"] == 1
    assert summary["strict_queue_authority"] is False


def test_execute_day_missing_cache_runs_exact_candidate_control_fallback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    policy_path, policy_sha = _policy(tmp_path / "policy.json")
    spec = _fake_spec()
    day = spec["immutable_prefix"]["ordered_utc_days"][0]
    cache_root = tmp_path / "baseline-cache"
    cache_root.mkdir()
    (cache_root / "execution-plan.json").write_text("{}\n", encoding="ascii")
    monkeypatch.setattr(
        study,
        "_frozen_context",
        lambda _root: (spec, {}, {"ordered_utc_days": study.baseline50.ordered_days(spec)}),
    )
    monkeypatch.setattr(
        study.baseline50,
        "_load_day_inputs",
        lambda *_args, **_kwargs: (SimpleNamespace(), (), {"window": "shared"}),
    )
    monkeypatch.setattr(
        study.baseline50,
        "_base_params",
        lambda _spec: ({"order_size": 0.001, "rng_seed": 42}, {"projection": "same"}),
    )
    monkeypatch.setattr(
        study,
        "_candidate_runtime",
        lambda **_kwargs: (
            None,
            None,
            {
                "supported": False,
                "reason": "daily_raw_m2_observation_cache_missing",
            },
        ),
    )

    def fake_arm(*, day: str, arm: str, **_kwargs):
        return (
            _summary(day, arm, supported=False),
            _campaigns(day, arm),
            _fills(day, arm),
            pd.DataFrame(columns=study.DECISION_COLUMNS),
        )

    monkeypatch.setattr(study, "_simulate_python_arm", fake_arm)

    result = study.execute_day(
        day,
        cache_root=cache_root,
        observation_cache_root=tmp_path / "observations",
        policy_path=policy_path,
        policy_sha256=policy_sha,
        output=tmp_path / "output",
    )

    assert result["candidate_supported"] is False
    manifest = study._load_admitted_day(tmp_path / "output", day)
    assert manifest is not None
    payload = study._load_json(Path(manifest["summary"]["path"]), role="summary")
    assert payload["unsupported_day_preserved_in_denominator"] is True
    assert payload["candidate_decision_count"] == 0
    assert len(pd.read_parquet(manifest["campaigns"]["path"])) == 2


def _write_synthetic_day(
    output: Path,
    day: str,
    *,
    supported: bool,
    candidate_delta: float,
) -> None:
    final = output / "days" / day
    final.mkdir(parents=True)
    control = _summary(day, study.CONTROL_ARM, supported=False, terminal=-1.0)
    candidate = _summary(
        day,
        study.CANDIDATE_ARM,
        supported=supported,
        terminal=-1.0 + candidate_delta,
    )
    candidate["closed_campaign_value_usdc"] += candidate_delta
    summary_path = final / "summary.json"
    campaigns_path = final / "campaigns.parquet"
    fills_path = final / "fills.parquet"
    decisions_path = final / "candidate_decisions.parquet"
    study._atomic_json(
        summary_path,
        {
            "identity": study.IDENTITY,
            "day": day,
            "arms": [control, candidate],
        },
    )
    pd.concat(
        [
            _campaigns(day, study.CONTROL_ARM, -1.0),
            _campaigns(day, study.CANDIDATE_ARM, -1.0 + candidate_delta),
        ],
        ignore_index=True,
    ).to_parquet(campaigns_path, index=False)
    pd.concat(
        [_fills(day, study.CONTROL_ARM), _fills(day, study.CANDIDATE_ARM)],
        ignore_index=True,
    ).to_parquet(fills_path, index=False)
    decisions = pd.DataFrame(columns=study.DECISION_COLUMNS)
    if supported:
        decisions = pd.DataFrame(
            [
                {
                    "day": day,
                    "exposure_fill_ordinal": 1,
                    "fill_visible_ts_ms": 1,
                    "side": "SELL",
                    "role_at_fill": "add",
                    "campaign_id": 1,
                    "order_id": 1,
                    "baseline_duration_ms": 170_000.0,
                    "action_id": "FIXED_166S",
                    "duration_ms": 166_000.0,
                    "fallback_reason": "",
                    "matched_rule_index": 0,
                    "policy_sha256": study.DEFAULT_POLICY_SHA256,
                    "predicate_bundle_sha256": "d" * 64,
                    "snapshot_id": f"snapshot-{day}",
                    "support_valid": True,
                }
            ]
        )
    decisions.to_parquet(decisions_path, index=False)
    manifest = {
        "identity": study.IDENTITY,
        "day": day,
        "implementation": {
            "runner_path": "/frozen/owner_full_path.py",
            "runner_sha256": "e" * 64,
            "backtest_tick_sha256": "f" * 64,
        },
        "candidate_support": {
            "supported": supported,
            "reason": "admitted" if supported else "missing",
        },
        "summary": {"path": str(summary_path), "sha256": study._sha256_file(summary_path)},
        "campaigns": {
            "path": str(campaigns_path),
            "sha256": study._sha256_file(campaigns_path),
        },
        "fills": {"path": str(fills_path), "sha256": study._sha256_file(fills_path)},
        "candidate_decisions": {
            "path": str(decisions_path),
            "sha256": study._sha256_file(decisions_path),
        },
    }
    study._atomic_json(final / "manifest.json", manifest)
    study._atomic_text(
        final / study.DAY_SUCCESS,
        study._sha256_file(final / "manifest.json") + "\n",
    )


def test_finalize_reports_prefix_added_and_pooled_with_all_permissions_false(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    policy_path, policy_sha = _policy(tmp_path / "policy.json")
    spec = _fake_spec()
    days = study.baseline50.ordered_days(spec)
    output = tmp_path / "output"
    for index, day in enumerate(days):
        supported = index < 33
        _write_synthetic_day(
            output,
            day,
            supported=supported,
            candidate_delta=1.0 if supported else 0.0,
        )
    cache_root = tmp_path / "baseline-cache"
    cache_root.mkdir()
    (cache_root / "execution-plan.json").write_text("{}\n", encoding="ascii")
    monkeypatch.setattr(study, "_frozen_context", lambda _root: (spec, {}, {}))

    report = study.finalize(
        cache_root=cache_root,
        policy_path=policy_path,
        policy_sha256=policy_sha,
        output=output,
        bootstrap_draws=99,
        bootstrap_seed=7,
    )

    economics = report["economics"]
    assert economics["prefix_40"]["days"] == 40
    assert economics["added_10"]["days"] == 10
    assert economics["pooled_50"]["days"] == 50
    assert economics["pooled_50"]["candidate_supported_days"] == 33
    assert economics["pooled_50"]["candidate_unsupported_days"] == 17
    assert economics["pooled_50"]["delta_candidate_minus_control"][
        "terminal_mtm_pnl_usdc"
    ] == 33.0
    assert report["candidate_support"]["denominator_days_dropped"] == 0
    assert not any(report["permissions"].values())
    assert report["implementation_binding"]["replay"]["runner_sha256"] == "e" * 64
    assert report["implementation_binding"][
        "replay_and_finalizer_runner_hash_differ"
    ]
    assert (output / "panel" / study.PANEL_SUCCESS).is_file()


def test_day_bootstrap_is_exact_for_constant_noop_days() -> None:
    result = study._bootstrap_day_mean(
        pd.Series([0.0] * 10).to_numpy(),
        draws=99,
        seed=1,
    )

    assert result["mean_per_day"] == 0.0
    assert result["ci95_per_day"] == [0.0, 0.0]


def test_paired_daily_validates_precomputed_campaign_tail_without_suffix_drift() -> None:
    day = "2026-01-01"
    daily = pd.DataFrame(
        [
            {
                **_summary(day, study.CONTROL_ARM, supported=False),
                "campaign_q10_usdc": -1.0,
                "campaign_cvar10_usdc": -1.0,
            },
            {
                **_summary(day, study.CANDIDATE_ARM, supported=True),
                "campaign_q10_usdc": -0.5,
                "campaign_cvar10_usdc": -0.5,
            },
        ]
    )
    campaigns = pd.concat(
        [
            _campaigns(day, study.CONTROL_ARM, -1.0),
            _campaigns(day, study.CANDIDATE_ARM, -0.5),
        ],
        ignore_index=True,
    )

    paired = study._paired_daily(daily, campaigns)

    assert paired.loc[0, "control_campaign_q10_usdc"] == -1.0
    assert paired.loc[0, "candidate_campaign_q10_usdc"] == -0.5
    assert paired.loc[0, "delta_campaign_q10_usdc"] == 0.5


def test_paired_daily_rejects_precomputed_campaign_tail_mismatch() -> None:
    day = "2026-01-01"
    daily = pd.DataFrame(
        [
            {
                **_summary(day, study.CONTROL_ARM, supported=False),
                "campaign_q10_usdc": -2.0,
                "campaign_cvar10_usdc": -1.0,
            },
            {
                **_summary(day, study.CANDIDATE_ARM, supported=True),
                "campaign_q10_usdc": -0.5,
                "campaign_cvar10_usdc": -0.5,
            },
        ]
    )
    campaigns = pd.concat(
        [
            _campaigns(day, study.CONTROL_ARM, -1.0),
            _campaigns(day, study.CANDIDATE_ARM, -0.5),
        ],
        ignore_index=True,
    )

    with pytest.raises(study.OwnerFullPathError, match="campaign-derived tail"):
        study._paired_daily(daily, campaigns)
