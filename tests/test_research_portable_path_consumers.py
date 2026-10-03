from __future__ import annotations

import hashlib
from pathlib import Path

from models.backtest_config import resolve_backtest_config_path
from research.families.f02_empirical_p3_touch.audit import (
    p3_touch_quote_path_comparison as f02_quote_path,
)
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_training_contract as f03_training,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_v2_preflight as f05_preflight,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_d1_support as f05_d1_support,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    freeze_multiscale_ema_boolean_cooldown_duration_policy as f05_freeze,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    multiscale_ema_boolean_cooldown_duration_policy_study as f05_study,
)
from research.families.f10_live_replay_attribution.audit import (
    current_live_held_ber_replay_baseline_50d as f10_baseline,
)
from research.families.f10_live_replay_attribution.audit import (
    current_live_held_ber_strict_native_latency_baseline_50d as f10_strict,
)
from research.governance.paths import resolve_research_path


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_backtest_config_resolves_public_live_config_placeholder(
    tmp_path: Path,
) -> None:
    assert resolve_backtest_config_path(
        "${NARROWGATE_LIVE_CONFIG}", root=tmp_path
    ) == (tmp_path / "docs/private/live_config.current.local.yaml").resolve()


def test_f02_identity_resolves_data_root_placeholder(
    tmp_path: Path,
    monkeypatch,
) -> None:
    receipt = tmp_path / "receipt.json"
    receipt.write_text("{}\n", encoding="utf-8")
    monkeypatch.setenv("NARROWGATE_DATA_ROOT", str(tmp_path))

    resolved = f02_quote_path._require_identity(
        {
            "path": "${NARROWGATE_DATA_ROOT}/receipt.json",
            "sha256": _sha256(receipt),
        },
        "portable receipt",
    )

    assert resolved == receipt.resolve()


def test_f03_training_contract_resolves_repository_placeholder(
    tmp_path: Path,
) -> None:
    source = tmp_path / "research/input.json"
    source.parent.mkdir(parents=True)
    source.write_text("{}\n", encoding="utf-8")

    assert f03_training._resolve(
        "${NARROWGATE_ROOT}/research/input.json", root=tmp_path
    ) == source.resolve()


def test_research_resolver_preserves_missing_portable_target(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("NARROWGATE_DATA_ROOT", str(tmp_path))

    assert resolve_research_path(
        "${NARROWGATE_DATA_ROOT}/future/manifest.json",
        require_exists=False,
    ) == (tmp_path / "future/manifest.json").resolve()


def test_f05_preflight_resolves_repository_placeholder() -> None:
    assert f05_preflight._repo_path(
        "${NARROWGATE_ROOT}/tests/test_research_portable_path_consumers.py"
    ) == Path(__file__).resolve()


def test_f10_baseline_resolves_project_data_placeholder(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("NARROWGATE_DATA_ROOT", str(tmp_path))

    assert f10_baseline._resolve_repo_path(
        "${NARROWGATE_DATA_ROOT}/baseline/execution-plan.json"
    ) == (tmp_path / "baseline/execution-plan.json").resolve()


def test_private_research_defaults_resolve_only_from_private_root(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("NARROWGATE_PRIVATE_RESEARCH_ROOT", str(tmp_path))

    assert f10_baseline._spec_path() == (
        tmp_path / "current_live_held_ber_replay_baseline_50d_spec_20260810.json"
    ).resolve()
    assert f10_strict._spec_path() == (
        tmp_path
        / "current_live_held_ber_strict_native_latency_baseline_50d_v1_spec_20260810.json"
    ).resolve()
    assert f05_d1_support._panel_spec_path() == f10_baseline._spec_path()
    assert f05_d1_support._strict_spec_path() == f10_strict._spec_path()
    assert f05_freeze._baseline_path() == (
        tmp_path / "current_live_held_ber_replay_baseline_40d_20260809.json"
    ).resolve()
    assert f05_study._current_baseline_path() == f05_freeze._baseline_path()


def test_private_research_defaults_fail_closed_without_private_root(monkeypatch) -> None:
    monkeypatch.delenv("NARROWGATE_PRIVATE_RESEARCH_ROOT", raising=False)

    consumers = (
        (f10_baseline._spec_path, f10_baseline.Baseline50Error),
        (f10_strict._spec_path, f10_strict.StrictNativeLatencyError),
        (f05_d1_support._panel_spec_path, f05_d1_support.SupportAuditError),
        (f05_freeze._baseline_path, f05_freeze.FreezeError),
        (f05_study._current_baseline_path, f05_study.StudyError),
    )
    for resolver, error in consumers:
        try:
            resolver()
        except error as exc:
            assert "NARROWGATE_PRIVATE_RESEARCH_ROOT" in str(exc)
        else:
            raise AssertionError(f"{resolver.__module__} did not fail closed")
