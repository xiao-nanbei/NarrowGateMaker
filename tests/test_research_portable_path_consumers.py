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
    causal_multichannel_window_boolean_cooldown_modeled_oof as f05_modeled_oof,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_v2_preflight as f05_preflight,
)
from research.families.f10_live_replay_attribution.audit import (
    current_live_held_ber_replay_baseline_50d as f10_baseline,
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


def test_f05_consumers_resolve_public_placeholders(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("NARROWGATE_DATA_ROOT", str(tmp_path))

    assert f05_preflight._repo_path(
        "${NARROWGATE_ROOT}/tests/test_research_portable_path_consumers.py"
    ) == Path(__file__).resolve()
    assert f05_modeled_oof._resolve_bound_path(
        "${NARROWGATE_DATA_ROOT}/modeled-oof"
    ) == (tmp_path / "modeled-oof").resolve()


def test_f10_baseline_resolves_project_data_placeholder(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("NARROWGATE_DATA_ROOT", str(tmp_path))

    assert f10_baseline._resolve_repo_path(
        "${NARROWGATE_DATA_ROOT}/baseline/execution-plan.json"
    ) == (tmp_path / "baseline/execution-plan.json").resolve()
