from pathlib import Path

import pandas as pd
import pytest

from features import feature_engineer as fe
from models import backtest_tick as bt


def test_btcusdc_replay_defaults_to_normalized_l2_root(tmp_path: Path) -> None:
    normalized = tmp_path / "normalized_l2_100ms_v2"

    bbo_dir, l2_dir = bt._default_replay_book_dirs(
        normalized_root=normalized,
        environ={},
    )

    assert bbo_dir == (normalized / "bbo").resolve()
    assert l2_dir == (normalized / "l2").resolve()


def test_replay_book_environment_overrides_remain_authoritative(
    tmp_path: Path,
) -> None:
    bbo_override = tmp_path / "custom-bbo"
    l2_override = tmp_path / "custom-l2"

    bbo_dir, l2_dir = bt._default_replay_book_dirs(
        normalized_root=tmp_path / "normalized",
        environ={
            "MM_BBO_DIR": str(bbo_override),
            "MM_L2_DIR": str(l2_override),
        },
    )

    assert bbo_dir == bbo_override.resolve()
    assert l2_dir == l2_override.resolve()


@pytest.mark.parametrize("key", ["MM_BBO_DIR", "MM_L2_DIR"])
def test_replay_rejects_partial_book_override(
    tmp_path: Path,
    key: str,
) -> None:
    with pytest.raises(ValueError, match="must be set together"):
        bt._default_replay_book_dirs(
            normalized_root=tmp_path / "normalized",
            environ={key: str(tmp_path / key.lower())},
        )


def test_feature_books_route_btcusdc_and_btcusdt_separately(
    tmp_path: Path,
) -> None:
    legacy = tmp_path / "legacy"
    normalized = tmp_path / "normalized_l2_100ms_v2"

    btcusdc = fe._book_dirs_for_symbol(
        "BTCUSDC",
        legacy_root=legacy,
        normalized_root=normalized,
        environ={},
    )
    btcusdt = fe._book_dirs_for_symbol(
        "BTCUSDT",
        legacy_root=legacy,
        normalized_root=normalized,
        environ={},
    )

    assert btcusdc == (
        (normalized / "bbo").resolve(),
        (normalized / "l2").resolve(),
    )
    assert btcusdt == (
        (legacy / "bbo").resolve(),
        (legacy / "l2").resolve(),
    )


def test_feature_book_environment_overrides_apply_to_both_symbols(
    tmp_path: Path,
) -> None:
    overrides = {
        "MM_BBO_DIR": str(tmp_path / "override-bbo"),
        "MM_L2_DIR": str(tmp_path / "override-l2"),
    }

    btcusdc = fe._book_dirs_for_symbol(
        "BTCUSDC",
        legacy_root=tmp_path / "legacy",
        normalized_root=tmp_path / "normalized",
        environ=overrides,
    )
    btcusdt = fe._book_dirs_for_symbol(
        "BTCUSDT",
        legacy_root=tmp_path / "legacy",
        normalized_root=tmp_path / "normalized",
        environ=overrides,
    )

    expected = (
        (tmp_path / "override-bbo").resolve(),
        (tmp_path / "override-l2").resolve(),
    )
    assert btcusdc == expected
    assert btcusdt == expected


@pytest.mark.parametrize("key", ["MM_BBO_DIR", "MM_L2_DIR"])
def test_feature_books_reject_partial_override(
    tmp_path: Path,
    key: str,
) -> None:
    with pytest.raises(ValueError, match="must be set together"):
        fe._book_dirs_for_symbol(
            "BTCUSDC",
            legacy_root=tmp_path / "legacy",
            normalized_root=tmp_path / "normalized",
            environ={key: str(tmp_path / key.lower())},
        )


def test_required_execution_l2_fails_instead_of_zero_fill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fe, "load_l2_summary_1s", lambda *_args, **_kwargs: None)
    frame = pd.DataFrame(index=pd.DatetimeIndex([], tz="UTC"))

    with pytest.raises(RuntimeError, match="required execution L2"):
        fe.add_execution_l2_features(
            frame,
            frame.index,
            "2026-01-01",
            "BTCUSDC",
            require_l2=True,
        )


def test_required_taker_tempo_fails_instead_of_zero_fill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        fe,
        "_load_taker_tempo_features",
        lambda *_args, **_kwargs: None,
    )
    frame = pd.DataFrame(index=pd.DatetimeIndex([], tz="UTC"))

    with pytest.raises(RuntimeError, match="required taker-tempo"):
        fe.add_taker_tempo_features(
            frame,
            "BTCUSDC",
            "2026-01-01",
            require_taker_tempo=True,
        )
