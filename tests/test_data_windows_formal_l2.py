from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from models import data_windows


def test_manifest_backed_quality_days_reach_every_window_loader(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    allowed = ("2026-05-06", "2026-05-07")
    observed: dict[str, tuple[str, ...]] = {}
    trades = pd.DataFrame(
        {
            "transact_time": np.asarray([0, 1000], dtype=np.int64),
            "price": np.asarray([100.0, 100.1]),
        }
    )
    bars = pd.DataFrame(
        {"close": [100.0, 100.1], "trade_count": [1.0, 1.0]},
        index=pd.Index([0, 1000], dtype=np.int64),
    )

    def capture(name, value):
        def loader(*_args, quality_allowed_days=(), **_kwargs):
            observed[name] = tuple(quality_allowed_days)
            return value

        return loader

    monkeypatch.setattr(
        data_windows.bt,
        "load_execution_trades",
        capture("trades", trades),
    )
    monkeypatch.setattr(
        data_windows.bt,
        "load_1s_bars",
        capture("bars", bars),
    )
    monkeypatch.setattr(
        data_windows.bt,
        "load_bbo_data",
        capture("bbo", object()),
    )
    monkeypatch.setattr(
        data_windows.bt,
        "load_l2_data",
        capture("l2", object()),
    )
    monkeypatch.setattr(
        data_windows.bt,
        "build_rolling_variance",
        lambda _bars: (np.asarray([0]), np.asarray([1.0])),
    )
    monkeypatch.setattr(
        data_windows.bt,
        "build_trade_intensity",
        lambda _bars: (np.asarray([0]), np.asarray([1.0])),
    )
    monkeypatch.setattr(
        data_windows.bt,
        "build_squared_returns",
        lambda _bars: (np.asarray([0]), np.asarray([0.0])),
    )

    data_windows.load_tick_window(
        "2026-05-07",
        {
            "execution_trade_source": "trades",
            "market_context_warmup_days": 1,
            "_formal_quality_allowed_days": list(allowed),
            "_formal_quality_day_manifest_sha256": "frozen-sha",
        },
        load_ml=False,
        require_ml=False,
        require_historical_bbo=False,
    )

    assert observed == {
        "trades": allowed,
        "bars": allowed,
        "bbo": allowed,
        "l2": allowed,
    }


def test_formal_gate_covers_loaded_market_context_days(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset_root = tmp_path / "normalized_l2_100ms_v2"
    monkeypatch.setattr(data_windows.bt, "BBO_DIR", dataset_root / "bbo")
    monkeypatch.setattr(data_windows.bt, "L2_DIR", dataset_root / "l2")
    observed: dict[str, object] = {}

    def capture(
        root: Path,
        days: list[str],
        *,
        verify_hashes: bool,
    ) -> None:
        observed.update(
            root=root,
            days=days,
            verify_hashes=verify_hashes,
        )
        raise RuntimeError("formal gate observed")

    monkeypatch.setattr(
        data_windows.l2_registry,
        "require_formal_days",
        capture,
    )

    with pytest.raises(RuntimeError, match="formal gate observed"):
        data_windows.load_tick_window(
            "2026-01-02",
            {
                "market_context_warmup_days": 1,
                "require_formal_l2": True,
            },
            load_ml=False,
            require_ml=False,
            require_formal_l2=True,
            verify_formal_l2_hashes=True,
        )

    assert observed == {
        "root": dataset_root.resolve(),
        "days": ["2026-01-01", "2026-01-02"],
        "verify_hashes": True,
    }
