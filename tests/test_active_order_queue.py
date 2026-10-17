from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from models.active_order_queue import (
    ActiveOrderQueueCoverageError,
    ActiveOrderQueueSeed,
    load_active_order_queue_data,
)
from models.backtest_tick import simulate_tick


def _write_artifact(root: Path) -> None:
    root.mkdir()
    pd.DataFrame(
        [
            {
                "watch_id": "exact",
                "side": "bid",
                "price_tick": 1000,
                "activate_ts_ms": 1_000,
                "seed_status": "exact",
                "seed_reason": "visible_quantity",
                "seed_qty": 2.5,
                "ambiguous": False,
            },
            {
                "watch_id": "unknown",
                "side": "ask",
                "price_tick": 1002,
                "activate_ts_ms": 2_000,
                "seed_status": "unknown",
                "seed_reason": "outside_snapshot_range",
                "seed_qty": None,
                "ambiguous": False,
            },
        ]
    ).to_parquet(root / "seeds.parquet", index=False)
    pd.DataFrame(
        {
            "watch_id": pd.Series(dtype=str),
            "exchange_ts_ms": pd.Series(dtype="int64"),
        }
    ).to_parquet(root / "level_events.parquet", index=False)
    (root / "summary.json").write_text(
        json.dumps(
            {
                "schema_version": "active_order_queue_tape_v2",
                "day": "2026-01-02",
                "symbol": "BTCUSDC",
                "tick_size": 0.1,
                "watch_count": 2,
                "watch_manifest_sha256": "manifest-hash",
                "missing_raw_hours": [],
                "missing_warmup_hours": [],
            }
        ),
        encoding="utf-8",
    )
    (root / "sequence_audit.json").write_text(
        json.dumps(
            {
                "strict_native_snapshot": True,
                "delta_bootstrap_allowed": False,
                "source_gap_count": 0,
                "time_reversal_count": 0,
                "sequence_stats": {
                    "sequence_gaps": 0,
                    "invalid_sequence_messages": 0,
                    "message_time_reversals": 0,
                },
            }
        ),
        encoding="utf-8",
    )


def test_loader_uses_market_identity_and_preserves_unusable_seed(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "artifact"
    _write_artifact(artifact)

    data = load_active_order_queue_data(
        artifact,
        expected_day="2026-01-02",
        expected_symbol="BTCUSDC",
        expected_tick_size=0.1,
    )
    exact = data.lookup_seed(
        side="BUY",
        price=100.0,
        activate_ts_ms=1_000,
    )
    unknown = data.lookup_seed(
        side="SELL",
        price=100.2,
        activate_ts_ms=2_000,
    )

    assert exact is not None
    assert exact.strict_usable is True
    assert exact.quantity == pytest.approx(2.5)
    assert unknown is not None
    assert unknown.strict_usable is False
    assert data.strict_usable_count == 1
    assert data.watch_count == 2


def test_loader_rejects_sequence_gap(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact"
    _write_artifact(artifact)
    sequence_path = artifact / "sequence_audit.json"
    sequence = json.loads(sequence_path.read_text(encoding="utf-8"))
    sequence["sequence_stats"]["sequence_gaps"] = 1
    sequence_path.write_text(json.dumps(sequence), encoding="utf-8")

    with pytest.raises(ValueError, match="sequence_gaps"):
        load_active_order_queue_data(
            artifact,
            expected_day="2026-01-02",
            expected_symbol="BTCUSDC",
            expected_tick_size=0.1,
        )


def _replay_inputs() -> tuple[pd.DataFrame, dict[str, object]]:
    trades = pd.DataFrame(
        {
            "transact_time": np.arange(0, 6_000, 1_000, dtype=np.int64),
            "price": np.full(6, 100.0),
            "quantity": np.zeros(6),
            "is_buyer_maker": np.ones(6, dtype=np.uint8),
        }
    )
    params: dict[str, object] = {
        "gamma": 0.01,
        "kappa": 1.0,
        "order_size": 0.001,
        "max_inventory": 0.01,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "maker_fee": 0.0,
        "taker_fee": 0.0,
        "eta": 0.0,
        "use_bar_pricing": True,
        "replay_event_clock": "trade",
        "collect_curves": False,
        "position_timeout": 0.0,
        "markout_ema_span_fills": 0,
        "max_exec_book_age_s": 0.0,
        "trace_quotes_max": 100,
        "queue_ahead_base_mult": 0.5,
    }
    return trades, params


class _ExactQueueData:
    def lookup_seed(self, *, side: str, price: float, activate_ts_ms: int):
        return ActiveOrderQueueSeed(
            watch_id=f"{side}:{price}:{activate_ts_ms}",
            side=side,
            price_tick=int(round(price / 0.1)),
            activate_ts_ms=activate_ts_ms,
            status="exact",
            reason="visible_quantity",
            quantity=3.0,
            ambiguous=False,
        )


class _MissingQueueData:
    def lookup_seed(self, *, side: str, price: float, activate_ts_ms: int):
        return None


def test_python_replay_uses_sparse_seed_before_queue_calibration() -> None:
    trades, params = _replay_inputs()
    params["active_order_queue_mode"] = "diagnostic"

    result = simulate_tick(
        trades,
        np.asarray([0], dtype=np.int64),
        np.asarray([1.0]),
        params,
        active_order_queue_data=_ExactQueueData(),
    )

    assert result["active_order_queue_lookup_count"] == 12
    assert result["active_order_queue_exact_count"] == 12
    assert result["active_order_queue_missing_count"] == 0
    assert {
        float(row["queue_init"]) for row in result["_quote_trace"]
    } == {1.5}


def test_strict_sparse_replay_fails_closed_on_missing_seed() -> None:
    trades, params = _replay_inputs()
    params["active_order_queue_mode"] = "strict_sparse"

    with pytest.raises(ActiveOrderQueueCoverageError, match="missing"):
        simulate_tick(
            trades,
            np.asarray([0], dtype=np.int64),
            np.asarray([1.0]),
            params,
            active_order_queue_data=_MissingQueueData(),
        )
