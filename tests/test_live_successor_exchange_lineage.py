from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from live.main import validate_startup_exchange_reconciliation_lineage
from scripts import deploy_f05_buy_e3_owner_v1 as deploy


def _sha(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    ).hexdigest()


def _receipt(path: Path, *, api_key: str = "key") -> list[dict[str, str | int]]:
    rows: list[dict[str, str | int]] = [
        {
            "symbol": "BTCUSDC",
            "position_side": "BOTH",
            "position_amt": "0.001",
            "entry_price": "64000.0",
            "update_time_ms": 123,
        }
    ]
    payload = {
        "schema_version": "narrowgate_stopped_exchange_reconciliation.v1",
        "status": "signed_open_orders_zero_exact_position_stable",
        "symbol": "BTCUSDC",
        "signed_endpoints": ["/fapi/v1/openOrders", "/fapi/v2/positionRisk"],
        "open_order_count": 0,
        "position_rows": rows,
        "account_key_sha256": hashlib.sha256(api_key.encode("utf-8")).hexdigest(),
        "position_lineage_sha256": _sha(rows),
    }
    payload["canonical_exchange_reconciliation_sha256"] = _sha(payload)
    path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="ascii",
    )
    path.chmod(0o600)
    return rows


def _bind_receipt(
    monkeypatch: pytest.MonkeyPatch, receipt: Path, *, api_key: str = "key"
) -> None:
    payload = json.loads(receipt.read_text(encoding="ascii"))
    monkeypatch.setenv(
        "NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_PATH", str(receipt)
    )
    monkeypatch.setenv(
        "NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_FILE_SHA256",
        hashlib.sha256(receipt.read_bytes()).hexdigest(),
    )
    monkeypatch.setenv(
        "NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_CANONICAL_SHA256",
        payload["canonical_exchange_reconciliation_sha256"],
    )
    monkeypatch.setenv(
        "NARROWGATE_STARTUP_EXCHANGE_RECONCILIATION_ACCOUNT_KEY_SHA256",
        hashlib.sha256(api_key.encode("utf-8")).hexdigest(),
    )


def _engine_for(rows: list[dict[str, str | int]]) -> SimpleNamespace:
    row = rows[0]
    return SimpleNamespace(
        inventory=SimpleNamespace(
            snapshot=SimpleNamespace(
                qty=float(row["position_amt"]),
                avg_entry_price=float(row["entry_price"]),
            ),
            reconciliation_snapshot=lambda: {
                "snapshot_update_time_ms": int(row["update_time_ms"])
            },
        )
    )


def test_startup_rebinds_the_exact_stopped_signed_position(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt = tmp_path / "exchange.json"
    rows = _receipt(receipt)
    rest = SimpleNamespace(
        get_orders=lambda **_kwargs: [],
        get_position_risk=lambda **_kwargs: [
            {
                "symbol": row["symbol"],
                "positionSide": row["position_side"],
                "positionAmt": row["position_amt"],
                "entryPrice": row["entry_price"],
                "updateTime": row["update_time_ms"],
            }
            for row in rows
        ],
    )
    _bind_receipt(monkeypatch, receipt)
    binding = validate_startup_exchange_reconciliation_lineage(
        rest,
        engine=_engine_for(rows),
        symbol="BTCUSDC",
        api_key="key",
    )
    assert binding["position_lineage_sha256"] == _sha(rows)
    assert binding["file_sha256"] == hashlib.sha256(receipt.read_bytes()).hexdigest()
    assert binding["account_key_sha256"] == hashlib.sha256(b"key").hexdigest()


def test_startup_rejects_position_drift_after_the_stopped_barrier(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt = tmp_path / "exchange.json"
    rows = _receipt(receipt)
    rows[0]["position_amt"] = "0.002"
    rest = SimpleNamespace(
        get_orders=lambda **_kwargs: [],
        get_position_risk=lambda **_kwargs: [
            {
                "symbol": row["symbol"],
                "positionSide": row["position_side"],
                "positionAmt": row["position_amt"],
                "entryPrice": row["entry_price"],
                "updateTime": row["update_time_ms"],
            }
            for row in rows
        ],
    )
    _bind_receipt(monkeypatch, receipt)
    with pytest.raises(RuntimeError, match="position differs"):
        validate_startup_exchange_reconciliation_lineage(
            rest,
            engine=_engine_for(_receipt(tmp_path / "local.json")),
            symbol="BTCUSDC",
            api_key="key",
        )


def test_startup_rejects_noop_sync_that_did_not_seed_local_inventory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt = tmp_path / "exchange.json"
    rows = _receipt(receipt)
    rest = SimpleNamespace(
        get_orders=lambda **_kwargs: [],
        get_position_risk=lambda **_kwargs: [
            {
                "symbol": row["symbol"],
                "positionSide": row["position_side"],
                "positionAmt": row["position_amt"],
                "entryPrice": row["entry_price"],
                "updateTime": row["update_time_ms"],
            }
            for row in rows
        ],
    )
    engine = SimpleNamespace(
        inventory=SimpleNamespace(
            snapshot=SimpleNamespace(qty=0.0, avg_entry_price=0.0),
            reconciliation_snapshot=lambda: {"snapshot_update_time_ms": 0},
        )
    )
    _bind_receipt(monkeypatch, receipt)
    with pytest.raises(RuntimeError, match="was not seeded"):
        validate_startup_exchange_reconciliation_lineage(
            rest,
            engine=engine,
            symbol="BTCUSDC",
            api_key="key",
        )


def test_signed_exchange_gate_cancels_then_freezes_a_stable_position(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from live import config as live_config
    from live import main as live_main

    config_path = tmp_path / "disabled.yaml"
    config_path.write_text("disabled", encoding="ascii")
    output = tmp_path / "exchange.json"
    cfg = SimpleNamespace(
        symbol="BTCUSDC",
        api=SimpleNamespace(key="key", secret="secret"),
    )
    order_calls = 0
    cancel_calls = 0

    class Rest:
        def get_orders(self, **_kwargs):
            nonlocal order_calls
            order_calls += 1
            return [{"orderId": 1}] if order_calls == 1 else []

        def cancel_open_orders(self, **_kwargs):
            nonlocal cancel_calls
            cancel_calls += 1

        def get_position_risk(self, **_kwargs):
            return [
                {
                    "symbol": "BTCUSDC",
                    "positionSide": "BOTH",
                    "positionAmt": "0.000",
                    "entryPrice": "0.0",
                    "updateTime": 456,
                }
            ]

    rest = Rest()
    monkeypatch.setattr(live_config, "load_config", lambda _path: cfg)
    monkeypatch.setattr(live_main, "create_rest_client", lambda _cfg, dry_run=False: rest)
    payload = deploy.signed_exchange_reconciliation(config_path, output)

    assert cancel_calls == 1
    assert payload["open_order_count"] == 0
    assert payload["position_rows"][0]["position_amt"] == "0.000"
    assert output.stat().st_mode & 0o777 == 0o600
