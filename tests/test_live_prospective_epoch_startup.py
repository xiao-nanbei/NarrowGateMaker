from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

import live.main as live_main
from execution.order_lifecycle_journal_storage_v2 import BOUNDED_REMOTE_SPOOL

EXAMPLE_REMOTE_COLLECTION_ROOT = "/srv/example-live/formal_collection"
EXAMPLE_STORAGE_ROOT = "/srv/example-storage"


def test_enabled_startup_binds_writer_before_websocket_and_main_loop(
    monkeypatch,
) -> None:
    events = []
    engine = SimpleNamespace(writer_attached=False, tick_count=0)

    def start_engine() -> None:
        events.append("engine_start")

    engine.start = start_engine

    def sync_position(*, required=False) -> bool:
        assert required is True
        assert events == ["engine_start"]
        events.append("position_converged")
        return True

    engine.sync_position = sync_position

    def initialize(**kwargs):
        assert kwargs["engine"] is engine
        assert events == ["engine_start", "position_converged"]
        engine.writer_attached = True
        events.append("epoch_published_writer_attached")
        return SimpleNamespace(epoch_id="epoch-1"), SimpleNamespace()

    monkeypatch.setattr(
        live_main,
        "initialize_prospective_lifecycle_collection",
        initialize,
    )

    def start_ws(_rest) -> None:
        assert engine.writer_attached is True
        assert engine.tick_count == 0
        events.append("ws_start")

    ws = SimpleNamespace(start=start_ws)
    cfg = SimpleNamespace(
        symbol="BTCUSDC",
        lifecycle_journal_v2=SimpleNamespace(enabled=True),
    )
    rest = SimpleNamespace(get_orders=lambda **_kwargs: [])
    epoch, writer = live_main.start_engine_with_prospective_collection(
        cfg=cfg,
        engine=engine,
        ws=ws,
        rest=rest,
        config_path=Path("config.yaml"),
        native_runtime={},
        dry_run=False,
    )

    assert epoch.epoch_id == "epoch-1"
    assert writer is not None
    assert events == [
        "engine_start",
        "position_converged",
        "epoch_published_writer_attached",
        "ws_start",
    ]


def test_unresolved_startup_order_blocks_epoch_and_websocket(monkeypatch) -> None:
    events = []
    engine = SimpleNamespace(
        start=lambda: events.append("engine_start"),
        sync_position=lambda **_kwargs: events.append("position_sync"),
    )
    ws = SimpleNamespace(start=lambda _rest: events.append("ws_start"))
    cfg = SimpleNamespace(
        symbol="BTCUSDC",
        lifecycle_journal_v2=SimpleNamespace(enabled=True),
    )
    rest = SimpleNamespace(
        get_orders=lambda **_kwargs: [
            {
                "symbol": "BTCUSDC",
                "clientOrderId": "mm_B_unresolved",
                "orderId": 42,
                "side": "BUY",
                "status": "NEW",
                "price": "100.0",
                "origQty": "0.001",
                "executedQty": "0",
            }
        ]
    )
    monkeypatch.setattr(
        live_main,
        "initialize_prospective_lifecycle_collection",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("epoch must not be published")
        ),
    )

    with pytest.raises(RuntimeError, match="ownership did not converge"):
        live_main.start_engine_with_prospective_collection(
            cfg=cfg,
            engine=engine,
            ws=ws,
            rest=rest,
            config_path=Path("config.yaml"),
            native_runtime={},
            dry_run=False,
        )

    assert events == ["engine_start"]


def test_post_cancel_position_sync_failure_blocks_epoch_and_websocket(
    monkeypatch,
) -> None:
    events = []

    def failed_sync(*, required=False):
        assert required is True
        events.append("position_sync")
        raise RuntimeError("position unavailable")

    engine = SimpleNamespace(
        start=lambda: events.append("engine_start"),
        sync_position=failed_sync,
    )
    ws = SimpleNamespace(start=lambda _rest: events.append("ws_start"))
    cfg = SimpleNamespace(
        symbol="BTCUSDC",
        lifecycle_journal_v2=SimpleNamespace(enabled=True),
    )
    rest = SimpleNamespace(get_orders=lambda **_kwargs: [])
    monkeypatch.setattr(
        live_main,
        "initialize_prospective_lifecycle_collection",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("epoch must not be published")
        ),
    )

    with pytest.raises(RuntimeError, match="position unavailable"):
        live_main.start_engine_with_prospective_collection(
            cfg=cfg,
            engine=engine,
            ws=ws,
            rest=rest,
            config_path=Path("config.yaml"),
            native_runtime={},
            dry_run=False,
        )

    assert events == ["engine_start", "position_sync"]


def test_nested_active_order_state_fails_before_epoch_publication(
    monkeypatch,
) -> None:
    publisher_called = False

    def publish(**_kwargs):
        nonlocal publisher_called
        publisher_called = True
        raise AssertionError("publisher must not run with active local orders")

    monkeypatch.setattr(live_main, "publish_prospective_baseline_epoch", publish)
    settings = SimpleNamespace(enabled=True)
    cfg = SimpleNamespace(
        symbol="BTCUSDC",
        lifecycle_journal_v2=settings,
    )
    engine = SimpleNamespace(
        prospective_epoch_initial_runtime_state=lambda **_kwargs: {
            "order_lifecycle": {
                "active_local_orders": [{"client_order_id": "still-open"}]
            }
        }
    )
    rest = SimpleNamespace(
        get_orders=lambda **_kwargs: [],
        account=lambda: {},
    )

    with pytest.raises(RuntimeError, match="zero active local orders"):
        live_main.initialize_prospective_lifecycle_collection(
            cfg=cfg,
            engine=engine,
            rest=rest,
            config_path=Path("config.yaml"),
            native_runtime={},
        )
    assert publisher_called is False


def test_remote_spool_profile_is_bound_into_epoch_and_writer(
    tmp_path: Path,
    monkeypatch,
) -> None:
    model_dir = tmp_path / "model"
    model_dir.mkdir()
    publish_kwargs = {}
    writer_kwargs = {}

    class Epoch:
        epoch_id = "epoch-remote"
        epoch_root = tmp_path / "epochs" / epoch_id

        @staticmethod
        def writer_runtime_identity():
            return {
                "baseline_epoch_id": "epoch-remote",
                "storage_profile": BOUNDED_REMOTE_SPOOL,
            }

    def publish(**kwargs):
        publish_kwargs.update(kwargs)
        return Epoch()

    class Writer:
        def __init__(self, _root, **kwargs):
            writer_kwargs.update(kwargs)

        def close(self, **_kwargs):
            return {}

    monkeypatch.setattr(live_main, "publish_prospective_baseline_epoch", publish)
    monkeypatch.setattr(live_main, "OrderLifecycleLiveWriterV2", Writer)
    monkeypatch.setattr(live_main, "snapshot_action_enablement", lambda _cfg: {"enabled": True})
    monkeypatch.setattr(live_main, "snapshot_data_source_identity", lambda _cfg: {"source": "test"})
    settings = SimpleNamespace(
        enabled=True,
        storage_profile=BOUNDED_REMOTE_SPOOL,
        root=f"{EXAMPLE_REMOTE_COLLECTION_ROOT}/journal",
        prospective_epoch_root=f"{EXAMPLE_REMOTE_COLLECTION_ROOT}/epochs",
        required_mount=EXAMPLE_STORAGE_ROOT,
        remote_spool_allowlisted_roots=(
            EXAMPLE_REMOTE_COLLECTION_ROOT,
        ),
        remote_session_max_duration_s=3600.0,
        remote_session_max_bytes=1024 * 1024,
        baseline_identity_path="baseline.json",
        baseline_identity_sha256="a" * 64,
        queue_size=8,
        storage_format="jsonl",
        heartbeat_interval_s=1.0,
        shutdown_drain_timeout_s=1.0,
    )
    cfg = SimpleNamespace(
        symbol="BTCUSDC",
        ml=SimpleNamespace(model_dir=str(model_dir)),
        lifecycle_journal_v2=settings,
    )
    initial_state = {"order_lifecycle": {"active_local_orders": []}}
    attached = []
    engine = SimpleNamespace(
        prospective_epoch_initial_runtime_state=lambda **_kwargs: initial_state,
        set_order_lifecycle_live_writer_v2=lambda writer, **kwargs: attached.append(
            (writer, kwargs)
        ),
    )
    rest = SimpleNamespace(get_orders=lambda **_kwargs: [], account=lambda: {})

    epoch, writer = live_main.initialize_prospective_lifecycle_collection(
        cfg=cfg,
        engine=engine,
        rest=rest,
        config_path=tmp_path / "config.yaml",
        native_runtime={},
    )

    assert epoch.epoch_id == "epoch-remote"
    assert writer is not None
    assert publish_kwargs["storage_profile"] == BOUNDED_REMOTE_SPOOL
    assert publish_kwargs["remote_spool_allowlisted_roots"] == (
        EXAMPLE_REMOTE_COLLECTION_ROOT,
    )
    assert publish_kwargs["collection_bounds"] == {
        "max_duration_s": 3600.0,
        "max_bytes": 1024 * 1024,
    }
    assert writer_kwargs["storage_profile"] == BOUNDED_REMOTE_SPOOL
    assert writer_kwargs["epoch_root"] == Epoch.epoch_root
    assert writer_kwargs["session_max_duration_s"] == 3600.0
    assert writer_kwargs["session_max_bytes"] == 1024 * 1024
    assert len(attached) == 1
