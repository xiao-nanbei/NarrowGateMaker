from __future__ import annotations

import json
import math
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from models.replay.baseline_epoch_manifest import REQUIRED_IDENTITY_FIELDS
from models.replay.prospective_baseline_epoch import (
    CPP_FEATURE_RECONSTRUCTION_CONTRACT,
    PROSPECTIVE_INITIAL_STATE_COMPLETENESS_SCHEMA_VERSION,
    PROSPECTIVE_INITIAL_STATE_DOMAIN_SCHEMAS,
    PROSPECTIVE_INITIAL_STATE_REQUIRED_DOMAINS,
    PROSPECTIVE_INITIAL_STATE_REQUIRED_FIELDS,
    PYTHON_FEATURE_STATE_CONTRACT,
    live_clock_semantics_identity,
    publish_prospective_baseline_epoch,
    require_external_collection_root,
    validate_initial_runtime_state_completeness,
)
from strategy.maker_engine import MakerEngine
from strategy.replay_controls import (
    LOSS_COOLDOWN_SNAPSHOT_SCHEMA,
    ConsecutiveLossCooldown,
)
from strategy.signal import Bar1s


def _sha(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fixture(tmp_path: Path) -> dict:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "runtime.py").write_text("VALUE = 1\n", encoding="utf-8")
    config = repo / "config.yaml"
    config.write_text("symbol: BTCUSDC\n", encoding="utf-8")
    baseline = repo / "baseline.json"
    baseline.write_text('{"baseline_id":"prospective"}\n', encoding="utf-8")
    model = repo / "model"
    model.mkdir()
    (model / "bundle_meta.json").write_text('{"heads":13}\n', encoding="utf-8")
    p3 = model / "fill_prob_params.json"
    p3.write_text('{"schema":"v2"}\n', encoding="utf-8")
    mount = tmp_path / "ORICO"
    mount.mkdir()
    return {
        "repo": repo,
        "config": config,
        "baseline": baseline,
        "model": model,
        "p3": p3,
        "mount": mount,
    }


def _complete_initial_state(*, unsupported: tuple[str, ...] = ()) -> dict:
    state = {}
    for domain in PROSPECTIVE_INITIAL_STATE_REQUIRED_DOMAINS:
        payload = {
            "schema_version": PROSPECTIVE_INITIAL_STATE_DOMAIN_SCHEMAS[domain]
        }
        for field in PROSPECTIVE_INITIAL_STATE_REQUIRED_FIELDS[domain]:
            payload[field] = [] if field.endswith("orders") else 0
        state[domain] = payload
    state["reward_path_loss_cooldown"] = {
        **ConsecutiveLossCooldown(
            max_consecutive_losses=0,
            cooldown_ms=0,
        ).snapshot(),
        "cooldown_until_wall_s": 0.0,
        "last_cooldown_cancel_time_wall_s": 0.0,
    }
    state["signal_feature_dag_warmup"].update(
        {
            "feature_dag_sha256": "a" * 64,
            "causal_cutoff_exclusive_ms": 0,
            "last_emitted_bucket_ms": 0,
            "bar_history_coverage": {
                "row_count": 0,
                "first_ts_ms": 0,
                "last_ts_ms": 0,
            },
            "feature_history_coverage": {
                "row_count": 0,
                "first_bucket_ms": 0,
                "last_bucket_ms": 0,
            },
            "state_sha256": "b" * 64,
            "cpp_engine_seeded": False,
            "cpp_backend_state": {
                "feature_engine_present": False,
                "reconstruction_contract": PYTHON_FEATURE_STATE_CONTRACT,
                "expected_bar_count": 0,
                "actual_bar_count": 0,
                "expected_history_count": 0,
                "actual_history_count": 0,
                "global_flow_native_enabled": False,
                "global_flow_boundary_event_count": 0,
                "cross_aggregator_count": 0,
            },
        }
    )
    state["completeness"] = {
        "schema_version": PROSPECTIVE_INITIAL_STATE_COMPLETENESS_SCHEMA_VERSION,
        "required_domains": list(PROSPECTIVE_INITIAL_STATE_REQUIRED_DOMAINS),
        "captured_domains": list(PROSPECTIVE_INITIAL_STATE_REQUIRED_DOMAINS),
        "unsupported_initial_state_fields": list(unsupported),
        "binding_status": "fully_bound" if not unsupported else "unsupported",
    }
    return state


def _engine_for_initial_state_test() -> MakerEngine:
    engine = MakerEngine.__new__(MakerEngine)
    engine.cfg = SimpleNamespace(
        symbol="BTCUSDC",
        risk=SimpleNamespace(
            max_consecutive_losses=2,
            cooldown_after_loss=30.0,
        ),
    )
    engine.orders = SimpleNamespace(get_active_orders=lambda: [])
    inventory = SimpleNamespace(_lock=threading.Lock())
    inventory_defaults = {
        "_qty": 0.001,
        "_avg_entry": 65_000.0,
        "_cost_basis": 65.0,
        "_realized_pnl": 1.0,
        "_unrealized_pnl": -0.1,
        "_state": SimpleNamespace(name="OPEN"),
        "_open_time": 1.0,
        "_mark_price": 64_900.0,
        "_total_volume": 2.0,
        "_total_commission": 0.1,
        "_open_commission": 0.01,
        "_round_trip_rpnl": 0.2,
        "_last_trade_pnl": -0.02,
        "_peak_pnl": 1.2,
        "_peak_unrealized_pnl": 0.3,
        "_daily_utc_day": 20_000,
        "_day_start_total_pnl": 0.5,
        "_day_buy_fill_qty": 0.001,
        "_day_sell_fill_qty": 0.0,
        "_day_buy_fill_notional": 65.0,
        "_day_sell_fill_notional": 0.0,
        "_sync_adjust_seq": 3,
        "_last_sync_adjust_time": 2.5,
        "_last_sync_adjust_delta": 0.0,
        "_sync_adjust_events": [],
        "_inventory_time_start_ts": 1.0,
        "_inventory_time_last_ts": 3.0,
        "_signed_inventory_time_s": 0.01,
        "_abs_inventory_time_s": 0.01,
        "_sq_inventory_time_s": 0.00001,
        "_signed_notional_inventory_time_s": 650.0,
        "_notional_inventory_time_s": 650.0,
        "_campaign_active": True,
        "_campaign_id": 7,
        "_campaign_start_time": 1.0,
        "_campaign_start_realized_pnl": 0.8,
        "_campaign_start_side": "LONG",
        "_campaign_max_abs_qty": 0.001,
        "_campaign_min_total_pnl": -0.1,
        "_campaign_total_pnl": -0.1,
        "_campaign_realized_pnl": 0.0,
        "_campaign_unrealized_pnl": -0.1,
        "_campaign_fills": 1,
        "_campaign_buy_fills": 1,
        "_campaign_sell_fills": 0,
        "_campaign_exposure_increasing_fills": 1,
        "_campaign_reducing_fills": 0,
        "_campaign_volume": 0.001,
        "_consecutive_losses": 2,
    }
    for name, value in inventory_defaults.items():
        setattr(inventory, name, value)
    inventory.reconciliation_snapshot = lambda: {
        "snapshot_update_time_ms": 2_000,
        "order_cumulative_filled_qty": {"10": 0.001},
        "local_order_cumulative_filled_qty": {"10": 0.001},
        "retained_post_snapshot_fill_count": 0,
    }
    engine.inventory = inventory

    global_flow = SimpleNamespace(
        native_enabled=False,
        _markets={},
        backend_stats=lambda: {},
    )
    fair_price = SimpleNamespace(
        _basis={},
        _last_source_identity={},
        _lead={"count": 0},
        _noise={"count": 0},
        _last_consensus_identity=None,
        _last_decision_ts_ns=0,
    )
    signal = SimpleNamespace(
        _lock=threading.Lock(),
        _bar_buffer=[],
        _current_bar=None,
        _current_bucket=0,
        _depth_history=[],
        _last_depth=None,
        _quote_market_generation=0,
        _depth_generation=0,
        _book_ticker_generation=0,
        _feat_history=[],
        _last_processed_bucket=None,
        _close_history=[],
        _sign_history=[],
        _signed_vol_cumsum=0.0,
        _prev_flow_velocity=0.0,
        _last_trade_side=0,
        _last_trade_run_len=0,
        _metrics_history=[],
        _last_metrics=None,
        _book_tickers={},
        _book_ticker_history={},
        _cross_bar_buffers={},
        _cross_current_bars={},
        _cross_current_buckets={},
        _cross_basis_history={},
        _global_bridge_basis_history=[],
        _market_source_state={},
        _last_prediction=None,
        _warmup_count=0,
        _pred_ret_ema=[0.0, 0.0, 0.0],
        _cpp_feature_engine_seeded=False,
        _cpp_feature_engine=None,
        _cpp_cross_current_dirty=set(),
        _cpp_cross_aggregators={},
        _cross_venue_fair_price=fair_price,
        _global_flow=global_flow,
    )
    engine.signal = signal
    engine._dynamic_fill_hazard_shadow_runtime = SimpleNamespace(_orders={})
    engine._dynamic_fill_hazard_action_hold = None
    engine._dynamic_fill_hazard_action_last_score = math.nan
    engine._last_quote_context = {}
    engine._last_quote_diagnostics = {}
    engine._last_quote_decision_snapshot = None
    engine._last_post_only_guard = None
    engine._last_prediction = None
    engine._post_fill_quote_response = SimpleNamespace(
        _add_side="BUY",
        _excitation=0.2,
        _last_update_ms=1000,
        _last_half_life_s=5.0,
    )
    engine._consecutive_quote_snapshot_blocks = 1
    engine._flat_unilateral_started = {"BUY": 0.0, "SELL": 0.0, "BOTH": 0.0}
    engine._best_bid = 64_999.9
    engine._best_ask = 65_000.1
    engine._consec_buy = 1.0
    engine._consec_sell = 0.0
    engine._fill_cooldown_until = {"BUY": 10.0, "SELL": 0.0}
    engine._fill_cooldown_deadline_identity = {"BUY": "B0", "SELL": "B0"}
    engine._fill_cooldown_restore_mode = "fresh_b0_no_checkpoint"
    engine._fill_cooldown_checkpoint_loaded = False
    engine._fill_cooldown_checkpoint_sequence = 0
    engine._last_same_side_fill_epoch_ms = {"BUY": 1, "SELL": 0}
    engine._last_fill_side = "BUY"
    engine._bid_cid = None
    engine._ask_cid = None
    engine._order_policy_context = {}
    engine._cooldown_until = 20.0
    engine._last_cooldown_cancel_time = 19.0
    engine._loss_cooldown_trigger_count = 1
    engine._loss_cooldown_expiry_count = 0
    engine._loss_cooldown_losing_round_trips = 2
    engine._loss_cooldown_winning_or_flat_round_trips = 0
    engine._loss_cooldown_max_observed_consecutive_losses = 2
    engine._mo_ema_bid = -0.1
    engine._mo_ema_ask = 0.0
    engine._mo_ema_all = -0.05
    engine._mo_pending = []
    engine._mo_ref = 50.0
    engine._mo_last_decay_time = 18.0
    engine._mo_pause_until = {"BUY": 30.0, "SELL": 0.0}
    engine._last_seen_sync_adjust_seq = 3
    engine._sync_adjust_degrade_until = 40.0
    engine._last_sync_adjust_user_reconnect = 2.0
    engine._running = True
    engine._last_requote_time = 3.0
    engine._requote_count = 4
    engine._ema_var_fast = 0.1
    engine._ema_var_slow = 0.2
    engine._prev_close = 65_000.0
    engine._dynamic_rq_ready = True
    engine._ber_ema_fast = 1.0
    engine._ber_ema_slow = 2.0
    engine._ber_ready = True
    engine._ber_active = False
    engine._close_gtx_rejects = 0
    engine._close_start_time = 0.0
    engine._state_conditioned_policy_campaigns = set()
    engine._buy_fill_selection_last_eval_time = 0.0
    engine._buy_fill_selection_last_hit_time = 0.0
    engine._buy_fill_selection_last_score = 0.0
    engine._buy_fill_selection_last_missing = 0
    return engine


def test_publishes_fully_bound_epoch_without_touching_historical_epochs(
    tmp_path: Path,
) -> None:
    paths = _fixture(tmp_path)
    historical = paths["mount"] / "historical-epochs.json"
    historical.write_text("do-not-rewrite\n", encoding="utf-8")
    before = historical.read_bytes()
    epoch = publish_prospective_baseline_epoch(
        output_root=paths["mount"] / "epochs",
        required_mount=paths["mount"],
        repo_root=paths["repo"],
        config_path=paths["config"],
        baseline_identity_path=paths["baseline"],
        expected_baseline_identity_sha256=_sha(paths["baseline"]),
        model_dir=paths["model"],
        p3_path=paths["p3"],
        feature_dag_sha256="a" * 64,
        runtime_code_paths=("runtime.py",),
        native_runtime={"profile": "test", "module": "disabled"},
        native_module_path="disabled",
        action_enablement={"schema_version": "actions.v1", "q90": False},
        initial_runtime_state=_complete_initial_state(),
        data_source_identity={"schema_version": "sources.v1", "book": "native"},
        clock_semantics=live_clock_semantics_identity(),
        start_ts_ns=1_800_000_000_000_000_000,
        require_mounted=False,
    )

    manifest = json.loads(epoch.manifest_path.read_text(encoding="utf-8"))
    assert manifest["binding_status"] == "fully_bound"
    assert set(manifest["identity"]) == set(REQUIRED_IDENTITY_FIELDS)
    assert all(len(value) == 64 for value in manifest["identity"].values())
    assert manifest["historical_epochs_backfilled"] is False
    assert manifest["formal_collection_valid"] is False
    assert manifest["permissions"]["lifecycle_estimation_authorized"] is False
    assert historical.read_bytes() == before
    assert not list((paths["mount"] / "epochs").glob(".*.partial-*"))


def test_baseline_hash_mismatch_fails_before_publication(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    with pytest.raises(ValueError, match="baseline identity SHA256 mismatch"):
        publish_prospective_baseline_epoch(
            output_root=paths["mount"] / "epochs",
            required_mount=paths["mount"],
            repo_root=paths["repo"],
            config_path=paths["config"],
            baseline_identity_path=paths["baseline"],
            expected_baseline_identity_sha256="f" * 64,
            model_dir=paths["model"],
            p3_path=paths["p3"],
            feature_dag_sha256="a" * 64,
            runtime_code_paths=("runtime.py",),
            native_runtime={"module": "disabled"},
            native_module_path="disabled",
            action_enablement={"enabled": False},
            initial_runtime_state=_complete_initial_state(),
            data_source_identity={"book": "native"},
            clock_semantics=live_clock_semantics_identity(),
            require_mounted=False,
        )
    assert not (paths["mount"] / "epochs").exists()


def test_collection_root_cannot_escape_required_mount(tmp_path: Path) -> None:
    mount = tmp_path / "ORICO"
    mount.mkdir()
    with pytest.raises(ValueError, match="inside the required mount"):
        require_external_collection_root(
            tmp_path / "internal-disk",
            required_mount=mount,
            require_mounted=False,
        )


def test_unsupported_initial_state_fails_before_publication(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    with pytest.raises(ValueError, match="unsupported fields"):
        publish_prospective_baseline_epoch(
            output_root=paths["mount"] / "epochs",
            required_mount=paths["mount"],
            repo_root=paths["repo"],
            config_path=paths["config"],
            baseline_identity_path=paths["baseline"],
            expected_baseline_identity_sha256=_sha(paths["baseline"]),
            model_dir=paths["model"],
            p3_path=paths["p3"],
            feature_dag_sha256="a" * 64,
            runtime_code_paths=("runtime.py",),
            native_runtime={"module": "disabled"},
            native_module_path="disabled",
            action_enablement={"enabled": False},
            initial_runtime_state=_complete_initial_state(
                unsupported=("signal.native_global_flow_internal_state",)
            ),
            data_source_identity={"book": "native"},
            clock_semantics=live_clock_semantics_identity(),
            require_mounted=False,
        )
    assert not (paths["mount"] / "epochs").exists()


def test_missing_initial_state_domain_fails_before_publication(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    state = _complete_initial_state()
    state.pop("adverse_markout_pause")
    with pytest.raises(ValueError, match="domains are missing"):
        publish_prospective_baseline_epoch(
            output_root=paths["mount"] / "epochs",
            required_mount=paths["mount"],
            repo_root=paths["repo"],
            config_path=paths["config"],
            baseline_identity_path=paths["baseline"],
            expected_baseline_identity_sha256=_sha(paths["baseline"]),
            model_dir=paths["model"],
            p3_path=paths["p3"],
            feature_dag_sha256="a" * 64,
            runtime_code_paths=("runtime.py",),
            native_runtime={"module": "disabled"},
            native_module_path="disabled",
            action_enablement={"enabled": False},
            initial_runtime_state=state,
            data_source_identity={"book": "native"},
            clock_semantics=live_clock_semantics_identity(),
            require_mounted=False,
        )
    assert not (paths["mount"] / "epochs").exists()


def test_loss_cooldown_domain_requires_full_v2_snapshot() -> None:
    state = _complete_initial_state()
    snapshot = state["reward_path_loss_cooldown"]
    assert snapshot["schema_version"] == LOSS_COOLDOWN_SNAPSHOT_SCHEMA
    ConsecutiveLossCooldown.restore(snapshot)

    snapshot.pop("open_commission")
    with pytest.raises(ValueError, match="domain fields are missing"):
        validate_initial_runtime_state_completeness(state)


def test_legacy_loss_cooldown_domain_fails_closed() -> None:
    state = _complete_initial_state()
    state["reward_path_loss_cooldown"]["schema_version"] = (
        "narrowgate_initial_state_reward_path_loss_cooldown.v1"
    )
    with pytest.raises(ValueError, match="domain schema mismatch"):
        validate_initial_runtime_state_completeness(state)


def test_captured_domain_placeholder_is_rejected(tmp_path: Path) -> None:
    paths = _fixture(tmp_path)
    state = _complete_initial_state()
    state["signal_feature_dag_warmup"] = {"captured": True}
    with pytest.raises(ValueError, match="domain schema mismatch"):
        publish_prospective_baseline_epoch(
            output_root=paths["mount"] / "epochs",
            required_mount=paths["mount"],
            repo_root=paths["repo"],
            config_path=paths["config"],
            baseline_identity_path=paths["baseline"],
            expected_baseline_identity_sha256=_sha(paths["baseline"]),
            model_dir=paths["model"],
            p3_path=paths["p3"],
            feature_dag_sha256="a" * 64,
            runtime_code_paths=("runtime.py",),
            native_runtime={"module": "disabled"},
            native_module_path="disabled",
            action_enablement={"enabled": False},
            initial_runtime_state=state,
            data_source_identity={"book": "native"},
            clock_semantics=live_clock_semantics_identity(),
            require_mounted=False,
        )
    assert not (paths["mount"] / "epochs").exists()


def test_real_engine_state_covers_policy_accounting_and_signal_domains() -> None:
    engine = _engine_for_initial_state_test()
    state = engine.prospective_epoch_initial_runtime_state(
        account_snapshot={"wallet_balance": "100.0"},
        exchange_open_orders=[],
    )

    validate_initial_runtime_state_completeness(state)
    assert state["completeness"]["binding_status"] == "fully_bound"
    assert state["reward_path_loss_cooldown"]["consecutive_losses"] == 2
    assert state["reward_path_loss_cooldown"]["last_cancel_ts_ms"] == 19_000
    assert state["adverse_markout_pause"]["pause_until_wall_s"]["BUY"] == 30.0
    assert state["sync_degrade"]["degrade_until_wall_s"] == 40.0
    assert state["inventory_accounting"]["abs_inventory_time_s"] == 0.01
    signal = state["signal_feature_dag_warmup"]
    assert signal["causal_cutoff_exclusive_ms"] == 0
    assert signal["last_emitted_bucket_ms"] == 0
    assert signal["bar_history_coverage"]["row_count"] == 0
    assert len(signal["state_sha256"]) == 64


def test_disabled_live_loss_cooldown_exports_no_cancel_clock() -> None:
    engine = _engine_for_initial_state_test()
    engine.cfg.risk.max_consecutive_losses = 0
    engine.cfg.risk.cooldown_after_loss = 0.0
    engine.inventory._consecutive_losses = 0
    engine._cooldown_until = 0.0
    engine._loss_cooldown_trigger_count = 0
    engine._loss_cooldown_expiry_count = 0
    engine._loss_cooldown_max_observed_consecutive_losses = 0

    state = engine.prospective_epoch_initial_runtime_state(
        account_snapshot={},
        exchange_open_orders=[],
    )

    assert state["reward_path_loss_cooldown"]["last_cancel_ts_ms"] == -1


def test_cpp_reconstruction_and_zero_native_boundary_are_fully_bound() -> None:
    engine = _engine_for_initial_state_test()
    engine.signal._cpp_feature_engine = SimpleNamespace(
        bar_count=lambda: 0,
        history_count=lambda: 0,
    )
    engine.signal._cpp_feature_engine_seeded = True
    engine.signal._global_flow.native_enabled = True
    engine.signal._global_flow.backend_stats = lambda: {
        "native": 1,
        "market_count": 0,
        "book_events_seen": 0,
        "book_events_accepted": 0,
        "trade_batches": 0,
        "trade_events_seen": 0,
        "trade_events_accepted": 0,
        "out_of_order_events": 0,
        "stale_trade_events": 0,
        "book_overflow_events": 0,
        "trade_overflow_events": 0,
    }
    state = engine.prospective_epoch_initial_runtime_state(
        account_snapshot={},
        exchange_open_orders=[],
    )

    validate_initial_runtime_state_completeness(state)
    assert state["completeness"]["binding_status"] == "fully_bound"
    backend = state["signal_feature_dag_warmup"]["cpp_backend_state"]
    assert backend["reconstruction_contract"] == (
        CPP_FEATURE_RECONSTRUCTION_CONTRACT
    )
    assert backend["actual_bar_count"] == backend["expected_bar_count"] == 0
    assert backend["global_flow_boundary_event_count"] == 0


def test_nonempty_python_feature_state_is_fully_bound_without_cpp() -> None:
    engine = _engine_for_initial_state_test()
    engine.signal._bar_buffer = [
        Bar1s(
            ts=1_000,
            open=65_000.0,
            high=65_001.0,
            low=64_999.0,
            close=65_000.5,
            volume=0.01,
            buy_volume=0.006,
            sell_volume=0.004,
            trade_count=3,
        )
    ]
    engine.signal._feat_history = [{"close": 65_000.5, "volume": 0.01}]
    engine.signal._last_processed_bucket = 10_000

    state = engine.prospective_epoch_initial_runtime_state(
        account_snapshot={},
        exchange_open_orders=[],
    )

    validate_initial_runtime_state_completeness(state)
    backend = state["signal_feature_dag_warmup"]["cpp_backend_state"]
    assert backend["feature_engine_present"] is False
    assert backend["reconstruction_contract"] == PYTHON_FEATURE_STATE_CONTRACT
    assert backend["expected_bar_count"] == 1
    assert backend["actual_bar_count"] == 0


def test_nonempty_native_backend_state_is_explicitly_unsupported() -> None:
    engine = _engine_for_initial_state_test()
    engine.signal._global_flow.native_enabled = True
    engine.signal._global_flow.backend_stats = lambda: {
        "native": 1,
        "market_count": 1,
        "book_events_seen": 2,
    }
    state = engine.prospective_epoch_initial_runtime_state(
        account_snapshot={},
        exchange_open_orders=[],
    )

    assert "signal.global_flow_nonzero_at_epoch_boundary" in state["completeness"][
        "unsupported_initial_state_fields"
    ]
    with pytest.raises(ValueError, match="global-flow boundary is nonempty"):
        validate_initial_runtime_state_completeness(state)


def test_cpp_reconstruction_count_mismatch_is_explicitly_unsupported() -> None:
    engine = _engine_for_initial_state_test()
    engine.signal._cpp_feature_engine = SimpleNamespace(
        bar_count=lambda: 1,
        history_count=lambda: 0,
    )
    engine.signal._cpp_feature_engine_seeded = True
    state = engine.prospective_epoch_initial_runtime_state(
        account_snapshot={},
        exchange_open_orders=[],
    )

    assert "signal.cpp_feature_engine_bar_count_mismatch" in state["completeness"][
        "unsupported_initial_state_fields"
    ]
    with pytest.raises(ValueError, match=r"C\+\+ bar count mismatch"):
        validate_initial_runtime_state_completeness(state)


def test_nonempty_cpp_cross_aggregator_boundary_is_rejected() -> None:
    engine = _engine_for_initial_state_test()
    engine.signal._cpp_cross_aggregators = {"BTCUSDT": object()}
    state = engine.prospective_epoch_initial_runtime_state(
        account_snapshot={},
        exchange_open_orders=[],
    )

    assert "signal.cpp_cross_aggregator_nonzero_at_epoch_boundary" in state[
        "completeness"
    ]["unsupported_initial_state_fields"]
    with pytest.raises(ValueError, match="cross-aggregator boundary is nonempty"):
        validate_initial_runtime_state_completeness(state)
