import datetime as dt
import json
from pathlib import Path
from types import SimpleNamespace

import lightgbm as lgb
import numpy as np
import pytest

import strategy.signal as signal_module
from live.config import Config
from strategy.boolean_cooldown_live import (
    LiveBooleanCooldownPolicy,
    RuntimeCooldownPolicyEvaluator,
)
from strategy.maker_engine import MakerEngine, _prospective_state_fingerprint
from strategy.model_contract import (
    REQUIRED_CALENDAR_TIMESTAMP_SEMANTICS,
    REQUIRED_FEATURE_DAG_ID,
    REQUIRED_FEATURE_DAG_SHA256,
    REQUIRED_FEATURE_SEMANTICS_VERSION,
    REQUIRED_LABEL_SEMANTICS_VERSION,
    REQUIRED_LABEL_WINDOW_SEMANTICS,
    REQUIRED_MODEL_HEADS,
    absolute_price_variance_unit_contract,
)
from strategy.quote_core import (
    QuoteCoreConfig,
    QuotePrediction,
    QuoteState,
    compute_quote_core,
)
from strategy.signal import (
    CPP_LIGHTGBM_INFERENCE_FLAG,
    EXECUTION_L2_FEATURE_COLS,
    EXECUTION_L2_POLICY_METRIC_COLS,
    PERP_MARKET,
    REF_PERP_FEATURE_NAMES,
    SIGNAL_MODEL_FEATURE_ROW_CPP_ABI_VERSION,
    SIGNAL_REF_PERP_CPP_ABI_VERSION,
    Bar1s,
    DepthSnapshot,
    QuoteDepthObservation,
    SignalEngine,
)

narrowgate_cpp = pytest.importorskip("narrowgate_cpp")

_SELL_LONG_CROSS = "predicate::ema_pair_h16s_h256s:cross_age_le_fast"
_SELL_SHORT_CROSS = "predicate::ema_pair_h4s_h16s:cross_age_le_slow"
_SELL_CAMPAIGN_AGE = "predicate::m0::campaign_age_gt_control_duration"

MODEL_BUNDLE = (
    Path(__file__).resolve().parents[1]
    / "examples"
    / "public_dry_run_model_bundle"
)
@pytest.fixture(scope="session")
def synthetic_model_bundle_173(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Build a public, non-authoritative canonical-width bundle for parity tests."""

    root = tmp_path_factory.mktemp("synthetic-model-bundle-173")
    feature_names = [str(name) for name in narrowgate_cpp.SIGNAL_MODEL_FEATURE_NAMES]
    sample_count = 24
    row_axis = np.arange(sample_count, dtype=np.float64).reshape(-1, 1)
    feature_axis = np.arange(len(feature_names), dtype=np.float64).reshape(1, -1)
    matrix = np.sin(row_axis * 0.17 + feature_axis * 0.013)
    labels = np.cos(np.arange(sample_count, dtype=np.float64) * 0.23)
    dataset = lgb.Dataset(
        matrix,
        label=labels,
        feature_name=feature_names,
        free_raw_data=False,
    )
    booster = lgb.train(
        {
            "objective": "regression",
            "verbosity": -1,
            "num_threads": 1,
            "deterministic": True,
            "force_col_wise": True,
            "min_data_in_leaf": 1,
            "min_data_in_bin": 1,
            "feature_pre_filter": False,
            "seed": 0x4E47,
        },
        dataset,
        num_boost_round=2,
    )

    metadata = {
        "calendar_timestamp_semantics": REQUIRED_CALENDAR_TIMESTAMP_SEMANTICS,
        "feature_cols": feature_names,
        "feature_dag_id": REQUIRED_FEATURE_DAG_ID,
        "feature_dag_sha256": REQUIRED_FEATURE_DAG_SHA256,
        "feature_manifest_sha256": "0" * 64,
        "feature_semantics_version": REQUIRED_FEATURE_SEMANTICS_VERSION,
        "feature_variant": "pytest_synthetic_canonical_173",
        "label_semantics_version": REQUIRED_LABEL_SEMANTICS_VERSION,
        "label_window_semantics": REQUIRED_LABEL_WINDOW_SEMANTICS,
        "promotion_authority": "public_dry_run_only",
        "source_profile": "synthetic_fixture",
        "symbol": "BTCUSDC",
        "training_experiment_id": "pytest_synthetic_model_bundle_173_v1",
        "volatility_unit_contract": absolute_price_variance_unit_contract(
            "BTCUSDC"
        ),
    }
    for name in REQUIRED_MODEL_HEADS:
        booster.save_model(str(root / f"{name}.txt"))
        head_metadata = dict(metadata)
        if name.startswith("vol_"):
            head_metadata["label_semantics"] = (
                "fixed_forward_h_absolute_price_variance"
            )
        (root / f"{name}_meta.json").write_text(
            json.dumps(head_metadata, sort_keys=True),
            encoding="utf-8",
        )
    return root


def test_native_build_surface_matches_exposed_runtime(
    synthetic_model_bundle_173: Path,
) -> None:
    is_full = narrowgate_cpp.NATIVE_BUILD_FLAVOR == "full"
    assert narrowgate_cpp.NATIVE_TICK_REPLAY_AVAILABLE is is_full
    assert narrowgate_cpp.NATIVE_RESEARCH_RUNTIME_AVAILABLE is is_full
    for name in (
        "TickReplayParams",
        "F03CausalV12OneSecondBatchEngine",
        "DynamicFillHazardRuntime",
        "integrate_variance_time_episode",
    ):
        assert hasattr(narrowgate_cpp, name) is is_full
    for name in (
        "SignalFeatureEngine",
        "SignalRefPerpFeatureEngine",
        "FeatureHistoryRow",
        "F05BooleanPolicy",
        "NativeLiveCooldownHotPath",
    ):
        assert hasattr(narrowgate_cpp, name)
    assert (
        narrowgate_cpp.SIGNAL_REF_PERP_FEATURE_ABI_VERSION
        == SIGNAL_REF_PERP_CPP_ABI_VERSION
    )
    assert tuple(narrowgate_cpp.SIGNAL_REF_PERP_FEATURE_NAMES) == (
        REF_PERP_FEATURE_NAMES
    )
    assert (
        narrowgate_cpp.SIGNAL_MODEL_FEATURE_ROW_ABI_VERSION
        == SIGNAL_MODEL_FEATURE_ROW_CPP_ABI_VERSION
    )
    assert tuple(narrowgate_cpp.SIGNAL_MODEL_FEATURE_NAMES) == tuple(
        _model_feature_names(synthetic_model_bundle_173)
    )
    assert len(narrowgate_cpp.SIGNAL_MODEL_FEATURE_NAMES) == 173
    assert narrowgate_cpp.SignalModelFeatureRow173.feature_count == 173
    assert hasattr(
        narrowgate_cpp.NativeLightgbmBundle,
        "predict_signal_row_173",
    )


def test_native_model_row_fixed_input_groups_land_on_named_columns() -> None:
    engine = narrowgate_cpp.SignalFeatureEngine(32, 64)
    for index in range(10):
        bar = narrowgate_cpp.Bar1s()
        bar.ts_ms = index * 1_000
        bar.open = 100.0 + index
        bar.high = 101.0 + index
        bar.low = 99.0 + index
        bar.close = 100.5 + index
        bar.volume = 1.0 + index
        bar.buy_volume = 0.6 + index
        bar.sell_volume = 0.4
        bar.trade_count = index + 1
        bar.buy_count = index
        bar.sell_count = 1
        engine.push_bar(bar)

    prepared = engine.prepare_bucket(0)
    groups = (
        (
            tuple(narrowgate_cpp.SIGNAL_EXECUTION_L2_FEATURE_NAMES),
            np.arange(1_001.0, 1_014.0, dtype=np.float64),
        ),
        (
            tuple(narrowgate_cpp.SIGNAL_METRIC_FEATURE_NAMES),
            np.arange(2_001.0, 2_014.0, dtype=np.float64),
        ),
        (
            tuple(narrowgate_cpp.SIGNAL_REF_PERP_FEATURE_NAMES),
            np.arange(3_001.0, 3_012.0, dtype=np.float64),
        ),
        (
            tuple(narrowgate_cpp.SIGNAL_TIME_FEATURE_NAMES),
            np.arange(4_001.0, 4_050.0, dtype=np.float64),
        ),
    )
    row = engine.assemble_model_row_173(
        prepared,
        groups[0][1],
        groups[1][1],
        groups[2][1],
        groups[3][1],
    )
    names = tuple(narrowgate_cpp.SIGNAL_MODEL_FEATURE_NAMES)
    values = np.asarray(row.values, dtype=np.float64)

    for group_names, sentinels in groups:
        for name, sentinel in zip(group_names, sentinels, strict=True):
            assert values[names.index(name)] == sentinel


def _active_lightgbm_library() -> str:
    return str(Path(lgb.basic._LIB._name).resolve(strict=True))  # noqa: SLF001


def _model_feature_names(bundle: Path = MODEL_BUNDLE) -> list[str]:
    metadata = json.loads(
        (bundle / "dir_10s_meta.json").read_text(encoding="utf-8")
    )
    return [str(name) for name in metadata["feature_cols"]]


def _depth_snapshot(ts: float, index: int, *, depth: int = 10) -> DepthSnapshot:
    mid = 60_000.0 + (index % 7) * 0.1
    return DepthSnapshot(
        ts=ts,
        bids=[
            (mid - 0.1 * level, 0.1 + level * 0.01 + (index % 5) * 0.001)
            for level in range(1, depth + 1)
        ],
        asks=[
            (mid + 0.1 * level, 0.11 + level * 0.012 + (index % 3) * 0.001)
            for level in range(1, depth + 1)
        ],
    )


def _python_execution_l2_values(
    snapshots: list[DepthSnapshot],
    bucket_end_ms: int,
) -> np.ndarray:
    engine = SignalEngine(enable_ml=False)
    engine._cpp_signal_features_enabled = False
    engine._depth_history.extend(snapshots)
    features: dict[str, float] = {}
    engine._compute_execution_l2_features(features, bucket_end_ms)
    return np.asarray(
        [features[name] for name in EXECUTION_L2_FEATURE_COLS],
        dtype=np.float64,
    )


def _native_execution_l2_values(
    snapshots: list[DepthSnapshot],
    bucket_end_ms: int,
) -> np.ndarray:
    return np.asarray(
        narrowgate_cpp.compute_signal_execution_l2_feature_values(
            snapshots,
            bucket_end_ms,
        ),
        dtype=np.float64,
    )


def _python_l2_policy_values(
    snapshots: list[DepthSnapshot],
    end_exchange_ms: float,
) -> np.ndarray:
    quote_snapshot = _policy_quote_snapshot(snapshots, end_exchange_ms)
    signal = SignalEngine(enable_ml=False)
    signal._cpp_signal_features_enabled = False
    engine = object.__new__(MakerEngine)
    engine.cfg = Config()
    engine.signal = signal
    metrics = engine._current_l2_policy_metrics(60_000.0, quote_snapshot)
    return np.asarray(
        [metrics[name] for name in EXECUTION_L2_POLICY_METRIC_COLS],
        dtype=np.float64,
    )


def _policy_quote_snapshot(
    snapshots: list[DepthSnapshot],
    end_exchange_ms: float,
):
    history = tuple(
        QuoteDepthObservation(
            exchange_ts_ms=int(item.ts),
            receive_ts_ns=int(item.ts * 1_000_000),
            bids=tuple(item.bids),
            asks=tuple(item.asks),
        )
        for item in snapshots
    )
    latest = snapshots[-1] if snapshots else DepthSnapshot()
    return SimpleNamespace(
        depth_visible_age_s=0.0,
        bids=tuple(latest.bids),
        asks=tuple(latest.asks),
        depth_history=history,
        depth_exchange_ts_ms=int(end_exchange_ms),
    )


def _native_l2_policy_values(
    snapshots: list[DepthSnapshot],
    end_exchange_ms: float,
) -> np.ndarray:
    return np.asarray(
        narrowgate_cpp.compute_signal_execution_l2_policy_metric_values(
            snapshots,
            end_exchange_ms,
        ),
        dtype=np.float64,
    )


def _sell_cooldown_evaluator() -> RuntimeCooldownPolicyEvaluator:
    return RuntimeCooldownPolicyEvaluator(
        rules=(
            (
                "FIXED_1748S",
                (
                    ((_SELL_SHORT_CROSS, False), (_SELL_CAMPAIGN_AGE, False)),
                    ((_SELL_SHORT_CROSS, True), (_SELL_CAMPAIGN_AGE, False)),
                ),
            ),
            (
                "FIXED_166S",
                (((_SELL_LONG_CROSS, False), (_SELL_CAMPAIGN_AGE, True)),),
            ),
            (
                "FIXED_211S",
                (((_SELL_LONG_CROSS, True), (_SELL_CAMPAIGN_AGE, True)),),
            ),
        ),
        policy_sha256="1" * 64,
        predicate_bundle_sha256="2" * 64,
    )


def test_live_build_f05_sell_cooldown_matches_python_windows_and_rules(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("NARROWGATE_CPP_COOLDOWN", "0")
    python_runtime = LiveBooleanCooldownPolicy(
        evaluator=_sell_cooldown_evaluator(),
        warmup_s=0.2,
        max_feature_age_s=0.5,
    )
    monkeypatch.setenv("NARROWGATE_CPP_COOLDOWN", "1")
    native_runtime = LiveBooleanCooldownPolicy(
        evaluator=_sell_cooldown_evaluator(),
        warmup_s=0.2,
        max_feature_age_s=0.5,
    )
    assert native_runtime._native_hot_path is not None  # noqa: SLF001

    for index, mid in enumerate(
        (100.0, 101.0, 99.0, 102.0, 98.0, 103.0, 97.0),
        start=1,
    ):
        event = {
            "receive_ts_ns": index * 100_000_000 + 1,
            "bids": ((mid - 0.1, 1.0),),
            "asks": ((mid + 0.1, 1.0),),
            "market_generation": index,
            "depth_generation": index,
        }
        python_runtime.observe_depth(**event)
        native_runtime.observe_depth(**event)

    for campaign_age_s in (1.0, 100.0):
        arguments = {
            "side": "SELL",
            "baseline_duration_ms": 85_000,
            "campaign_age_s": campaign_age_s,
            "decision_ts_ns": 800_000_000,
            "snapshot_id": f"sell-{campaign_age_s}",
        }
        assert native_runtime.evaluate(**arguments) == python_runtime.evaluate(**arguments)


def test_cpp_execution_l2_batch_matches_python_exactly_at_102_snapshots() -> None:
    bucket_end_ms = 1_000_000
    bucket_start_ms = bucket_end_ms - 10_000
    snapshots = [_depth_snapshot(bucket_start_ms - 1, -1)]
    snapshots.extend(
        _depth_snapshot(bucket_start_ms + index * 97, index)
        for index in range(102)
    )

    assert tuple(narrowgate_cpp.SIGNAL_EXECUTION_L2_FEATURE_NAMES) == tuple(
        EXECUTION_L2_FEATURE_COLS
    )
    expected = _python_execution_l2_values(snapshots, bucket_end_ms)
    actual = _native_execution_l2_values(snapshots, bucket_end_ms)
    assert actual.flags.c_contiguous
    assert np.array_equal(actual, expected)

    engine = SignalEngine(enable_ml=False)
    engine._cpp_signal = narrowgate_cpp
    engine._cpp_signal_features_enabled = True
    engine._depth_history.extend(snapshots)
    integrated: dict[str, float] = {}
    engine._compute_execution_l2_features(integrated, bucket_end_ms)
    assert np.array_equal(
        np.asarray(
            [integrated[name] for name in EXECUTION_L2_FEATURE_COLS],
            dtype=np.float64,
        ),
        expected,
    )


def test_cpp_execution_l2_incremental_engine_matches_batch_and_bounds_ring() -> None:
    bucket_end_ms = 1_000_000
    snapshots = [
        _depth_snapshot(980_000 + index * 100, index)
        for index in range(200)
    ]
    native = narrowgate_cpp.SignalExecutionL2Engine(120)
    for snapshot in snapshots:
        native.push_snapshot(snapshot.ts, snapshot.bids, snapshot.asks)

    retained = snapshots[-120:]
    assert native.snapshot_count() == 120
    assert np.array_equal(
        np.asarray(native.compute_feature_values(bucket_end_ms)),
        _native_execution_l2_values(retained, bucket_end_ms),
    )
    assert np.array_equal(
        np.asarray(native.compute_policy_metric_values(bucket_end_ms)),
        _native_l2_policy_values(retained, bucket_end_ms),
    )

    native.reset()
    assert native.snapshot_count() == 0
    assert np.array_equal(
        np.asarray(native.compute_feature_values(bucket_end_ms)),
        np.zeros(len(EXECUTION_L2_FEATURE_COLS), dtype=np.float64),
    )


def test_quote_snapshot_captures_native_l2_metrics_without_copying_history() -> None:
    end_exchange_ms = 1_000_000
    snapshots = [
        _depth_snapshot(end_exchange_ms - 9_900 + index * 100, index)
        for index in range(100)
    ]
    engine = SignalEngine(enable_ml=False)
    engine._cpp_signal = narrowgate_cpp
    engine._cpp_signal_features_enabled = True
    engine._cpp_execution_l2_engine = narrowgate_cpp.SignalExecutionL2Engine(300)
    for index, snapshot in enumerate(snapshots):
        engine.on_depth(
            {
                "T": snapshot.ts,
                "b": snapshot.bids,
                "a": snapshot.asks,
            },
            receive_ts_ns=int(snapshot.ts * 1_000_000) + index + 1,
        )

    quote_snapshot = engine.quote_decision_snapshot(
        now_ns=int(end_exchange_ms * 1_000_000) + 1_000,
    )

    assert quote_snapshot.depth_history == ()
    assert np.array_equal(
        np.asarray(quote_snapshot.l2_policy_metric_values),
        _native_l2_policy_values(snapshots, end_exchange_ms),
    )
    maker = object.__new__(MakerEngine)
    maker.cfg = Config()
    maker.signal = engine
    metrics = maker._current_l2_policy_metrics(60_000.0, quote_snapshot)
    assert np.array_equal(
        np.asarray(
            [metrics[name] for name in EXECUTION_L2_POLICY_METRIC_COLS]
        ),
        _native_l2_policy_values(snapshots, end_exchange_ms),
    )


def test_quote_snapshot_rejects_drifted_native_policy_order_before_fast_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    end_exchange_ms = 1_000_000
    engine = SignalEngine(enable_ml=False)
    engine._cpp_signal = SimpleNamespace(
        SIGNAL_EXECUTION_L2_POLICY_METRIC_NAMES=tuple(
            reversed(EXECUTION_L2_POLICY_METRIC_COLS)
        )
    )
    engine._cpp_signal_features_enabled = True
    engine._cpp_execution_l2_engine = narrowgate_cpp.SignalExecutionL2Engine(300)
    monkeypatch.setattr(signal_module, "_cpp_signal_strict", lambda: False)
    snapshot = _depth_snapshot(end_exchange_ms, 0)
    engine.on_depth(
        {"T": snapshot.ts, "b": snapshot.bids, "a": snapshot.asks},
        receive_ts_ns=int(snapshot.ts * 1_000_000) + 1,
    )

    quote_snapshot = engine.quote_decision_snapshot(
        now_ns=int(end_exchange_ms * 1_000_000) + 2,
    )

    assert quote_snapshot.l2_policy_metric_values == ()
    assert quote_snapshot.depth_history
    assert engine._cpp_l2_policy_disabled_after_error is True
    assert engine._cpp_execution_l2_engine is not None


@pytest.mark.parametrize(
    "snapshots",
    [
        [],
        [
            DepthSnapshot(
                ts=999_500,
                bids=[(100.0, -1.0)],
                asks=[(100.2, 2.0)],
            )
        ],
        [
            _depth_snapshot(989_999, 0, depth=3),
            _depth_snapshot(999_999, 1, depth=1),
        ],
    ],
    ids=("empty", "single_shallow", "sparse_with_previous"),
)
def test_cpp_execution_l2_batch_matches_python_empty_and_sparse_histories(
    snapshots: list[DepthSnapshot],
) -> None:
    expected = _python_execution_l2_values(snapshots, 1_000_000)
    actual = _native_execution_l2_values(snapshots, 1_000_000)
    assert np.array_equal(actual, expected)


def test_cpp_execution_l2_batch_preserves_cutoff_and_invalid_latest_state() -> None:
    bucket_end_ms = 1_000_000
    visible = [
        _depth_snapshot(989_999, 0),
        _depth_snapshot(990_000, 1),
        _depth_snapshot(999_999, 2),
    ]
    expected = _native_execution_l2_values(visible, bucket_end_ms)
    future = _depth_snapshot(bucket_end_ms, 100)
    future.bids[0] = (1.0, 1_000_000.0)
    future.asks[0] = (1_000_000.0, 1_000_000.0)
    assert np.array_equal(
        _native_execution_l2_values([*visible, future], bucket_end_ms),
        expected,
    )

    invalid_latest = DepthSnapshot(ts=bucket_end_ms - 1, bids=[], asks=[])
    with_invalid = [*visible, invalid_latest]
    assert np.array_equal(
        _native_execution_l2_values(with_invalid, bucket_end_ms),
        _python_execution_l2_values(with_invalid, bucket_end_ms),
    )
    assert np.array_equal(
        _native_execution_l2_values(with_invalid, bucket_end_ms),
        np.zeros(len(EXECUTION_L2_FEATURE_COLS), dtype=np.float64),
    )


def test_cpp_l2_policy_batch_matches_python_inclusive_exchange_window() -> None:
    end_exchange_ms = 1_000_000.0
    snapshots = [
        _depth_snapshot(989_999.0, 0),
        _depth_snapshot(990_000.0, 1, depth=1),
        DepthSnapshot(ts=995_000.0, bids=[], asks=[]),
        _depth_snapshot(999_999.0, 2, depth=3),
        _depth_snapshot(1_000_000.0, 3, depth=10),
        _depth_snapshot(1_000_001.0, 4),
    ]

    assert tuple(narrowgate_cpp.SIGNAL_EXECUTION_L2_POLICY_METRIC_NAMES) == (
        EXECUTION_L2_POLICY_METRIC_COLS
    )
    expected = _python_l2_policy_values(snapshots, end_exchange_ms)
    actual = _native_l2_policy_values(snapshots, end_exchange_ms)
    assert actual.flags.c_contiguous
    assert np.array_equal(actual, expected)

    engine = SignalEngine(enable_ml=False)
    engine._cpp_signal = narrowgate_cpp
    engine._cpp_signal_features_enabled = True
    integrated = engine._compute_cpp_l2_policy_values(
        snapshots,
        end_exchange_ms,
    )
    assert integrated is not None
    assert np.array_equal(integrated, expected)


@pytest.mark.parametrize(
    "snapshots",
    [
        [],
        [DepthSnapshot(ts=1_000_000.0, bids=[], asks=[])],
        [_depth_snapshot(989_999.0, 0)],
        [_depth_snapshot(1_000_000.0, 1, depth=1)],
    ],
    ids=("empty", "invalid", "outside_window", "single_shallow_at_end"),
)
def test_cpp_l2_policy_batch_matches_python_empty_sparse_and_boundary(
    snapshots: list[DepthSnapshot],
) -> None:
    expected = _python_l2_policy_values(snapshots, 1_000_000.0)
    actual = _native_l2_policy_values(snapshots, 1_000_000.0)
    assert np.array_equal(actual, expected)


def test_cpp_l2_policy_missing_abi_falls_back_or_fails_strict(monkeypatch) -> None:
    engine = SignalEngine(enable_ml=False)
    engine._cpp_signal = object()
    engine._cpp_signal_features_enabled = True
    monkeypatch.setattr(signal_module, "_cpp_signal_strict", lambda: False)
    assert engine._compute_cpp_l2_policy_values([], 1_000_000.0) is None
    assert engine._cpp_l2_policy_disabled_after_error is True

    strict_engine = SignalEngine(enable_ml=False)
    strict_engine._cpp_signal = object()
    strict_engine._cpp_signal_features_enabled = True
    monkeypatch.setattr(signal_module, "_cpp_signal_strict", lambda: True)
    with pytest.raises(
        RuntimeError,
        match="missing execution L2 policy metric batch ABI",
    ):
        strict_engine._compute_cpp_l2_policy_values([], 1_000_000.0)


def test_maker_l2_policy_metrics_native_path_matches_python_fallback() -> None:
    depth_snapshots = [
        _depth_snapshot(990_000.0 + index * 100.0, index)
        for index in range(101)
    ]
    quote_snapshot = _policy_quote_snapshot(depth_snapshots, 1_000_000.0)
    signal = SignalEngine(enable_ml=False)
    engine = object.__new__(MakerEngine)
    engine.cfg = Config()
    engine.signal = signal

    signal._cpp_signal_features_enabled = False
    expected = engine._current_l2_policy_metrics(
        60_000.0,
        quote_snapshot,
    )
    signal._cpp_signal = narrowgate_cpp
    signal._cpp_signal_features_enabled = True
    actual = engine._current_l2_policy_metrics(
        60_000.0,
        quote_snapshot,
    )

    assert actual == expected


def test_cpp_signal_feature_overlay_matches_python_core_features():
    engine = SignalEngine(enable_ml=False)
    for i in range(80):
        price = 100.0 + (i % 7 - 3) * 0.1 + i * 0.005
        bar = Bar1s(
            ts=1_000 * (i + 1),
            open=price - 0.05,
            high=price + 0.1,
            low=price - 0.1,
            close=price,
            volume=1.0 + (i % 5) * 0.1,
            buy_volume=0.55 + (i % 3) * 0.03,
            sell_volume=0.45 + (i % 2) * 0.04,
            trade_count=4 + (i % 4),
            buy_count=2 + (i % 2),
            sell_count=2 + (i % 3),
            quote_qty=(1.0 + (i % 5) * 0.1) * price,
            buy_quote_qty=(0.55 + (i % 3) * 0.03) * price,
            sell_quote_qty=(0.45 + (i % 2) * 0.04) * price,
            max_same_side_run=1 + (i % 4),
            buy_price_high=price + 0.05,
            buy_price_low=price - 0.02,
            sell_price_high=price + 0.04,
            sell_price_low=price - 0.06,
        )
        engine._finalize_bar(bar)

    all_bars = list(engine._bar_buffer)
    bars_10 = all_bars[-10:]
    bar_10s = engine._aggregate_bars(bars_10)

    engine._cpp_signal_features_enabled = False
    py_features = engine._compute_features(bar_10s, all_bars)
    engine._cpp_signal = narrowgate_cpp
    engine._cpp_signal_features_enabled = True
    cpp_features = engine._compute_features(bar_10s, all_bars)
    cpp_overlay = engine._compute_cpp_feature_overlay(bar_10s, all_bars)
    cpp_values = engine._compute_cpp_feature_values(bar_10s, all_bars)

    assert cpp_overlay is not None
    assert len(cpp_overlay) == 80
    assert cpp_values is not None
    assert cpp_values.shape == (80,)
    assert cpp_values.flags.c_contiguous
    for key, value in cpp_overlay.items():
        assert value == pytest.approx(py_features[key], abs=1e-12), key

    keys = [
        "tick_streak",
        "tick_mom_3s",
        "tick_mom_5s",
        "tick_mom_10s",
        "tick_ewm_3s",
        "tick_ewm_10s",
        "micro_ret_std",
        "micro_ret_skew",
        "micro_ret_kurt",
        "tick_reversal_freq",
        "flow_velocity",
        "flow_acceleration",
        "tick_streak_max",
        "tick_mom_range",
        "volatility_30s",
        "volatility_60s",
        "volume_imbalance",
        "volume_imbalance_60s",
        "trade_intensity_60s",
        "vpin_60s",
        "taker_quote_imbalance_30s",
        "taker_signed_quote_sum_60s",
        "taker_trade_count_sum_60s",
        "taker_max_same_side_run_60s",
        "taker_buy_sweep_score_60s",
        "taker_sell_iceberg_pressure_sum_60s",
        "price_velocity",
        "price_acceleration",
        "price_change_60s",
        "avg_trade_size",
        "avg_trade_size_60s",
        "large_trade_ratio",
        "volume_zscore",
        "bar_spread_bps",
        "return_1",
        "return_abs",
        "vol_regime_6h",
        "vol_regime_24h",
        "vol_regime_zscore",
    ]
    for key in keys:
        assert cpp_features[key] == pytest.approx(py_features[key], abs=1e-12)


def test_cpp_signal_feature_ring_buffer_wrap_matches_stateless_tail():
    max_bars = 32
    max_history = 64
    engine = narrowgate_cpp.SignalFeatureEngine(max_bars, max_history)
    bars = []
    history = []

    for i in range(96):
        price = 100.0 + i * 0.01 + (i % 5 - 2) * 0.03
        bar = narrowgate_cpp.Bar1s()
        bar.ts_ms = (i + 1) * 1_000
        bar.open = price - 0.01
        bar.high = price + 0.04
        bar.low = price - 0.05
        bar.close = price
        bar.volume = 1.0 + (i % 7) * 0.1
        bar.buy_volume = 0.6 + (i % 3) * 0.02
        bar.sell_volume = 0.4 + (i % 4) * 0.02
        bar.trade_count = 3 + i % 5
        bar.buy_count = 2 + i % 3
        bar.sell_count = 1 + i % 4
        bar.buy_quote_qty = bar.buy_volume * price
        bar.sell_quote_qty = bar.sell_volume * price
        bar.max_same_side_run = 1 + i % 4
        bar.buy_price_high = price + 0.02
        bar.buy_price_low = price - 0.01
        bar.sell_price_high = price + 0.01
        bar.sell_price_low = price - 0.03
        bars.append(bar)
        engine.push_bar(bar)

        row = narrowgate_cpp.FeatureHistoryRow()
        row.close = price
        row.volume = bar.volume
        row.buy_volume = bar.buy_volume
        row.sell_volume = bar.sell_volume
        row.trade_count = bar.trade_count
        row.flow_velocity = bar.buy_volume - bar.sell_volume
        row.avg_trade_size = bar.volume / bar.trade_count
        row.price_velocity = 0.01
        row.return_abs = abs(0.0001 * (i % 5 - 2))
        row.vol_regime_6h = 0.001 + i * 1e-6
        history.append(row)
        engine.push_history(row)

    bar_10s = narrowgate_cpp.Bar1s()
    bar_10s.ts_ms = 97_000
    bar_10s.open = 100.8
    bar_10s.high = 101.1
    bar_10s.low = 100.7
    bar_10s.close = 101.0
    bar_10s.volume = 8.0
    bar_10s.buy_volume = 4.5
    bar_10s.sell_volume = 3.5
    bar_10s.trade_count = 24

    persistent = engine.compute(bar_10s)
    persistent_values = engine.compute_values(bar_10s)
    stateless = narrowgate_cpp.compute_signal_feature_overlay(
        bars[-max_bars:], history[-max_history:], bar_10s
    )

    assert engine.bar_count() == max_bars
    assert engine.history_count() == max_history
    assert len(persistent) == 80
    assert tuple(narrowgate_cpp.SIGNAL_FEATURE_NAMES) == tuple(persistent.keys())
    assert list(persistent_values) == pytest.approx(list(persistent.values()), abs=1e-12)
    assert persistent.keys() == stateless.keys()
    for key in persistent:
        assert persistent[key] == pytest.approx(stateless[key], abs=1e-12)


def test_cpp_signal_bucket_pipeline_matches_python_aggregate_and_full_feature_row():
    python_engine = SignalEngine(enable_ml=False, ret_demean_halflife=0)
    native_engine = SignalEngine(enable_ml=False, ret_demean_halflife=0)
    native_engine._cpp_signal = narrowgate_cpp
    native_engine._cpp_signal_features_enabled = True
    native_engine._cpp_signal_feature_names = tuple(
        narrowgate_cpp.SIGNAL_FEATURE_NAMES
    )
    native_engine._cpp_feature_engine = narrowgate_cpp.SignalFeatureEngine(3700, 60480)
    native_engine._cpp_feature_engine_seeded = True

    bars: list[Bar1s] = []
    for index in range(80):
        price = 100.0 + index * 0.01 + (index % 5 - 2) * 0.02
        bar = Bar1s(
            ts=index * 1_000,
            open=price - 0.01,
            high=price + 0.04,
            low=price - 0.03,
            close=price,
            volume=1.0 + index % 7 * 0.1,
            buy_volume=0.55 + index % 3 * 0.02,
            sell_volume=0.45 + index % 4 * 0.01,
            trade_count=3 + index % 5,
            buy_count=2 + index % 3,
            sell_count=1 + index % 4,
            quote_qty=price * (1.0 + index % 7 * 0.1),
            buy_quote_qty=price * (0.55 + index % 3 * 0.02),
            sell_quote_qty=price * (0.45 + index % 4 * 0.01),
            max_same_side_run=1 + index % 4,
            max_buy_run=1 + index % 3,
            max_sell_run=1 + index % 2,
            buy_price_high=price + 0.02,
            buy_price_low=price - 0.01,
            sell_price_high=price + 0.01,
            sell_price_low=price - 0.02,
        )
        bars.append(bar)
        python_engine._bar_buffer.append(bar)
        native_engine._bar_buffer.append(bar)
        native_engine._cpp_feature_engine.push_bar(native_engine._bar_to_cpp(bar))

    expected_aggregate = python_engine._aggregate_bars(bars[-10:])
    expected = python_engine._compute_features(expected_aggregate, bars)
    cpp_bar, cpp_values = native_engine._cpp_feature_engine.compute_bucket_values(70_000)
    actual_aggregate = native_engine._aggregate_from_cpp_bar(cpp_bar)
    actual = native_engine._features_from_cpp_values(
        actual_aggregate,
        np.asarray(cpp_values, dtype=np.float64),
        signal_module.FeatureCutoff(80_000),
    )

    for key, value in expected_aggregate.items():
        assert actual_aggregate[key] == pytest.approx(value, abs=1e-12), key
    assert actual.keys() == expected.keys()
    for key, value in expected.items():
        assert actual[key] == pytest.approx(value, abs=1e-10), key


def test_cpp_ref_perp_prepare_is_non_mutating_and_rejects_stale_token():
    engine = narrowgate_cpp.SignalRefPerpFeatureEngine()
    engine.update_book_ticker(1_000.0, 1_005.0, 99.9, 100.1)

    first = engine.prepare(1_005, 100.0)
    second = engine.prepare(1_005, 100.0)

    assert engine.basis_count() == 0
    assert np.array_equal(first.values, second.values)
    with pytest.raises(RuntimeError, match="prepared state is stale"):
        narrowgate_cpp.SignalRefPerpFeatureEngine().commit(first)
    engine.commit(first)
    assert engine.basis_count() == 1
    with pytest.raises(RuntimeError, match="prepared state is stale"):
        engine.commit(second)


def test_cpp_ref_perp_dual_clock_freshness_matches_python_boundaries():
    def prepared_at(*, event_ms: float, receive_ms: float, target_ms: int):
        engine = narrowgate_cpp.SignalRefPerpFeatureEngine()
        engine.update_book_ticker(event_ms, receive_ms, 99.9, 100.1)
        return np.asarray(engine.prepare(target_ms, 100.0).values)

    receive_not_visible = prepared_at(
        event_ms=1_000.0,
        receive_ms=1_501.0,
        target_ms=1_500,
    )
    exact_boundary = prepared_at(
        event_ms=1_000.0,
        receive_ms=1_000.0,
        target_ms=31_000,
    )
    just_stale = prepared_at(
        event_ms=1_000.0,
        receive_ms=1_000.0,
        target_ms=31_001,
    )

    assert receive_not_visible[-1] == 0.0
    assert exact_boundary[-1] == 1.0
    assert exact_boundary[-2] == 30.0
    assert just_stale[-1] == 0.0


def test_cpp_ref_perp_matches_python_for_all_fields_and_basis_history():
    python_engine = SignalEngine(enable_ml=False, ret_demean_halflife=0)
    native_engine = SignalEngine(enable_ml=False, ret_demean_halflife=0)
    native_engine._cpp_ref_perp_engine = (
        narrowgate_cpp.SignalRefPerpFeatureEngine()
    )

    # Exceed both native retained rings so parity also covers wrapped storage.
    for index in range(3_805):
        event_ms = index * 1_000 + 100
        price = 100.0 + index * 0.01 + (index % 5) * 0.002
        quantity = 0.1 + (index % 7) * 0.01
        trade = {
            "s": "BTCUSDT",
            "T": event_ms,
            "p": str(price),
            "q": str(quantity),
            "m": bool(index % 2),
        }
        ticker = {
            "s": "BTCUSDT",
            "E": index * 1_000 + 500,
            "b": str(price - 0.05),
            "a": str(price + 0.05),
        }
        for engine in (python_engine, native_engine):
            engine.on_cross_agg_trade(
                trade,
                market_type=PERP_MARKET,
                receive_ts_ns=(event_ms + 7) * 1_000_000,
            )
            engine.on_book_ticker(
                ticker,
                market_type=PERP_MARKET,
                receive_ts_ns=(index * 1_000 + 509) * 1_000_000,
            )

    for target_ms in range(3_489_000, 3_800_000, 10_000):
        python_features: dict[str, float] = {}
        native_features: dict[str, float] = {}
        assert python_engine._compute_cross_market_features(
            python_features, 100.0, target_ms
        ) is None
        prepared = native_engine._compute_cross_market_features(
            native_features, 100.0, target_ms
        )
        native_engine._commit_native_ref_perp_features(
            prepared,
            native_features,
            target_ms,
        )
        expected = np.asarray(
            [python_features[name] for name in REF_PERP_FEATURE_NAMES]
        )
        actual = np.asarray(
            [native_features[name] for name in REF_PERP_FEATURE_NAMES]
        )
        assert actual == pytest.approx(expected, rel=0.0, abs=1e-14)

    assert native_engine._cpp_ref_perp_engine.basis_count() == len(
        python_engine._cross_basis_history["cv_ref_perp"]
    )


def test_cpp_ref_perp_preserves_model_prediction_and_quote_action(
    monkeypatch,
    synthetic_model_bundle_173: Path,
):
    monkeypatch.setenv("NARROWGATE_CPP_SIGNAL_FEATURES", "1")
    monkeypatch.setenv(CPP_LIGHTGBM_INFERENCE_FLAG, "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    reference = SignalEngine(
        model_dir=synthetic_model_bundle_173,
        ret_demean_halflife=0,
    )
    native = SignalEngine(
        model_dir=synthetic_model_bundle_173,
        ret_demean_halflife=0,
    )
    reference._cpp_ref_perp_engine = None
    assert native._cpp_ref_perp_engine is not None
    assert native._cpp_model_row_173_enabled is True
    assert len(native._shared_model_feature_schema()) == 173

    # Cross the 2026 US spring-forward boundary so the fused row proves it
    # preserves the authoritative timezone/DST calendar semantics rather than
    # substituting a native fixed-offset approximation.
    base_ms = int(
        dt.datetime(
            2026,
            3,
            8,
            6,
            59,
            20,
            tzinfo=dt.UTC,
        ).timestamp()
        * 1_000
    )
    metric_history = [
        {
            "ts_ms": base_ms - (12 - index) * 300_000,
            "oi": 100_000.0 + index * 100.0,
            "top_ls": 1.0 + index * 0.01,
            "crowd_ls": 0.9 + index * 0.005,
            "taker_ls": 1.1 - index * 0.004,
        }
        for index in range(12)
    ]
    for engine in (reference, native):
        engine._metrics_history.extend(metric_history)
        engine._last_metrics = metric_history[-1]

    reference_prediction = None
    native_prediction = None
    for index in range(80):
        event_ms = base_ms + index * 1_000 + 100
        execution_price = 60_000.0 + index * 0.03 + (index % 4) * 0.01
        reference_price = 60_002.0 + index * 0.031 + (index % 5) * 0.012
        execution_trade = {
            "s": "BTCUSDC",
            "T": event_ms,
            "p": str(execution_price),
            "q": str(0.01 + (index % 3) * 0.002),
            "m": bool(index % 2),
        }
        reference_trade = {
            "s": "BTCUSDT",
            "T": event_ms,
            "p": str(reference_price),
            "q": str(0.05 + (index % 4) * 0.003),
            "m": bool((index + 1) % 2),
        }
        ticker = {
            "s": "BTCUSDT",
            "E": base_ms + index * 1_000 + 500,
            "b": str(reference_price - 0.05),
            "a": str(reference_price + 0.05),
        }
        for engine in (reference, native):
            engine.on_agg_trade(
                execution_trade,
                receive_ts_ns=(event_ms + 7) * 1_000_000,
            )
            engine.on_cross_agg_trade(
                reference_trade,
                market_type=PERP_MARKET,
                receive_ts_ns=(event_ms + 9) * 1_000_000,
            )
            engine.on_book_ticker(
                ticker,
                market_type=PERP_MARKET,
                receive_ts_ns=(base_ms + index * 1_000 + 511) * 1_000_000,
            )
        reference_prediction = reference.compute_signal()
        native_prediction = native.compute_signal()

    assert reference_prediction is not None
    assert native_prediction is not None
    feature_names = native._shared_model_feature_schema()
    reference_row = np.asarray(
        [reference_prediction.feature_dict[name] for name in feature_names]
    )
    native_row = np.asarray(
        [native_prediction.feature_dict[name] for name in feature_names]
    )
    assert native_row == pytest.approx(reference_row, rel=0.0, abs=1e-10)
    assert not isinstance(native_prediction.feature_dict, dict)
    assert set(native_prediction.feature_dict) == set(
        reference_prediction.feature_dict
    )
    for name, value in reference_prediction.feature_dict.items():
        assert native_prediction.feature_dict[name] == pytest.approx(
            value,
            rel=0.0,
            abs=1e-10,
        ), name
    assert native_prediction.feature_dict["cal_us_hour"] == 3.0
    assert native_prediction.feature_dict["cal_us_is_sunday"] == 1.0
    assert native_prediction.feature_dict["oi_log"] > 0.0
    assert native_prediction.feature_dict["toptrader_ls_ratio"] > 1.0
    assert "cv_exec_spot_typo" not in native_prediction.feature_dict
    with pytest.raises(KeyError):
        _ = native_prediction.feature_dict["cv_exec_spot_typo"]
    assert native_prediction.features == pytest.approx(
        reference_prediction.features,
        rel=0.0,
        abs=1e-10,
    )
    normalized_prediction, _ = (
        _prospective_state_fingerprint(
            native_prediction,
            path="signal.last_prediction",
            unsupported=[],
        )
    )
    assert normalized_prediction["feature_dict"]["cal_us_hour"] == 3.0
    for name in REQUIRED_MODEL_HEADS:
        assert getattr(native_prediction, name) == getattr(
            reference_prediction, name
        )
    state = QuoteState(
        mid=60_002.0,
        inventory=0.001,
        sigma_sq=4.0,
        best_bid=60_001.9,
        best_ask=60_002.1,
    )
    config = QuoteCoreConfig(
        gamma=0.046,
        kappa=0.01,
        tick_size=0.1,
        lot_size=0.001,
        maker_fee=0.0,
        order_size=0.001,
        max_inventory=0.026,
        ml_enabled=True,
        vol_blend=0.5,
        dir_threshold=0.05,
        skew_strength=0.1,
    )

    def quote(prediction):
        return compute_quote_core(
            state,
            config,
            QuotePrediction(
                dir_10s=prediction.dir_10s,
                vol_10s=prediction.vol_10s,
                ret_10s=prediction.ret_10s,
                tox_bid=prediction.tox_bid_10s,
                tox_ask=prediction.tox_ask_10s,
            ),
        )

    assert quote(native_prediction) == quote(reference_prediction)


def test_cpp_ref_perp_activation_follows_startup_model_schema(
    monkeypatch,
    synthetic_model_bundle_173: Path,
):
    monkeypatch.setenv("NARROWGATE_CPP_SIGNAL_FEATURES", "1")
    one_feature = SignalEngine(
        model_dir=MODEL_BUNDLE,
        ret_demean_halflife=0,
    )
    source_aware = SignalEngine(
        model_dir=synthetic_model_bundle_173,
        ret_demean_halflife=0,
    )

    assert one_feature._cpp_ref_perp_engine is None
    assert source_aware._cpp_ref_perp_engine is not None


def test_native_model_row_catch_up_matches_stepwise_inference(
    monkeypatch,
    synthetic_model_bundle_173: Path,
):
    monkeypatch.setenv("NARROWGATE_CPP_SIGNAL_FEATURES", "1")
    monkeypatch.setenv(CPP_LIGHTGBM_INFERENCE_FLAG, "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    stepwise = SignalEngine(
        model_dir=synthetic_model_bundle_173,
        ret_demean_halflife=7,
    )
    catch_up = SignalEngine(
        model_dir=synthetic_model_bundle_173,
        ret_demean_halflife=7,
    )
    assert stepwise._cpp_model_row_173_enabled is True
    assert catch_up._cpp_model_row_173_enabled is True

    seen_transactions: list[bool] = []
    original_predict = catch_up._predict

    def track_transaction(features):
        seen_transactions.append(
            isinstance(features, signal_module._NativeFeatureTransaction)
        )
        return original_predict(features)

    monkeypatch.setattr(catch_up, "_predict", track_transaction)
    base_ms = int(
        dt.datetime(2026, 3, 8, 6, 59, tzinfo=dt.UTC).timestamp() * 1_000
    )
    stepwise_prediction = None
    for index in range(101):
        event_ms = base_ms + index * 1_000 + 100
        execution_price = 60_000.0 + index * 0.02 + (index % 3) * 0.01
        reference_price = 60_002.0 + index * 0.021 + (index % 4) * 0.01
        execution_trade = {
            "s": "BTCUSDC",
            "T": event_ms,
            "p": str(execution_price),
            "q": str(0.01 + (index % 3) * 0.002),
            "m": bool(index % 2),
        }
        reference_trade = {
            "s": "BTCUSDT",
            "T": event_ms,
            "p": str(reference_price),
            "q": str(0.05 + (index % 4) * 0.003),
            "m": bool((index + 1) % 2),
        }
        ticker = {
            "s": "BTCUSDT",
            "E": event_ms + 400,
            "b": str(reference_price - 0.05),
            "a": str(reference_price + 0.05),
        }
        for engine in (stepwise, catch_up):
            engine.on_agg_trade(
                execution_trade,
                receive_ts_ns=(event_ms + 7) * 1_000_000,
            )
            engine.on_cross_agg_trade(
                reference_trade,
                market_type=PERP_MARKET,
                receive_ts_ns=(event_ms + 9) * 1_000_000,
            )
            engine.on_book_ticker(
                ticker,
                market_type=PERP_MARKET,
                receive_ts_ns=(event_ms + 411) * 1_000_000,
            )
        stepwise_prediction = stepwise.compute_signal()

    catch_up_prediction = catch_up.compute_signal()
    assert len(seen_transactions) > 1
    assert all(seen_transactions)
    assert stepwise._last_processed_bucket == catch_up._last_processed_bucket
    assert tuple(stepwise._pred_ret_ema) == tuple(catch_up._pred_ret_ema)
    assert stepwise_prediction is not None
    feature_names = stepwise._shared_model_feature_schema()
    assert [catch_up_prediction.feature_dict[name] for name in feature_names] == (
        pytest.approx(
            [stepwise_prediction.feature_dict[name] for name in feature_names],
            rel=0.0,
            abs=1e-10,
        )
    )
    assert catch_up_prediction.features == pytest.approx(
        stepwise_prediction.features,
        rel=0.0,
        abs=1e-10,
    )
    for name in REQUIRED_MODEL_HEADS:
        assert getattr(catch_up_prediction, name) == getattr(
            stepwise_prediction,
            name,
        )


def test_native_model_row_keeps_current_mapping_after_nonstrict_commit_failure(
    monkeypatch,
    synthetic_model_bundle_173: Path,
) -> None:
    monkeypatch.setenv("NARROWGATE_CPP_SIGNAL_FEATURES", "1")
    monkeypatch.setenv(CPP_LIGHTGBM_INFERENCE_FLAG, "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "0")
    engine = SignalEngine(
        model_dir=synthetic_model_bundle_173,
        ret_demean_halflife=0,
    )
    base_ms = int(
        dt.datetime(2026, 3, 8, 6, 59, tzinfo=dt.UTC).timestamp() * 1_000
    )
    for index in range(31):
        event_ms = base_ms + index * 1_000 + 100
        engine.on_agg_trade(
            {
                "s": "BTCUSDC",
                "T": event_ms,
                "p": str(60_000.0 + index * 0.01),
                "q": "0.01",
                "m": bool(index % 2),
            },
            receive_ts_ns=(event_ms + 7) * 1_000_000,
        )

    native_ref = engine._cpp_ref_perp_engine
    assert native_ref is not None

    class CommitFailure:
        def prepare(self, *args):
            return native_ref.prepare(*args)

        @staticmethod
        def commit(_prepared):
            raise RuntimeError("synthetic commit failure")

    engine._cpp_ref_perp_engine = CommitFailure()
    transaction = engine._prepare_native_model_row_173(base_ms)

    assert isinstance(transaction, signal_module._NativeFeatureTransaction)
    assert transaction.mapping["close"] > 0.0
    assert transaction.mapping["price_change_5s"] == pytest.approx(
        transaction.row.value_at(
            transaction.names.index("price_change_5s")
        )
    )
    assert engine._cpp_model_row_173_enabled is False
    assert engine._cpp_ref_perp_engine is None


def test_cpp_ref_perp_keeps_spot_computation_for_declared_diagnostics():
    engine = SignalEngine(
        enable_ml=False,
        preserve_full_cross_market_features=True,
        ret_demean_halflife=0,
    )
    engine._cpp_ref_perp_engine = narrowgate_cpp.SignalRefPerpFeatureEngine()
    event_ms = 10_100
    engine.on_cross_agg_trade(
        {
            "s": "BTCUSDT",
            "T": event_ms,
            "p": "100.0",
            "q": "0.1",
            "m": False,
        },
        market_type=PERP_MARKET,
        receive_ts_ns=10_105_000_000,
    )
    engine.on_cross_agg_trade(
        {
            "s": "BTCUSDC",
            "T": event_ms,
            "p": "99.9",
            "q": "0.2",
            "m": True,
        },
        market_type=signal_module.SPOT_MARKET,
        receive_ts_ns=10_106_000_000,
    )

    features: dict[str, float] = {}
    prepared = engine._compute_cross_market_features(features, 100.0, 19_000)
    engine._commit_native_ref_perp_features(prepared, features, 19_000)

    assert features["cv_ref_perp_available"] == 1.0
    assert features["cv_exec_spot_available"] == 1.0


def test_cpp_ref_perp_keeps_spot_computation_for_model_consumers(monkeypatch):
    monkeypatch.setenv("NARROWGATE_CPP_SIGNAL_FEATURES", "1")
    schema = [*REF_PERP_FEATURE_NAMES, "cv_exec_spot_available"]

    def load_models(engine):
        engine._models = {name: object() for name in REQUIRED_MODEL_HEADS}
        engine._model_feature_cols = {
            name: list(schema) for name in REQUIRED_MODEL_HEADS
        }

    monkeypatch.setattr(SignalEngine, "_load_models", load_models)
    engine = SignalEngine(enable_ml=True, ret_demean_halflife=0)
    assert engine._cpp_ref_perp_engine is not None
    assert engine._model_requires_full_cross_market_features is True

    event_ms = 10_100
    engine.on_cross_agg_trade(
        {"s": "BTCUSDT", "T": event_ms, "p": "100.0", "q": "0.1", "m": False},
        market_type=PERP_MARKET,
        receive_ts_ns=10_105_000_000,
    )
    engine.on_cross_agg_trade(
        {"s": "BTCUSDC", "T": event_ms, "p": "99.9", "q": "0.2", "m": True},
        market_type=signal_module.SPOT_MARKET,
        receive_ts_ns=10_106_000_000,
    )

    features: dict[str, float] = {}
    prepared = engine._compute_cross_market_features(features, 100.0, 19_000)
    engine._commit_native_ref_perp_features(prepared, features, 19_000)

    assert features["cv_exec_spot_available"] == 1.0


def test_native_new_bucket_does_not_iterate_or_copy_python_bar_ring():
    class BoundaryOnlyRing:
        def __init__(self, first: Bar1s, last: Bar1s) -> None:
            self.first = first
            self.last = last

        def __len__(self) -> int:
            return 30

        def __getitem__(self, index: int) -> Bar1s:
            if index == 0:
                return self.first
            if index == -1:
                return self.last
            raise AssertionError("native bucket path read an interior Python bar")

        def __iter__(self):
            raise AssertionError("native bucket path copied the Python bar ring")

    engine = SignalEngine(enable_ml=False, ret_demean_halflife=0)
    engine._cpp_signal = narrowgate_cpp
    engine._cpp_signal_features_enabled = True
    engine._cpp_signal_feature_names = tuple(narrowgate_cpp.SIGNAL_FEATURE_NAMES)
    engine._cpp_feature_engine = narrowgate_cpp.SignalFeatureEngine(3700, 60480)
    engine._cpp_feature_engine_seeded = True
    bars = [
        Bar1s(ts=index * 1_000, open=100.0, high=100.0, low=100.0, close=100.0)
        for index in range(30)
    ]
    for bar in bars:
        engine._cpp_feature_engine.push_bar(engine._bar_to_cpp(bar))
    engine._last_processed_bucket = 10_000
    engine._bar_buffer = BoundaryOnlyRing(bars[0], bars[-1])  # type: ignore[assignment]

    prediction = engine.compute_signal()

    assert engine._last_processed_bucket == 20_000
    assert prediction.feature_dict is not None
    assert prediction.feature_dict["_feature_ts_ms"] == 29_000.0


def test_native_bucket_catch_up_matches_legacy_native_processing_order():
    def ready_engine() -> SignalEngine:
        engine = SignalEngine(enable_ml=False, ret_demean_halflife=0)
        engine._cpp_signal = narrowgate_cpp
        engine._cpp_signal_features_enabled = True
        engine._cpp_signal_feature_names = tuple(narrowgate_cpp.SIGNAL_FEATURE_NAMES)
        engine._cpp_feature_engine = narrowgate_cpp.SignalFeatureEngine(3700, 60480)
        engine._cpp_feature_engine_seeded = True
        for index in range(80):
            price = 100.0 + index * 0.01
            bar = Bar1s(
                ts=index * 1_000,
                open=price,
                high=price + 0.01,
                low=price - 0.01,
                close=price,
                volume=1.0,
                buy_volume=0.6,
                sell_volume=0.4,
                trade_count=2,
                buy_count=1,
                sell_count=1,
                quote_qty=price,
                buy_quote_qty=0.6 * price,
                sell_quote_qty=0.4 * price,
                max_same_side_run=1,
            )
            engine._bar_buffer.append(bar)
            engine._cpp_feature_engine.push_bar(engine._bar_to_cpp(bar))
        engine._last_processed_bucket = 40_000
        return engine

    legacy = ready_engine()
    native = ready_engine()
    expected = legacy._process_completed_feature_buckets_locked(
        list(legacy._bar_buffer)
    )
    actual = native._process_completed_feature_buckets_native_locked()

    assert len(actual) == len(expected) == 3
    assert native._last_processed_bucket == legacy._last_processed_bucket == 70_000
    assert len(native._feat_history) == len(legacy._feat_history) == 3
    for actual_row, expected_row in zip(actual, expected, strict=True):
        assert actual_row.keys() == expected_row.keys()
        for key, value in expected_row.items():
            assert actual_row[key] == pytest.approx(value, abs=1e-10), key


def test_cpp_signal_feature_incremental_vol_regime_matches_stateless_history():
    engine = narrowgate_cpp.SignalFeatureEngine(8, 10_000)
    history = []
    for i in range(9_000):
        row = narrowgate_cpp.FeatureHistoryRow()
        row.close = 100.0 + i * 0.001
        row.return_abs = 0.0001 + (i % 31) * 1e-7
        row.vol_regime_6h = 0.0002 + (i % 101) * 1e-8
        history.append(row)
        engine.push_history(row)

    bar_10s = narrowgate_cpp.Bar1s()
    bar_10s.close = 109.01
    bar_10s.high = 109.02
    bar_10s.low = 109.00
    persistent = engine.compute(bar_10s)
    stateless = narrowgate_cpp.compute_signal_feature_overlay([], history, bar_10s)

    for key in ("vol_regime_6h", "vol_regime_24h", "vol_regime_zscore"):
        assert persistent[key] == pytest.approx(stateless[key], abs=1e-10), key


def test_native_lightgbm_bundle_matches_python_boosters_bit_for_bit() -> None:
    feature_names = _model_feature_names()
    model_paths = [MODEL_BUNDLE / f"{name}.txt" for name in REQUIRED_MODEL_HEADS]
    python_models = [lgb.Booster(model_file=str(path)) for path in model_paths]
    native_bundle = narrowgate_cpp.NativeLightgbmBundle(
        _active_lightgbm_library(),
        [str(path.resolve(strict=True)) for path in model_paths],
        len(feature_names),
    )
    assert tuple(narrowgate_cpp.LIGHTGBM_BUNDLE_HEAD_NAMES) == tuple(
        REQUIRED_MODEL_HEADS
    )
    assert native_bundle.feature_count == len(feature_names)
    assert native_bundle.head_count == len(REQUIRED_MODEL_HEADS)
    assert native_bundle.num_threads == 1

    rng = np.random.default_rng(0x4E474D)
    rows = rng.normal(size=(32, len(feature_names))).astype(np.float64)
    rows[0, ::31] = np.nan
    rows[1, ::17] = np.nextafter(rows[1, ::17], np.inf)
    rows[2, ::19] = np.nextafter(rows[2, ::19], -np.inf)
    for row in rows:
        matrix = np.ascontiguousarray(row.reshape(1, -1))
        expected = np.asarray(
            [float(model.predict(matrix)[0]) for model in python_models],
            dtype=np.float64,
        )
        actual = np.asarray(native_bundle.predict(matrix), dtype=np.float64)
        assert np.array_equal(actual.view(np.uint64), expected.view(np.uint64))


def test_native_lightgbm_partial_bundle_construction_fails_safely() -> None:
    feature_names = _model_feature_names()
    model_paths = [
        str((MODEL_BUNDLE / f"{name}.txt").resolve(strict=True))
        for name in REQUIRED_MODEL_HEADS
    ]
    model_paths[-1] = str(MODEL_BUNDLE / "missing-head.txt")

    with pytest.raises(RuntimeError):
        narrowgate_cpp.NativeLightgbmBundle(
            _active_lightgbm_library(),
            model_paths,
            len(feature_names),
        )


def test_native_lightgbm_inference_is_default_off_and_loads_on_ml_enable(
    monkeypatch,
) -> None:
    monkeypatch.delenv(CPP_LIGHTGBM_INFERENCE_FLAG, raising=False)
    engine = SignalEngine(
        model_dir=MODEL_BUNDLE,
        enable_ml=False,
        ret_demean_halflife=0,
    )

    assert engine._native_inference_requested is False
    assert engine._native_model_bundle is None

    monkeypatch.setenv(CPP_LIGHTGBM_INFERENCE_FLAG, "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    engine._enable_ml = True
    engine.reload_models()

    assert engine._native_inference_requested is True
    assert engine._native_model_bundle is not None


def test_native_lightgbm_preserves_final_prediction_and_demean_state(
    monkeypatch,
) -> None:
    monkeypatch.setenv(CPP_LIGHTGBM_INFERENCE_FLAG, "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    monkeypatch.setattr(signal_module.time, "time", lambda: 1_725_000_000.0)
    engine = SignalEngine(
        model_dir=MODEL_BUNDLE,
        ret_demean_halflife=7,
    )
    native_bundle = engine._native_model_bundle
    assert native_bundle is not None
    feature_names = engine._shared_model_feature_schema()

    rows = []
    for row_index in range(4):
        rows.append(
            {
                name: float((feature_index + 1) * (row_index + 1)) / 10_000.0
                for feature_index, name in enumerate(feature_names)
            }
        )
    rows[0][feature_names[0]] = float("nan")
    native_predictions = [engine._predict(row) for row in rows]
    native_ema = tuple(engine._pred_ret_ema)

    engine._native_model_bundle = None
    engine._pred_ret_ema = [0.0, 0.0, 0.0]
    engine._demean_log_cnt = 0
    python_predictions = [engine._predict(row) for row in rows]
    python_ema = tuple(engine._pred_ret_ema)

    compared_fields = tuple(REQUIRED_MODEL_HEADS)
    for native_prediction, python_prediction in zip(
        native_predictions,
        python_predictions,
        strict=True,
    ):
        assert native_prediction.ts == python_prediction.ts == 1_725_000_000.0
        for field_name in compared_fields:
            native_bits = np.float64(getattr(native_prediction, field_name)).view(
                np.uint64
            )
            python_bits = np.float64(getattr(python_prediction, field_name)).view(
                np.uint64
            )
            assert native_bits == python_bits, field_name
        assert np.array_equal(
            native_prediction.features.view(np.uint64),
            python_prediction.features.view(np.uint64),
        )
        assert native_prediction.feature_dict is not None
        assert python_prediction.feature_dict is not None
        native_feature_bits = np.asarray(
            [native_prediction.feature_dict[name] for name in feature_names],
            dtype=np.float64,
        ).view(np.uint64)
        python_feature_bits = np.asarray(
            [python_prediction.feature_dict[name] for name in feature_names],
            dtype=np.float64,
        ).view(np.uint64)
        assert np.array_equal(native_feature_bits, python_feature_bits)
    assert np.array_equal(
        np.asarray(native_ema, dtype=np.float64).view(np.uint64),
        np.asarray(python_ema, dtype=np.float64).view(np.uint64),
    )


def test_native_lightgbm_runtime_failure_is_strict_or_one_way_fallback(
    monkeypatch,
) -> None:
    class BrokenNativeBundle:
        def predict(self, _row):
            raise RuntimeError("synthetic native failure")

    engine = SignalEngine(enable_ml=False, ret_demean_halflife=0)
    engine._enable_ml = True
    engine._models = {
        name: SimpleNamespace(
            predict=lambda _row, value=index / 100.0: np.asarray([value])
        )
        for index, name in enumerate(REQUIRED_MODEL_HEADS)
    }
    engine._model_feature_cols = {
        name: ["feature"] for name in REQUIRED_MODEL_HEADS
    }

    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "0")
    engine._native_model_bundle = BrokenNativeBundle()
    prediction = engine._predict({"feature": 1.0})
    assert prediction.dir_10s == 0.0
    assert prediction.ret_10s == 0.06
    assert engine._native_model_bundle is None
    assert engine._native_inference_disabled_after_error is True

    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    engine._native_model_bundle = BrokenNativeBundle()
    with pytest.raises(RuntimeError, match="strict native LightGBM prediction"):
        engine._predict({"feature": 1.0})


def test_native_lightgbm_failed_strict_reload_keeps_admitted_bundle(
    monkeypatch,
) -> None:
    monkeypatch.setenv(CPP_LIGHTGBM_INFERENCE_FLAG, "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    engine = SignalEngine(model_dir=MODEL_BUNDLE, ret_demean_halflife=0)
    old_models = engine._models
    old_native_bundle = engine._native_model_bundle

    with pytest.raises(RuntimeError, match="runtime bundle is invalid"):
        engine.reload_models(MODEL_BUNDLE / "missing-bundle")
    assert engine._models is old_models
    assert engine._native_model_bundle is old_native_bundle
    assert engine._model_dir == MODEL_BUNDLE

    def reject_candidate(_lgb_module, *, model_dir, feature_count):
        assert model_dir == MODEL_BUNDLE
        raise RuntimeError(f"rejected width {feature_count}")

    monkeypatch.setattr(engine, "_build_native_model_bundle", reject_candidate)
    with pytest.raises(RuntimeError, match="initialization failed"):
        engine.reload_models()

    assert engine._models is old_models
    assert engine._native_model_bundle is old_native_bundle
    assert engine._model_dir == MODEL_BUNDLE


def test_native_173_row_rejection_is_atomic_during_strict_reload(
    monkeypatch,
    synthetic_model_bundle_173: Path,
) -> None:
    monkeypatch.setenv("NARROWGATE_CPP_SIGNAL_FEATURES", "1")
    monkeypatch.setenv(CPP_LIGHTGBM_INFERENCE_FLAG, "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    engine = SignalEngine(
        model_dir=synthetic_model_bundle_173,
        ret_demean_halflife=0,
    )
    old_models = engine._models
    old_feature_cols = engine._model_feature_cols
    old_feature_schema = engine._model_feature_schema
    old_metadata = engine._model_metadata
    old_native_bundle = engine._native_model_bundle
    old_row_state = engine._cpp_model_row_173_state

    with pytest.raises(
        RuntimeError,
        match="native 173-row order differs from model bundle schema",
    ):
        engine.reload_models(MODEL_BUNDLE)

    assert engine._models is old_models
    assert engine._model_feature_cols is old_feature_cols
    assert engine._model_feature_schema is old_feature_schema
    assert engine._model_metadata is old_metadata
    assert engine._native_model_bundle is old_native_bundle
    assert engine._cpp_model_row_173_state is old_row_state
    assert engine._model_dir == synthetic_model_bundle_173


def test_native_lightgbm_failed_nonstrict_initialization_uses_python_bundle(
    monkeypatch,
) -> None:
    monkeypatch.setenv(CPP_LIGHTGBM_INFERENCE_FLAG, "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "0")
    engine = SignalEngine(
        model_dir=MODEL_BUNDLE,
        enable_ml=False,
        ret_demean_halflife=0,
    )
    engine._enable_ml = True

    def reject_candidate(_lgb_module, *, model_dir, feature_count):
        assert model_dir == MODEL_BUNDLE
        raise RuntimeError(f"rejected width {feature_count}")

    monkeypatch.setattr(engine, "_build_native_model_bundle", reject_candidate)
    engine.reload_models()

    assert set(engine._models) == set(REQUIRED_MODEL_HEADS)
    assert engine._native_inference_requested is True
    assert engine._native_model_bundle is None
    assert engine._native_inference_disabled_after_error is True
    prediction = engine._predict({"close": 1.0})
    assert isinstance(prediction, signal_module.Prediction)
