import hashlib
import json
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from execution.runtime_evidence_writer import RuntimeEvidenceQueueFull
import strategy.maker_engine as maker_engine_module
from live.config import Config, _validate_config
from strategy.maker_engine import POLICY_REASON_FILL_COOLDOWN, MakerEngine
from strategy.order_manager import OrderManager, Side
from strategy.policy_guards import CommonSidePolicyResult
from strategy.signal import Prediction


def _policy_authority(paths: dict[str, Path]) -> dict[str, dict[str, str]]:
    return {
        "model_policy_member_paths": {
            role: str(path.resolve()) for role, path in paths.items()
        },
        "model_policy_member_sha256": {
            role: hashlib.sha256(path.read_bytes()).hexdigest()
            for role, path in paths.items()
        },
    }


def _write_live_p3(path: Path) -> None:
    path.write_text(json.dumps({
        "schema_version": "narrowgate_p3_touch_calibration.v2",
        "model_type": "empirical_survival",
        "delta_grid": [0.1, 14.0, 30.0],
        "probability_grid": [0.8, 0.2, 0.01],
        "metadata": {
            "event_type": "touch", "horizon_s": 10.0, "distance_unit": "USDC_per_BTC",
        },
        "delta_star": 14.0,
        "kappa_eff": 0.067,
    }), encoding="utf-8")


def test_boolean_cooldown_loader_uses_envelope_members_not_yaml_leaves(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy_path = tmp_path / "boolean-policy.json"
    bundle_path = tmp_path / "boolean-bundle.json"
    policy_path.write_text("{}\n", encoding="utf-8")
    bundle_path.write_text("{}\n", encoding="utf-8")
    cfg = Config()
    cfg.strategy.boolean_cooldown_policy_enabled = True
    cfg.strategy.boolean_cooldown_policy_path = "/wrong/policy.json"
    cfg.strategy.boolean_cooldown_policy_sha256 = "0" * 64
    cfg.strategy.boolean_cooldown_predicate_bundle_path = "/wrong/bundle.json"
    cfg.strategy.boolean_cooldown_predicate_bundle_sha256 = "1" * 64
    observed: dict[str, object] = {}
    runtime = SimpleNamespace(observe_depth=lambda *_args: None)

    def fake_from_files(_cls, **kwargs):
        observed.update(kwargs)
        return runtime

    monkeypatch.setattr(
        maker_engine_module.LiveBooleanCooldownPolicy,
        "from_files",
        classmethod(fake_from_files),
    )
    authority = _policy_authority(
        {
            "boolean_policy": policy_path,
            "boolean_predicate_bundle": bundle_path,
        }
    )

    loaded = maker_engine_module._load_boolean_cooldown_live_policy(  # noqa: SLF001
        cfg,
        artifact_authority=authority,
    )

    assert loaded is runtime
    assert observed["policy_path"] == policy_path.resolve()
    assert observed["policy_sha256"] == hashlib.sha256(
        policy_path.read_bytes()
    ).hexdigest()
    assert observed["predicate_bundle_path"] == bundle_path.resolve()
    assert observed["predicate_bundle_sha256"] == hashlib.sha256(
        bundle_path.read_bytes()
    ).hexdigest()


def test_buy_e3_loader_uses_envelope_members_and_manifest_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest_path = tmp_path / "buy-manifest.json"
    policy_path = tmp_path / "buy-policy.json"
    bundle_path = tmp_path / "buy-bundle.json"
    artifact_sha256 = "a" * 64
    manifest_path.write_text(
        json.dumps({"artifact_sha256": artifact_sha256}) + "\n",
        encoding="utf-8",
    )
    policy_path.write_text("{}\n", encoding="utf-8")
    bundle_path.write_text("{}\n", encoding="utf-8")
    cfg = Config()
    cfg.strategy.buy_e3_cooldown_policy_enabled = True
    cfg.strategy.buy_e3_cooldown_artifact_manifest_path = "/wrong/manifest.json"
    cfg.strategy.buy_e3_cooldown_artifact_manifest_sha256 = "0" * 64
    cfg.strategy.buy_e3_cooldown_artifact_sha256 = "1" * 64
    cfg.strategy.buy_e3_cooldown_policy_path = "/wrong/policy.json"
    cfg.strategy.buy_e3_cooldown_policy_sha256 = "2" * 64
    cfg.strategy.buy_e3_cooldown_predicate_bundle_path = "/wrong/bundle.json"
    cfg.strategy.buy_e3_cooldown_predicate_bundle_sha256 = "3" * 64
    observed: dict[str, object] = {}
    runtime = SimpleNamespace(observe_depth=lambda *_args: None)

    def fake_from_files(_cls, **kwargs):
        observed.update(kwargs)
        return runtime

    monkeypatch.setattr(
        maker_engine_module.LiveBuyE3CooldownPolicy,
        "from_files",
        classmethod(fake_from_files),
    )
    authority = _policy_authority(
        {
            "artifact_manifest": manifest_path,
            "policy": policy_path,
            "predicate_bundle": bundle_path,
        }
    )

    loaded = maker_engine_module._load_buy_e3_cooldown_live_policy(  # noqa: SLF001
        cfg,
        artifact_authority=authority,
    )

    assert loaded is runtime
    assert observed["artifact_manifest_path"] == manifest_path.resolve()
    assert observed["artifact_manifest_sha256"] == hashlib.sha256(
        manifest_path.read_bytes()
    ).hexdigest()
    assert observed["expected_artifact_sha256"] == artifact_sha256
    assert observed["policy_path"] == policy_path.resolve()
    assert observed["predicate_bundle_path"] == bundle_path.resolve()


def test_policy_artifact_authority_fails_closed_on_missing_or_drifted_member(
    tmp_path: Path,
) -> None:
    policy_path = tmp_path / "boolean-policy.json"
    policy_path.write_text("{}\n", encoding="utf-8")
    cfg = Config()
    cfg.strategy.boolean_cooldown_policy_enabled = True
    missing = _policy_authority({"boolean_policy": policy_path})

    with pytest.raises(ValueError, match="required_roles_missing"):
        maker_engine_module._load_boolean_cooldown_live_policy(  # noqa: SLF001
            cfg,
            artifact_authority=missing,
        )

    bundle_path = tmp_path / "boolean-bundle.json"
    bundle_path.write_text("{}\n", encoding="utf-8")
    drifted = _policy_authority(
        {
            "boolean_policy": policy_path,
            "boolean_predicate_bundle": bundle_path,
        }
    )
    drifted["model_policy_member_sha256"]["boolean_policy"] = "0" * 64
    with pytest.raises(ValueError, match="policy_file_sha256_mismatch"):
        maker_engine_module._load_boolean_cooldown_live_policy(  # noqa: SLF001
            cfg,
            artifact_authority=drifted,
        )


def test_live_artifact_authority_matches_enabled_config_locators(
    tmp_path: Path,
) -> None:
    paths = {
        role: tmp_path / f"{role}.json"
        for role in (
            "model_authorization",
            "p3",
            "state_conditioned_quote_policy",
            "boolean_policy",
            "boolean_predicate_bundle",
            "artifact_manifest",
            "policy",
            "predicate_bundle",
        )
    }
    for path in paths.values():
        path.write_text("{}\n", encoding="utf-8")
    _write_live_p3(paths["p3"])
    cfg = Config()
    cfg.strategy.state_conditioned_policy_mode = "active"
    cfg.strategy.state_conditioned_policy_model_path = str(paths["state_conditioned_quote_policy"])
    cfg.strategy.boolean_cooldown_policy_enabled = True
    cfg.strategy.boolean_cooldown_policy_path = str(paths["boolean_policy"])
    cfg.strategy.boolean_cooldown_predicate_bundle_path = str(
        paths["boolean_predicate_bundle"]
    )
    cfg.strategy.buy_e3_cooldown_policy_enabled = True
    cfg.strategy.buy_e3_cooldown_artifact_manifest_path = str(
        paths["artifact_manifest"]
    )
    cfg.strategy.buy_e3_cooldown_policy_path = str(paths["policy"])
    cfg.strategy.buy_e3_cooldown_predicate_bundle_path = str(
        paths["predicate_bundle"]
    )
    authority = _policy_authority(paths)

    maker_engine_module.validate_live_artifact_authority(
        cfg,
        artifact_authority=authority,
        model_authorization_path=paths["model_authorization"],
        p3_path=paths["p3"],
    )

    cfg.strategy.buy_e3_cooldown_policy_path = str(tmp_path / "other.json")
    with pytest.raises(ValueError, match="config_path_drifted:policy"):
        maker_engine_module.validate_live_artifact_authority(
            cfg,
            artifact_authority=authority,
            model_authorization_path=paths["model_authorization"],
            p3_path=paths["p3"],
        )


@pytest.mark.parametrize("ml_enabled", [False, True])
def test_live_artifact_authority_requires_only_enabled_ml_and_independent_p3(
    tmp_path: Path, ml_enabled: bool,
) -> None:
    cfg = Config()
    cfg.ml.enabled = ml_enabled
    p3_path = tmp_path / "p3.json"
    _write_live_p3(p3_path)
    authority = _policy_authority({"p3": p3_path})
    if ml_enabled:
        with pytest.raises(ValueError, match="model_authorization_required"):
            maker_engine_module.validate_live_artifact_authority(
                cfg, artifact_authority=authority, model_authorization_path=None, p3_path=p3_path,
            )
        return

    maker_engine_module.validate_live_artifact_authority(
        cfg, artifact_authority=authority, model_authorization_path=None, p3_path=p3_path,
    )
    with pytest.raises(ValueError, match="p3_artifact_required"):
        maker_engine_module.validate_live_artifact_authority(
            cfg, artifact_authority=authority, model_authorization_path=None,
        )
    with pytest.raises(ValueError, match="required_roles_missing"):
        maker_engine_module.validate_live_artifact_authority(
            cfg, artifact_authority=_policy_authority({}),
            model_authorization_path=None, p3_path=p3_path,
        )


@pytest.mark.parametrize(
    ("role", "drift"),
    [("p3", "path"), ("p3", "bytes"), ("state_conditioned_quote_policy", "path")],
)
def test_live_artifact_authority_rejects_p3_and_state_policy_drift(
    tmp_path: Path, role: str, drift: str,
) -> None:
    cfg = Config()
    cfg.ml.enabled = False
    cfg.strategy.state_conditioned_policy_mode = "active"
    paths = {name: tmp_path / f"{name}.json" for name in ("p3", "state_conditioned_quote_policy")}
    for path in paths.values():
        path.write_text("{}\n", encoding="utf-8")
    _write_live_p3(paths["p3"])
    cfg.strategy.state_conditioned_policy_model_path = str(paths["state_conditioned_quote_policy"])
    authority = _policy_authority(paths)
    if drift == "path":
        changed_path = tmp_path / "changed.json"
        changed_path.write_text("{}\n", encoding="utf-8")
        paths[role] = changed_path
        cfg.strategy.state_conditioned_policy_model_path = str(
            paths["state_conditioned_quote_policy"]
        )
        error = f"config_path_drifted:{role}"
    else:
        # Keep the artifact usable so this exercises byte binding separately
        # from the malformed-P3 checks below.
        paths[role].write_bytes(paths[role].read_bytes() + b"\n")
        error = f"file_sha256_mismatch:{role}"
    with pytest.raises(ValueError, match=error):
        maker_engine_module.validate_live_artifact_authority(
            cfg, artifact_authority=authority, model_authorization_path=None, p3_path=paths["p3"],
        )


@pytest.mark.parametrize("invalid", ("public_fixture", "malformed", "invalid_horizon"))
def test_ml_off_startup_rejects_unusable_p3_without_optional_preflight(
    tmp_path: Path, invalid: str,
) -> None:
    cfg = Config()
    cfg.ml.enabled = False
    p3_path = tmp_path / "p3.json"
    _write_live_p3(p3_path)
    p3 = json.loads(p3_path.read_text(encoding="utf-8"))
    if invalid == "public_fixture":
        p3["metadata"]["authority"] = "public_dry_run_only"
        error = "public_dry_run_only"
    elif invalid == "invalid_horizon":
        p3["metadata"]["horizon_s"] = 0.0
        error = "horizon_s"
    else:
        p3["delta_star"] = -1.0
        error = "positive"
    p3_path.write_text(json.dumps(p3), encoding="utf-8")
    with pytest.raises(ValueError, match=error):
        maker_engine_module.validate_live_artifact_authority(
            cfg, artifact_authority=_policy_authority({"p3": p3_path}),
            model_authorization_path=None, p3_path=p3_path,
        )


@pytest.mark.parametrize("loaded", ("missing", "different_bytes"))
def test_quote_preparation_refuses_p3_changed_after_startup_admission(
    monkeypatch: pytest.MonkeyPatch, loaded: str,
) -> None:
    engine = object.__new__(MakerEngine)
    engine.cfg = Config()
    engine._model_dir = Path("synthetic")
    engine._p3_artifact_sha256 = "a" * 64
    model = None if loaded == "missing" else SimpleNamespace(
        optimal_delta=lambda: 1.0,
        effective_kappa=lambda _delta: 1.0,
        semantic_identity=lambda **_kwargs: {"artifact_sha256": "b" * 64},
    )
    monkeypatch.setattr(maker_engine_module, "_get_fill_model", lambda _path: model)
    with pytest.raises(RuntimeError, match="loaded P3 differs"):
        engine._prepare_quote_runtime()


def test_legacy_ml_envelope_still_uses_model_authorization_p3_binding(tmp_path: Path) -> None:
    cfg = Config()
    cfg.ml.enabled = True
    authorization_path = tmp_path / "model_authorization.json"
    p3_path = tmp_path / "p3.json"
    authorization_path.write_text("{}\n", encoding="utf-8")
    p3_path.write_text("{}\n", encoding="utf-8")
    maker_engine_module.validate_live_artifact_authority(
        cfg, artifact_authority=_policy_authority({"model_authorization": authorization_path}),
        model_authorization_path=authorization_path, p3_path=p3_path,
    )


def test_stateful_fill_cooldown_requires_checkpoint_path() -> None:
    cfg = Config()
    cfg.strategy.fill_cooldown = 85.0
    cfg.logging.fill_cooldown_checkpoint = ""

    with pytest.raises(
        ValueError,
        match="stateful fill cooldown requires logging.fill_cooldown_checkpoint",
    ):
        _validate_config(cfg)


def _engine_with_active_fill_cooldowns() -> MakerEngine:
    cfg = Config()
    cfg.strategy.max_inventory = 0.026
    cfg.strategy.fill_cooldown = 41.0
    cfg.strategy.fill_cooldown_reducing = 0.0
    cfg.strategy.fill_cooldown_reducing_vol_ref = 0.0
    cfg.strategy.markout_spread_scale = 0.0
    cfg.strategy.thin_depth_threshold = 1.0
    cfg.strategy.kappa_depth_baseline = 1.0
    cfg.risk.max_exec_book_age_s = 0.0

    engine = object.__new__(MakerEngine)
    engine.cfg = cfg
    engine._mo_ema_bid = 0.0
    engine._mo_ema_ask = 0.0
    engine._mo_ref = 50.0
    engine._last_quote_context = {}
    engine._model_dir = ""
    engine._last_prediction = None
    engine._fill_cooldown_until = {
        "BUY": time.time() + 60.0,
        "SELL": time.time() + 60.0,
    }
    engine._toxicity_probs = lambda pred: (0.0, 0.0)
    engine._current_l2_policy_metrics = lambda mid: {
        "depth_age_s": 0.0,
        "microprice_shift_bps": 0.0,
        "l2_quote_flip_rate": 0.0,
        "l2_book_refresh_ratio": 1.0,
        "l2_book_cancel_ratio": 0.0,
        "l2_near_depth_total": 10.0,
    }
    return engine


def test_fill_cooldown_blocks_only_exposure_increasing_side() -> None:
    engine = _engine_with_active_fill_cooldowns()
    pred = Prediction()

    # Long inventory: BUY adds exposure, SELL reduces inventory.
    long_buy = engine._build_side_policy(Side.BUY, mid=100.0, q=0.005, pred=pred)
    long_sell = engine._build_side_policy(Side.SELL, mid=100.0, q=0.005, pred=pred)
    assert not long_buy.allow_post
    assert long_buy.reason_mask & POLICY_REASON_FILL_COOLDOWN
    assert long_sell.allow_post
    assert not (long_sell.reason_mask & POLICY_REASON_FILL_COOLDOWN)

    # Short inventory: SELL adds exposure, BUY reduces inventory.
    short_buy = engine._build_side_policy(Side.BUY, mid=100.0, q=-0.005, pred=pred)
    short_sell = engine._build_side_policy(Side.SELL, mid=100.0, q=-0.005, pred=pred)
    assert short_buy.allow_post
    assert not (short_buy.reason_mask & POLICY_REASON_FILL_COOLDOWN)
    assert not short_sell.allow_post
    assert short_sell.reason_mask & POLICY_REASON_FILL_COOLDOWN


def test_fill_cooldown_blocks_both_sides_when_flat() -> None:
    engine = _engine_with_active_fill_cooldowns()
    pred = Prediction()

    flat_buy = engine._build_side_policy(Side.BUY, mid=100.0, q=0.0, pred=pred)
    flat_sell = engine._build_side_policy(Side.SELL, mid=100.0, q=0.0, pred=pred)
    assert not flat_buy.allow_post
    assert flat_buy.reason_mask & POLICY_REASON_FILL_COOLDOWN
    assert not flat_sell.allow_post
    assert flat_sell.reason_mask & POLICY_REASON_FILL_COOLDOWN


def test_native_common_policy_keeps_cooldown_expiry_at_b0_policy_clock(
    monkeypatch,
) -> None:
    engine = _engine_with_active_fill_cooldowns()
    engine._fill_cooldown_until["BUY"] = 100.0
    mutations = []

    def expire(side: str, now: float) -> None:
        mutations.append((side, now))
        if now >= engine._fill_cooldown_until[side]:
            engine._fill_cooldown_until[side] = 0.0

    engine._expire_fill_cooldown_state = expire
    engine._apply_buy_fill_selection_live_arm = lambda **kwargs: None
    monkeypatch.setattr(maker_engine_module.time, "time", lambda: 100.001)
    native_stateless = CommonSidePolicyResult()

    result = engine._build_side_policy(
        Side.BUY,
        mid=100.0,
        q=0.0,
        pred=Prediction(),
        native_common=native_stateless,
    )

    assert mutations == [("BUY", 100.001)]
    assert result.allow_post
    assert not (result.reason_mask & POLICY_REASON_FILL_COOLDOWN)


def test_reducing_fill_cooldown_can_pace_reducing_side_when_enabled() -> None:
    engine = _engine_with_active_fill_cooldowns()
    engine.cfg.strategy.fill_cooldown_reducing = 12.0
    pred = Prediction()

    # Long inventory: SELL reduces inventory, but the separate reducing-side
    # cooldown can still pace repeated same-side reducer fills.
    long_sell = engine._build_side_policy(Side.SELL, mid=100.0, q=0.005, pred=pred)
    assert not long_sell.allow_post
    assert long_sell.reason_mask & POLICY_REASON_FILL_COOLDOWN

    # Short inventory: BUY reduces inventory and is paced by the same knob.
    short_buy = engine._build_side_policy(Side.BUY, mid=100.0, q=-0.005, pred=pred)
    assert not short_buy.allow_post
    assert short_buy.reason_mask & POLICY_REASON_FILL_COOLDOWN


def test_reducing_fill_cooldown_vol_multiplier_clamps() -> None:
    engine = _engine_with_active_fill_cooldowns()
    engine.cfg.strategy.fill_cooldown_reducing_vol_ref = 10.0
    engine.cfg.strategy.fill_cooldown_reducing_vol_min_mult = 0.5
    engine.cfg.strategy.fill_cooldown_reducing_vol_max_mult = 2.0

    engine._last_prediction = Prediction(vol_10s=30.0)
    assert engine._reducing_cooldown_vol_mult() == 2.0

    engine._last_prediction = Prediction(vol_10s=2.0)
    assert engine._reducing_cooldown_vol_mult() == 0.5

    engine._last_prediction = Prediction(vol_10s=10.0)
    assert engine._reducing_cooldown_vol_mult() == 1.0


def test_reducing_cooldown_campaign_gate_requires_inventory_or_age_hit() -> None:
    engine = _engine_with_active_fill_cooldowns()
    engine.cfg.strategy.fill_cooldown_reducing_campaign_only = True
    engine.cfg.strategy.fill_cooldown_reducing_inv_ratio = 6.0
    engine.cfg.strategy.fill_cooldown_reducing_age_s = 20.0 * 60.0
    engine.cfg.strategy.order_size = 0.001
    engine.inventory = SimpleNamespace(
        campaign_snapshot=lambda: SimpleNamespace(active=True, age_s=300.0)
    )

    # 5x order_size and a young campaign: ordinary natural reducing should not
    # start a cooldown.
    assert not engine._reducing_cooldown_campaign_gate_active(0.005)

    # 6x order_size is explicitly a high-inventory state.
    assert engine._reducing_cooldown_campaign_gate_active(0.006)

    # Even modest inventory can be paced once the campaign has stayed open long
    # enough to become a campaign-risk problem.
    engine.inventory = SimpleNamespace(
        campaign_snapshot=lambda: SimpleNamespace(active=True, age_s=1300.0)
    )
    assert engine._reducing_cooldown_campaign_gate_active(0.003)


def test_boolean_cooldown_changes_only_sell_exposure_duration() -> None:
    engine = object.__new__(MakerEngine)

    class StubPolicy:
        def evaluate(self, **kwargs):
            assert kwargs["side"] == "SELL"
            assert kwargs["baseline_duration_ms"] == 170_000
            return SimpleNamespace(
                action_id="FIXED_211S",
                duration_ms=211_000,
            )

    engine._boolean_cooldown_policy = StubPolicy()
    selected, decision = engine._select_boolean_cooldown_duration(
        side="SELL",
        exposure_increasing_fill=True,
        baseline_duration_s=170.0,
        campaign_age_s=50.0,
        fill_visible_ts_ns=1_800_000_000_000_000_000,
        snapshot_id="fill-1",
    )
    assert selected == 211.0
    assert decision.action_id == "FIXED_211S"

    buy, buy_decision = engine._select_boolean_cooldown_duration(
        side="BUY",
        exposure_increasing_fill=True,
        baseline_duration_s=170.0,
        campaign_age_s=50.0,
        fill_visible_ts_ns=1_800_000_000_000_000_000,
        snapshot_id="fill-2",
    )
    assert buy == 170.0
    assert buy_decision is None

    reducing, reducing_decision = engine._select_boolean_cooldown_duration(
        side="SELL",
        exposure_increasing_fill=False,
        baseline_duration_s=12.0,
        campaign_age_s=50.0,
        fill_visible_ts_ns=1_800_000_000_000_000_000,
        snapshot_id="fill-3",
    )
    assert reducing == 12.0
    assert reducing_decision is None


def test_boolean_cooldown_control_fallback_preserves_exact_baseline() -> None:
    engine = object.__new__(MakerEngine)

    class StubPolicy:
        def evaluate(self, **kwargs):
            return SimpleNamespace(
                action_id="CONTROL_85N",
                duration_ms=127_500,
            )

    engine._boolean_cooldown_policy = StubPolicy()
    selected, decision = engine._select_boolean_cooldown_duration(
        side="SELL",
        exposure_increasing_fill=True,
        baseline_duration_s=127.5004,
        campaign_age_s=50.0,
        fill_visible_ts_ns=1_800_000_000_000_000_000,
        snapshot_id="fill-4",
    )
    assert selected == 127.5004
    assert decision.action_id == "CONTROL_85N"


def test_buy_e3_changes_only_buy_exposure_duration_and_uses_total_action() -> None:
    engine = object.__new__(MakerEngine)

    class StubPolicy:
        def evaluate(self, **kwargs):
            assert kwargs["side"] == "BUY"
            assert kwargs["baseline_duration_ms"] == 255_000
            return SimpleNamespace(action_id="FIXED_2048S", duration_ms=2_048_000)

    engine._buy_e3_cooldown_policy = StubPolicy()
    selected, decision = engine._select_buy_e3_cooldown_duration(
        side="BUY",
        exposure_increasing_fill=True,
        baseline_duration_s=255.0,
        campaign_age_s=500.0,
        fill_visible_ts_ns=1_800_000_000_000_000_000,
        snapshot_id="buy-fill-1",
    )
    assert selected == 2_048.0
    assert decision.action_id == "FIXED_2048S"

    reducing, reducing_decision = engine._select_buy_e3_cooldown_duration(
        side="BUY",
        exposure_increasing_fill=False,
        baseline_duration_s=12.0,
        campaign_age_s=500.0,
        fill_visible_ts_ns=1_800_000_000_000_000_000,
        snapshot_id="buy-fill-2",
    )
    assert reducing == 12.0
    assert reducing_decision is None

    sell, sell_decision = engine._select_buy_e3_cooldown_duration(
        side="SELL",
        exposure_increasing_fill=True,
        baseline_duration_s=255.0,
        campaign_age_s=500.0,
        fill_visible_ts_ns=1_800_000_000_000_000_000,
        snapshot_id="buy-fill-3",
    )
    assert sell == 255.0
    assert sell_decision is None


def test_buy_e3_control_fallback_preserves_exact_b0_duration() -> None:
    engine = object.__new__(MakerEngine)

    class StubPolicy:
        def evaluate(self, **kwargs):
            return SimpleNamespace(action_id="CONTROL_85N", duration_ms=255_000)

    engine._buy_e3_cooldown_policy = StubPolicy()
    selected, decision = engine._select_buy_e3_cooldown_duration(
        side="BUY",
        exposure_increasing_fill=True,
        baseline_duration_s=255.0004,
        campaign_age_s=500.0,
        fill_visible_ts_ns=1_800_000_000_000_000_000,
        snapshot_id="buy-fill-control",
    )
    assert selected == 255.0004
    assert decision.action_id == "CONTROL_85N"


class _FillInventory:
    def __init__(self, qty: float = 0.0) -> None:
        self.snapshot = SimpleNamespace(qty=float(qty))
        self._snapshot_update_time_ms = 0
        self._snapshot_order_cursors = {}
        self._local_order_cursors = {}
        self._seen_trade_ids = set()
        self._runtime_evidence_error = None

    @property
    def net_position(self) -> float:
        return float(self.snapshot.qty)

    def on_fill(
        self,
        side,
        qty,
        _price,
        _commission,
        _trade_time_ms,
        **identity,
    ) -> float:
        order_id = identity.get("order_id")
        trade_id = identity.get("trade_id")
        cumulative = identity.get("cumulative_filled_qty")
        if trade_id is not None and str(trade_id) in self._seen_trade_ids:
            return 0.0
        if order_id is not None and cumulative is not None:
            order_key = str(order_id)
            previous = self._local_order_cursors.get(order_key, 0.0)
            effective = max(0.0, float(cumulative) - previous)
            self._local_order_cursors[order_key] = max(previous, float(cumulative))
            if trade_id is not None:
                self._seen_trade_ids.add(str(trade_id))
            if effective <= 1e-10:
                return 0.0
            qty = effective
        signed = float(qty) if side == "BUY" else -float(qty)
        self.snapshot.qty += signed
        return float(qty)

    @property
    def consecutive_losses(self) -> int:
        return 0

    def pop_runtime_evidence_error(self):
        error = self._runtime_evidence_error
        self._runtime_evidence_error = None
        return error

    def campaign_snapshot(self):
        return SimpleNamespace(age_s=500.0)

    def reconciliation_snapshot(self):
        return {
            "snapshot_update_time_ms": self._snapshot_update_time_ms,
            "order_cumulative_filled_qty": dict(self._snapshot_order_cursors),
            "local_order_cumulative_filled_qty": dict(self._local_order_cursors),
            "retained_post_snapshot_fill_count": 0,
        }

    def sync_from_exchange(
        self,
        exchange_qty,
        _exchange_entry,
        *,
        snapshot_update_time_ms,
        order_cumulative_filled_qty,
        included_trade_ids=(),
        included_trade_identities=None,
    ):
        identities = included_trade_identities or {}
        assert set(map(str, included_trade_ids)) == set(map(str, identities))
        if self._snapshot_update_time_ms > 0:
            assert self.snapshot.qty == pytest.approx(float(exchange_qty))
        else:
            self.snapshot.qty = float(exchange_qty)
        self._snapshot_update_time_ms = int(snapshot_update_time_ms)
        self._snapshot_order_cursors = {
            str(order_id): float(cumulative)
            for order_id, cumulative in order_cumulative_filled_qty.items()
        }
        self._local_order_cursors.update(self._snapshot_order_cursors)
        self._seen_trade_ids.update(map(str, included_trade_ids))
        return {"seeded": self._snapshot_update_time_ms > 0}


def _fill_callback_engine(
    *,
    action_id: str = "FIXED_2048S",
    duration_ms: int = 2_048_000,
    initial_qty: float = 0.0,
    policy_enabled: bool = True,
):
    cfg = Config()
    cfg.strategy.order_size = 0.001
    cfg.lot_size = 0.0001
    cfg.strategy.fill_cooldown = 85.0
    cfg.strategy.fill_cooldown_reducing = 0.0
    cfg.strategy.markout_ema_span_fills = 0
    cfg.strategy.markout_spread_scale = 0.0
    cfg.strategy.max_inventory = 1.0
    evaluator_calls = []
    canceled_sides = []

    class StubPolicy:
        deadline_identity = "BUY_E3:fixture"

        def evaluate(self, **kwargs):
            evaluator_calls.append(dict(kwargs))
            return SimpleNamespace(
                action_id=action_id,
                duration_ms=duration_ms,
                support_valid=action_id != "CONTROL_85N",
                matched_rule_index=0 if action_id != "CONTROL_85N" else None,
                fallback_reason=None if action_id != "CONTROL_85N" else "no_rule_matched",
                feature_age_ms=0.0,
                artifact_sha256="a" * 64,
                policy_sha256="b" * 64,
                predicate_bundle_sha256="c" * 64,
            )

    engine = object.__new__(MakerEngine)
    engine.cfg = cfg
    engine.inventory = _FillInventory(initial_qty)
    engine._post_fill_quote_response = SimpleNamespace(record_fill=lambda **_kwargs: None)
    engine._base_asset = "BTC"
    engine._quote_asset = "USDC"
    engine._settlement_asset = "USDC"
    engine._commission_unit_error = None
    engine._log_order_outcome = lambda *_args, **_kwargs: None
    engine._consec_buy = 0.0
    engine._consec_sell = 0.0
    engine._fill_cooldown_until = {"BUY": 0.0, "SELL": 0.0}
    engine._fill_cooldown_deadline_identity = {"BUY": "B0", "SELL": "B0"}
    engine._fill_cooldown_natural_b0_until = {"BUY": 0.0, "SELL": 0.0}
    engine._last_same_side_fill_epoch_ms = {"BUY": 0, "SELL": 0}
    engine._last_fill_side = ""
    engine._adaptive_add_cooldown_multiplier = lambda *_args: 1.0
    engine._boolean_cooldown_policy = None
    engine._buy_e3_cooldown_policy = StubPolicy() if policy_enabled else None
    engine._cancel_cooldown_side_order = canceled_sides.append
    engine._mo_pending = []
    engine._bid_cid = None
    engine._ask_cid = None
    engine._pop_order_context = lambda _cid: None
    return engine, evaluator_calls, canceled_sides


def _fill_order(side: Side = Side.BUY) -> SimpleNamespace:
    return SimpleNamespace(
        side=side,
        price=70_000.0,
        client_order_id=f"{side.value.lower()}-fill-fixture",
        is_terminal=False,
    )


def _fill_event(*, qty: float, trade_id: int = 1) -> dict:
    return {
        "_fill_qty": float(qty),
        "_fill_price": 70_000.0,
        "_fill_commission": 0.0,
        "_fill_commission_asset": "USDC",
        "_local_receive_ts_ns": 1_900_000_000_000_000_000,
        "T": 1_900_000_000_000,
        "t": int(trade_id),
    }


def test_rest_only_account_trade_runs_the_normal_fill_and_cooldown_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixed_now = 1_900_000_000.0
    monkeypatch.setattr(
        maker_engine_module,
        "time",
        SimpleNamespace(
            time=lambda: fixed_now,
            time_ns=lambda: int(fixed_now * 1e9),
        ),
    )
    engine, evaluator_calls, canceled_sides = _fill_callback_engine()
    engine.inventory.sync_from_exchange(
        0.0,
        0.0,
        snapshot_update_time_ms=1_000,
        order_cumulative_filled_qty={},
        included_trade_ids=(),
    )
    engine._reconciliation_lock = threading.Lock()
    engine._reconciliation_trade_identity_by_id = {}
    position = [
        {
            "symbol": "BTCUSDC",
            "positionSide": "BOTH",
            "positionAmt": "0.003",
            "entryPrice": "70000",
            "updateTime": 2_000,
        }
    ]
    engine.rest = SimpleNamespace(
        get_position_risk=Mock(return_value=position),
        get_account_trades=Mock(
            return_value=[
                {
                    "id": 91,
                    "orderId": 41,
                    "qty": "0.003",
                    "price": "70000",
                    "commission": "0",
                    "time": 2_000,
                    "buyer": True,
                }
            ]
        ),
    )
    engine.order_gateway = engine.rest
    engine.reconciliation_client = engine.rest
    engine.orders = OrderManager(on_fill=engine._on_fill)
    cid = engine.orders.create_order("BTCUSDC", Side.BUY, 70_000.0, 0.003)
    engine.orders.confirm_new(cid, 41)

    assert engine.sync_position(required=True) is True

    assert engine.inventory.snapshot.qty == pytest.approx(0.003)
    assert [call["baseline_duration_ms"] for call in evaluator_calls] == [255_000]
    assert engine._fill_cooldown_until["BUY"] == fixed_now + 2_048.0
    assert canceled_sides == ["BUY"]
    assert engine.orders.get_order(cid).is_terminal
    engine.rest.get_position_risk.assert_called_with(symbol="BTCUSDC")
    assert engine.rest.get_position_risk.call_count == 2


@pytest.mark.parametrize(
    ("action_id", "duration_ms", "expected_seconds", "expected_identity"),
    (
        ("FIXED_2048S", 2_048_000, 2_048.0, "BUY_E3:fixture"),
        ("CONTROL_85N", 255_000, 255.0, "B0"),
    ),
)
def test_real_fill_callback_preserves_total_e3_and_control_units(
    monkeypatch: pytest.MonkeyPatch,
    action_id: str,
    duration_ms: int,
    expected_seconds: float,
    expected_identity: str,
) -> None:
    fixed_now = 1_900_000_000.0
    monkeypatch.setattr(
        maker_engine_module,
        "time",
        SimpleNamespace(time=lambda: fixed_now, time_ns=lambda: int(fixed_now * 1e9)),
    )
    engine, evaluator_calls, canceled_sides = _fill_callback_engine(
        action_id=action_id,
        duration_ms=duration_ms,
    )
    engine._on_fill(_fill_order(), _fill_event(qty=0.003))

    assert [call["baseline_duration_ms"] for call in evaluator_calls] == [255_000]
    assert engine._fill_cooldown_until["BUY"] == fixed_now + expected_seconds
    assert engine._fill_cooldown_deadline_identity["BUY"] == expected_identity
    assert engine._fill_cooldown_natural_b0_until["BUY"] == fixed_now + 255.0
    assert canceled_sides == ["BUY"]


@pytest.mark.parametrize(
    "failure_source",
    ("trade_row", "outcome_row", "checkpoint_sync"),
)
def test_fill_evidence_failure_cancels_risk_before_becoming_fatal(
    monkeypatch: pytest.MonkeyPatch,
    failure_source: str,
) -> None:
    fixed_now = 1_900_000_000.0
    monkeypatch.setattr(
        maker_engine_module,
        "time",
        SimpleNamespace(time=lambda: fixed_now, time_ns=lambda: int(fixed_now * 1e9)),
    )
    engine, _evaluator_calls, canceled_sides = _fill_callback_engine()
    failure = RuntimeEvidenceQueueFull(f"simulated {failure_source} failure")
    if failure_source == "trade_row":
        engine.inventory._runtime_evidence_error = failure
    elif failure_source == "outcome_row":
        engine._log_order_outcome = Mock(side_effect=failure)
    else:
        engine._persist_fill_cooldown_checkpoint = Mock(side_effect=failure)

    with pytest.raises(RuntimeError, match="after risk cancellation"):
        engine._on_fill(_fill_order(), _fill_event(qty=0.001))

    assert engine.inventory.snapshot.qty == pytest.approx(0.001)
    assert engine._fill_cooldown_until["BUY"] == fixed_now + 2_048.0
    assert canceled_sides == ["BUY"]


def test_fill_risk_cancel_precedes_checkpoint_sync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixed_now = 1_900_000_000.0
    monkeypatch.setattr(
        maker_engine_module,
        "time",
        SimpleNamespace(time=lambda: fixed_now, time_ns=lambda: int(fixed_now * 1e9)),
    )
    engine, _evaluator_calls, _canceled_sides = _fill_callback_engine()
    observed: list[str] = []
    engine._cancel_cooldown_side_order = lambda _side: observed.append("cancel")
    engine._persist_fill_cooldown_checkpoint = lambda: observed.append("checkpoint")

    engine._on_fill(_fill_order(), _fill_event(qty=0.001))

    assert observed == ["cancel", "checkpoint"]


def test_max_inventory_cancel_precedes_checkpoint_sync_without_cooldown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixed_now = 1_900_000_000.0
    monkeypatch.setattr(
        maker_engine_module,
        "time",
        SimpleNamespace(time=lambda: fixed_now, time_ns=lambda: int(fixed_now * 1e9)),
    )
    engine, _evaluator_calls, _canceled_sides = _fill_callback_engine(
        policy_enabled=False
    )
    engine.cfg.strategy.fill_cooldown = 0.0
    engine.cfg.strategy.max_inventory = 0.001
    engine._bid_cid = "active-bid"
    observed: list[str] = []
    engine._cancel_tracked_order_before_replacement = (
        lambda side: observed.append(f"max_cancel:{side.value}") or True
    )
    engine._persist_fill_cooldown_checkpoint = lambda: observed.append("checkpoint")

    engine._on_fill(_fill_order(), _fill_event(qty=0.001))

    assert observed == ["max_cancel:BUY", "checkpoint"]


@pytest.mark.parametrize(
    (
        "initial_qty",
        "fill_qty",
        "expected_qty",
        "expected_evaluator_calls",
        "expected_deadline_s",
        "expected_identity",
    ),
    (
        (-0.003, 0.001, -0.002, 0, 0.0, "B0"),
        (-0.003, 0.003, 0.0, 0, 0.0, "B0"),
        (-0.003, 0.004, 0.001, 1, 2_048.0, "BUY_E3:fixture"),
        (-0.002, 0.004, 0.002, 1, 2_048.0, "BUY_E3:fixture"),
        (-0.001, 0.004, 0.003, 1, 2_048.0, "BUY_E3:fixture"),
    ),
    ids=(
        "reducing_short",
        "exact_close",
        "cross_zero_lower_abs",
        "cross_zero_equal_abs",
        "cross_zero_higher_abs",
    ),
)
def test_real_buy_fill_callback_uses_campaign_absolute_exposure_role(
    monkeypatch: pytest.MonkeyPatch,
    initial_qty: float,
    fill_qty: float,
    expected_qty: float,
    expected_evaluator_calls: int,
    expected_deadline_s: float,
    expected_identity: str,
) -> None:
    fixed_now = 1_900_000_000.0
    monkeypatch.setattr(
        maker_engine_module,
        "time",
        SimpleNamespace(time=lambda: fixed_now, time_ns=lambda: int(fixed_now * 1e9)),
    )
    engine, evaluator_calls, canceled_sides = _fill_callback_engine(
        initial_qty=initial_qty,
    )

    engine._on_fill(_fill_order(), _fill_event(qty=fill_qty))

    assert engine.inventory.snapshot.qty == pytest.approx(expected_qty)
    assert len(evaluator_calls) == expected_evaluator_calls
    assert engine._fill_cooldown_deadline_identity["BUY"] == expected_identity
    if expected_evaluator_calls:
        assert evaluator_calls[0]["baseline_duration_ms"] == int(
            round(85_000.0 * (fill_qty / engine.cfg.strategy.order_size))
        )
        assert engine._fill_cooldown_until["BUY"] == fixed_now + expected_deadline_s
        assert engine._fill_cooldown_natural_b0_until["BUY"] == fixed_now + (
            85.0 * (fill_qty / engine.cfg.strategy.order_size)
        )
        assert canceled_sides == ["BUY"]
    else:
        assert engine._fill_cooldown_until["BUY"] == 0.0
        assert engine._fill_cooldown_natural_b0_until["BUY"] == 0.0
        assert canceled_sides == []


def test_buy_e3_disabled_real_callback_is_exact_b0(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixed_now = 1_900_000_000.0
    monkeypatch.setattr(
        maker_engine_module,
        "time",
        SimpleNamespace(time=lambda: fixed_now, time_ns=lambda: int(fixed_now * 1e9)),
    )
    disabled, disabled_calls, disabled_cancels = _fill_callback_engine(
        policy_enabled=False,
    )
    control, control_calls, control_cancels = _fill_callback_engine(
        action_id="CONTROL_85N",
        duration_ms=255_000,
    )
    event = _fill_event(qty=0.003)

    disabled._on_fill(_fill_order(), dict(event))
    control._on_fill(_fill_order(), dict(event))

    assert disabled_calls == []
    assert [call["baseline_duration_ms"] for call in control_calls] == [255_000]
    assert disabled_cancels == control_cancels == ["BUY"]
    assert disabled.inventory.snapshot.qty == control.inventory.snapshot.qty
    assert disabled.fill_cooldown_state_snapshot(
        now_ms=int(fixed_now * 1_000.0)
    ) == control.fill_cooldown_state_snapshot(now_ms=int(fixed_now * 1_000.0))
    assert disabled._fill_cooldown_until["BUY"] == fixed_now + 255.0
    assert disabled._fill_cooldown_deadline_identity["BUY"] == "B0"


def test_sell_fill_callback_never_enters_buy_e3_evaluator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixed_now = 1_900_000_000.0
    monkeypatch.setattr(
        maker_engine_module,
        "time",
        SimpleNamespace(time=lambda: fixed_now, time_ns=lambda: int(fixed_now * 1e9)),
    )
    engine, _, canceled_sides = _fill_callback_engine()

    class BuyEvaluatorGuard:
        deadline_identity = "BUY_E3:must-not-run"

        def evaluate(self, **_kwargs):
            raise AssertionError("SELL callback entered BUY E3 evaluator")

    engine._buy_e3_cooldown_policy = BuyEvaluatorGuard()
    engine._on_fill(_fill_order(Side.SELL), _fill_event(qty=0.003))

    assert engine.inventory.snapshot.qty == pytest.approx(-0.003)
    assert engine._fill_cooldown_until["SELL"] == fixed_now + 255.0
    assert engine._fill_cooldown_deadline_identity["SELL"] == "B0"
    assert canceled_sides == ["SELL"]


@pytest.mark.parametrize(
    ("initial_qty", "fill_qty", "expected_identity", "expected_remaining_ms"),
    (
        (0.0, 0.003, "BUY_E3:fixture", 2_048_000),
        (0.0, 0.0005, "BUY_E3:fixture", 2_048_000),
        (-0.003, 0.001, "B0", 0),
    ),
    ids=("e3_consecutive", "e3_partial", "reducing_fill_units_only"),
)
def test_every_fill_state_transition_is_checkpointed(
    monkeypatch: pytest.MonkeyPatch,
    initial_qty: float,
    fill_qty: float,
    expected_identity: str,
    expected_remaining_ms: int,
) -> None:
    fixed_now = 1_900_000_000.0
    monkeypatch.setattr(
        maker_engine_module,
        "time",
        SimpleNamespace(time=lambda: fixed_now, time_ns=lambda: int(fixed_now * 1e9)),
    )
    engine, _, _ = _fill_callback_engine(initial_qty=initial_qty)
    persisted = []
    engine._persist_fill_cooldown_checkpoint = lambda: persisted.append(
        engine.fill_cooldown_state_snapshot(now_ms=int(fixed_now * 1_000.0))
    )

    engine._on_fill(_fill_order(), _fill_event(qty=fill_qty))

    assert len(persisted) == 1
    assert persisted[0]["consec_buy"] == pytest.approx(fill_qty / 0.001)
    assert persisted[0]["buy_deadline_identity"] == expected_identity
    assert persisted[0]["buy_remaining_ms"] == expected_remaining_ms
