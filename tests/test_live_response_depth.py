from dataclasses import asdict

import pytest
import yaml

from live import config
from strategy import quote_core as qc


def configured(levels=None, *, asym=False):
    return config._parse({
        "strategy": {"trace_book_imb_levels": levels},
        "websocket": {"depth_levels": 20},
        "ml": {"model_dir": "", "feature_protocol": "execution_v1", "ret_skew": 0},
        "depth_execution": {"imbalance_asym": {
            "enabled": asym, "levels": 15, "strength": 0.2,
        }},
    })


@pytest.mark.parametrize("asym", [False, True])
def test_explicit_response_depth_changes_only_trace_context(asym):
    before, after = configured(asym=asym), configured(10, asym=asym)
    before_core = asdict(qc.quote_core_config_from_live_config(before))
    after_core = asdict(qc.quote_core_config_from_live_config(after))
    assert before_core["trace_book_imb_levels"] == (15 if asym else 20)
    assert after_core["trace_book_imb_levels"] == 10
    after_core["trace_book_imb_levels"] = before_core["trace_book_imb_levels"]
    assert after_core == before_core
    for section in ("websocket", "ml", "depth_execution", "risk", "fees"):
        assert asdict(getattr(after, section)) == asdict(getattr(before, section))
    assert config.to_backtest_params(after)["trace_book_imb_levels"] == 10
    old_params = config.to_backtest_params(before)
    assert old_params.get("trace_book_imb_levels") == (15 if asym else None)


@pytest.mark.parametrize("value", [0, -1, True, False, 10.0, "10"])
def test_invalid_response_depth_rejected(value):
    with pytest.raises(ValueError, match="strategy.trace_book_imb_levels"):
        config._validate_config(configured(value), validate_live_storage=False)


def test_yaml_loader_keeps_model_and_feed_depth(tmp_path):
    path = tmp_path / "candidate.yaml"
    path.write_text(yaml.safe_dump({
        "strategy": {"trace_book_imb_levels": 10},
        "websocket": {"depth_levels": 20},
        "ml": {"model_dir": "", "feature_protocol": "execution_v1"},
    }))
    cfg = config._load_config_candidate(path)
    assert cfg.strategy.trace_book_imb_levels == 10
    assert cfg.websocket.depth_levels == 20
    assert cfg.ml.feature_protocol == "execution_v1"


def test_response_depth_does_not_truncate_supplied_book():
    bids = [(100.0 - i * 0.1, 1.0) for i in range(20)]
    asks = [(100.1 + i * 0.1, 1.0) for i in range(20)]
    full = qc.DepthSnapshot(bids=bids, asks=asks)
    changed = qc.DepthSnapshot(
        bids=bids[:10] + [(p, 10.0) for p, _ in bids[10:]], asks=asks,
    )
    core = qc.quote_core_config_from_live_config(configured(10))
    assert qc._depth_imbalance(full, core.trace_book_imb_levels)[0] == 0
    assert qc._depth_imbalance(changed, core.trace_book_imb_levels)[0] == 0
    assert qc._depth_imbalance(changed, 20)[0] > 0
    assert len(changed.bids) == len(changed.asks) == 20
