import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from features.feature_engineer import (
    _contiguous_warmup_paths,
    _load_label_quote_params,
    _quote_half_spread,
    add_sample_weights,
    chronological_good_day_split,
)


def test_warmup_uses_only_immediately_contiguous_prior_days() -> None:
    paths = {
        "2026-07-01": Path("day1"),
        "2026-07-03": Path("day3"),
        "2026-07-04": Path("day4"),
        "2026-07-05": Path("day5"),
    }
    assert _contiguous_warmup_paths(
        "2026-07-05", paths, warmup_days=7
    ) == [Path("day3"), Path("day4"), Path("day5")]


def test_warmup_never_reads_future_day() -> None:
    paths = {
        "2026-07-04": Path("day4"),
        "2026-07-05": Path("day5"),
        "2026-07-06": Path("day6"),
    }
    assert _contiguous_warmup_paths(
        "2026-07-05", paths, warmup_days=1
    ) == [Path("day4"), Path("day5")]


def test_chronological_split_keeps_embargo_and_future_out_of_training() -> None:
    tags = [f"2026-01-{day:02d}" for day in range(1, 32)]
    split = chronological_good_day_split(
        tags,
        validation_days=6,
        test_days=6,
        embargo_good_days=1,
    )
    assert len(split["train"]) == 17
    assert len(split["embargo_1"]) == 1
    assert len(split["validation"]) == 6
    assert len(split["embargo_2"]) == 1
    assert len(split["test"]) == 6
    assert max(split["train"]) < min(split["validation"])
    assert max(split["validation"]) < min(split["test"])


def test_sample_weights_use_explicit_lambda_and_reference_date() -> None:
    index = pd.to_datetime(
        ["2026-05-01T00:00:00Z", "2026-06-01T00:00:00Z"],
        utc=True,
    )
    low_decay = add_sample_weights(
        pd.DataFrame(index=index),
        reference_date="2026-06-01",
        lam=0.1,
    )
    high_decay = add_sample_weights(
        pd.DataFrame(index=index),
        reference_date="2026-06-01",
        lam=1.0,
    )

    assert low_decay["sample_weight"].iloc[-1] == pytest.approx(1.0)
    assert high_decay["sample_weight"].iloc[-1] == pytest.approx(1.0)
    assert high_decay["sample_weight"].iloc[0] < low_decay["sample_weight"].iloc[0]


def _write_feature_config(tmp_path: Path, artifact: dict) -> tuple[Path, Path]:
    model_dir = tmp_path / "model"
    model_dir.mkdir()
    artifact_path = model_dir / "fill_prob_params.json"
    artifact_path.write_text(json.dumps(artifact), encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "symbol: BTCUSDC",
                "tick_size: 0.1",
                "strategy: {}",
                "regime: {}",
                "fees: {maker: 0.0}",
                f"ml: {{model_dir: {model_dir}}}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return config_path, artifact_path


def test_label_quote_params_use_explicit_empirical_p3_artifact(tmp_path: Path) -> None:
    config, artifact = _write_feature_config(
        tmp_path,
        {
            "schema_version": "narrowgate_p3_touch_calibration.v2",
            "model_type": "empirical_survival",
            "delta_grid": [0.1, 1.0, 2.0, 3.0],
            "probability_grid": [1.0, 0.8, 0.4, 0.1],
            "metadata": {
                "event_type": "touch",
                "horizon_s": 10.0,
                "distance_unit": "USDC_per_BTC",
            },
        },
    )

    params = _load_label_quote_params("BTCUSDC", config)

    assert params["fill_probability_model_path"] == str(artifact.resolve())
    assert params["fill_probability_sha256"] == hashlib.sha256(
        artifact.read_bytes()
    ).hexdigest()
    assert params["fill_probability_schema_version"] == "narrowgate_p3_touch_calibration.v2"
    assert params["fill_probability_model_type"] == "empirical_survival"
    assert params["p3_delta_star"] > 0.0
    assert params["p3_kappa_eff"] > 0.0


def test_label_quote_params_reject_legacy_su_artifact(tmp_path: Path) -> None:
    config, _ = _write_feature_config(
        tmp_path,
        {"xi": 0.0, "lam": 1.0, "gamma": 0.0, "delta0": 1.0},
    )

    with pytest.raises(ValueError, match="empirical causal P3 v2"):
        _load_label_quote_params("BTCUSDC", config)


def test_quote_labels_use_empirical_kappa_horizon_and_dynamic_cap() -> None:
    sigma_sq = np.asarray([100.0, 400.0])
    close = np.asarray([10_000.0, 10_000.0])
    params = {
        "gamma": 0.05,
        "kappa": 999.0,
        "p3_kappa_eff": 0.1,
        "kappa_ratio": 1.0,
        "quote_horizon_s": 2.0,
        "liq_baseline": 0.0,
        "vol_baseline": 10.0,
        "vol_power": 0.0,
        "gamma_scale_min": 1.0,
        "gamma_scale_max": 1.0,
        "gamma_liq_scale_min": 1.0,
        "gamma_liq_scale_max": 1.0,
        "p3_delta_star": 0.0,
        "tick_size": 0.1,
        "maker_fee": 0.0,
        "max_spread_bps": 99.0,
        "dynamic_cap_enabled": True,
        "dynamic_cap_base_bps": 10.0,
        "dynamic_cap_alpha": 0.5,
        "dynamic_cap_min_mult": 1.0,
        "dynamic_cap_max_mult": 2.0,
        "dynamic_cap_var_baseline": 100.0,
    }

    result = _quote_half_spread(pd.DataFrame(index=range(2)), close, sigma_sq, params)

    uncapped = 0.05 * sigma_sq * 2.0 + (2.0 / 0.05) * np.log1p(0.05 / 0.1)
    pair_cap = np.asarray([10.0, 20.0])
    np.testing.assert_allclose(result, 0.5 * np.minimum(uncapped, pair_cap))
