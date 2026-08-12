from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from research.families.f02_empirical_p3_touch.audit import (
    p3_reach_time_cache as label_node,
)
from research.families.f02_empirical_p3_touch.audit import (
    p3_reach_time_conditioned_hazard as hazard_node,
)
from research.families.f02_empirical_p3_touch.audit import (
    p3_reach_time_context as context_node,
)
from research.families.f02_empirical_p3_touch.audit import (
    p3_reach_time_hazard_training as training,
)
from research.families.f02_empirical_p3_touch.audit.p3_reach_time_hazard_training import (
    CacheCatalog,
    CacheEntry,
    FoldContract,
    LoadedDay,
    _dispose_store,
    _run_fold,
    _validate_cache_data_manifest,
    accumulate_empirical_cdf_counts,
    enforce_expansion_cap,
    evaluate_loaded_day,
    expansion_upper_bound_rows,
    first_reach_upper_endpoints,
    load_frozen_training_contract,
    materialize_risk_row_store,
    paired_day_bootstrap,
    sha256_rank_origin_indices,
)
from research.families.f02_empirical_p3_touch.audit.p3_reach_time_surface import (
    ReachTimeLabelSurface,
)
from research.governance.paths import resolve_research_path
from scripts import build_p3_reach_time_caches as cache_builder
from scripts.run_p3_reach_time_hazard_training import _parser


def _context(day: str, rows: int = 16) -> pd.DataFrame:
    start = int(pd.Timestamp(day, tz="UTC").timestamp() * 1_000)
    origin = start + 60_000 + np.arange(rows, dtype=np.int64) * 10_000
    bid = 600_000 + np.arange(rows, dtype=np.int64)
    ask = bid + 2
    fast_variance = np.linspace(1.0, 2.0, rows)
    slow_variance = np.linspace(2.0, 3.0, rows)
    return pd.DataFrame(
        {
            "day": [day] * rows,
            "source_profile": ["native"] * rows,
            "origin_ts_ms": origin,
            "feature_ready_ts_ms": origin - 10,
            "best_bid_ticks": bid,
            "best_ask_ticks": ask,
            "mid_usdc_per_btc": 0.05 * (bid + ask),
            "spread_ticks": ask - bid,
            "spread_bps": np.full(rows, 0.03),
            "fast_variance_usdc2_per_s": fast_variance,
            "slow_variance_usdc2_per_s": slow_variance,
            "fast_sigma_usdc_per_sqrt_s": np.sqrt(fast_variance),
            "slow_sigma_usdc_per_sqrt_s": np.sqrt(slow_variance),
            "volatility_ratio": np.sqrt(fast_variance / slow_variance),
            "book_age_ms": np.full(rows, 10.0),
        },
        columns=context_node.CONTEXT_COLUMNS,
    )


def _surface(rows: int = 16) -> ReachTimeLabelSurface:
    time = hazard_node.DEFAULT_GRID_SPEC.time_upper_ms()
    buy = np.empty((rows, len(time)), dtype=np.int16)
    sell = np.empty_like(buy)
    for origin in range(rows):
        buy[origin] = np.minimum((np.arange(len(time)) + origin) // 7, 60)
        sell[origin] = np.minimum((np.arange(len(time)) + 2 * origin) // 9, 55)
    return ReachTimeLabelSurface(
        time_upper_ms=time,
        buy_cumulative_reach_ticks=buy,
        sell_cumulative_reach_ticks=sell,
    )


def _load_structural_contract(spec_path: Path = training.DEFAULT_SPEC_PATH):
    original = training._require_file_identity

    def verify(identity, *, label):
        if label.startswith("F02 implementation "):
            path = resolve_research_path(str(identity.get("path", "")))
            assert path.is_file()
            assert len(str(identity.get("sha256", ""))) == 64
            return path
        return original(identity, label=label)

    with patch.object(training, "_require_file_identity", side_effect=verify):
        return load_frozen_training_contract(spec_path)


def _mini_contract():
    contract = _load_structural_contract()
    return replace(
        contract,
        train_origins_per_day=8,
        calibration_origins_per_day=8,
        evaluation_origins_per_day=8,
        distance_ticks=tuple(range(5, 21)),
        distance_queries_per_origin=4,
    )


def _risk_rows(day: str, *, side: str, seed_offset: int):
    frame = _context(day, rows=24)
    surface = _surface(rows=24)
    distance = np.arange(5, 21, dtype=np.int64)
    endpoints = first_reach_upper_endpoints(
        surface.buy_cumulative_reach_ticks
        if side == "BUY"
        else surface.sell_cumulative_reach_ticks,
        distance_ticks=distance,
        time_upper_ms=surface.time_upper_ms,
    )
    queries = hazard_node.sample_distance_queries(
        origin_ids=frame["origin_ts_ms"].to_numpy(dtype=np.int64),
        distance_ticks=distance,
        samples_per_origin=8,
        side=side,
        seed=20260804 + seed_offset,
    )
    names = (
        hazard_node.FAST_SIGMA_FEATURE,
        hazard_node.SLOW_SIGMA_FEATURE,
        "spread_ticks",
        "spread_bps",
        "volatility_ratio",
        "book_age_ms",
    )
    return hazard_node.build_hazard_risk_rows(
        first_reach_upper_ms=endpoints,
        queries=queries,
        context_features={name: frame[name].to_numpy(dtype=np.float64) for name in names},
        context_feature_names=names,
        tick_size=0.1,
    )


def test_actual_frozen_spec_is_strictly_bound() -> None:
    with pytest.raises(ValueError, match="implementation .* hash mismatch"):
        load_frozen_training_contract()
    contract = _load_structural_contract()
    assert len(contract.folds) == 4
    assert len(contract.fit_days) == 156
    assert len(contract.historical_diagnostic_days) == 44
    assert len(contract.overlap_days) == 48
    assert contract.lightgbm_parameters["seed"] == 20260804


def test_spec_tamper_fails_before_data_read(tmp_path: Path) -> None:
    contract = _load_structural_contract()
    payload = json.loads(contract.spec_path.read_text(encoding="utf-8"))
    payload["model_contract"]["num_boost_round"] = 181
    tampered = tmp_path / "tampered_spec.json"
    tampered.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="canonical spec hash"):
        _load_structural_contract(tampered)


def test_origin_sampling_is_deterministic_and_outcome_blind() -> None:
    origins = np.arange(100, 140, dtype=np.int64)
    first = sha256_rank_origin_indices(
        origins,
        count=8,
        seed=7,
        day="2026-01-01",
        purpose="train",
    )
    second = sha256_rank_origin_indices(
        origins,
        count=8,
        seed=7,
        day="2026-01-01",
        purpose="train",
    )
    assert np.array_equal(first, second)
    assert len(np.unique(first)) == 8
    assert not np.array_equal(
        first,
        sha256_rank_origin_indices(
            origins,
            count=8,
            seed=7,
            day="2026-01-01",
            purpose="evaluation",
        ),
    )


def test_first_passage_conversion_and_empirical_counts() -> None:
    cumulative = np.array(
        [
            [0, 5, 5, 9],
            [1, 2, 3, 4],
        ],
        dtype=np.int16,
    )
    distances = np.array([2, 5, 8], dtype=np.int64)
    times = np.array([100, 200, 300, 400], dtype=np.int32)
    endpoints = first_reach_upper_endpoints(
        cumulative, distance_ticks=distances, time_upper_ms=times
    )
    assert endpoints.tolist() == [[200, 200, 400], [200, -1, -1]]

    counts = np.zeros((3, 4), dtype=np.int64)
    accumulate_empirical_cdf_counts(counts, cumulative, distance_ticks=distances)
    assert counts.tolist() == [[0, 2, 2, 2], [0, 1, 1, 1], [0, 0, 0, 1]]


def test_expansion_cap_uses_outcome_blind_worst_case() -> None:
    rows = expansion_upper_bound_rows(
        day_count=144,
        origins_per_day=64,
        distance_queries_per_origin=8,
        time_bins=300,
    )
    assert rows == 22_118_400
    enforce_expansion_cap(rows, 25_000_000)
    with pytest.raises(RuntimeError, match="exceeds safety cap"):
        enforce_expansion_cap(rows, 20_000_000)


def test_paired_day_bootstrap_is_deterministic_and_positive() -> None:
    values = np.array([0.01, 0.02, -0.001, 0.03], dtype=np.float64)
    first = paired_day_bootstrap(values, seed=11, draws=2_000)
    second = paired_day_bootstrap(values, seed=11, draws=2_000)
    assert first == second
    assert first["mean_improvement"] > 0.0
    assert first["daily_positive_rate"] == 0.75


def test_cache_manifest_validation_detects_payload_tamper(tmp_path: Path) -> None:
    frame = _context("2026-01-01", rows=8)
    path = tmp_path / "context.parquet"
    identity = {"node": "synthetic"}
    context_node.write_context_cache(
        path,
        frame=frame,
        cache_key="a" * 64,
        identity=identity,
    )
    _validate_cache_data_manifest(
        path,
        expected_key="a" * 64,
        expected_schema=context_node.SCHEMA_VERSION,
        expected_rows=8,
        expected_identity=identity,
    )
    path.write_bytes(path.read_bytes() + b"tamper")
    with pytest.raises(ValueError, match="payload hash mismatch"):
        _validate_cache_data_manifest(
            path,
            expected_key="a" * 64,
            expected_schema=context_node.SCHEMA_VERSION,
            expected_rows=8,
            expected_identity=identity,
        )


def test_cache_catalog_binds_builder_and_node_code_hashes(tmp_path: Path) -> None:
    day = "2026-01-01"
    source_record_sha = "1" * 64
    bbo_sha = "2" * 64
    trade_sha = "3" * 64
    parameters = dict(training.EXPECTED_CACHE_PARAMETERS)
    module_hashes = {
        "orchestrator": training.sha256_file(Path(cache_builder.__file__).resolve()),
        "context_node": training.sha256_file(Path(context_node.__file__).resolve()),
        "label_cache_node": training.sha256_file(Path(label_node.__file__).resolve()),
        "label_surface_node": training.sha256_file(
            training.ROOT
            / "research/families/f02_empirical_p3_touch/audit/p3_reach_time_surface.py"
        ),
        "source_manifest_node": training.sha256_file(
            training.ROOT / "research/families/f02_empirical_p3_touch/audit/"
            "p3_reach_time_source_manifest.py"
        ),
    }
    common = {
        "day": day,
        "source_profile": "native",
        "panel": "fit",
        "weighted": True,
        "selection_mode": "weighted_primary",
        "source_record_sha256": source_record_sha,
        "parameters": parameters,
        "economic_outcomes_read": False,
        "queue_inputs_read": False,
        "order_lifecycle_inputs_read": False,
    }
    context_identity = {
        **common,
        "node": cache_builder.CONTEXT_CACHE_NODE,
        "bbo_sha256": bbo_sha,
        "context_code_identity_sha256": hazard_node.canonical_sha256(
            {
                "schema_version": context_node.SCHEMA_VERSION,
                "context_module_sha256": module_hashes["context_node"],
                "source_record_sha256": source_record_sha,
                "parameters": parameters,
            }
        ),
    }
    context_path = tmp_path / "context.parquet"
    context_node.write_context_cache(
        context_path,
        frame=_context(day, rows=16),
        cache_key="4" * 64,
        identity=context_identity,
    )
    label_identity = {
        **common,
        "node": cache_builder.LABEL_CACHE_NODE,
        "context_cache_key": "4" * 64,
        "official_aggtrades_sha256": trade_sha,
        "label_code_identity_sha256": hazard_node.canonical_sha256(
            {
                "schema_version": label_node.SCHEMA_VERSION,
                "label_cache_module_sha256": module_hashes["label_cache_node"],
                "label_surface_module_sha256": module_hashes["label_surface_node"],
                "source_record_sha256": source_record_sha,
                "parameters": parameters,
            }
        ),
    }
    label_path = tmp_path / "labels.npz"
    label_node.write_label_cache(
        label_path,
        origins_ms=_context(day, rows=16)["origin_ts_ms"].to_numpy(dtype=np.int64),
        surface=_surface(rows=16),
        cache_key="5" * 64,
        identity=label_identity,
    )
    source_path = tmp_path / "source_manifest.json"
    source_path.write_text("{}\n", encoding="utf-8")
    canonical_source = "6" * 64
    contract = replace(
        _mini_contract(),
        source_manifest_path=source_path,
        source_manifest_file_sha256=training.sha256_file(source_path),
        source_manifest_canonical_sha256=canonical_source,
        source_manifest={
            "weighted_day_records": [
                {
                    "date": day,
                    "primary_source": "native",
                    "panel": "fit",
                    "source_record_sha256": source_record_sha,
                }
            ],
            "overlap_records": [],
            "provider_records": [],
            "native_records": [
                {
                    "date": day,
                    "source": "native",
                    "record_sha256": source_record_sha,
                    "files": {
                        "bbo": {"sha256": bbo_sha},
                        "official_aggtrades": {"sha256": trade_sha},
                    },
                }
            ],
        },
    )
    summary = {
        "schema_version": cache_builder.SCHEMA_VERSION,
        "created_at_utc": "2026-08-04T00:00:00+00:00",
        "source_manifest": {
            "path": str(source_path),
            "sha256": training.sha256_file(source_path),
            "canonical_manifest_sha256": canonical_source,
        },
        "cache_root": str(tmp_path),
        "summary_path": str(tmp_path / "summary.json"),
        "dry_run": False,
        "selection": {},
        "parameters": parameters,
        "module_sha256": module_hashes,
        "storage_preflight": {"passed": True},
        "job_count": 1,
        "counts": {},
        "jobs": [
            {
                "day": day,
                "source": "native",
                "panel": "fit",
                "source_record_sha256": source_record_sha,
                "weighted": True,
                "selection_mode": "weighted_primary",
                "context_cache_key": "4" * 64,
                "label_cache_key": "5" * 64,
                "context_path": str(context_path),
                "label_path": str(label_path),
                "context_status": "built",
                "label_status": "built",
                "context_rows": 16,
                "label_rows": 16,
            }
        ],
        "economic_outcomes_read": False,
        "queue_inputs_read": False,
        "order_lifecycle_inputs_read": False,
        "cache_is_reproducible_and_disposable": True,
    }
    summary["canonical_summary_sha256"] = hazard_node.canonical_sha256(summary)
    summary_path = tmp_path / "summary.json"
    summary_path.write_text(json.dumps(summary), encoding="utf-8")
    catalog = training.load_cache_catalog([summary_path], contract=contract)
    assert catalog.require(day, "native").context_rows == 16

    summary["module_sha256"]["context_node"] = "0" * 64
    summary.pop("canonical_summary_sha256")
    summary["canonical_summary_sha256"] = hazard_node.canonical_sha256(summary)
    summary_path.write_text(json.dumps(summary), encoding="utf-8")
    with pytest.raises(ValueError, match="module identity drifted"):
        training.load_cache_catalog([summary_path], contract=contract)


def test_memmap_materialization_loads_days_incrementally(tmp_path: Path) -> None:
    day = "2026-01-01"
    frame = _context(day, rows=16)
    surface = _surface(rows=16)
    context_path = tmp_path / "context.parquet"
    label_path = tmp_path / "labels.npz"
    context_node.write_context_cache(
        context_path,
        frame=frame,
        cache_key="b" * 64,
        identity={"source_record_sha256": "c" * 64},
    )
    label_node.write_label_cache(
        label_path,
        origins_ms=frame["origin_ts_ms"].to_numpy(dtype=np.int64),
        surface=surface,
        cache_key="d" * 64,
        identity={"source_record_sha256": "c" * 64},
    )
    entry = CacheEntry(
        day=day,
        source="native",
        panel="fit",
        weighted=True,
        source_record_sha256="c" * 64,
        context_cache_key="b" * 64,
        label_cache_key="d" * 64,
        context_path=context_path,
        label_path=label_path,
        context_rows=16,
        label_rows=16,
        summary_sha256="e" * 64,
    )
    contract = replace(
        _mini_contract(),
        source_manifest={
            "weighted_day_records": [{"date": day, "primary_source": "native", "panel": "fit"}]
        },
    )
    store = materialize_risk_row_store(
        days=[day],
        side="BUY",
        origin_count=8,
        purpose="train",
        contract=contract,
        catalog=CacheCatalog(entries={(day, "native"): entry}, summary_identities=()),
        scratch_root=tmp_path / "scratch",
        maximum_expanded_rows=20_000,
        collect_empirical_baseline=True,
    )
    try:
        assert isinstance(store.matrix, np.memmap)
        assert 0 < store.row_count <= 8 * 4 * 300
        assert store.empirical_cdf is not None
        assert store.empirical_cdf.shape == (16, 300)
    finally:
        _dispose_store(store)


def test_synthetic_side_model_evaluates_full_cdf_without_economics() -> None:
    train = _risk_rows("2026-01-01", side="BUY", seed_offset=0)
    calibration = _risk_rows("2026-01-02", side="BUY", seed_offset=1)
    model = hazard_node.fit_side_hazard_model(
        train,
        calibration,
        lightgbm_parameters={
            "num_threads": 1,
            "num_leaves": 7,
            "min_data_in_leaf": 10,
            "learning_rate": 0.1,
            "monotone_constraints_method": "advanced",
        },
        num_boost_round=8,
    )
    frame = _context("2026-01-03", rows=16)
    surface = _surface(rows=16)
    loaded = LoadedDay(
        entry=CacheEntry(
            day="2026-01-03",
            source="native",
            panel="test",
            weighted=True,
            source_record_sha256="f" * 64,
            context_cache_key="a" * 64,
            label_cache_key="b" * 64,
            context_path=Path("/unused/context"),
            label_path=Path("/unused/label"),
            context_rows=16,
            label_rows=16,
            summary_sha256="c" * 64,
        ),
        context=frame,
        surface=surface,
    )
    counts = np.zeros((16, 300), dtype=np.int64)
    accumulate_empirical_cdf_counts(
        counts,
        surface.buy_cumulative_reach_ticks,
        distance_ticks=np.arange(5, 21),
    )
    result = evaluate_loaded_day(
        loaded,
        model=model,
        empirical_cdf=counts / 16.0,
        side="BUY",
        contract=_mini_contract(),
    )
    assert np.isfinite(result["model_integrated_brier"])
    assert np.isfinite(result["train_empirical_integrated_brier"])
    assert result["query_count"] == 32
    assert result["invariants"]["time_cdf_monotonicity_violations"] == 0
    assert result["invariants"]["distance_cdf_monotonicity_violations"] == 0
    assert result["invariants"]["maximum_probability_mass_error"] < 1e-10


def test_fold_models_and_results_publish_as_one_atomic_directory(tmp_path: Path) -> None:
    days = [f"2026-01-{day:02d}" for day in range(1, 7)]
    entries: dict[tuple[str, str], CacheEntry] = {}
    weighted: list[dict[str, str]] = []
    for index, day in enumerate(days):
        day_root = tmp_path / "cache" / day
        context_path = day_root / "context.parquet"
        label_path = day_root / "labels.npz"
        source_record_sha = f"{index + 1:064x}"
        frame = _context(day, rows=16)
        context_node.write_context_cache(
            context_path,
            frame=frame,
            cache_key=f"{index + 20:064x}",
            identity={"source_record_sha256": source_record_sha},
        )
        label_node.write_label_cache(
            label_path,
            origins_ms=frame["origin_ts_ms"].to_numpy(dtype=np.int64),
            surface=_surface(rows=16),
            cache_key=f"{index + 40:064x}",
            identity={"source_record_sha256": source_record_sha},
        )
        entries[(day, "native")] = CacheEntry(
            day=day,
            source="native",
            panel="fit",
            weighted=True,
            source_record_sha256=source_record_sha,
            context_cache_key=f"{index + 20:064x}",
            label_cache_key=f"{index + 40:064x}",
            context_path=context_path,
            label_path=label_path,
            context_rows=16,
            label_rows=16,
            summary_sha256="a" * 64,
        )
        weighted.append({"date": day, "primary_source": "native", "panel": "fit"})
    contract = replace(
        _mini_contract(),
        source_manifest={"weighted_day_records": weighted},
        lightgbm_parameters={
            "num_threads": 1,
            "num_leaves": 7,
            "min_data_in_leaf": 10,
            "learning_rate": 0.1,
            "monotone_constraints_method": "advanced",
            "seed": 20260804,
        },
        num_boost_round=6,
    )
    output = tmp_path / "output"
    output.mkdir()
    result = _run_fold(
        fold=FoldContract(
            fold=1,
            train_days=tuple(days[:2]),
            calibration_days=tuple(days[2:4]),
            test_days=tuple(days[4:]),
        ),
        output_root=output,
        contract=contract,
        catalog=CacheCatalog(entries=entries, summary_identities=()),
        scratch_root=tmp_path / "scratch",
        maximum_expanded_rows=50_000,
        progress_stream=None,
    )
    fold_path = output / "fold_01"
    assert result["fold"] == 1
    assert (fold_path / "fold_result.json").is_file()
    for side in hazard_node.SIDES:
        assert (fold_path / side / "model" / "metadata.json").is_file()
        assert (fold_path / side / "model" / "model.txt").is_file()
        assert (fold_path / side / "binding.json").is_file()
        assert (fold_path / side / "train_empirical_side_distance_time_cdf.npz").is_file()
    assert not list(output.glob(".fold_01-*"))


def test_cli_exposes_only_resource_cap_not_model_tuning() -> None:
    args = _parser().parse_args(["--cache-summary", "/tmp/cache.json"])
    assert args.max_expanded_rows == 25_000_000
    assert not hasattr(args, "learning_rate")
    assert not hasattr(args, "num_boost_round")
