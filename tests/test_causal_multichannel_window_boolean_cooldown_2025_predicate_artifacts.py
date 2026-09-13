from __future__ import annotations

import csv
import json
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_2025_predicate_artifacts as materializer,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_source_manifest as source_manifest,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    BASE_WINDOW_WIDTH_NS,
    CHANNELS_BY_BLOCK,
    CausalWindowObservation,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_predicates import (
    PredicateArtifact,
)


def _day_start_ns(day: str) -> int:
    parsed = datetime.combine(
        date.fromisoformat(day),
        datetime.min.time(),
        tzinfo=timezone.utc,
    )
    return int(parsed.timestamp() * 1_000_000_000)


def _write_csv(path: Path, header: tuple[str, ...], rows: list[tuple[object, ...]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)


def _write_market_day(root: Path, day: str) -> dict[str, dict[str, object]]:
    start_ms = _day_start_ns(day) // 1_000_000
    timestamps = np.asarray([start_ms + 100, start_ms + 200, start_ms + 400])
    bbo_path = root / "bbo" / f"BTCUSDC-bbo-{day}.parquet"
    l2_path = root / "l2" / f"BTCUSDC-l2-{day}.parquet"
    bbo_path.parent.mkdir(parents=True, exist_ok=True)
    l2_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "timestamp": timestamps,
            "best_bid": [100.0, 100.1, 100.2],
            "best_bid_qty": [1.0, 1.1, 1.2],
            "best_ask": [100.1, 100.2, 100.3],
            "best_ask_qty": [1.2, 1.1, 1.0],
        }
    ).to_parquet(bbo_path, index=False)
    l2_columns: dict[str, object] = {"timestamp": timestamps}
    for level in range(1, 21):
        l2_columns[f"bid_px_{level}"] = 100.0 - 0.1 * (level - 1)
        l2_columns[f"bid_qty_{level}"] = 0.1 + 0.01 * level
        l2_columns[f"ask_px_{level}"] = 100.1 + 0.1 * (level - 1)
        l2_columns[f"ask_qty_{level}"] = 0.2 + 0.01 * level
    pd.DataFrame(l2_columns).to_parquet(l2_path, index=False)

    individual_path = root / "trades" / f"BTCUSDC-trades-{day}.csv"
    agg_path = root / "agg" / f"BTCUSDC-aggTrades-{day}.csv"
    _write_csv(
        individual_path,
        ("id", "price", "qty", "quote_qty", "time", "is_buyer_maker"),
        [
            (1, 100.0, 0.01, 1.0, start_ms + 100, True),
            (2, 100.1, 0.02, 2.002, start_ms + 250, False),
        ],
    )
    _write_csv(
        agg_path,
        (
            "agg_trade_id",
            "price",
            "quantity",
            "first_trade_id",
            "last_trade_id",
            "transact_time",
            "is_buyer_maker",
        ),
        [(1, 100.0, 0.01, 1, 1, start_ms + 100, True)],
    )

    def identity(path: Path, authority: str, clock: str) -> dict[str, object]:
        return {
            "path": str(path),
            "sha256": materializer.sha256_file(path),
            "size_bytes": path.stat().st_size,
            "authority": authority,
            "source_clock": clock,
        }

    return {
        "bbo": identity(
            bbo_path,
            source_manifest.SOURCE_AUTHORITY,
            "provider_local_receive_time_right_boundary_100ms",
        ),
        "l2": identity(
            l2_path,
            source_manifest.SOURCE_AUTHORITY,
            "provider_local_receive_time_right_boundary_100ms",
        ),
        "individual_trades": identity(
            individual_path,
            source_manifest.TRADE_AUTHORITY,
            "exchange_trade_time",
        ),
        "aggtrades": identity(
            agg_path,
            source_manifest.TRADE_AUTHORITY,
            "exchange_trade_time",
        ),
    }


def _source_fixture(tmp_path: Path) -> Path:
    warmup_day = "2025-08-01"
    target_day = "2025-08-02"
    rows = []
    for day in (warmup_day, target_day):
        rows.append({"day": day, **_write_market_day(tmp_path / "market", day)})
    manifest = {
        "identity": source_manifest.IDENTITY,
        "schema_version": source_manifest.SCHEMA_VERSION,
        "symbol": "BTCUSDC",
        "purpose": "outcome_blind_feature_scale_missingness_and_predicate_support",
        "target_days": [target_day],
        "target_day_count": 1,
        "unique_source_day_count": 2,
        "target_windows": [
            {
                "target_day": target_day,
                "warmup_day": warmup_day,
                "warmup_duration_hours": 24,
                "target_source_authority": source_manifest.SOURCE_AUTHORITY,
            }
        ],
        "source_days": rows,
        "clock_contract": {
            "bbo_l2_clock": "provider_local_receive_time_right_boundary_100ms",
            "trade_clock": "binance_exchange_trade_time",
            "exact_historical_receive_time_present": False,
            "feature_ready_clock": "provider_local_receive_bucket_right_boundary",
            "book_trade_joint_visibility_authority": False,
            "trade_derived_M2_action_grade_support": False,
            "trade_derived_M2_role": "exchange_time_channel_diagnostic_only",
            "live_transport_authority": False,
        },
        "permission_boundary": {
            "economic_outcomes_read": False,
            "queue_or_lifecycle_authority": False,
            "exact_queue_policy_eligible": False,
            "action_authorized": False,
            "live_authorized": False,
            "allowed_uses": [
                "outcome_blind_feature_scaling",
                "outcome_blind_missingness_support",
                "outcome_blind_predicate_candidate_freeze",
                "per_channel_clock_separated_support_only",
            ],
        },
    }
    manifest["canonical_manifest_sha256"] = source_manifest.canonical_manifest_sha256(manifest)
    path = tmp_path / "outcome_blind_2025_source_manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return path


def _values(clock_group: str, value: float) -> dict[str, float | None]:
    values: dict[str, float | None] = {spec.name: None for spec in CHANNELS_BY_BLOCK["M2"]}
    if clock_group == materializer.BOOK_CLOCK_GROUP:
        values.update(
            {
                "mid_usdc_per_btc": value,
                "spread_bps": 1.0 + value * 0.001,
                "best_bid_qty_btc": 1.0 + value * 0.0001,
                "best_ask_qty_btc": 1.2 + value * 0.0001,
                "bbo_imbalance": -0.05,
                "microprice_deviation_bps": 0.01,
                "topk_bid_depth_btc": 10.0,
                "topk_ask_depth_btc": 11.0,
                "depth_imbalance": -1.0 / 21.0,
                "bid_depth_slope_btc_per_tick": 0.2,
                "ask_depth_slope_btc_per_tick": 0.3,
                "bid_depth_convexity_btc_per_tick2": 0.01,
                "ask_depth_convexity_btc_per_tick2": 0.02,
                "topk_bid_displayed_depth_increase_btc_per_s": 0.1,
                "topk_bid_displayed_depth_decrease_btc_per_s": 0.2,
                "topk_ask_displayed_depth_increase_btc_per_s": 0.3,
                "topk_ask_displayed_depth_decrease_btc_per_s": 0.4,
            }
        )
    else:
        values.update(
            {
                "aggressive_buy_qty_btc_per_s": max(value, 0.0),
                "aggressive_sell_qty_btc_per_s": max(1.0 - value, 0.0),
                "signed_flow_imbalance": value - 0.5,
                "trade_count_per_s": 10.0,
                "buy_run_length": 2.0,
                "sell_run_length": 0.0,
                "last_aggressive_buy_age_s": 0.05,
                "last_aggressive_sell_age_s": 0.15,
            }
        )
    return values


def _observations(
    *,
    left_ts_ns: int,
    count: int,
    generation_start: int,
    clock_group: str,
    first_value: float,
    step: float,
) -> list[CausalWindowObservation]:
    result = []
    for offset in range(count):
        left = left_ts_ns + offset * BASE_WINDOW_WIDTH_NS
        right = left + BASE_WINDOW_WIDTH_NS
        result.append(
            CausalWindowObservation(
                left_ts_ns=left,
                right_ts_ns=right,
                feature_ready_ts_ns=right,
                market_generation=generation_start + offset + 1,
                depth_generation=generation_start + offset + 1,
                values=_values(clock_group, first_value + step * offset),
            )
        )
    return result


def _reference_frames() -> tuple[
    dict[str, pd.DataFrame],
    dict[str, dict[str, materializer.ReferenceRowsAudit]],
    materializer.TimestampHashSamplingContract,
]:
    target_day = "2025-08-02"
    target_left = _day_start_ns(target_day)
    sampling = materializer.TimestampHashSamplingContract(
        numerator=700,
        denominator=1000,
        salt="synthetic-contract-test",
    )
    frames: dict[str, pd.DataFrame] = {}
    audits: dict[str, dict[str, materializer.ReferenceRowsAudit]] = {}
    for clock_group, allowed in (
        (materializer.BOOK_CLOCK_GROUP, materializer.PROVIDER_BOOK_CHANNELS),
        (materializer.TRADE_CLOCK_GROUP, materializer.OFFICIAL_TRADE_CHANNELS),
    ):
        warmup = _observations(
            left_ts_ns=target_left - 8 * BASE_WINDOW_WIDTH_NS,
            count=8,
            generation_start=0,
            clock_group=clock_group,
            first_value=100.0 if clock_group == "book" else 0.1,
            step=0.0,
        )
        target = _observations(
            left_ts_ns=target_left,
            count=80,
            generation_start=8,
            clock_group=clock_group,
            first_value=200.0 if clock_group == "book" else 0.2,
            step=0.01,
        )
        frame, audit = materializer.build_clock_reference_rows(
            warmup_observations=warmup,
            target_observations=target,
            target_day=target_day,
            clock_group=clock_group,
            allowed_channels=allowed,
            warmup_identity=materializer.canonical_sha256(
                {"clock_group": clock_group, "day": "2025-08-01"}
            ),
            sampling=sampling,
        )
        frames[clock_group] = frame
        audits[clock_group] = {target_day: audit}
    return frames, audits, sampling


def test_source_manifest_and_real_market_schemas_are_strict(tmp_path: Path) -> None:
    manifest_path = _source_fixture(tmp_path)
    manifest = materializer.load_and_validate_source_manifest(manifest_path)
    rows = {row["day"]: row for row in manifest["source_days"]}
    target = rows["2025-08-02"]
    bbo, l2 = materializer.load_provider_book_day(
        bbo_path=Path(target["bbo"]["path"]),
        l2_path=Path(target["l2"]["path"]),
        day="2025-08-02",
    )
    trades = materializer.load_official_individual_trades(
        path=Path(target["individual_trades"]["path"]),
        day="2025-08-02",
    )
    assert len(bbo.ts_ms) == 3
    assert l2.bid_px.shape == (3, 20)
    assert tuple(trades.columns) == (
        "id",
        "price",
        "qty",
        "quote_qty",
        "time",
        "is_buyer_maker",
    )

    bbo_path = Path(target["bbo"]["path"])
    bbo_path.write_bytes(bbo_path.read_bytes() + b"tamper")
    with pytest.raises(materializer.PredicateMaterializationError, match="hash drifted"):
        materializer.load_and_validate_source_manifest(manifest_path)


def test_timestamp_hash_sample_is_deterministic_but_not_a_fixed_cadence() -> None:
    contract = materializer.TimestampHashSamplingContract(
        numerator=250,
        denominator=1000,
        salt="irregular-sample-test",
    )
    day = "2025-08-02"
    left = _day_start_ns(day)
    timestamps = [left + (index + 1) * BASE_WINDOW_WIDTH_NS for index in range(500)]
    selected = [
        timestamp
        for timestamp in timestamps
        if contract.selected(
            clock_group=materializer.BOOK_CLOCK_GROUP,
            target_day=day,
            right_ts_ns=timestamp,
        )
    ]
    repeated = [
        timestamp
        for timestamp in timestamps
        if contract.selected(
            clock_group=materializer.BOOK_CLOCK_GROUP,
            target_day=day,
            right_ts_ns=timestamp,
        )
    ]
    trade_selected = [
        timestamp
        for timestamp in timestamps
        if contract.selected(
            clock_group=materializer.TRADE_CLOCK_GROUP,
            target_day=day,
            right_ts_ns=timestamp,
        )
    ]
    assert selected == repeated
    assert selected != trade_selected
    assert len(set(np.diff(np.asarray(selected, dtype=np.int64)).tolist())) > 1


def test_d_minus_one_state_updates_every_window_and_cutoff_is_causal() -> None:
    frames, audits, _ = _reference_frames()
    book = frames[materializer.BOOK_CLOCK_GROUP]
    audit = audits[materializer.BOOK_CLOCK_GROUP]["2025-08-02"]
    first_buy = book.loc[book["side"] == "BUY"].sort_values("sample_ts_ns").iloc[0]
    long_ema = first_buy["value::mid_usdc_per_btc::ema::h256s"]
    assert 100.0 < long_ema < 200.0
    assert audit.warmup_window_count == 8
    assert audit.target_window_count == 80
    assert audit.output_row_count == 2 * audit.selected_window_count
    assert audit.all_windows_updated_before_sampling is True
    assert audit.sampling_is_policy_or_feature_cadence is False
    assert audit.distinct_sample_interval_count > 1
    assert not any("exact_level" in column for column in book.columns)
    assert "cooldown_deadline_owner" not in book.columns

    target_day = "2025-08-02"
    target_left = _day_start_ns(target_day)
    warmup = _observations(
        left_ts_ns=target_left - BASE_WINDOW_WIDTH_NS,
        count=1,
        generation_start=0,
        clock_group="book",
        first_value=100.0,
        step=0.0,
    )
    target = _observations(
        left_ts_ns=target_left,
        count=1,
        generation_start=1,
        clock_group="book",
        first_value=101.0,
        step=0.0,
    )
    target[0] = replace(target[0], feature_ready_ts_ns=target[0].right_ts_ns + 1)
    with pytest.raises(
        materializer.PredicateMaterializationError,
        match="feature-ready state crossed",
    ):
        materializer.build_clock_reference_rows(
            warmup_observations=warmup,
            target_observations=target,
            target_day=target_day,
            clock_group="book",
            allowed_channels=materializer.PROVIDER_BOOK_CHANNELS,
            warmup_identity="a" * 64,
            sampling=materializer.TimestampHashSamplingContract(
                numerator=2,
                denominator=2,
                salt="select-all-for-cutoff-test",
            ),
        )


def test_official_trade_boundaries_use_exchange_time_without_book_join() -> None:
    day = "2025-08-02"
    left_ns = _day_start_ns(day)
    right_ms = (left_ns + BASE_WINDOW_WIDTH_NS) // 1_000_000
    trades = pd.DataFrame(
        {
            "time": [right_ms],
            "qty": [0.01],
            "is_buyer_maker": [False],
        }
    )
    windows = list(
        materializer.stream_official_trade_windows(
            trades=trades,
            left_ts_ns=left_ns,
            right_ts_ns=left_ns + 2 * BASE_WINDOW_WIDTH_NS,
        )
    )
    assert windows[0].values["trade_count_per_s"] == 0.0
    assert windows[1].values["trade_count_per_s"] == 10.0
    assert windows[1].values["aggressive_buy_qty_btc_per_s"] == pytest.approx(0.1)
    assert all(
        windows[1].values[channel] is None for channel in materializer.PROVIDER_BOOK_CHANNELS
    )


def test_clock_separated_atomic_admission_resume_and_hash_validation(
    tmp_path: Path,
) -> None:
    source_path = _source_fixture(tmp_path)
    frames, audits, sampling = _reference_frames()
    output_root = tmp_path / "admitted"
    result = materializer.admit_reference_frames(
        source_manifest_path=source_path,
        output_root=output_root,
        reference_frames=frames,
        audits=audits,
        sampling=sampling,
    )
    assert result.resumed is False
    manifest = materializer.validate_admission(
        result.admission_dir,
        source_manifest_path=source_path,
        rehash_sources=True,
    )
    assert manifest["support_boundary"]["economic_outcomes_read"] is False
    assert manifest["support_boundary"]["action_authorized"] is False
    assert manifest["support_boundary"]["live_authorized"] is False
    assert manifest["clock_contract"]["book_trade_reference_frames_joined"] is False
    assert set(manifest["predicate_bundle"]["book"]) == {"BUY", "SELL"}
    assert set(manifest["predicate_bundle"]["trade"]) == {"BUY", "SELL"}
    assert (
        manifest["predicate_bundle"]["cross_clock_clause_authorized_on_2025_reference_rows"]
        is False
    )
    assert manifest["predicate_bundle"]["cross_clock_clause_authorized"] is False
    assert manifest["predicate_bundle"]["cross_clock_clause_scope"] == "2025_reference_rows_only"
    assert (
        manifest["predicate_bundle"]["strict_2026_target_snapshot"][
            "book_trade_predicates_may_be_combined_by_study"
        ]
        is True
    )
    assert (
        manifest["predicate_bundle"]["strict_2026_target_snapshot"]["authority_owner"]
        == "2026_strict_denominator_study"
    )
    assert manifest["predicate_bundle"]["m0"] == {
        "materialized": False,
        "owner": "inner_chronological_development_builder",
        "required_partition_keys": [
            "panel_scope",
            "side",
            "outer_fold_id",
            "inner_fold_id",
        ],
        "required_source_role": "inner_chronological_development",
        "required_api": "fit_predicate_artifact",
        "reason": (
            "2025 market sources contain no decision-visible order, inventory, "
            "campaign, or cooldown action context"
        ),
    }

    study_bundle_record = manifest["study_predicate_bundle"]
    study_bundle = json.loads(
        (result.admission_dir / study_bundle_record["path"]).read_text(encoding="ascii")
    )
    assert study_bundle["schema_version"] == materializer.STUDY_PREDICATE_BUNDLE_SCHEMA
    assert study_bundle["identity"] == materializer.IDENTITY
    assert set(study_bundle["book"]) == {"BUY", "SELL"}
    assert set(study_bundle["trade"]) == {"BUY", "SELL"}
    assert study_bundle["m0_artifacts"] == []
    assert study_bundle["cross_clock_clause_authorized"] is False
    assert study_bundle["cross_clock_clause_scope"] == "2025_reference_rows_only"
    assert (
        study_bundle["strict_2026_target_snapshot"][
            "book_trade_predicates_may_be_combined_by_study"
        ]
        is True
    )
    assert study_bundle["canonical_sha256"] == materializer.canonical_document_sha256(
        study_bundle,
        "canonical_sha256",
    )

    artifacts = {}
    for key, record in manifest["artifacts"].items():
        artifact = PredicateArtifact.from_json(
            (result.admission_dir / record["path"]).read_text(encoding="ascii")
        )
        artifacts[key] = artifact
    assert {
        definition.clock_group
        for definition in artifacts["book_buy"].definitions
        if definition.clock_group in {"book", "trade"}
    } <= {"book"}
    assert {
        definition.clock_group
        for definition in artifacts["trade_sell"].definitions
        if definition.clock_group in {"book", "trade"}
    } <= {"trade"}
    assert not any(
        "exact_level" in definition.source_field for definition in artifacts["book_buy"].definitions
    )
    assert not any(
        definition.clock_group == "context" or definition.block == "M0"
        for artifact in artifacts.values()
        for definition in artifact.definitions
    )
    bundle = materializer.load_2025_predicate_bundle(
        result.admission_dir,
        expected_manifest_sha256=manifest["canonical_manifest_sha256"],
    )
    assert set(bundle["book"]) == {"BUY", "SELL"}
    assert set(bundle["trade"]) == {"BUY", "SELL"}
    assert bundle["m0"]["materialized"] is False
    assert bundle["compatibility"]["cross_clock_clause_authorized"] is False
    assert bundle["compatibility"]["cross_clock_clause_scope"] == "2025_reference_rows_only"
    assert bundle["compatibility"]["cross_clock_clause_authorized_on_2025_reference_rows"] is False
    assert (
        bundle["compatibility"]["strict_2026_target_snapshot"][
            "book_trade_predicates_may_be_combined_by_study"
        ]
        is True
    )
    assert bundle["book"]["BUY"].canonical_sha256 == artifacts["book_buy"].canonical_sha256
    assert bundle["study_predicate_bundle_path"] == (result.admission_dir / "predicate_bundle.json")
    assert bundle["study_predicate_bundle_sha256"] == study_bundle_record["sha256"]

    resumed = materializer.admit_reference_frames(
        source_manifest_path=source_path,
        output_root=output_root,
        reference_frames=frames,
        audits=audits,
        sampling=sampling,
    )
    assert resumed.resumed is True
    assert resumed.admission_dir == result.admission_dir

    artifact_path = result.admission_dir / manifest["artifacts"]["book_buy"]["path"]
    artifact_path.write_text(
        artifact_path.read_text(encoding="ascii") + " ",
        encoding="ascii",
    )
    with pytest.raises(
        materializer.PredicateMaterializationError,
        match="file hash drifted",
    ):
        materializer.validate_admission(result.admission_dir)
