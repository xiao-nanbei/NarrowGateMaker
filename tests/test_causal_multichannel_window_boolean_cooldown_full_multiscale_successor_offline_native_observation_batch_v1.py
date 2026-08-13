from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from types import MappingProxyType

import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_native_observation_batch_v1 as batch,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_v1 as offline,
)


def _layout(tmp_path: Path) -> offline.OfflineSourceLayout:
    project = tmp_path / "project"
    market = tmp_path / "market"
    raw = market / "cryptohftdata/binance_futures"
    normalized = project / "normalized"
    individual = project / "raw_trades/BTCUSDC"
    for path in (raw, normalized, individual, project / "raw"):
        path.mkdir(parents=True, exist_ok=True)
    return offline.OfflineSourceLayout(
        project_data_root=project,
        marketdata_root=market,
        raw_orderbook_root=raw,
        normalized_roots=(normalized,),
        aggtrades_root=project / "raw",
        individual_trades_root=individual,
        sequence_audit_paths=(project / "sequence.json",),
    )


def _source_manifest() -> dict[str, object]:
    selected = offline.PRIMARY_TARGET_DAYS
    receipts = []
    for position, day in enumerate(offline.CANDIDATE_TARGET_DAYS, start=1):
        current = date.fromisoformat(day)
        receipt: dict[str, object] = {
            "utc_day": day,
            "candidate_position": position,
            "source_gate_eligible": day in selected,
            "context_days": {
                "D_minus_1": (current - timedelta(days=1)).isoformat(),
                "D": day,
                "D_plus_1": (current + timedelta(days=1)).isoformat(),
            },
        }
        receipt["day_receipt_sha256"] = offline.canonical_document_sha256(
            receipt, "day_receipt_sha256"
        )
        receipts.append(receipt)
    by_day = {str(row["utc_day"]): row for row in receipts}
    selection_body = {
        "identity": offline.IDENTITY,
        "panel_role": offline.PANEL_ROLE,
        "required_days": offline.REQUIRED_DAYS,
        "candidate_order": list(offline.CANDIDATE_TARGET_DAYS),
        "consumed_exclusions": list(offline.CONSUMED_TARGET_DAYS),
        "selected_days": list(selected),
        "selected_day_receipts": [by_day[day]["day_receipt_sha256"] for day in selected],
    }
    selection_sha = offline.canonical_sha256(selection_body)
    return {
        "status": "offline_canonical_source_gate_passed_panel_mechanics_required",
        "canonical_manifest_sha256": "a" * 64,
        "selection_sha256": selection_sha,
        "selected_days": list(selected),
        "target_day_receipts": receipts,
        "fold_manifest": {"selection_sha256": selection_sha},
        "source_day_receipt_files": {},
    }


def test_source_contract_revalidates_and_derives_exact_30_days(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = _layout(tmp_path)
    source_path = layout.project_data_root / "canonical_source_manifest.json"
    source_path.write_text("{}", encoding="ascii")
    source = _source_manifest()
    calls: list[dict[str, object]] = []

    def fake_validate(path, *, rehash_sources, layout):
        calls.append({"path": path, "rehash_sources": rehash_sources, "layout": layout})
        return source

    monkeypatch.setattr(offline, "validate_canonical_manifest", fake_validate)
    validated, days = batch.load_source_contract(source_path, layout=layout)
    observation_days, continuation_only = batch._observation_context_days(
        validated, days
    )

    assert validated is source
    assert days == offline.PRIMARY_TARGET_DAYS
    assert len(days) == 30
    assert len(observation_days) == 34
    assert continuation_only == (
        "2026-06-29",
        "2026-07-03",
        "2026-07-16",
        "2026-08-06",
    )
    assert calls == [
        {"path": source_path.resolve(), "rehash_sources": True, "layout": layout}
    ]


def test_source_contract_rejects_selection_drift_and_old_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = _layout(tmp_path)
    source_path = layout.project_data_root / "canonical_source_manifest.json"
    source_path.write_text("{}", encoding="ascii")
    source = _source_manifest()
    source["selection_sha256"] = "b" * 64
    monkeypatch.setattr(
        offline,
        "validate_canonical_manifest",
        lambda *args, **kwargs: source,
    )
    with pytest.raises(batch.OfflineNativeObservationBatchError, match="selection hash"):
        batch.load_source_contract(source_path, layout=layout)

    monkeypatch.setattr(
        offline,
        "validate_canonical_manifest",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            offline.OfflineSourceGateError("offline canonical manifest identity drifted")
        ),
    )
    with pytest.raises(offline.OfflineSourceGateError, match="identity drifted"):
        batch.load_source_contract(source_path, layout=layout)


def test_batch_has_no_arbitrary_day_interface_and_bounds_workers() -> None:
    with pytest.raises(SystemExit):
        batch._parser().parse_args(
            [
                "build",
                "--source-manifest",
                "canonical_source_manifest.json",
                "--days",
                "2026-04-17",
            ]
        )
    with pytest.raises(batch.OfflineNativeObservationBatchError, match="between 1 and 8"):
        batch.run_batch(
            source_manifest_path=Path("unused"),
            workers=9,
            output_root=Path("unused"),
            native_book_cache=Path("unused"),
            progress_path=Path("unused"),
            manifest_path=Path("unused"),
            layout=offline.default_layout(),
        )


def test_run_batch_builds_only_manifest_days_and_atomically_binds_cache_hashes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = _layout(tmp_path)
    source_path = layout.project_data_root / "canonical_source_manifest.json"
    source_path.write_text("source", encoding="ascii")
    source = _source_manifest()
    source["canonical_manifest_sha256"] = "c" * 64
    target_days = offline.PRIMARY_TARGET_DAYS
    observation_days, continuation_only = batch._observation_context_days(
        source, target_days
    )
    output_root = layout.project_data_root / "observations"
    progress_path = output_root / batch.PROGRESS_NAME
    manifest_path = output_root / batch.MANIFEST_NAME
    built: list[str] = []
    validated: list[tuple[str, bool]] = []

    monkeypatch.setattr(
        batch,
        "load_source_contract",
        lambda *args, **kwargs: (source, target_days),
    )

    def fake_build(arguments):
        day = arguments[0]
        built.append(day)
        day_root = output_root / day
        day_root.mkdir(parents=True)
        (day_root / "manifest.json").write_text(day, encoding="ascii")
        (day_root / "observations.parquet").write_text(day, encoding="ascii")
        return {"utc_day": day}

    def fake_row(
        source_arg,
        day,
        *,
        output_root,
        selected_target_days,
        layout,
        deep,
    ):
        validated.append((day, deep))
        is_target = day in target_days
        return {
            "utc_day": day,
            "observation_role": "selected_target" if is_target else "continuation_only",
            "target_assignment_eligible": is_target,
            "observation_receipt_sha256": "1" * 64,
            "cache_canonical_manifest_sha256": "2" * 64,
            "cache_manifest_file_sha256": "3" * 64,
            "cache_parquet_sha256": "4" * 64,
            "cache_observation_sha256": "5" * 64,
            "observation_count": 10,
            "source_binding_sha256": "6" * 64,
        }

    monkeypatch.setattr(batch, "_build_one", fake_build)
    monkeypatch.setattr(batch, "_validated_day_row", fake_row)
    monkeypatch.setattr(
        batch.cache,
        "cache_contract",
        lambda: {"economic_outcomes_read": False},
    )
    result = batch.run_batch(
        source_manifest_path=source_path,
        workers=1,
        output_root=output_root,
        native_book_cache=layout.project_data_root / "native-book-cache",
        progress_path=progress_path,
        manifest_path=manifest_path,
        layout=layout,
    )

    assert tuple(built) == observation_days
    assert result["selected_target_days"] == list(target_days)
    assert result["selected_target_day_count"] == 30
    assert result["observation_context_days"] == list(observation_days)
    assert result["continuation_only_days"] == list(continuation_only)
    assert result["observation_context_day_count"] == 34
    assert result["continuation_days_create_target_assignments"] is False
    assert len(result["days"]) == 34
    assert sum(row["target_assignment_eligible"] for row in result["days"]) == 30
    assert all(value is False for value in result["permissions"].values())
    assert sum(deep for _, deep in validated) == 34
    assert manifest_path.is_file()
    assert json.loads(progress_path.read_text(encoding="ascii"))["status"] == "complete"


def test_day_source_binding_matches_canonical_raw_and_trade_hashes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_hashes = tuple(f"{index:064x}" for index in range(48))
    trade_hashes = ("a" * 64, "b" * 64)
    monkeypatch.setattr(
        batch,
        "_expected_day_sources",
        lambda *args, **kwargs: (
            raw_hashes,
            trade_hashes,
            "d" * 64,
            "selected_target",
        ),
    )
    manifest = {
        "source_binding": {
            "raw_native_tape_identity": {
                "day": "2026-06-27",
                "warmup_hours": 24,
                "continuation_hours": 0,
                "strict_complete": True,
                "files": [{"sha256": value} for value in raw_hashes],
            },
            "official_individual_trades": [
                {"sha256": value} for value in trade_hashes
            ],
            "symbol": "BTCUSDC",
            "receive_time_transport_authority": False,
        }
    }
    assert batch._assert_day_source_binding(
        {},
        "2026-06-27",
        MappingProxyType(manifest),
        selected_target_days=("2026-06-27",),
        layout=offline.default_layout(),
    ) == ("d" * 64, "selected_target")

    manifest["source_binding"]["official_individual_trades"][0]["sha256"] = "e" * 64
    with pytest.raises(batch.OfflineNativeObservationBatchError, match="do not match"):
        batch._assert_day_source_binding(
            {},
            "2026-06-27",
            manifest,
            selected_target_days=("2026-06-27",),
            layout=offline.default_layout(),
        )
