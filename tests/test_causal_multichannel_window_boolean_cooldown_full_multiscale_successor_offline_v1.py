from __future__ import annotations

import csv
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_v1 as offline,
)


def _write_csv(path: Path, header: tuple[str, ...], row: tuple[object, ...]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerow(row)


def _write_normalized(root: Path, day: str) -> None:
    start = int(offline.np.datetime64(day, "ms").astype(offline.np.int64))
    timestamps = [start + 100, start + 200]
    bbo = {
        "timestamp": timestamps,
        "best_bid": [100.0, 100.1],
        "best_bid_qty": [1.0, 1.0],
        "best_ask": [100.2, 100.3],
        "best_ask_qty": [1.0, 1.0],
    }
    l2: dict[str, list[float] | list[int]] = {"timestamp": timestamps}
    for level in range(1, 21):
        l2[f"bid_px_{level}"] = [100.0 - level / 10, 100.1 - level / 10]
        l2[f"bid_qty_{level}"] = [1.0, 1.0]
        l2[f"ask_px_{level}"] = [100.2 + level / 10, 100.3 + level / 10]
        l2[f"ask_qty_{level}"] = [1.0, 1.0]
    for kind, values in (("bbo", bbo), ("l2", l2)):
        path = root / kind / f"BTCUSDC-{kind}-{day}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table(values), path)


def _fixture_layout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> offline.OfflineSourceLayout:
    targets = ("2026-01-02", "2026-01-03")
    monkeypatch.setattr(offline, "PRIMARY_TARGET_DAYS", targets)
    monkeypatch.setattr(offline, "BACKUP_TARGET_DAYS", ())
    monkeypatch.setattr(offline, "CANDIDATE_TARGET_DAYS", targets)
    monkeypatch.setattr(offline, "CONSUMED_TARGET_DAYS", ("2026-01-01",))
    monkeypatch.setattr(offline, "REQUIRED_DAYS", 2)
    project = tmp_path / "project-data"
    market = tmp_path / "market-data"
    raw = market / "cryptohftdata/binance_futures"
    normalized = project / "normalized"
    agg = project / "raw"
    individual = project / "raw_trades/BTCUSDC"
    sequence_rows = {}
    for day_number in range(1, 5):
        day = f"2026-01-{day_number:02d}"
        for hour in offline.RAW_HOURS:
            path = raw / day / hour / "BTCUSDC_orderbook.parquet.zst"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(f"{day}:{hour}".encode())
        _write_normalized(normalized, day)
        timestamp = int(offline.np.datetime64(day, "ms").astype(offline.np.int64)) + 1
        _write_csv(
            agg / f"BTCUSDC-aggTrades-{day}.csv",
            offline._AGG_COLUMNS,
            (1, 100.0, 1.0, 10, 10, timestamp, False),
        )
        _write_csv(
            individual / f"BTCUSDC-trades-{day}.csv",
            offline._TRADE_COLUMNS,
            (10, 100.0, 1.0, 100.0, timestamp, False),
        )
        sequence_rows[day] = {
            "eligible": True,
            "target_initialized_at_start": True,
            "target_initialization_source_at_start": "snapshot",
            "target_accepted_updates": 1,
            "target_sequence_gaps": 0,
            "target_invalid_sequence_messages": 0,
            "target_message_time_reversals": 0,
            "target_duplicate_messages": 0,
            "target_stale_updates": 0,
        }
    sequence = project / "sequence.json"
    sequence.parent.mkdir(parents=True, exist_ok=True)
    sequence.write_text(json.dumps({"day_audits": sequence_rows}), encoding="utf-8")
    return offline.OfflineSourceLayout(
        project_data_root=project,
        marketdata_root=market,
        raw_orderbook_root=raw,
        normalized_roots=(normalized,),
        aggtrades_root=agg,
        individual_trades_root=individual,
        sequence_audit_paths=(sequence,),
    )


def test_outcome_blind_source_gate_admits_frozen_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = _fixture_layout(tmp_path, monkeypatch)
    output = layout.project_data_root / "reports/offline"
    manifest = offline.audit_historical_sources(
        layout=layout,
        output_dir=output,
        workers=2,
    )

    assert manifest["selected_days"] == ["2026-01-02", "2026-01-03"]
    assert manifest["permissions"]["economic_outcomes_read"] is False
    assert manifest["fold_manifest"]["selection_sha256"] == manifest["selection_sha256"]
    validated = offline.validate_canonical_manifest(
        output / "canonical_source_manifest.json",
        layout=layout,
    )
    assert validated["canonical_manifest_sha256"] == manifest["canonical_manifest_sha256"]


def test_source_gate_manifest_and_receipt_tampering_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = _fixture_layout(tmp_path, monkeypatch)
    output = layout.project_data_root / "reports/offline"
    offline.audit_historical_sources(layout=layout, output_dir=output, workers=1)
    manifest_path = output / "canonical_source_manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["selected_days"].reverse()
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(offline.OfflineSourceGateError, match="manifest hash"):
        offline.validate_canonical_manifest(manifest_path, layout=layout)


def test_source_gate_rejects_economic_fields_without_rehash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = _fixture_layout(tmp_path, monkeypatch)
    output = layout.project_data_root / "reports/offline"
    offline.audit_historical_sources(layout=layout, output_dir=output, workers=1)
    manifest_path = output / "canonical_source_manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["pnl_usdc"] = 1.0
    payload["canonical_manifest_sha256"] = offline.canonical_document_sha256(
        payload, "canonical_manifest_sha256"
    )
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(offline.OfflineSourceGateError, match="economic field"):
        offline.validate_canonical_manifest(
            manifest_path,
            rehash_sources=False,
            layout=layout,
        )
