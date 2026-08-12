from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np

from models.replay_cache_dag import REPLAY_WINDOW_CACHE_GRAPH_IDENTITY
from models.replay_cache_node_audit import audit_replay_cache_nodes


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _canonical(payload: dict[str, object]) -> str:
    raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return hashlib.sha256(raw).hexdigest()


def test_audit_is_read_only_and_finds_superseded_duplicate(
    tmp_path: Path,
    monkeypatch,
) -> None:
    cache = tmp_path / "cache"
    external = tmp_path / "external"
    repo = tmp_path / "repo"
    repo.mkdir()
    adapter = (
        repo / "research/families/f04_external_market_alpha/audit/cross_venue_causal_fair_price.py"
    )
    estimator = repo / "strategy/cross_venue_fair_price.py"
    adapter.parent.mkdir(parents=True)
    estimator.parent.mkdir(parents=True)
    adapter.write_text("current adapter\n", encoding="utf-8")
    estimator.write_text("current estimator\n", encoding="utf-8")

    monkeypatch.setattr(
        "models.native_exchange_book_cache.native_book_parser_identity",
        lambda: "a" * 64,
    )
    fair = cache / "replay_dag/cross_venue_fair_price_trade_1s_v1/all_venues"
    fair.mkdir(parents=True)
    current_sha = _sha256(adapter)
    estimator_sha = _sha256(estimator)
    payload_bytes = b"same payload"
    for label, adapter_sha in (("current", current_sha), ("old", "b" * 64)):
        payload = fair / f"2026-01-01-{label}.parquet"
        payload.write_bytes(payload_bytes)
        identity = {
            "schema_version": "cross_venue_causal_fair_price_trade_1s.v1",
            "utc_day": "2026-01-01",
            "omitted_venue": "",
            "implementation": {
                "adapter_sha256": adapter_sha,
                "estimator_sha256": estimator_sha,
            },
        }
        manifest = {
            **identity,
            "cache_identity_sha256": _canonical(identity),
            "output_path": str(payload),
            "output_sha256": _sha256(payload),
            "rows": 1,
            "valid_rows": 1,
            "valid_fraction": 1.0,
            "reason_counts": {},
        }
        payload.with_suffix(".parquet.manifest.json").write_text(
            json.dumps(manifest),
            encoding="utf-8",
        )

    overlay = cache / "p3_conditional_quote_overlay_v1"
    overlay.mkdir(parents=True)
    key = "c" * 64
    with (overlay / f"BTCUSDC-2026-01-01-{key}.npz").open("wb") as handle:
        np.savez_compressed(handle, cache_key=np.asarray(key), value=np.asarray([1.0]))

    before = sorted((path, path.stat().st_mtime_ns) for path in cache.rglob("*"))
    result = audit_replay_cache_nodes(
        cache_root=cache,
        external_cache_root=external,
        repository_root=repo,
        reference_roots=(repo,),
        verify_payload_hashes=True,
    )
    after = sorted((path, path.stat().st_mtime_ns) for path in cache.rglob("*"))

    assert before == after
    assert result["cache_files_modified"] == 0
    assert result["cache_files_deleted"] == 0
    assert len(result["deletion_candidates"]) == 1
    assert "old.parquet" in result["deletion_candidates"][0]["paths"][0]
    overlay_node = next(
        row for row in result["nodes"] if row["node_id"] == "p3_conditional_quote_overlay_v1"
    )
    assert overlay_node["npz_integrity"]["embedded_cache_key_mismatches"] == 0


def test_exact_hash_reference_blocks_superseded_candidate(
    tmp_path: Path,
    monkeypatch,
) -> None:
    cache = tmp_path / "cache"
    external = tmp_path / "external"
    repo = tmp_path / "repo"
    adapter = (
        repo / "research/families/f04_external_market_alpha/audit/cross_venue_causal_fair_price.py"
    )
    estimator = repo / "strategy/cross_venue_fair_price.py"
    adapter.parent.mkdir(parents=True)
    estimator.parent.mkdir(parents=True)
    adapter.write_text("current adapter\n", encoding="utf-8")
    estimator.write_text("current estimator\n", encoding="utf-8")
    monkeypatch.setattr(
        "models.native_exchange_book_cache.native_book_parser_identity",
        lambda: "a" * 64,
    )

    fair = cache / "replay_dag/cross_venue_fair_price_trade_1s_v1/all_venues"
    fair.mkdir(parents=True)
    payload_sha = ""
    for label, adapter_sha in (("current", _sha256(adapter)), ("old", "b" * 64)):
        payload = fair / f"2026-01-01-{label}.parquet"
        payload.write_bytes(b"same payload")
        payload_sha = _sha256(payload)
        identity = {
            "schema_version": "cross_venue_causal_fair_price_trade_1s.v1",
            "utc_day": "2026-01-01",
            "omitted_venue": "",
            "implementation": {
                "adapter_sha256": adapter_sha,
                "estimator_sha256": _sha256(estimator),
            },
        }
        manifest = {
            **identity,
            "cache_identity_sha256": _canonical(identity),
            "output_path": str(payload),
            "output_sha256": payload_sha,
            "rows": 1,
            "valid_rows": 1,
            "valid_fraction": 1.0,
            "reason_counts": {},
        }
        payload.with_suffix(".parquet.manifest.json").write_text(
            json.dumps(manifest),
            encoding="utf-8",
        )
    (repo / "frozen_spec.json").write_text(payload_sha, encoding="utf-8")

    result = audit_replay_cache_nodes(
        cache_root=cache,
        external_cache_root=external,
        repository_root=repo,
        reference_roots=(repo,),
        verify_payload_hashes=True,
    )
    assert result["deletion_candidates"] == []
    assert result["summary"]["file_group_candidate_bytes"] == 0
    assert REPLAY_WINDOW_CACHE_GRAPH_IDENTITY
