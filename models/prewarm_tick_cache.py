#!/usr/bin/env python3
"""Prewarm source-bound daily tick windows on the internal cache volume.

This command materializes only reproducible cache state.  Market data and the
frozen cache manifest remain on the external data volume.  Provider-normalized
Tardis days are forced onto ``provider_visible_level`` queue semantics and are
never represented as exact-queue or policy-authoritative replay.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import hashlib
import json
import os
import shutil
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from data_paths import data_root, window_cache_root

ROOT = Path(__file__).resolve().parent.parent


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _days(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None or "day" not in reader.fieldnames:
            raise ValueError(f"days file must contain day: {path}")
        values = sorted(
            {
                date.fromisoformat(str(row["day"]).strip()).isoformat()
                for row in reader
                if str(row.get("day", "")).strip()
            }
        )
    if not values:
        raise ValueError(f"days file is empty: {path}")
    return values


def _quality_authorities(book_root: Path) -> dict[str, str]:
    path = book_root / "daily_quality.csv"
    if not path.is_file():
        raise ValueError(f"book root lacks daily_quality.csv: {book_root}")
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        required = {"day", "source_authority"}
        if reader.fieldnames is None or not required.issubset(reader.fieldnames):
            raise ValueError(
                f"book quality lacks {sorted(required)}: {path}"
            )
        return {
            str(row["day"]): str(row["source_authority"])
            for row in reader
        }


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _task(payload: dict[str, Any]) -> dict[str, Any]:
    from models import backtest_tick as bt
    from models.backtest_config import disable_ml_params, load_tick_base_params
    from models.data_windows import (
        _window_cache_path,
        _window_market_context_cache_path,
        _window_model_overlay_cache_path,
        load_tick_window,
    )

    day = str(payload["day"])
    book_root = Path(str(payload["book_root"]))
    cache_dir = Path(str(payload["cache_dir"]))
    with_ml = bool(payload.get("with_ml", False))
    feature_dir = (
        Path(str(payload["feature_dir"])).resolve() if with_ml else None
    )
    model_dir = (
        Path(str(payload["model_dir"])).resolve() if with_ml else None
    )
    bt.BBO_DIR = (book_root / "bbo").resolve()
    bt.L2_DIR = (book_root / "l2").resolve()
    bt.configure_symbol(
        "BTCUSDC",
        model_dir_override=model_dir if with_ml else None,
    )
    params = load_tick_base_params(
        symbol="BTCUSDC",
        configure_symbol=bt.configure_symbol,
        require_historical_bbo=True,
        include_fill_probability=False,
        include_queue_calibration=False,
    )
    if with_ml:
        params.update(
            {
                "ml_enabled": True,
                "model_dir": str(model_dir),
                "resolved_model_dir": str(model_dir),
            }
        )
    else:
        params["ml_enabled"] = False
        disable_ml_params(params)
    authority = str(payload["source_authority"])
    params.update(
        {
            "execution_trade_source": "trades",
            "market_context_warmup_days": 1,
            "replay_event_clock": "merged",
            "queue_ahead_mode": (
                "provider_visible_level"
                if authority == "provider_normalized_causal"
                else "exact_level"
            ),
            "queue_l2_cancel_ahead_enabled": False,
            "_formal_quality_allowed_days": [
                (
                    date.fromisoformat(day) - timedelta(days=1)
                ).isoformat(),
                day,
            ],
            "window_cache_write_enabled": True,
        }
    )
    window = load_tick_window(
        day,
        params,
        load_ml=with_ml,
        require_ml=with_ml,
        run_ml_inference=with_ml,
        feature_dir=feature_dir,
        require_target_feature_files=with_ml,
        cross_market_enabled=with_ml,
        with_ml_cache=False,
        require_historical_bbo=True,
        require_formal_l2=False,
        cache_dir=cache_dir,
        refresh_cache=bool(payload["refresh"]),
    )
    cache_path = _window_cache_path(
        cache_dir,
        day,
        params,
        load_ml=with_ml,
        require_ml=with_ml,
        run_ml_inference=with_ml,
        feature_dir=feature_dir or Path(bt.FEATURES_DIR),
        require_target_feature_files=with_ml,
        cross_market_enabled=with_ml,
        with_ml_cache=False,
        require_historical_bbo=True,
    )
    market_context_path = _window_market_context_cache_path(
        cache_dir,
        day,
        params,
    )
    component_paths = [market_context_path]
    if with_ml:
        component_paths.append(
            _window_model_overlay_cache_path(
                cache_dir,
                day,
                params,
                feature_dir=feature_dir or Path(bt.FEATURES_DIR),
                run_ml_inference=True,
                cross_market_enabled=True,
                market_context_path=market_context_path,
            )
        )
    use_legacy_cache = cache_path.is_file() and not bool(payload["refresh"])
    published_paths = (
        [cache_path]
        if use_legacy_cache
        else [path for path in component_paths if path.is_file()]
    )
    if len(published_paths) != (1 if use_legacy_cache else len(component_paths)):
        raise RuntimeError(
            f"{day}: expected replay components were not published: "
            f"{component_paths}"
        )
    return {
        "day": day,
        "cache_path": str(published_paths[0]),
        "cache_paths": [str(path) for path in published_paths],
        "size_bytes": sum(path.stat().st_size for path in published_paths),
        "source_authority": window.book_source_authority,
        "book_dataset_version": window.book_dataset_version,
        "formal_lifecycle_replay_eligible": (
            window.formal_lifecycle_replay_eligible
        ),
        "provider_sensitivity_replay_eligible": (
            window.provider_sensitivity_replay_eligible
        ),
        "exact_queue_policy_eligible": window.exact_queue_policy_eligible,
        "trades": len(window.trades),
        "bbo_rows": (
            len(window.bbo_data.ts_ms) if window.bbo_data is not None else 0
        ),
        "l2_rows": (
            len(window.l2_data.ts_ms) if window.l2_data is not None else 0
        ),
        "ml_prediction_rows": (
            len(window.ml_data[0]) if window.ml_data is not None else 0
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days-file", type=Path, required=True)
    parser.add_argument("--book-root", type=Path, required=True)
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=window_cache_root(ROOT),
    )
    parser.add_argument("--manifest-json", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--with-ml", action="store_true")
    parser.add_argument("--feature-dir", type=Path, default=None)
    parser.add_argument("--model-dir", type=Path, default=None)
    parser.add_argument("--reserve-gib", type=float, default=60.0)
    parser.add_argument("--estimated-gib-per-day", type=float, default=1.25)
    args = parser.parse_args()

    days_file = args.days_file.expanduser().resolve()
    book_root = args.book_root.expanduser().resolve()
    cache_dir = args.cache_dir.expanduser().resolve()
    manifest_json = args.manifest_json.expanduser().resolve()
    feature_dir = (
        args.feature_dir.expanduser().resolve()
        if args.feature_dir is not None
        else None
    )
    model_dir = (
        args.model_dir.expanduser().resolve()
        if args.model_dir is not None
        else None
    )
    if args.with_ml and (feature_dir is None or model_dir is None):
        raise SystemExit("--with-ml requires --feature-dir and --model-dir")
    if not args.with_ml and (feature_dir is not None or model_dir is not None):
        raise SystemExit("--feature-dir/--model-dir require --with-ml")
    if feature_dir is not None and not (
        feature_dir / "causal_feature_manifest.json"
    ).is_file():
        raise SystemExit(f"causal feature manifest missing: {feature_dir}")
    if model_dir is not None and not (
        model_dir / "training_summary.json"
    ).is_file():
        raise SystemExit(f"training summary missing: {model_dir}")
    if not (book_root / "manifest.json").is_file():
        raise SystemExit(f"versioned book manifest missing: {book_root}")
    if args.workers < 1 or args.workers > 2:
        raise SystemExit("--workers must be in [1, 2] for full tick windows")
    try:
        days = _days(days_file)
        authorities = _quality_authorities(book_root)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    missing = [day for day in days if day not in authorities]
    if missing:
        raise SystemExit(
            "days are missing from the book quality contract: "
            + ", ".join(missing[:10])
        )

    cache_dir.mkdir(parents=True, exist_ok=True)
    if cache_dir.is_relative_to(data_root(ROOT)):
        raise SystemExit(
            f"cache directory must not live under external market data: {cache_dir}"
        )
    usage = shutil.disk_usage(cache_dir)
    estimated_bytes = int(len(days) * args.estimated_gib_per_day * 2**30)
    required_bytes = int(args.reserve_gib * 2**30 + estimated_bytes)
    if usage.free < required_bytes:
        raise SystemExit(
            "cache storage gate failed: "
            f"free={usage.free} required={required_bytes}"
        )

    payloads = [
        {
            "day": day,
            "book_root": str(book_root),
            "cache_dir": str(cache_dir),
            "source_authority": authorities[day],
            "refresh": bool(args.refresh),
            "with_ml": bool(args.with_ml),
            "feature_dir": str(feature_dir) if feature_dir is not None else "",
            "model_dir": str(model_dir) if model_dir is not None else "",
        }
        for day in days
    ]
    if args.workers == 1:
        rows = [_task(payload) for payload in payloads]
    else:
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=args.workers
        ) as executor:
            rows = list(executor.map(_task, payloads))
    rows.sort(key=lambda row: str(row["day"]))

    manifest = {
        "schema_version": "narrowgate.tick_window_cache_prewarm.v1",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "cache_is_reproducible_and_disposable": True,
        "engine_target": "cpp",
        "cache_dir": str(cache_dir),
        "days_file": {
            "path": str(days_file),
            "sha256": _sha256(days_file),
        },
        "book_manifest": {
            "path": str((book_root / "manifest.json").resolve()),
            "sha256": _sha256(book_root / "manifest.json"),
        },
        "ml_identity": (
            {
                "enabled": True,
                "feature_dir": str(feature_dir),
                "feature_manifest_sha256": _sha256(
                    feature_dir / "causal_feature_manifest.json"
                ),
                "model_dir": str(model_dir),
                "training_summary_sha256": _sha256(
                    model_dir / "training_summary.json"
                ),
            }
            if args.with_ml
            else {"enabled": False}
        ),
        "day_count": len(rows),
        "total_size_bytes": sum(int(row["size_bytes"]) for row in rows),
        "storage_preflight": {
            "free_bytes_before": usage.free,
            "reserve_bytes": int(args.reserve_gib * 2**30),
            "estimated_new_bytes": estimated_bytes,
            "required_bytes": required_bytes,
        },
        "permission_boundary": {
            "provider_cache_is_sensitivity_only": True,
            "exact_queue_policy_authority": False,
            "action_authority": False,
            "live_authority": False,
        },
        "windows": rows,
    }
    _atomic_json(manifest_json, manifest)
    print(
        json.dumps(
            {
                "days": len(rows),
                "total_gib": manifest["total_size_bytes"] / 2**30,
                "cache_dir": str(cache_dir),
                "manifest": str(manifest_json),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
