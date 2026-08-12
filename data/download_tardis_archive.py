#!/usr/bin/env python3
"""Plan, download, and integrity-check a bounded Tardis historical delivery.

The downloader preserves the provider's venue/dataset/date hierarchy and never
writes Tardis payloads into the CryptoHFTData tree.  A plan resolves exact
remote sizes before download so the external-volume space contract can fail
closed instead of filling the disk midway through a batch.

This command is not NarrowGate's recurring daily updater.  Daily increments
continue to use the existing Binance Vision, CryptoHFTData, Bitget, Bybit, and
OKX pipelines; Tardis remains a source-separated one-off historical delivery.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import shutil
import sys
import tempfile
import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import requests
import zstandard

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_paths import marketdata_root  # noqa: E402

DEFAULT_BASE_URL = "https://data.yutsing.work/0730-beinan/tardis"
DEFAULT_OUTPUT_ROOT = marketdata_root() / "tardis"
LEGACY_OUTPUT_ROOT = DEFAULT_OUTPUT_ROOT / "0730-beinan"
GIB = 1024**3


def resolve_tardis_artifact_path(path: Path | str) -> Path:
    """Resolve frozen pre-flattening paths without changing manifest bytes."""

    candidate = Path(path).expanduser()
    if candidate.exists():
        return candidate
    try:
        relative = candidate.relative_to(LEGACY_OUTPUT_ROOT)
    except ValueError:
        return candidate
    relocated = DEFAULT_OUTPUT_ROOT / relative
    return relocated if relocated.exists() else candidate


@dataclass(frozen=True)
class Contract:
    venue: str
    dataset: str
    symbol: str


@dataclass(frozen=True)
class RemoteTarget:
    venue: str
    dataset: str
    symbol: str
    day: str
    url: str
    relative_path: str
    exists: bool
    content_length: int
    etag: str
    last_modified: str
    accept_ranges: str
    error: str


def _atomic_json(payload: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _parse_day(value: str) -> date:
    return date.fromisoformat(value)


def _days(start: date, end: date) -> list[date]:
    if end < start:
        raise ValueError("end day precedes start day")
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


def _parse_contract(value: str) -> Contract:
    fields = [field.strip() for field in value.split(",")]
    if len(fields) != 3 or not all(fields):
        raise argparse.ArgumentTypeError(
            "contract must be VENUE,DATASET,SYMBOL"
        )
    if any("/" in field or ".." in field for field in fields):
        raise argparse.ArgumentTypeError("contract fields must be path-safe")
    return Contract(*fields)


def _target(base_url: str, contract: Contract, day: date) -> tuple[str, str]:
    relative = (
        f"{contract.venue}/{contract.dataset}/{day:%Y/%m/%d}/"
        f"{contract.symbol}.csv.zst"
    )
    return f"{base_url.rstrip('/')}/{relative}", relative


def _head_one(
    base_url: str,
    contract: Contract,
    day: date,
    *,
    timeout_s: float,
    attempts: int,
) -> RemoteTarget:
    url, relative = _target(base_url, contract, day)
    error = ""
    for attempt in range(1, attempts + 1):
        try:
            response = requests.head(
                url,
                allow_redirects=True,
                timeout=(10.0, timeout_s),
            )
            if response.status_code == 404:
                return RemoteTarget(
                    contract.venue,
                    contract.dataset,
                    contract.symbol,
                    day.isoformat(),
                    url,
                    relative,
                    False,
                    0,
                    "",
                    "",
                    "",
                    "HTTP 404",
                )
            response.raise_for_status()
            size = int(response.headers.get("content-length", "0"))
            if size <= 0:
                raise RuntimeError("missing positive Content-Length")
            return RemoteTarget(
                contract.venue,
                contract.dataset,
                contract.symbol,
                day.isoformat(),
                url,
                relative,
                True,
                size,
                response.headers.get("etag", "").strip(),
                response.headers.get("last-modified", "").strip(),
                response.headers.get("accept-ranges", "").strip(),
                "",
            )
        except Exception as exc:  # noqa: BLE001 - preserve transport failure
            error = f"{type(exc).__name__}: {exc}"
            if attempt < attempts:
                time.sleep(min(2**attempt, 8))
    return RemoteTarget(
        contract.venue,
        contract.dataset,
        contract.symbol,
        day.isoformat(),
        url,
        relative,
        False,
        0,
        "",
        "",
        "",
        error,
    )


def build_plan(
    *,
    base_url: str,
    contracts: Sequence[Contract],
    days: Sequence[date],
    workers: int,
    timeout_s: float,
    attempts: int,
) -> list[RemoteTarget]:
    requests_to_make = [
        (contract, target_day) for contract in contracts for target_day in days
    ]
    rows: list[RemoteTarget] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _head_one,
                base_url,
                contract,
                target_day,
                timeout_s=timeout_s,
                attempts=attempts,
            ): (contract, target_day)
            for contract, target_day in requests_to_make
        }
        for future in concurrent.futures.as_completed(futures):
            rows.append(future.result())
    return sorted(rows, key=lambda row: (row.day, row.venue, row.dataset, row.symbol))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_zstd(path: Path) -> dict[str, Any]:
    decompressed_bytes = 0
    newline_count = 0
    prefix = bytearray()
    tail = bytearray()
    with path.open("rb") as raw:
        with zstandard.ZstdDecompressor().stream_reader(raw) as reader:
            for chunk in iter(lambda: reader.read(8 * 1024 * 1024), b""):
                decompressed_bytes += len(chunk)
                newline_count += chunk.count(b"\n")
                if len(prefix) < 256 * 1024:
                    prefix.extend(chunk[: 256 * 1024 - len(prefix)])
                tail.extend(chunk)
                if len(tail) > 256 * 1024:
                    del tail[: len(tail) - 256 * 1024]
    if decompressed_bytes <= 0:
        raise RuntimeError(f"empty zstd payload: {path}")
    prefix_lines = bytes(prefix).splitlines()
    tail_lines = bytes(tail).splitlines()
    if len(prefix_lines) < 2 or not tail_lines:
        raise RuntimeError(f"zstd payload lacks CSV header/data rows: {path}")
    ends_with_newline = bool(tail.endswith(b"\n"))
    physical_lines = newline_count + (0 if ends_with_newline else 1)
    return {
        "decompressed_bytes": decompressed_bytes,
        "csv_rows": max(0, physical_lines - 1),
        "header": prefix_lines[0].decode("utf-8"),
        "first_data_row": prefix_lines[1].decode("utf-8"),
        "last_data_row": next(
            line.decode("utf-8") for line in reversed(tail_lines) if line
        ),
    }


def _existing_bytes(target: RemoteTarget, output_root: Path) -> int:
    final = output_root / target.relative_path
    part = final.with_suffix(final.suffix + ".part")
    if final.is_file():
        size = final.stat().st_size
        return min(size, target.content_length)
    if part.is_file():
        return min(part.stat().st_size, target.content_length)
    return 0


def _space_preflight(
    targets: Sequence[RemoteTarget],
    output_root: Path,
    *,
    reserve_gib: float,
    factor: float,
    min_free_gib: float,
) -> dict[str, float | int]:
    output_root.mkdir(parents=True, exist_ok=True)
    missing_bytes = sum(
        max(0, target.content_length - _existing_bytes(target, output_root))
        for target in targets
        if target.exists
    )
    free_bytes = shutil.disk_usage(output_root).free
    required_bytes = int(reserve_gib * GIB + factor * missing_bytes)
    absolute_floor = int(min_free_gib * GIB)
    if free_bytes < absolute_floor or free_bytes < required_bytes:
        raise RuntimeError(
            "insufficient space for Tardis admission: "
            f"free={free_bytes} missing={missing_bytes} required={required_bytes} "
            f"absolute_floor={absolute_floor}"
        )
    return {
        "free_bytes": free_bytes,
        "missing_bytes": missing_bytes,
        "required_bytes": required_bytes,
        "min_free_bytes": absolute_floor,
        "space_factor": factor,
        "reserve_bytes": int(reserve_gib * GIB),
    }


def _download_one(
    target: RemoteTarget,
    output_root: Path,
    *,
    timeout_s: float,
    attempts: int,
) -> dict[str, Any]:
    final = output_root / target.relative_path
    part = final.with_suffix(final.suffix + ".part")
    final.parent.mkdir(parents=True, exist_ok=True)
    if final.is_file() and final.stat().st_size == target.content_length:
        status = "existing"
    else:
        if final.exists():
            if part.exists():
                part.unlink()
            final.replace(part)
        status = "downloaded"
        error = ""
        for attempt in range(1, attempts + 1):
            try:
                offset = part.stat().st_size if part.exists() else 0
                headers = {"Range": f"bytes={offset}-"} if offset else {}
                with requests.get(
                    target.url,
                    headers=headers,
                    stream=True,
                    timeout=(15.0, timeout_s),
                ) as response:
                    if offset and response.status_code == 200:
                        part.unlink(missing_ok=True)
                        offset = 0
                    elif offset and response.status_code != 206:
                        response.raise_for_status()
                        raise RuntimeError(
                            f"range resume returned HTTP {response.status_code}"
                        )
                    else:
                        response.raise_for_status()
                    mode = "ab" if offset else "wb"
                    with part.open(mode) as handle:
                        for chunk in response.iter_content(4 * 1024 * 1024):
                            if chunk:
                                handle.write(chunk)
                if part.stat().st_size != target.content_length:
                    raise RuntimeError(
                        f"size mismatch: {part.stat().st_size} != {target.content_length}"
                    )
                os.replace(part, final)
                break
            except Exception as exc:  # noqa: BLE001 - retries preserve partial bytes
                error = f"{type(exc).__name__}: {exc}"
                if attempt == attempts:
                    raise RuntimeError(f"download failed for {target.url}: {error}") from exc
                time.sleep(min(2**attempt, 15))
    sha256 = _sha256(final)
    zstd_validation = _validate_zstd(final)
    return {
        **asdict(target),
        "path": str(final.resolve()),
        "status": status,
        "size_bytes": final.stat().st_size,
        "sha256": sha256,
        "zstd_valid": True,
        **zstd_validation,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--start", type=_parse_day, required=True)
    parser.add_argument("--end", type=_parse_day, required=True)
    parser.add_argument(
        "--contract",
        action="append",
        type=_parse_contract,
        required=True,
        help="Repeat VENUE,DATASET,SYMBOL",
    )
    parser.add_argument("--head-workers", type=int, default=12)
    parser.add_argument("--download-workers", type=int, default=2)
    parser.add_argument("--timeout-s", type=float, default=180.0)
    parser.add_argument("--attempts", type=int, default=5)
    parser.add_argument("--reserve-gib", type=float, default=60.0)
    parser.add_argument("--space-factor", type=float, default=2.5)
    parser.add_argument("--min-free-gib", type=float, default=50.0)
    parser.add_argument("--allow-missing", action="store_true")
    parser.add_argument("--plan-only", action="store_true")
    parser.add_argument("--manifest", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    output_root = args.output_root.expanduser().resolve()
    plan = build_plan(
        base_url=args.base_url,
        contracts=args.contract,
        days=_days(args.start, args.end),
        workers=max(1, args.head_workers),
        timeout_s=args.timeout_s,
        attempts=max(1, args.attempts),
    )
    available = [target for target in plan if target.exists]
    missing = [target for target in plan if not target.exists]
    if missing and not args.allow_missing:
        sample = ", ".join(target.relative_path for target in missing[:5])
        raise RuntimeError(f"{len(missing)} remote targets unavailable: {sample}")
    space = _space_preflight(
        available,
        output_root,
        reserve_gib=args.reserve_gib,
        factor=args.space_factor,
        min_free_gib=args.min_free_gib,
    )
    manifest = args.manifest or (
        output_root
        / "manifests"
        / f"tardis_{args.start:%Y%m%d}_{args.end:%Y%m%d}.json"
    )
    payload: dict[str, Any] = {
        "schema_version": "narrowgate.tardis_archive_admission.v1",
        "base_url": args.base_url,
        "output_root": str(output_root),
        "contracts": [asdict(contract) for contract in args.contract],
        "start": args.start.isoformat(),
        "end": args.end.isoformat(),
        "space_preflight": space,
        "available_targets": len(available),
        "missing_targets": len(missing),
        "plan": [asdict(target) for target in plan],
        "downloads": [],
        "complete": False,
    }
    _atomic_json(payload, manifest)
    print(
        json.dumps(
            {
                "available_targets": len(available),
                "missing_targets": len(missing),
                "remote_bytes": sum(target.content_length for target in available),
                "space_preflight": space,
                "manifest": str(manifest),
            },
            sort_keys=True,
        ),
        flush=True,
    )
    if args.plan_only:
        return 0
    downloads: list[dict[str, Any]] = []
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max(1, args.download_workers)
    ) as executor:
        futures = {
            executor.submit(
                _download_one,
                target,
                output_root,
                timeout_s=args.timeout_s,
                attempts=max(1, args.attempts),
            ): target
            for target in available
        }
        for future in concurrent.futures.as_completed(futures):
            row = future.result()
            downloads.append(row)
            print(
                json.dumps(
                    {
                        "downloaded": row["relative_path"],
                        "size_bytes": row["size_bytes"],
                        "sha256": row["sha256"],
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            payload["downloads"] = sorted(
                downloads, key=lambda item: item["relative_path"]
            )
            _atomic_json(payload, manifest)
    payload["downloads"] = sorted(downloads, key=lambda item: item["relative_path"])
    payload["complete"] = len(downloads) == len(available)
    payload["completed_at_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    _atomic_json(payload, manifest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
