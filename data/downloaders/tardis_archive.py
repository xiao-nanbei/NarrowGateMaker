#!/usr/bin/env python3
"""Plan, download, and integrity-check a bounded Tardis historical delivery.

Purchased originals retain the delivery hierarchy. Current market inputs and
their converted derivatives use Tardis only. The legacy planner resolves remote
sizes up front. Private --delivery-config --archive-only runs
instead stream bounded concurrent downloads, retain resumable per-file state,
and reserve disk only for active transfers; they never publish canonical data.

This command downloads only an explicitly authorized purchase; it never buys
data or silently repairs it with another provider. Archive completion does not
assert a gap-free exchange calendar or economic admission.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import fcntl
import hashlib
import json
import lzma
import os
import re
import shutil
import signal
import sys
import tempfile
import threading
import time
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from email.message import Message
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

import requests
import zstandard

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_paths import daily_market_path, marketdata_root, raw_data_root, tardis_raw_root  # noqa: E402

DEFAULT_BASE_URL = "https://data.yutsing.work/0730-beinan/tardis"
# Historical manifest resolution only; never a destination for new downloads.
HISTORICAL_ARTIFACT_ROOT = raw_data_root() / ".incoming" / "tardis"
LEGACY_OUTPUT_ROOT = marketdata_root() / "tardis" / "0730-beinan"
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
    relocated = HISTORICAL_ARTIFACT_ROOT / relative
    return relocated if relocated.exists() else candidate


def _archive_output_root(explicit: Path | None) -> Path:
    """Require one selected retained batch, without inventing a staging root."""
    selected = explicit if explicit is not None else tardis_raw_root()
    if selected is None:
        raise ValueError("set --output-root or configure the retained purchased archive root")
    return Path(selected).expanduser().resolve()


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
        handle.flush()
        os.fsync(handle.fileno())
        temporary = Path(handle.name)
    os.replace(temporary, path)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


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


def _finish_book_retirement(row: Mapping[str, Any]) -> dict[str, Any]:
    import pyarrow.parquet as pq

    from data.daily_raw import sha256_file

    source = Path(str(row["path"]))
    stage = Path(str(row["converted_source_path"]))
    if row.get("retirement_intent") is not True or stage != source.with_suffix(".fusion-input.parquet"):
        raise ValueError("Tardis retirement must use the recorded archive and its own converted staging path")
    if row.get("boundary_status") == "BOUNDARY_PENDING":
        raise ValueError("Tardis retirement cannot precede native boundary verification")
    canonical = daily_market_path(str(row["day"]), str(row["symbol"]), "incremental_book_L2")
    metadata = pq.ParquetFile(canonical).metadata.metadata or {}
    included = json.loads(metadata.get(b"narrowgate.included_sources", b"[]"))
    if not any(item.get("source_id") == "tardis"
               and item.get("sha256") == row["converted_source_sha256"] for item in included):
        raise ValueError("Canonical raw day no longer includes the Tardis retirement source")
    sources = [(source, row["sha256"]), (stage, row["converted_source_sha256"])]
    if row.get("crypto_context_path"):
        context = Path(row["crypto_context_path"])
        if context != source.with_suffix(".cryptohft-context.parquet"):
            raise ValueError("Tardis retirement cannot remove an unrelated native context")
        if not any(item.get("source_id") == "cryptohft"
                   and item.get("sha256") == row["crypto_context_sha256"] for item in included):
            raise ValueError("Canonical raw day no longer includes the native boundary context")
        sources.append((context, row["crypto_context_sha256"]))
    for path, expected in sources:
        if path.exists() and sha256_file(path) != expected:
            raise ValueError("Tardis input changed before source retirement")
    for path, _ in sources:
        path.unlink(missing_ok=True)
    return {**row, "archive_retired": True}


def _publish_downloaded_book(row: Mapping[str, Any], *, keep_archive: bool,
                             persist=None) -> dict[str, Any]:
    """Publish a verified delivery through the same daily fusion as new hours.

    Other datasets/markets remain archive-only. A publication failure retains
    both the delivery and converted input; downstream derived outputs are not
    implicitly declared rebuilt.
    """
    if (row["venue"], row["dataset"], row["symbol"]) != ("binance-futures", "incremental_book_L2", "BTCUSDC"):
        return dict(row)
    from data.daily_raw import convert_day_channel, fuse_orderbook_day, sha256_file
    from data.downloaders.cryptohft_orderbook import _crypto_boundary_context
    import pyarrow.parquet as pq

    source = Path(str(row["path"]))
    if sha256_file(source) != row["sha256"]:
        raise ValueError("Tardis delivery changed before canonical publication")
    stage = source.with_suffix(".fusion-input.parquet")
    day, symbol = str(row["day"]), str(row["symbol"])
    converted = convert_day_channel(source, stage, day, "incremental_book_L2", symbol)
    canonical = daily_market_path(day, symbol, "incremental_book_L2")
    crypto_present = False
    original_crypto = False
    if canonical.is_file():
        old = pq.ParquetFile(canonical)
        metadata = old.metadata.metadata or {}
        crypto_present = any(item.get("source_id") == "cryptohft"
                             for item in json.loads(metadata.get(b"narrowgate.included_sources", b"[]")))
        if not crypto_present and {"received_time", "event_time"} <= set(old.schema_arrow.names):
            first = next(old.iter_batches(columns=["received_time", "event_time"], batch_size=1024), None)
            crypto_present = first is not None and any(column.null_count < first.num_rows for column in first.columns)
        original_crypto = crypto_present and not metadata.get(b"narrowgate.book_fusion")
    next_day = (date.fromisoformat(day) + timedelta(days=1)).isoformat()
    next_crypto_stage = (raw_data_root() / ".incoming" / "cryptohft_fusion" / "binance_futures"
                         / symbol / "incremental_book_L2" / f"{next_day}.parquet")
    # A converted staging file is not trusted merely because it exists. The
    # Crypto downloader admits its own exact 24-hour export before exposing it.
    if next_crypto_stage.is_file():
        receipt = next_crypto_stage.with_suffix(".json")
        saved = json.loads(receipt.read_text()) if receipt.is_file() else {}
        if (saved.get("day") != next_day or saved.get("symbol") != symbol
                or saved.get("round_trip_all_original_columns") is not True
                or [entry["hour"] for entry in saved.get("sources", [])] != list(range(24))
                or saved.get("output_sha256") != sha256_file(next_crypto_stage)):
            raise ValueError("Next-day CryptoHFT staging lacks a matching complete export")
    context = (_crypto_boundary_context(day, symbol, next_stage=next_crypto_stage) if crypto_present else
               {"status": "TARDIS_UTC_SOURCE_ONLY", "next_sources": {}})
    # An incoming Tardis day must not overwrite the sole still-needed native
    # stream while its next capture-day prefix is missing. Keep this temporary
    # exact context until the same verified retirement intent covers it.
    crypto_context = source.with_suffix(".cryptohft-context.parquet")
    context_fields = {}
    if row.get("crypto_context_path"):
        if Path(row["crypto_context_path"]) != crypto_context:
            raise ValueError("Retained CryptoHFT boundary context has an unrelated path")
        if not crypto_context.exists() and canonical.is_file() and sha256_file(canonical) == row["crypto_context_sha256"]:
            try:
                os.link(canonical, crypto_context)
            except OSError:
                shutil.copyfile(canonical, crypto_context)
        if not crypto_context.is_file() or sha256_file(crypto_context) != row["crypto_context_sha256"]:
            raise ValueError("Retained CryptoHFT boundary context changed or is missing")
        context_fields = {"crypto_context_path": str(crypto_context),
                          "crypto_context_sha256": row["crypto_context_sha256"]}
    elif crypto_context.exists():
        raise ValueError("Unrecorded CryptoHFT boundary context must be reconciled before replacement")
    elif original_crypto and context["status"] == "BOUNDARY_PENDING":
        expected = sha256_file(canonical)
        context_fields = {"crypto_context_path": str(crypto_context), "crypto_context_sha256": expected}
        if persist is None:
            raise ValueError("Pending native context requires durable ingestion progress")
        persist({**row, **context_fields, "boundary_status": "BOUNDARY_PENDING"})
        try:
            os.link(canonical, crypto_context)
        except OSError:
            shutil.copyfile(canonical, crypto_context)
        if sha256_file(crypto_context) != expected or sha256_file(canonical) != expected:
            raise ValueError("CryptoHFT context changed during temporary preservation")
    inputs = {"tardis": stage}
    if context_fields:
        inputs["cryptohft"] = crypto_context
    fused = fuse_orderbook_day(inputs, canonical, day, symbol=symbol,
                              next_sources=context["next_sources"])
    if (sha256_file(canonical) != fused["sha256"]
            or not any(item.get("source_id") == "tardis"
                       and item.get("sha256") == converted["sha256"]
                       and item.get("rows") == converted["rows"]
                       for item in fused["included_sources"])):
        raise ValueError("Canonical raw day does not include the verified Tardis delivery")
    result = {**row, **context_fields, "canonical_publication": fused,
              "converted_source_sha256": converted["sha256"],
              "converted_source_path": str(stage),
              "boundary_status": context["status"],
              "derived_rebuilt": False, "archive_retired": False}
    # Verify all owned sources before retiring either. An interrupted cleanup
    # leaves the canonical output and the exact remaining source untouched.
    if context["status"] == "BOUNDARY_PENDING":
        result["status"] = "PUBLISHED_BOUNDARY_PENDING"
        if persist is not None:
            persist(result)
        return result
    if not keep_archive:
        if sha256_file(source) != row["sha256"] or sha256_file(stage) != converted["sha256"]:
            raise ValueError("Tardis input changed before source retirement")
        if persist is None:
            raise ValueError("Tardis retirement requires durable publication of its intent")
        result["retirement_intent"] = True
        persist(result)
        result = _finish_book_retirement(result)
    return result


class _DeliveryFailure(RuntimeError):
    """Only fixed, URL-free operational codes may escape delivery transport."""


def _delivery_codec(response) -> str:
    message = Message()
    message["Content-Disposition"] = response.headers.get("Content-Disposition", "")
    names = [message.get_filename() or "", unquote(urlsplit(response.url).path).rsplit("/", 1)[-1]]
    codecs = []
    for name in names:
        match = re.search(r"\.csv\.(xz|zst|zstd)$", name, re.IGNORECASE)
        if match:
            codecs.append(match.group(1).lower())
    # Content-Disposition describes the delivered object; the stable route may
    # deliberately end in .xz even when the actual object is zstd.
    if not codecs:
        raise _DeliveryFailure("unsupported_compression_identity")
    return codecs[0]


def _validate_archive(path: Path, codec: str) -> dict[str, Any]:
    """Check every frame and its EOF, without materializing a decompressed CSV.

    stream_reader/LZMAFile alone can accept a truncated final frame or ignore
    trailing garbage. Per-frame decoders make that failure explicit. Zstd feeds
    are deliberately tiny: decompressobj has no maximum-output-length argument.
    """
    magic = b"\xfd7zXZ\x00" if codec == "xz" else b"\x28\xb5\x2f\xfd"
    total = newlines = frames = 0
    prefix = bytearray()
    last_byte = b""
    with path.open("rb") as handle:
        if handle.read(len(magic)) != magic:
            raise _DeliveryFailure("compression_magic_mismatch")
        handle.seek(0)
        decoder = None
        padding = 0
        while True:
            pending = handle.read(65536 if codec == "xz" else 256)
            if not pending:
                break
            while pending:
                if decoder is None or decoder.eof:
                    if codec == "xz" and frames:
                        count = len(pending) - len(pending.lstrip(b"\x00"))
                        padding += count
                        pending = pending[count:]
                        if not pending:
                            continue
                        if padding % 4:
                            raise _DeliveryFailure("invalid_xz_stream_padding")
                        padding = 0
                    decoder = (lzma.LZMADecompressor(format=lzma.FORMAT_XZ, memlimit=256 * 1024**2)
                               if codec == "xz" else zstandard.ZstdDecompressor(
                                   max_window_size=256 * 1024**2).decompressobj(read_across_frames=False))
                    frames += 1
                output = (decoder.decompress(pending, max_length=1024 * 1024)
                          if codec == "xz" else decoder.decompress(pending))
                pending = b""
                while True:
                    total += len(output)
                    newlines += output.count(b"\n")
                    if len(prefix) < 256 * 1024:
                        prefix.extend(output[:256 * 1024 - len(prefix)])
                    if output:
                        last_byte = output[-1:]
                    if decoder.eof:
                        pending = decoder.unused_data
                        break
                    if codec != "xz" or decoder.needs_input:
                        break
                    output = decoder.decompress(b"", max_length=1024 * 1024)
        if decoder is None or not decoder.eof or padding % 4:
            raise _DeliveryFailure("incomplete_compression_frame")
    lines = bytes(prefix).splitlines()
    if len(lines) < 2 or b"," not in lines[0]:
        raise _DeliveryFailure("missing_csv_header_or_data")
    return {"compression": codec, "compression_verified": True,
            "decompressed_bytes": total,
            "physical_data_lines": newlines + int(last_byte != b"\n") - 1,
            "header": lines[0].decode("utf-8-sig"), "first_data_row": lines[1].decode("utf-8")}


def _validate_delivery_content(path: Path, key: str) -> dict[str, Any]:
    """Scan every CSV row's market/values, independently of compression EOF.

    Arrival-date archives can contain adjacent exchange dates; report those
    rows and clock regressions, never silently remove them or claim admission.
    This does not reconstruct the book or prove missing events did not exist.
    """
    import numpy as np
    from data.normalize_tardis_orderbook import _open_csv

    venue, dataset, year, month, day, symbol = key.split("/")
    from datetime import datetime, timezone
    start = int(datetime(int(year), int(month), int(day), tzinfo=timezone.utc).timestamp()) * 1_000_000
    count = outside = regressions = 0
    first = last = previous = None
    maximum_gap = 0
    with _open_csv(path) as reader:
        for batch in reader:
            f = batch.to_pandas()
            if (not f.exchange.eq(venue).all() or not f.symbol.eq(symbol).all()
                    or not f.side.isin(('bid', 'ask') if dataset == 'incremental_book_L2' else ('buy', 'sell')).all()):
                raise _DeliveryFailure('delivered_csv_content_market_mismatch')
            ts = f.timestamp.to_numpy()
            p, q = f.price.to_numpy(dtype=float), f.amount.to_numpy(dtype=float)
            if (not np.issubdtype(ts.dtype, np.integer) or not (ts > 0).all()
                    or not (np.isfinite(p) & (p > 0) & np.isfinite(q) & (q >= 0)).all()
                    or (dataset == 'trades' and not (q > 0).all())):
                raise _DeliveryFailure('delivered_csv_content_invalid_values')
            if dataset == 'incremental_book_L2':
                if f.is_snapshot.dtype != bool or f.is_snapshot.isna().any():
                    raise _DeliveryFailure('delivered_csv_invalid_snapshot_flag')
            elif (not np.issubdtype(f.id.dtype, np.integer) or not (f.id >= 0).all()):
                raise _DeliveryFailure('delivered_csv_invalid_trade_identity')
            delta = np.diff(ts) if previous is None else np.diff(np.r_[previous, ts])
            regressions += int((delta < 0).sum())
            if len(delta):
                maximum_gap = max(maximum_gap, int(delta.max()))
            previous = int(ts[-1])
            first = int(ts.min()) if first is None else min(first, int(ts.min()))
            last = int(ts.max()) if last is None else max(last, int(ts.max()))
            outside += int(((ts < start) | (ts >= start + 86_400_000_000)).sum())
            count += len(f)
    if not count:
        raise _DeliveryFailure('empty_delivered_csv')
    return {'content_identity_verified': True, 'content_rows': count,
            'first_exchange_timestamp_us': first, 'last_exchange_timestamp_us': last,
            'exchange_clock_regressions': regressions,
            'adjacent_exchange_day_rows': outside,
            'maximum_recorded_exchange_gap_us': maximum_gap,
            'full_exchange_coverage_proven': False, 'economic_admission': False}


def _delivery_retry_after(headers, minimum: float) -> float:
    value = headers.get("Retry-After", "")
    try:
        delay = float(value)
    except (ValueError, TypeError):
        try:
            delay = parsedate_to_datetime(value).timestamp() - time.time()
        except (ValueError, TypeError, OverflowError):
            delay = 0.0
    return max(minimum, delay)


class _DeliveryBudget:
    """Reserve only active transfers, protecting shared disk and request pacing."""

    def __init__(self, root: Path, reserve_gib: float, requests_per_second: float):
        self.root = root
        self.reserve = int(reserve_gib * GIB)
        self.remaining: dict[str, int] = {}
        self.lock = threading.Lock()
        self.next_request = 0.0
        self.cooldown_until = 0.0
        self.gap = 1.0 / requests_per_second
        self.blocked = threading.Event()
        self.stopping = threading.Event()
        self.stop_reason = "delivery_paused"

    def wait(self):
        while not self.blocked.is_set() and not self.stopping.is_set():
            with self.lock:
                now = time.time()
                delay = max(self.next_request, self.cooldown_until) - now
                if delay <= 0:
                    self.next_request = now + self.gap
                    return
            self.stopping.wait(min(delay, 30))
        raise _DeliveryFailure("delivery_stop_requested" if self.stopping.is_set() else self.stop_reason)

    def cooldown(self, delay):
        with self.lock:
            self.cooldown_until = max(self.cooldown_until, time.time() + delay)

    def claim(self, key: str, remaining: int):
        with self.lock:
            if shutil.disk_usage(self.root).free < self.reserve + sum(self.remaining.values()) + remaining:
                raise _DeliveryFailure("insufficient_inflight_disk_space")
            self.remaining[key] = remaining

    def write(self, key: str, handle, chunk: bytes):
        with self.lock:
            own = self.remaining.get(key, 0)
            required = self.reserve + sum(self.remaining.values()) + max(0, len(chunk) - own)
            if shutil.disk_usage(self.root).free < required:
                raise _DeliveryFailure("insufficient_stream_disk_space")
            handle.write(chunk)
            self.remaining[key] = max(0, own - len(chunk))

    def release(self, key: str):
        with self.lock:
            self.remaining.pop(key, None)


def _delivery_contained(root: Path, relative: str) -> Path:
    path = root / relative
    if path.is_symlink() or path.absolute() != path.resolve():
        raise _DeliveryFailure("delivery_path_is_not_real")
    if not path.is_relative_to(root):
        raise _DeliveryFailure("delivery_path_escapes_root")
    return path


def _delivery_saved(root: Path, key: str) -> tuple[Path, dict]:
    state_path = _delivery_contained(root, ".delivery-state/" + key + ".json")
    state = json.loads(state_path.read_text()) if state_path.is_file() else {"key": key, "status": "queued"}
    if state.get("key") != key:
        raise _DeliveryFailure("delivery_state_identity_mismatch")
    return state_path, state


def _delivery_step(key: str, root: Path, base_url: str, config: dict, budget: _DeliveryBudget) -> dict:
    # Different explicitly partitioned processes may share the final archive tree.
    # Re-read durable state only after acquiring the per-object lock.
    lock_path = _delivery_contained(root, '.delivery-key-locks/' + key + '.lock')
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open('a+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        return _delivery_step_locked(key, root, base_url, config, budget)


def _delivery_step_locked(key: str, root: Path, base_url: str, config: dict, budget: _DeliveryBudget) -> dict:
    state_path, state = _delivery_saved(root, key)
    part = _delivery_contained(root, key + ".part")
    try:
        if state.get("status") == "completed":
            final = _delivery_contained(root, key + ".csv." + state["compression"])
            if (not final.is_file() or final.stat().st_size != state["size_bytes"]
                    or _sha256(final) != state["sha256"] or not state.get("compression_verified")):
                raise _DeliveryFailure("completed_archive_identity_mismatch")
            return state
        if config.get('assignment_name'):
            if any(_delivery_contained(root, key + '.csv.' + codec).exists()
                   for codec in ('xz', 'zst', 'zstd')):
                raise _DeliveryFailure('existing_archive_requires_validation')
            if state.get('next_attempt_at', 0) > time.time():
                return state
        budget.wait()
        offset = 0
        etag = state.get("etag", "")
        if (part.is_file() and not state.get("restart_required") and etag and not etag.startswith("W/")
                and state.get("total_bytes") is not None and state.get("part_bytes", 0) > 0):
            verified = state["part_bytes"]
            if part.stat().st_size >= verified:
                digest = hashlib.sha256()
                with part.open("rb") as handle:
                    remaining = verified
                    while remaining:
                        chunk = handle.read(min(4 * 1024**2, remaining))
                        digest.update(chunk)
                        remaining -= len(chunk)
                if digest.hexdigest() == state.get("part_sha256"):
                    with part.open("r+b") as handle:
                        handle.truncate(verified)
                    offset = verified
        headers = {"Accept-Encoding": "identity"}
        if offset:
            headers.update({"Range": f"bytes={offset}-", "If-Range": etag})
        # GET only: signed download routes may explicitly reject HEAD. Neither
        # response.url, Location nor transport exception strings are persisted.
        with requests.get(base_url.rstrip("/") + "/" + key + ".csv.xz", headers=headers,
                          stream=True, allow_redirects=True, timeout=(15.0, config["timeout_s"])) as response:
            code = response.status_code
            state["http_status"] = code
            if code in (401, 403, 409):
                budget.stop_reason = "delivery_authorization_blocked"
                budget.blocked.set()
                state.update(status="authorization_blocked", error_code=f"HTTP_{code}")
            elif code in (404, 410):
                state.update(status="unavailable", error_code=f"HTTP_{code}")
            elif code == 202 or code == 429 or 500 <= code < 600:
                counter = "polls" if code == 202 else "failures"
                state[counter] = state.get(counter, 0) + 1
                minimum = config["poll_interval"] if code == 202 else min(300.0, 2 ** state[counter])
                delay = _delivery_retry_after(response.headers, minimum)
                if code == 429:
                    budget.cooldown(delay)
                exhausted = state[counter] >= config["max_polls" if code == 202 else "attempts"]
                state.update(status="poll_limit" if exhausted and code == 202 else "failed" if exhausted else "pending",
                             error_code=f"HTTP_{code}", next_attempt_at=time.time() + delay)
            elif code not in (200, 206):
                raise _DeliveryFailure(f"unexpected_HTTP_{code}")
            else:
                codec = _delivery_codec(response)
                current_etag = response.headers.get("ETag", "").strip()
                length = response.headers.get("Content-Length")
                total = int(length) if length is not None else None
                if code == 206:
                    match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", response.headers.get("Content-Range", ""))
                    if not match:
                        raise _DeliveryFailure("invalid_content_range")
                    first, last, range_total = map(int, match.groups())
                    if (not offset or first != offset or last != range_total - 1 or range_total != state.get("total_bytes")
                            or current_etag != etag or codec != state.get("compression")
                            or (total is not None and total != last - first + 1)):
                        raise _DeliveryFailure("range_identity_mismatch")
                    total = range_total
                else:
                    offset = 0  # Full 200 after If-Range is a new object, never an append.
                if total is not None and total <= 0:
                    raise _DeliveryFailure("invalid_content_length")
                if response.headers.get("Content-Encoding", "identity").lower() not in ("", "identity"):
                    raise _DeliveryFailure("unexpected_content_encoding")
                budget.claim(key, max(0, total - offset) if total is not None else 0)
                part.parent.mkdir(parents=True, exist_ok=True)
                state.update(status="downloading", compression=codec, etag=current_etag, total_bytes=total,
                             restart_required=False, part_bytes=offset, next_attempt_at=0)
                _atomic_json(state, state_path)
                digest = hashlib.sha256()
                if offset:
                    with part.open("rb") as handle:
                        for chunk in iter(lambda: handle.read(4 * 1024**2), b""):
                            digest.update(chunk)
                with part.open("ab" if offset else "wb") as handle:
                    checkpoint_bytes = offset
                    for chunk in response.iter_content(4 * 1024**2):
                        if budget.stopping.is_set():
                            raise _DeliveryFailure("delivery_stop_requested")
                        if not chunk:
                            continue
                        if offset == 0:
                            magic = b"\xfd7zXZ\x00" if codec == "xz" else b"\x28\xb5\x2f\xfd"
                            if len(chunk) >= len(magic) and not chunk.startswith(magic):
                                raise _DeliveryFailure("compression_magic_mismatch")
                        budget.write(key, handle, chunk)
                        digest.update(chunk)
                        offset += len(chunk)
                        if offset - checkpoint_bytes >= 32 * 1024**2:
                            handle.flush()
                            os.fsync(handle.fileno())
                            state.update(part_bytes=offset, part_sha256=digest.hexdigest())
                            _atomic_json(state, state_path)
                            checkpoint_bytes = offset
                    handle.flush()
                    os.fsync(handle.fileno())
                if total is not None and offset != total:
                    raise _DeliveryFailure("download_size_mismatch")
                validation = _validate_archive(part, codec)
                columns = next(csv.reader([validation["header"]]))
                first_row = next(csv.reader([validation.pop("first_data_row")]))
                venue, dataset, *_, symbol = key.split("/")
                required = {"exchange", "symbol", "timestamp", "local_timestamp", "side", "price", "amount",
                            "is_snapshot" if dataset == "incremental_book_L2" else "id"}
                if (not required <= set(columns) or len(first_row) != len(columns)
                        or first_row[columns.index("exchange")] != venue
                        or first_row[columns.index("symbol")] != symbol):
                    raise _DeliveryFailure("delivered_csv_identity_mismatch")
                if config.get('validate_all_csv_identity'):
                    validation.update(_validate_delivery_content(part, key))
                final = _delivery_contained(root, key + ".csv." + codec)
                os.replace(part, final)
                directory = os.open(final.parent, os.O_RDONLY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
                state.update(validation, status="completed", size_bytes=offset, sha256=digest.hexdigest(),
                             completed_at_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), error_code="")
        _atomic_json(state, state_path)
        return state
    except Exception as exc:  # Never persist exception text, URLs or signed headers.
        code = str(exc) if isinstance(exc, _DeliveryFailure) else type(exc).__name__
        if code == "delivery_stop_requested":
            # A deliberate stop is neither a provider failure nor corruption.
            # Preserve validators and Retry-After so a later run can resume.
            state.update(status="pending", error_code=code)
        else:
            state["failures"] = state.get("failures", 0) + 1
            permanent = code in {"completed_archive_identity_mismatch", "existing_archive_requires_validation", "delivery_authorization_blocked",
                                 "delivery_paused", "insufficient_inflight_disk_space", "insufficient_stream_disk_space"}
            if code in {"insufficient_inflight_disk_space", "insufficient_stream_disk_space"}:
                budget.blocked.set()
            state.update(status="failed" if permanent or state["failures"] >= config["attempts"] else "pending",
                         error_code=code, next_attempt_at=time.time() + min(300, 2 ** state["failures"]))
            if isinstance(exc, (_DeliveryFailure, lzma.LZMAError, zstandard.ZstdError)):
                state["restart_required"] = True
        if part.is_file():
            with part.open("rb") as handle:
                os.fsync(handle.fileno())
            state.update(part_bytes=part.stat().st_size, part_sha256=_sha256(part))
        _atomic_json(state, state_path)
        return state
    finally:
        budget.release(key)


def _delivery_keys(config: Mapping[str, Any]) -> list[str]:
    """Prioritize work without changing target identity or the full denominator.

    ``priority_ranges`` contains ordered inclusive ``start``/``end`` objects.
    ``priority_contracts`` contains configured VENUE,DATASET,SYMBOL strings.
    Across the union of priority ranges, listed contracts precede other
    contracts, then the declared range order applies. Thus another channel on
    the first priority day cannot delay the requested channel on later days.
    Unlisted dates follow in their original day order. Without priorities the
    original day order stays.
    """
    contracts = list(dict.fromkeys(_parse_contract(value) for value in config["contracts"]))
    days = _days(_parse_day(config["start"]), _parse_day(config["end"]))
    ranges = []
    for item in config.get("priority_ranges", []):
        start, end = _parse_day(item["start"]), _parse_day(item["end"])
        if end < start or start < days[0] or end > days[-1]:
            raise _DeliveryFailure("priority_range_outside_delivery_calendar")
        ranges.append((start, end))
    preferred = [_parse_contract(value) for value in config.get("priority_contracts", [])]
    if len(set(preferred)) != len(preferred) or not set(preferred) <= set(contracts):
        raise _DeliveryFailure("priority_contract_not_unique_or_not_in_delivery")
    ranks = {contract: rank for rank, contract in enumerate(preferred)}
    targets = [(day, contract) for day in days for contract in contracts]
    def priority(item):
        day, contract = item
        range_rank = next((rank for rank, (start, end) in enumerate(ranges)
                           if start <= day <= end), len(ranges))
        if ranges and range_rank == len(ranges):
            return (1, 0, 0, day)
        return (0, ranks.get(contract, len(preferred)), range_rank, day)

    targets.sort(key=priority)
    keys = [f"{contract.venue}/{contract.dataset}/{day:%Y/%m/%d}/{contract.symbol}"
            for day, contract in targets]
    universe = set(keys)
    for field in ("include_keys", "exclude_keys"):
        if field in config:
            selected = config[field]
            if (not isinstance(selected, list) or not all(isinstance(key, str) for key in selected)
                    or len(selected) != len(set(selected)) or not set(selected) <= universe):
                raise _DeliveryFailure("invalid_delivery_assignment")
    included = set(config.get("include_keys", keys))
    excluded = set(config.get("exclude_keys", []))
    return [key for key in keys if key in included and key not in excluded]


@contextmanager
def _delivery_assignment(root: Path, name: str | None):
    if name is not None and (not isinstance(name, str) or not re.fullmatch(r'[a-z0-9_-]{1,64}', name)):
        raise _DeliveryFailure('invalid_delivery_assignment_name')
    with _delivery_contained(root, '.delivery.lock').open('a+') as global_lock:
        try:
            fcntl.flock(global_lock, (fcntl.LOCK_SH if name else fcntl.LOCK_EX) | fcntl.LOCK_NB)
        except BlockingIOError:
            raise _DeliveryFailure('delivery_already_running') from None
        if name is None:
            yield root / 'manifest.json'
        else:
            with _delivery_contained(root, f'.delivery-{name}.lock').open('a+') as assignment_lock:
                try:
                    fcntl.flock(assignment_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    raise _DeliveryFailure('delivery_assignment_already_running') from None
                yield root / f'manifest-{name}.json'


def _delivery_main(args) -> int:
    if not args.archive_only:
        raise _DeliveryFailure("delivery_config_requires_archive_only")
    config_path = args.delivery_config.expanduser()
    if config_path.is_symlink() or config_path.stat().st_mode & 0o077:
        raise _DeliveryFailure("delivery_config_requires_private_real_file")
    config = json.loads(config_path.read_text())
    base_url = config.pop("base_url")
    parsed = urlsplit(base_url)
    if parsed.scheme != "https" or not parsed.netloc or parsed.query or parsed.fragment or parsed.username:
        raise _DeliveryFailure("invalid_private_delivery_base_url")
    root = Path(args.output_root or config["output_root"]).expanduser().absolute()
    if root != root.resolve():
        raise _DeliveryFailure("delivery_root_is_not_real")
    keys = _delivery_keys(config)
    for field, default in (("workers", 4), ("reserve_gib", 40), ("poll_interval", 300),
                           ("max_polls", 288), ("attempts", 5), ("timeout_s", 180), ("requests_per_second", 2)):
        config.setdefault(field, default)
    if args.poll_interval is not None:
        config["poll_interval"] = args.poll_interval
    if args.max_polls is not None:
        config["max_polls"] = args.max_polls
    if any(float(config[field]) <= 0 for field in ("workers", "poll_interval", "max_polls", "attempts", "timeout_s", "requests_per_second")) or config["reserve_gib"] < 0:
        raise _DeliveryFailure("invalid_delivery_resource_bounds")
    previous_umask = os.umask(0o077)
    try:
        root.mkdir(parents=True, exist_ok=True)
        with _delivery_assignment(root, config.get('assignment_name')) as manifest_path:
            budget = _DeliveryBudget(root, config["reserve_gib"], config["requests_per_second"])
            states = {key: _delivery_saved(root, key)[1] for key in keys}
            # Completed objects get one local SHA verification, but no request.
            terminal = {"unavailable", "authorization_blocked", "failed", "poll_limit"}
            verified = set()
            workers = min(16, int(config["workers"]))
            futures = {}
            stop_signal = 0

            def stop(signum, _frame):
                nonlocal stop_signal
                stop_signal = signum
                budget.stopping.set()

            def save_progress():
                counts = {status: sum(row.get("status") == status for row in states.values())
                          for status in sorted({row.get("status", "ready") for row in states.values()})}
                _atomic_json({"schema_version": "narrowgate.private_delivery.v1", "archive_only": True,
                              "start": config["start"], "end": config["end"], "targets": len(keys),
                              "counts": counts, "complete": len(verified) == len(keys),
                              "priority_ranges": config.get("priority_ranges", []),
                              "priority_contracts": config.get("priority_contracts", []),
                              "assignment_only": "include_keys" in config or bool(config.get("exclude_keys")),
                              "excluded_targets": len(config.get("exclude_keys", [])),
                              "active_transfers": len(futures), "stop_signal": stop_signal}, manifest_path)
                return counts

            handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
            try:
                for sig in handlers:
                    signal.signal(sig, stop)
                save_progress()
                with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                    try:
                        while True:
                            if not budget.stopping.is_set() and not budget.blocked.is_set():
                                active = set(futures.values())
                                prior_active = len(futures)
                                # Unchecked targets (deadline zero) precede retries.
                                # Oldest due retry goes next; stable ties retain the
                                # configured symbol/date priority. A 202 must not
                                # repeatedly reclaim the head of the download queue.
                                due_order = sorted(states, key=lambda key: (
                                    states[key].get("status") != "queued",
                                    states[key].get("next_attempt_at", 0)))
                                ready = (key for key in due_order if key not in verified and key not in active
                                         and (state := states[key]).get("status") not in terminal
                                         and state.get("next_attempt_at", 0) <= time.time())
                                for key in ready:
                                    if len(futures) >= workers or budget.stopping.is_set() or budget.blocked.is_set():
                                        break
                                    futures[executor.submit(_delivery_step, key, root, base_url, config, budget)] = key
                                if len(futures) != prior_active:
                                    save_progress()
                            if futures:
                                done, _ = concurrent.futures.wait(futures, timeout=0.5,
                                                                  return_when=concurrent.futures.FIRST_COMPLETED)
                                for future in done:
                                    key = futures.pop(future)
                                    states[key] = future.result()
                                    if states[key].get("status") == "completed":
                                        verified.add(key)
                                    counts = save_progress()
                                    print(json.dumps({"file": key, "status": states[key]["status"], "counts": counts}), flush=True)
                                continue
                            waiting = [state for key, state in states.items() if key not in verified
                                       and state.get("status") not in terminal]
                            if not waiting or budget.blocked.is_set() or budget.stopping.is_set():
                                break
                            budget.stopping.wait(min(30, max(0.01, min(state.get("next_attempt_at", 0)
                                                                      for state in waiting) - time.time())))
                    finally:
                        budget.stopping.set()
                save_progress()
            finally:
                for sig, handler in handlers.items():
                    signal.signal(sig, handler)
            return 128 + stop_signal if stop_signal else 0 if len(verified) == len(keys) else 2
    finally:
        os.umask(previous_umask)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--start", type=_parse_day)
    parser.add_argument("--end", type=_parse_day)
    parser.add_argument("--delivery-config", type=Path, help="Private JSON for resumable archive-only delivery")
    parser.add_argument("--archive-only", action="store_true", help="Never publish, fuse or retire canonical inputs")
    parser.add_argument("--poll-interval", type=float)
    parser.add_argument("--max-polls", type=int)
    parser.add_argument(
        "--contract",
        action="append",
        type=_parse_contract,
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
    parser.add_argument("--keep-book-archives", action="store_true",
                        help="Keep book archives after verified canonical fusion (other channels are always retained)")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.delivery_config:
        try:
            return _delivery_main(args)
        except Exception as exc:
            code = str(exc) if isinstance(exc, _DeliveryFailure) else type(exc).__name__
            print(json.dumps({"status": "failed", "error_code": code}), flush=True)
            return 2
    if args.start is None or args.end is None or not args.contract:
        _parser().error("--start, --end and --contract are required without --delivery-config")
    output_root = _archive_output_root(args.output_root)
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
    previous = json.loads(manifest.read_text()) if manifest.is_file() else {}
    previous_rows = {row["relative_path"]: row for row in previous.get("downloads", [])}
    target_paths = {target.relative_path for target in available}
    boundary_pending = [] if args.archive_only else [row for key, row in previous_rows.items()
                        if key not in target_paths and row.get("boundary_status") == "BOUNDARY_PENDING"
                        and (row.get("venue"), row.get("dataset"), row.get("symbol"))
                        == ("binance-futures", "incremental_book_L2", "BTCUSDC")
                        and row.get("path") == str((output_root / key).resolve())]
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
        "downloads": [previous_rows[target.relative_path] for target in available
                      if target.relative_path in previous_rows] + boundary_pending,
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
    downloads: list[dict[str, Any]] = list(payload["downloads"])

    def persist_download(row):
        downloads[:] = [item for item in downloads if item["relative_path"] != row["relative_path"]]
        downloads.append(dict(row))
        payload["downloads"] = sorted(downloads, key=lambda item: item["relative_path"])
        _atomic_json(payload, manifest)

    pending = []
    for target in available:
        old = previous_rows.get(target.relative_path, {})
        same_remote = bool(target.etag and old.get("etag") == target.etag
                           and old.get("content_length") == target.content_length
                           and old.get("url") == target.url
                           and old.get("path") == str((output_root / target.relative_path).resolve())
                           and all(old.get(key) == getattr(target, key)
                                   for key in ("venue", "dataset", "symbol", "day")))
        if not args.archive_only and old.get("retirement_intent") and same_remote:
            persist_download(_finish_book_retirement(old))
        else:
            pending.append(target)
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
            for target in pending
        }
        for future in concurrent.futures.as_completed(futures):
            row = future.result()
            old = previous_rows.get(row["relative_path"], {})
            row = {**{key: old[key] for key in ("crypto_context_path", "crypto_context_sha256") if key in old}, **row}
            # Save the accessible source first, including when fusion fails.
            persist_download(row)
            published = (row if args.archive_only else _publish_downloaded_book(
                row, keep_archive=args.keep_book_archives, persist=persist_download))
            persist_download(published)
            row = published
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
    # Refresh the immediately retained ingestion context, without redownloading
    # a prior delivery or expanding the requested remote date range.
    for old in sorted(boundary_pending, key=lambda item: item["day"]):
        persist_download(_publish_downloaded_book(old, keep_archive=args.keep_book_archives,
                                                 persist=persist_download))
    payload["downloads"] = sorted(downloads, key=lambda item: item["relative_path"])
    payload["downloads_complete"] = target_paths <= {row["relative_path"] for row in downloads}
    payload["boundary_pending_days"] = sorted({row["day"] for row in downloads
                                                if row.get("boundary_status") == "BOUNDARY_PENDING"})
    payload["complete"] = payload["downloads_complete"] and not payload["boundary_pending_days"]
    payload["completed_at_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    _atomic_json(payload, manifest)
    return 0 if payload["complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
