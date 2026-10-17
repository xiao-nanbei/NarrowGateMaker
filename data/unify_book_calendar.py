"""Convert a fixed current book calendar, one verified atomic day at a time.

Raw events and generated views stay in their existing separate directories.
This operation does not run a strategy, infer trades, or grant research use.
"""
from __future__ import annotations

import argparse
import copy
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import shutil
import tempfile
import time

import numpy as np
import pyarrow.parquet as pq

from data.daily_raw import DAILY_BOOK_MARKER, unify_orderbook_day
from data.daily_schema_cutover import (identity, prepare_transaction, publish_transaction,
                                      planned_files, rebind_csv, rebind_json, usage_digest)
from data.trade_union_cutover import _days, _read, _sync_directory, _write


RAW_ID = "btcusdc-daily-raw-l2"
BOOK_ID = "btcusdc-baseline401-selected-book"


def _now():
    return datetime.now(timezone.utc).isoformat()


def _dataset(owner, name):
    matches = [item for item in owner["datasets"] if item["id"] == name]
    if len(matches) != 1:
        raise ValueError("current catalog lacks a unique daily dataset")
    return matches[0]


def _write_parquet(table, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, compression="zstd")
    if not pq.read_table(path).equals(table, check_metadata=False):
        raise ValueError("daily view did not round-trip")
    return identity(path)


def _grid_predecessor(day: str, previous: tuple[int, int] | None) -> dict:
    """A sampled-grid predecessor is not the book's last consumed message."""
    start = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp())*1_000_000
    if previous is not None and (
            len(previous) != 2 or any(type(value) is not int for value in previous)
            or not 0 < previous[1] <= previous[0] < start):
        raise ValueError("invalid verified previous-grid clock boundary")
    return {"status": "VERIFIED_PREVIOUS_GRID" if previous is not None else "NO_VERIFIED_PREDECESSOR",
            "previous": list(previous) if previous is not None else None,
            "scope": "sampled_grid_only; deep_and_top_continuity_require_separate_evidence"}


def _grid_tail(day: str, grid: dict) -> tuple[int, int] | None:
    if not {"last_output_us", "last_observed_us"} <= grid.keys():
        raise ValueError("verified publication lacks its sampled-grid tail")
    values = grid["last_output_us"], grid["last_observed_us"]
    if values == (None, None):
        return None
    validated_day = _days(day, day)[0]
    end = int(datetime.fromisoformat(validated_day).replace(tzinfo=timezone.utc).timestamp())*1_000_000 + 86_400_000_000
    if any(type(value) is not int for value in values) or not 0 < values[1] <= values[0] < end:
        raise ValueError("verified publication has an invalid sampled-grid tail")
    return values


def _resume_grid_tail(state_root: Path, day: str, entry: dict) -> tuple[int, int] | None:
    if "observation_grid_tail" in entry:
        tail = entry["observation_grid_tail"]
        if tail is not None and (not isinstance(tail, (list, tuple)) or len(tail) != 2):
            raise ValueError("invalid saved sampled-grid tail")
        return _grid_tail(day, {"last_output_us": tail[0] if tail is not None else None,
                                "last_observed_us": tail[1] if tail is not None else None})
    # Historical statuses already bind this operation's verified publication.
    # Read its small journal, never load raw/state or substitute last_message.
    journal = _read(state_root / day / "publication.json")
    if (journal.get("day") != day or journal.get("status") != "FILES_PUBLISHED"
            or not journal.get("catalog_prepared") or journal.get("files") != entry["files"]):
        raise ValueError("previous-grid recovery requires the same verified operation publication")
    return _grid_tail(day, journal.get("observation_grid", {}))


def _resume_deep_state(state_root: Path, day: str, entry: dict) -> dict | None:
    """Read only the last required book state, from its immutable daily journal."""
    path = state_root / day / "publication.json"
    reference = entry.get("deep_final_state_ref")
    before = identity(path)
    if reference is not None and (reference.get("path") != str(path)
            or any(reference.get(key) != before[key] for key in ("sha256", "size_bytes"))):
        raise ValueError("saved daily continuation journal identity differs")
    journal = _read(path)
    if (journal.get("day") != day or journal.get("status") != "FILES_PUBLISHED"
            or not journal.get("catalog_prepared") or journal.get("files") != entry["files"]
            or identity(path)["sha256"] != before["sha256"]):
        raise ValueError("daily continuation requires the same verified publication")
    state = journal.get("deep_final_state")
    if "deep_final_state" in entry and entry["deep_final_state"] != state:
        raise ValueError("inline daily continuation differs from its verified journal")
    if reference is None:
        entry["deep_final_state_ref"] = {key: before[key] for key in ("path", "sha256", "size_bytes")}
    entry.pop("deep_final_state", None)
    return state


def prepare_day(data_root: Path, book_root: Path, stage: Path, day: str,
                previous_top_state: dict | None, *, supplement: Path | None = None,
                stream_id: str | None = None, previous_deep_state: dict | None = None,
                logical_stream_id: str | None = None, previous_stream_id: str | None = None,
                capture_mode: str = "tardis", previous_grid: tuple[int, int] | None = None,
                write_workers: int = 1, write_max_pending_bytes: int = 32 * 1024**2) -> dict:
    from data.book_top import (apply_top_observations, select_top_observations,
                               to_daily_book_rows)
    from data.daily_raw import supplement_capture_contract

    supplement_capture_contract(capture_mode, stream_id or "", logical_stream_id)
    if stream_id is None and capture_mode != "tardis":
        raise ValueError("native capture mode requires an explicit supplement stream")
    predecessor = _grid_predecessor(day, previous_grid)

    journal = stage / "publication.json"
    if journal.exists():
        previous = _read(journal)
        if previous.get("capture_contract", {}).get("capture_mode", "tardis") != capture_mode:
            raise ValueError("prepared supplement capture mode cannot change")
        if previous.get("observation_grid_predecessor", predecessor if previous_grid is None else None) != predecessor:
            raise ValueError("prepared sampled-grid predecessor contract differs")
        return previous
    raw = data_root / "raw/binance_futures/BTCUSDC" / day / "incremental_book_L2.parquet"
    ticker = data_root / "raw/history/binance_futures/BTCUSDC" / day / "book_ticker.parquet"
    current = {kind: book_root / kind / f"BTCUSDC-{kind}-{day}.parquet" for kind in ("bbo", "l2", "clock")}
    quality_path = book_root / "quality" / f"BTCUSDC-{day}.json"
    quality = _read(quality_path)
    before = {kind: identity(path) for kind, path in current.items()}
    raw_before = identity(raw)
    if (quality.get("raw_source", {}).get("sha256") != raw_before["sha256"]
            or any(quality[f"{kind}_output"]["sha256"] != entry["sha256"] for kind, entry in before.items())):
        raise ValueError("current daily input identities no longer match their quality receipt")
    # Only one day is staged. Preserve ample room for the largest atomic file.
    reserve = (2 * 1024**3 if os.environ.get("NARROWGATE_BOOK_WORK_ROOT") and stream_id is not None
               else 60 * 1024**3 + raw_before["size_bytes"] * 3)
    if shutil.disk_usage(data_root).free < reserve:
        raise OSError("insufficient free space for one verified atomic day")
    if stream_id is not None:
        return _prepare_book_supplement(raw, raw_before, quality, quality_path, current,
                                        stage, day, supplement, stream_id, previous_deep_state, previous_top_state,
                                        logical_stream_id, previous_stream_id, capture_mode, previous_grid,
                                        write_workers, write_max_pending_bytes)
    if any(item is not None for item in (supplement, previous_deep_state, logical_stream_id, previous_stream_id)):
        raise ValueError("book supplement requires an explicit anonymous stream")
    base_bbo, clock = pq.read_table(current["bbo"]), pq.read_table(current["clock"])
    if ticker.exists():
        ticker_before = identity(ticker)
        top, selection = select_top_observations(ticker, clock)
    else:
        import pyarrow as pa
        from data.book_top import TOP_SCHEMA
        top, selection, ticker_before = pa.Table.from_pylist([], schema=TOP_SCHEMA), {"status": "NO_INDEPENDENT_TOP_FILE"}, None
    new_bbo, new_clock, final_top, metrics = apply_top_observations(base_bbo, clock, top, previous_top_state)
    stage.mkdir(parents=True, exist_ok=True)
    staged = {kind: stage / f"{kind}.parquet" for kind in ("bbo", "clock")}
    generated = {kind: _write_parquet(table, staged[kind]) for kind, table in (("bbo", new_bbo), ("clock", new_clock))}
    normalized = {kind: {key: value[key] for key in ("rows", "sha256")}
                  for kind, value in {**before, **generated}.items()}
    result = unify_orderbook_day(raw, stage / "incremental_book_L2.parquet", day,
        normalized=normalized, top_supplements=to_daily_book_rows(top),
        top_initial_state=previous_top_state, top_final_state=final_top)
    from data.quality.calendar_content import observation_grid, verify_separate_top_view
    verification = verify_separate_top_view(Path(result["path"]), staged["bbo"], current["l2"], staged["clock"])
    grid = observation_grid(day, new_clock["timestamp"].to_numpy()*1000,
                            new_clock["last_observation_timestamp_us"].to_numpy(), previous=previous_grid)
    bid, ask = new_bbo["best_bid"].to_numpy(), new_bbo["best_ask"].to_numpy()
    invalid_spread_buckets = int((~np.isfinite(bid) | ~np.isfinite(ask) | (bid <= 0) | (ask <= bid)).sum())
    updated = copy.deepcopy(quality)
    for key in ("included_sources", "next_day_boundary", "source_kind", "source_quality"):
        updated.pop(key, None)
    updated.update(raw_reconstruction=DAILY_BOOK_MARKER,
        raw_source={"path": str(raw), "sha256": result["sha256"], "rows": result["rows"]},
        top_observation_metrics=metrics, source_clock_binding={"kind": "FUSION_SAME_PASS"},
        independent_channel_clocks=True, economic_admission=False,
        provider_normalized_replay_candidate=False, exact_queue=False,
        observation_grid=grid, observation_grid_predecessor=predecessor,
        max_stale_age_us=grid["max_stale_age_us"], unknown_grid_rows=grid["unknown_state_rows"],
        future_fill_violations=grid["future_fill_violations"], invalid_spread_buckets=invalid_spread_buckets)
    for kind, entry in generated.items():
        updated[f"{kind}_output"] = {**entry, "path": str(current[kind])}
    _write(stage / "quality.json", updated)
    if ticker_before and identity(ticker)["sha256"] != ticker_before["sha256"]:
        raise ValueError("independent top changed during selection")
    state = prepare_transaction(journal, [
        (staged["bbo"], current["bbo"]), (staged["clock"], current["clock"]),
        (Path(result["path"]), raw), (stage / "quality.json", quality_path)], day=day,
        metadata={"raw_rows": result["rows"], "top_rows": len(top), "top_selection": selection,
                  "top_verification": verification, "top_metrics": metrics, "top_final_state": final_top,
                  "observation_grid": grid, "observation_grid_predecessor": predecessor,
                  "invalid_spread_buckets": invalid_spread_buckets,
                  "independent_top": ticker_before, "created_at_utc": _now(), "catalog_prepared": False})
    return state


def _prepare_book_supplement(raw, raw_before, quality, quality_path, current, stage,
                             day, supplement, stream_id, previous_deep_state, previous_top_state,
                             logical_stream_id=None, previous_stream_id=None, capture_mode="tardis", previous_grid=None,
                             write_workers=1, write_max_pending_bytes=32 * 1024**2):
    """Rebuild all three views and prepare the existing recoverable transaction."""
    from data.daily_raw import supplement_daily_book_day
    from data.quality.calendar_content import observation_grid, verify_separate_top_view

    stage.mkdir(parents=True, exist_ok=True)
    mapping = ({"logical_stream_id": logical_stream_id, "previous_stream_id": previous_stream_id}
               if logical_stream_id is not None else {})
    if capture_mode != "tardis":
        mapping["capture_mode"] = capture_mode
    if write_workers != 1 or write_max_pending_bytes != 32 * 1024**2:
        mapping.update(write_workers=write_workers, write_max_pending_bytes=write_max_pending_bytes)
    work = stage
    input_raw, input_supplement = raw, supplement
    scratch = os.environ.get("NARROWGATE_BOOK_WORK_ROOT")
    if scratch:
        scratch_root = Path(scratch).expanduser().resolve()
        scratch_root.mkdir(parents=True, exist_ok=True)
        required = (raw.stat().st_size + (supplement.stat().st_size if supplement else 0)) * 8
        if shutil.disk_usage(scratch_root).free < required + 8 * 1024**3:
            raise OSError("insufficient local scratch capacity for current book day")
        work = Path(tempfile.mkdtemp(prefix=day + "-", dir=scratch_root))
        input_raw = work / "input.parquet"
        shutil.copyfile(raw, input_raw)
        if identity(input_raw)["sha256"] != raw_before["sha256"]:
            raise ValueError("local book input copy mismatch")
        if supplement is not None:
            expected = identity(supplement)["sha256"]
            input_supplement = work / "supplement.parquet"
            shutil.copyfile(supplement, input_supplement)
            if identity(input_supplement)["sha256"] != expected:
                raise ValueError("local supplement copy mismatch")
    result = supplement_daily_book_day(input_raw, input_supplement, work / "incremental_book_L2.parquet", day,
        stream_id=stream_id, previous_state=previous_deep_state, previous_top_state=previous_top_state,
        normalized_root=work / "views", **mapping)
    generated = result["stats"]["normalized"]
    if set(generated) != {"bbo", "l2", "clock"}:
        raise ValueError("book supplement did not generate the complete view triplet")
    staged = {kind: Path(item["path"]) for kind, item in generated.items()}
    verified = {kind: identity(path) for kind, path in staged.items()}
    if any(verified[k]["sha256"] != generated[k]["sha256"] or
           verified[k]["rows"] != generated[k]["rows"] for k in generated):
        raise ValueError("supplemented views differ from their producer identity")
    bbo, clock = pq.read_table(staged["bbo"]), pq.read_table(staged["clock"])
    axis = bbo["timestamp"].to_numpy()
    if (not np.array_equal(axis, clock["timestamp"].to_numpy()) or
            not np.array_equal(axis, pq.read_table(staged["l2"], columns=["timestamp"])["timestamp"].to_numpy())):
        raise ValueError("supplemented BBO/L2/clock axes differ")
    observed = clock["last_observation_timestamp_us"].to_numpy()
    age = clock["observation_age_us"].to_numpy()
    if np.any(observed <= 0) or not np.array_equal(age, axis * 1000 - observed):
        raise ValueError("supplemented observation age is not bound to the real source clock")
    predecessor = _grid_predecessor(day, previous_grid)
    grid = observation_grid(day, axis * 1000, observed, previous=previous_grid)
    verification = verify_separate_top_view(Path(result["path"]), staged["bbo"], staged["l2"], staged["clock"])
    bid, ask = bbo["best_bid"].to_numpy(), bbo["best_ask"].to_numpy()
    invalid = ~np.isfinite(bid) | ~np.isfinite(ask) | (bid <= 0) | (ask <= bid)
    if np.any(invalid & clock["bbo_usable"].to_numpy()):
        raise ValueError("invalid supplemented top was marked usable")
    if work != stage:
        # Copy verified products onto the target filesystem before the existing
        # atomic publication transaction. Never replace across filesystems.
        return_bytes = sum(item["size_bytes"] for item in verified.values()) + Path(result["path"]).stat().st_size
        if shutil.disk_usage(stage).free < return_bytes + 2 * 1024**3:
            raise OSError("insufficient external capacity for verified product return")
        for kind, source in list(staged.items()):
            target = stage / "views" / kind / source.name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
            copied = identity(target)
            if copied["sha256"] != verified[kind]["sha256"]:
                raise ValueError("book view return copy mismatch")
            staged[kind], verified[kind] = target, copied
        target = stage / "incremental_book_L2.parquet"
        shutil.copyfile(Path(result["path"]), target)
        if identity(target)["sha256"] != result["sha256"]:
            raise ValueError("raw book return copy mismatch")
        result["path"] = str(target)
    updated = {"schema": "book_fusion_same_pass.v1", "observation_schema": "narrowgate.book_observation.v2",
        "day": day, "symbol": "BTCUSDC", "timestamp_source": "exchange",
        "gap_policy": "causal_state_carry_with_bound_observation_age", "raw_reconstruction": DAILY_BOOK_MARKER,
        "raw_source": {"path": str(raw), "sha256": result["sha256"], "rows": result["rows"]},
        "source_clock_binding": {"kind": "FUSION_SAME_PASS"}, "independent_channel_clocks": True,
        "cross_channel_contract_valid": False, "provider_normalized_replay_candidate": False,
        "exact_queue": False, "economic_admission": False,
        "research_use": copy.deepcopy(quality.get("research_use", {})),
        "capture_completeness": "UNKNOWN", "account_order_fifo_continuity": "NOT_TESTED",
        "emitted_rows": len(axis), "possible_rows": grid["grid_rows"],
        "unknown_grid_rows": grid["unknown_state_rows"],
        "future_fill_violations": grid["future_fill_violations"],
        "max_stale_age_us": grid["max_stale_age_us"],
        "observed_at_grid_rows": int((age == 0).sum()), "carried_age_rows": int((age > 0).sum()),
        "invalid_spread_buckets": int(invalid.sum()),
        "observation_grid": grid,
        "observation_grid_predecessor": predecessor,
        "top_observation_metrics": result["top_metrics"]}
    capture_metadata = ({"capture_contract": result["capture_contract"],
                         "capture_clock_selection": result["capture_clock_selection"]}
                        if capture_mode != "tardis" else {})
    updated.update(capture_metadata)
    for kind, entry in verified.items():
        updated[f"{kind}_output"] = {**entry, "path": str(current[kind])}
    _write(stage / "quality.json", updated)
    if identity(raw)["sha256"] != raw_before["sha256"]:
        raise ValueError("current daily book changed during supplement preparation")
    transaction = prepare_transaction(stage / "publication.json", [
        *((staged[kind], current[kind]) for kind in ("bbo", "l2", "clock")),
        (Path(result["path"]), raw), (stage / "quality.json", quality_path)], day=day,
        metadata={**capture_metadata, "raw_rows": result["rows"], "top_rows": 0,
                  "top_selection": {"status": "PRESERVED_EXISTING_DAILY_TOP"},
                  "top_verification": verification, "top_metrics": result["top_metrics"],
                  "top_final_state": result["top_final_state"],
                  "deep_initial_state": result["initial_state"], "deep_final_state": result["final_state"],
                  "consumed_inputs": result["consumed_inputs"], "stream_id": stream_id,
                  "write_pipeline": result["stats"].get("write_pipeline"),
                  "logical_stream_id": logical_stream_id, "seam_transition": result.get("seam_transition"),
                  "observation_grid": grid, "invalid_spread_buckets": int(invalid.sum()),
                  "observation_grid_predecessor": predecessor,
                  "created_at_utc": _now(), "catalog_prepared": False})
    if work != stage:
        shutil.rmtree(work)
    return transaction


def prepare_catalogs(journal: Path, *, data_root: Path, book_root: Path, catalog_root: Path) -> dict:
    """Stage ALL current metadata before publication; resume uses frozen hashes."""
    state = _read(journal)
    required = {"raw_rows", "top_rows", "top_final_state", "top_metrics", "files", "day"}
    if not required <= state.keys():
        raise ValueError("incomplete daily transaction; no files may be published")
    if state.get("catalog_prepared"):
        return state
    for item in state["files"]:
        if identity(Path(item["before"]["path"]))["sha256"] != item["before"]["sha256"]:
            raise ValueError("cannot prepare metadata after an unbound data publication")
    day, records = state["day"], state["files"]
    by_path = {r["after"]["path"]: r["after"] for r in records}
    raw = data_root / "raw/binance_futures/BTCUSDC" / day / "incremental_book_L2.parquet"
    owner_path, readability_path = catalog_root / "owner-manifest.json", catalog_root / "readability.json"
    owner = _read(owner_path)
    rights = usage_digest(_read(readability_path))
    raw_csv = Path(_dataset(owner, RAW_ID)["audit"]["path"])
    book_csv = Path(_dataset(owner, BOOK_ID)["audit"]["path"])
    extras = []
    pairs = []
    def stage_path(target):
        output = target.with_name(f".{target.name}.daily-schema-{day}.staged")
        pairs.append((output, target))
        return output
    def neutral_raw(index):
        row = next(r for r in index["records"] if r["day"] == day and r["symbol"] == "BTCUSDC" and r["channel"] == "incremental_book_L2")
        for key in ("included_sources", "prior_canonical_sha256", "source_kind"):
            row.pop(key, None)
        row.update(mode=DAILY_BOOK_MARKER, status="PUBLISHED", rows=state["raw_rows"])
    path = data_root / "raw/daily-index.json"
    extras.append(rebind_json(path, records, mutate=neutral_raw, output=stage_path(path)))
    grid = state.get("observation_grid")
    quality_path = book_root / "quality" / f"BTCUSDC-{day}.json"
    quality_identity = by_path[str(quality_path)]
    for path in (raw_csv, book_csv, book_root / "daily_quality.csv"):
        updates = None
        if grid is not None:
            # A hash rebind must not attach the predecessor's gap statistics or
            # whole-calendar acceptance to newly rebuilt source observations.
            updates = {"source_quality_path": str(quality_path),
                       "source_quality_sha256": quality_identity["sha256"],
                       "source_max_gap_us": "UNKNOWN"}
            if path == raw_csv:
                updates.update(rows=state["raw_rows"], source_max_gap_s="UNKNOWN",
                    source_quality_status="FUSION_REBUILT_DAILY_CLOCK_VERIFIED",
                    future_fill_violations=grid["future_fill_violations"],
                    max_stale_age_us=grid["max_stale_age_us"])
            elif path == book_csv:
                updates.update(continuous_read_verified="false", rows=state["top_verification"]["rows"],
                    known_state_coverage=grid["known_state_rows"] / grid["grid_rows"],
                    **{k: grid[k] for k in ("fresh_state_rows", "stale_state_rows", "unknown_state_rows", "grid_rows")},
                    max_stale_age_s=grid["max_stale_age_us"] / 1_000_000,
                    notes="Rebuilt daily clock checked; full-calendar shared-reader acceptance pending.")
            else:
                updates.update(source_dataset_id=DAILY_BOOK_MARKER,
                    normalized_min_coverage=grid["known_state_rows"] / grid["grid_rows"],
                    source_invalid_spread_buckets=state["invalid_spread_buckets"],
                    formal_eligible="False", exact_queue_policy_eligible="False")
        extras.append(rebind_csv(path, records, day=day, updates=updates, output=stage_path(path)))
    def neutral_selection(selection):
        row = next(r for r in selection["sources"] if r["day"] == day)
        row.pop("included_sources", None)
        row["source_kind"] = DAILY_BOOK_MARKER
        row["quality_sha256"] = by_path[str(book_root / "quality" / f"BTCUSDC-{day}.json")]["sha256"]
    path = book_root / "manifest.json"
    extras.append(rebind_json(path, records + extras, mutate=neutral_selection, output=stage_path(path)))
    def neutral_owner(value):
        for name in (RAW_ID, BOOK_ID):
            dataset = _dataset(value, name)
            dataset["source"], dataset["version"] = "current_dataset", "current"
            for inv in dataset["inventories"]:
                if inv.get("node") == "local":
                    for row in inv.get("files_by_day", {}).get(day, []):
                        match = by_path.get(row["path"])
                        if match:
                            row.update({k: match[k] for k in ("size_bytes", "sha256")})
    extras.append(rebind_json(owner_path, records + extras, mutate=neutral_owner, output=stage_path(owner_path)))
    def neutral_readability(value):
        row = next(r for r in value["records"] if r["calendar_date"] == day)
        for channel in row["channels"]:
            if channel["source_id"] == RAW_ID:
                channel["provider"], channel["version"] = "current_dataset", "current"
                validation = channel.setdefault("source_content_validation", {})
                validation.pop("included_sources", None)
                validation.update(sha256=by_path[str(raw)]["sha256"], physical_schema=DAILY_BOOK_MARKER,
                                  encoding_verified=True, all_projected_rows_roundtrip_verified=True)
            elif channel["source_id"] == BOOK_ID:
                channel["independent_top_observations"] = state["top_metrics"]
                channel["source_clock_binding"] = {"kind": "FUSION_SAME_PASS", "independent_channel_clocks": True}
                if grid is not None:
                    channel.update(observation_grid=grid, max_stale_age_us=grid["max_stale_age_us"],
                        max_no_new_observation_us="UNKNOWN", future_fill_violations=grid["future_fill_violations"],
                        cross_day_data_state="FULL_CALENDAR_RECHECK_PENDING", economic_admission=False,
                        reader_reason="Rebuilt daily clock checked; full-calendar shared-reader acceptance pending.")
            elif channel.get("source_id") == "btcusdc-baseline401-selected-features":
                channel["book_dependency_status"] = "REFRESH_REQUIRED"
        if usage_digest(value) != rights:
            raise ValueError("data-only publication attempted to change previous-use")
    staged_readability = stage_path(readability_path)
    rebind_json(readability_path, records + extras, mutate=neutral_readability, output=staged_readability)
    if usage_digest(_read(staged_readability)) != rights:
        raise ValueError("previous-use changed after current-view publication")
    state["files"].extend(planned_files(pairs))
    state.update(catalog_prepared=True, research_use_sha256=rights)
    _write(journal, state)
    return state


def verify_publication(state: dict, *, catalog_root: Path) -> None:
    if not state.get("catalog_prepared"):
        raise ValueError("daily data and current metadata were not prepared together")
    for item in state["files"]:
        if identity(Path(item["after"]["path"]))["sha256"] != item["after"]["sha256"]:
            raise ValueError("current publication identity differs")
    if usage_digest(_read(catalog_root / "readability.json")) != state["research_use_sha256"]:
        raise ValueError("previous-use changed during data-only publication")


def supplement_stream_plan(plan: dict, execution_days: list[str]) -> tuple[dict[str, str], str | None]:
    """Resolve explicit day slots; never guess that a one-slot view is source s0."""
    from data.daily_raw import supplement_capture_contract
    contract = supplement_capture_contract(plan.get("capture_mode", "tardis"), plan.get("stream_id", ""),
                                           plan.get("logical_stream_id"))
    if "capture_clock" in plan and (contract is None or plan["capture_clock"] != contract["clock"]):
        raise ValueError("capture clock differs from the frozen native mode")
    if any("capture_mode" in item or "capture_clock" in item for item in plan.get("days", {}).values()):
        raise ValueError("capture mode is frozen for the whole stream, not selected per day")
    if plan.get("schema") == "daily_book_supplements.v1":
        if not plan.get("stream_id") or "stream_ids_by_day" in plan or "logical_stream_id" in plan:
            raise ValueError("v1 requires one fixed anonymous stream without seam remapping")
        return dict.fromkeys(execution_days, plan["stream_id"]), None
    if plan.get("schema") != "daily_book_supplements.v2":
        raise ValueError("unsupported supplement plan schema")
    logical, slots = plan.get("logical_stream_id"), plan.get("stream_ids_by_day", {})
    if (not isinstance(logical, str) or not logical or "stream_id" in plan
            or not isinstance(slots, dict) or set(slots) != set(execution_days)
            or any(not isinstance(slot, str) or not slot.startswith("s") or not slot[1:].isdigit()
                   for slot in slots.values())):
        raise ValueError("v2 requires a stable logical stream and explicit slots for the full repair suffix")
    return slots, logical


def _batch_predecessor(path: Path, first_day: str, start: str, end: str,
                       plan: dict, data_root: Path, book_root: Path):
    """Continue a new source batch from an unchanged, published adjacent day."""
    from datetime import date, timedelta
    prior = _read(path)
    previous_day = (date.fromisoformat(first_day) - timedelta(days=1)).isoformat()
    entries = prior.get("days", {})
    if ((prior.get("start"), prior.get("end")) != (start, end)
            or not entries or max(entries) != previous_day
            or any(row.get("status") != "PUBLISHED_VERIFIED" for row in entries.values())):
        raise ValueError("batch predecessor must end on the adjacent verified day")
    old_ref = prior.get("supplement_manifest") or {}
    old_path = Path(old_ref.get("path", ""))
    if not old_path.is_file() or identity(old_path)["sha256"] != old_ref.get("sha256"):
        raise ValueError("batch predecessor source plan changed")
    old_plan = _read(old_path)
    if (old_plan.get("symbol") != plan.get("symbol")
            or old_plan.get("capture_mode", "tardis") != plan.get("capture_mode", "tardis")
            or old_plan.get("logical_stream_id") != plan.get("logical_stream_id")
            or (not plan.get("logical_stream_id") and old_plan.get("stream_id") != plan.get("stream_id"))):
        raise ValueError("batch predecessor stream contract differs")
    entry = entries[previous_day]
    # Shared catalogs may have advanced for other channels. Bind the actual
    # predecessor book files, not obsolete copies of those global catalogs.
    checked = 0
    for row in entry["files"]:
        after = row["after"]
        target = Path(after["path"])
        if (target.is_relative_to(book_root)
                or target == data_root / "raw/binance_futures/BTCUSDC" / previous_day / "incremental_book_L2.parquet"):
            if not target.is_file() or identity(target)["sha256"] != after["sha256"]:
                raise ValueError("batch predecessor book changed after publication")
            checked += 1
    if not checked:
        raise ValueError("batch predecessor has no bound current book files")
    deep = _resume_deep_state(path.parent, previous_day, entry)
    grid = _resume_grid_tail(path.parent, previous_day, entry)
    return entry["top_final_state"], deep, entry["stream_id"], grid


def run(*, data_root: Path, book_root: Path, catalog_root: Path, state_root: Path,
        start: str, end: str, limit: int | None = None, retire_top: bool = False,
        supplement_manifest: Path | None = None, write_workers: int = 1,
        write_max_pending_mib: int = 32, predecessor_state: Path | None = None) -> dict:
    if type(write_workers) is not int or write_workers not in (1, 2):
        raise ValueError("write_workers must be 1 or 2 (producer plus at most one background writer)")
    if type(write_max_pending_mib) is not int or write_max_pending_mib <= 0:
        raise ValueError("write_max_pending_mib must be a positive integer")
    state_root.mkdir(parents=True, exist_ok=True)
    with (state_root / "operation.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        days = _days(start, end)
        owner = _read(catalog_root / "owner-manifest.json")
        if (owner["start_day"], owner["end_day"]) != (start, end):
            raise ValueError("fixed calendar must match the current owner window")
        supplement_plan = _read(supplement_manifest) if supplement_manifest is not None else None
        supplement_identity = identity(supplement_manifest) if supplement_manifest is not None else None
        if supplement_plan is not None:
            selected = supplement_plan.get("days", {})
            if (supplement_plan.get("symbol") != "BTCUSDC" or not selected or
                    not set(selected) <= set(days)):
                raise ValueError("supplement plan must bind existing calendar days and one anonymous stream")
            # A data repair range is not a research split. Propagate changed
            # source state through the calendar end, including days without a
            # new source; never splice the old final state onto the new one.
            execution_days = days[days.index(min(selected)):]
            stream_slots, logical_stream = supplement_stream_plan(supplement_plan, execution_days)
        else:
            selected, execution_days = {}, days
        predecessor_identity = identity(predecessor_state) if predecessor_state is not None else None
        if predecessor_state is not None and (supplement_plan is None or execution_days[0] == start):
            raise ValueError("batch predecessor requires a later supplement batch")
        status_path = state_root / "current.json"
        status = (_read(status_path) if status_path.exists() else {
            "schema": "daily_book_calendar_unification.v1", "start": start, "end": end,
            "days": {}, "live": False, "economic_admission": False,
            "supplement_manifest": supplement_identity,
            "predecessor_state": predecessor_identity,
            "execution_days": execution_days, "calendar_days": len(days)})
        if (status["start"], status["end"]) != (start, end):
            raise ValueError("cannot change an in-progress calendar")
        if ((status.get("supplement_manifest") or {}).get("sha256") !=
                (supplement_identity or {}).get("sha256")):
            raise ValueError("cannot change an in-progress supplement manifest")
        if ((status.get("predecessor_state") or {}).get("sha256") !=
                (predecessor_identity or {}).get("sha256")):
            raise ValueError("cannot change an in-progress batch predecessor")
        completed = [day for day in execution_days if status["days"].get(day, {}).get("status") == "PUBLISHED_VERIFIED"]
        if completed != execution_days[:len(completed)]:
            raise ValueError("verified daily publications must be a continuous operation prefix")
        status["write_pipeline_requested"] = {"workers": write_workers,
                                              "max_pending_bytes": write_max_pending_mib * 1024**2}
        processed, previous_top, previous_deep, previous_slot, previous_grid = 0, None, None, None, None
        if predecessor_state is not None:
            previous_top, previous_deep, previous_slot, previous_grid = _batch_predecessor(
                predecessor_state, execution_days[0], start, end, supplement_plan, data_root, book_root)
        for day in execution_days:
            entry = status["days"].get(day)
            if entry and entry["status"] == "PUBLISHED_VERIFIED":
                previous_grid = _resume_grid_tail(state_root, day, entry)
                previous_top = entry["top_final_state"]
                # Historical statuses embedded every full-depth state. Migrate
                # those entries after binding the existing journal, but never
                # retain an O(calendar length) collection of Python book trees.
                if "deep_final_state_ref" not in entry or "deep_final_state" in entry or day == completed[-1]:
                    restored = _resume_deep_state(state_root, day, entry)
                    if day == completed[-1]:
                        previous_deep = restored
                    del restored
                previous_slot = stream_slots[day] if supplement_plan is not None else None
                if day == completed[-1]:
                    _write(status_path, status)
                continue
            if limit is not None and processed >= limit:
                break
            began = time.monotonic()
            stage = state_root / day
            try:
                kwargs = {}
                if supplement_plan is not None:
                    source = selected.get(day)
                    if source and identity(Path(source["path"]))["sha256"] != source["sha256"]:
                        raise ValueError("supplement source no longer matches its declared content")
                    kwargs = {"supplement": Path(source["path"]) if source else None,
                              "stream_id": stream_slots[day], "previous_deep_state": previous_deep}
                    if supplement_plan.get("capture_mode", "tardis") != "tardis":
                        kwargs["capture_mode"] = supplement_plan["capture_mode"]
                    if logical_stream is not None:
                        kwargs.update(logical_stream_id=logical_stream, previous_stream_id=previous_slot)
                prepared = prepare_day(data_root, book_root, stage, day, previous_top,
                                       previous_grid=previous_grid, write_workers=write_workers,
                                       write_max_pending_bytes=write_max_pending_mib * 1024**2, **kwargs)
                prepared = prepare_catalogs(stage / "publication.json", data_root=data_root,
                                            book_root=book_root, catalog_root=catalog_root)
                published = publish_transaction(stage / "publication.json")
                verify_publication(published, catalog_root=catalog_root)
                next_grid = _grid_tail(day, published.get("observation_grid", {}))
                top = prepared.get("independent_top")
                deleted = 0
                if retire_top and top:
                    path = Path(top["path"])
                    if path.exists():
                        if identity(path)["sha256"] != top["sha256"]:
                            raise ValueError("independent BBO changed before retirement")
                        path.unlink()
                        _sync_directory(path.parent)
                    deleted = top["size_bytes"]
                previous_top = published["top_final_state"]
                previous_deep = published.get("deep_final_state")
                previous_slot = stream_slots[day] if supplement_plan is not None else None
                previous_grid = next_grid
                publication_identity = identity(stage / "publication.json")
                status["days"][day] = {"status": "PUBLISHED_VERIFIED", "raw_rows": published["raw_rows"],
                    "top_rows": published["top_rows"], "top_metrics": published["top_metrics"],
                    "top_final_state": previous_top, "files": published["files"],
                    "deep_final_state_ref": {key: publication_identity[key] for key in ("path", "sha256", "size_bytes")},
                    "observation_grid_tail": list(previous_grid) if previous_grid is not None else None,
                    "stream_id": previous_slot, "logical_stream_id": (logical_stream if supplement_plan is not None else None),
                    "seam_transition": published.get("seam_transition"),
                    "new_source_available": day in selected if supplement_plan is not None else None,
                    "write_pipeline": published.get("write_pipeline"),
                    "retired_top_bytes": deleted, "elapsed_s": time.monotonic()-began}
                status.update(status="RUNNING", updated_at_utc=_now())
                _write(status_path, status)
                print(json.dumps({"day": day, "completed": len(status["days"]), "total": len(execution_days),
                                  "elapsed_s": status["days"][day]["elapsed_s"], "status": "PUBLISHED_VERIFIED"}), flush=True)
                processed += 1
            except Exception as exc:
                status.update(status="FAILED", failed_day=day, error=f"{type(exc).__name__}: {exc}", updated_at_utc=_now())
                _write(status_path, status)
                raise
        status.update(status="COMPLETED" if len(status["days"]) == len(execution_days) else "PARTIAL", updated_at_utc=_now())
        _write(status_path, status)
        return status


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("data-root", "book-root", "catalog-root", "state-root"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--retire-top", action="store_true")
    parser.add_argument("--supplement-manifest", type=Path,
                        help="verified source identities; repair the suffix without changing the research calendar")
    parser.add_argument("--write-workers", type=int, choices=(1, 2), default=1,
                        help="1=serial; 2=one sequential book producer plus one bounded background writer")
    parser.add_argument("--write-max-pending-mib", type=int, default=32,
                        help="maximum buffered writer batch; oversized batches drain and write synchronously")
    args = parser.parse_args()
    run(**vars(args))


if __name__ == "__main__":
    main()
