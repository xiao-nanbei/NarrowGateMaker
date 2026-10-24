"""Causal independent top observations, separate from the depth/queue clock.

The eight input fields are real book-ticker observations.  A top observation is
never a depth update, snapshot or queue reset.  ``bbo_usable`` describes quote
validity, NOT execution freshness: consumers must retain their existing age
and depth-risk limits.  The five-second gap is only a supplementation scope.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

TOP_SCHEMA = pa.schema([
    ("exchange", pa.string()), ("symbol", pa.string()),
    ("timestamp", pa.int64()), ("local_timestamp", pa.int64()),
    ("ask_amount", pa.string()), ("ask_price", pa.string()),
    ("bid_price", pa.string()), ("bid_amount", pa.string()),
])
STATE_SCHEMA = "narrowgate.book_top_state.v1"
_CLOCK = "exchange_microseconds"
_QUOTES = ("bid_price", "bid_amount", "ask_price", "ask_amount")
_BBO = ("best_bid", "best_bid_qty", "best_ask", "best_ask_qty")
BBO_CLOCK_FIELDS = (
    "bbo_last_observation_timestamp_us", "bbo_observation_age_us",
    "bbo_observation_kind", "bbo_usable",
)


def _valid_quote(row: Mapping) -> bool:
    try:
        bid, bid_qty, ask, ask_qty = (float(row[k]) for k in _QUOTES)
        return bool(all(np.isfinite(v) and v > 0 for v in (bid, bid_qty, ask, ask_qty))
                    and bid < ask)
    except (TypeError, ValueError, KeyError):
        return False


def _event(row: Mapping, symbol: str) -> dict:
    if set(row) != set(TOP_SCHEMA.names):
        raise ValueError("top observation must contain exactly the eight input fields")
    out = dict(row)
    if out["exchange"] != "binance-futures" or out["symbol"] != symbol:
        raise ValueError("top observation market/symbol mismatch")
    ts = out["timestamp"]
    if isinstance(ts, bool) or not isinstance(ts, int) or ts <= 0:
        raise ValueError("top observation requires an actual exchange timestamp")
    local = out["local_timestamp"]
    if local is not None and (isinstance(local, bool) or not isinstance(local, int) or local <= 0):
        raise ValueError("invalid top local timestamp")
    for name in _QUOTES:
        if out[name] is not None:
            out[name] = str(out[name])
    return out


def _top_table(table: pa.Table, symbol: str) -> pa.Table:
    if set(table.column_names) != set(TOP_SCHEMA.names):
        raise ValueError("top table requires the eight real observation fields")
    table = table.select(TOP_SCHEMA.names).cast(TOP_SCHEMA)
    rows = [_event(r, symbol) for r in table.to_pylist()]
    times = [r["timestamp"] for r in rows]
    if any(a > b for a, b in zip(times, times[1:], strict=False)):
        raise ValueError("top exchange timestamps regress")
    return pa.Table.from_pylist(rows, schema=TOP_SCHEMA)


def _raw_table(table: pa.Table, symbol: str) -> pa.Table:
    """Validate scanned groups without materializing millions of Python rows."""
    table = table.select(TOP_SCHEMA.names).cast(TOP_SCHEMA)
    for name, expected in (("exchange", "binance-futures"), ("symbol", symbol)):
        if table[name].null_count or pc.any(pc.not_equal(table[name], expected)).as_py():
            raise ValueError("raw top observation market/symbol mismatch")
    if table["timestamp"].null_count:
        raise ValueError("raw top observation lacks exchange timestamp")
    times = table["timestamp"].to_numpy(zero_copy_only=False)
    if np.any(times <= 0) or np.any(np.diff(times) < 0):
        raise ValueError("raw top exchange timestamps regress or are invalid")
    return table


def validate_top_state(state, *, symbol="BTCUSDC", boundary_us=None):
    """Validate/copy the small, explicit cross-day state; never modify input."""
    if state is None:
        return None
    keys = {"schema", "symbol", "clock", "last_grid_us", "latest_top",
            "last_valid_top", "last_bbo"}
    if not isinstance(state, Mapping) or set(state) != keys:
        raise ValueError("invalid independent top state fields")
    if (state["schema"] != STATE_SCHEMA or state["symbol"] != symbol
            or state["clock"] != _CLOCK):
        raise ValueError("independent top state symbol/clock/schema mismatch")
    grid = state["last_grid_us"]
    if isinstance(grid, bool) or not isinstance(grid, int) or grid <= 0:
        raise ValueError("invalid independent top state grid")
    if boundary_us is not None and grid > boundary_us:
        raise ValueError("independent top state is after the boundary")
    out = dict(state)
    for key in ("latest_top", "last_valid_top"):
        out[key] = None if state[key] is None else _event(state[key], symbol)
        if out[key] is not None and out[key]["timestamp"] > grid:
            raise ValueError("future top observation in continuation")
    valid = out["last_valid_top"]
    latest = out["latest_top"]
    if valid is not None and (not _valid_quote(valid) or latest is None
                              or valid["timestamp"] > latest["timestamp"]):
        raise ValueError("invalid last-valid top continuation")
    bbo = state["last_bbo"]
    if bbo is not None:
        expected = {*_QUOTES, "observed_timestamp_us", "local_timestamp", "observation_kind"}
        if not isinstance(bbo, Mapping) or set(bbo) != expected or not _valid_quote(bbo):
            raise ValueError("invalid display BBO continuation")
        bbo = dict(bbo)
        observed = bbo["observed_timestamp_us"]
        if (isinstance(observed, bool) or not isinstance(observed, int)
                or not 0 < observed <= grid):
            raise ValueError("future/invalid display BBO clock")
        if bbo["observation_kind"] not in {"depth", "top"}:
            raise ValueError("invalid display BBO observation kind")
        out["last_bbo"] = bbo
    return out


def _grid_and_observations(clock: pa.Table):
    grid = clock["timestamp"].combine_chunks().to_numpy(zero_copy_only=False).astype(np.int64) * 1000
    raw = clock["last_observation_timestamp_us"].to_pylist()
    observed = np.asarray([int(v) if v is not None else -1 for v in raw], dtype=np.int64)
    if len(grid) and (np.any(grid <= 0) or np.any(np.diff(grid) <= 0)):
        raise ValueError("BBO grid must be strictly increasing exchange milliseconds")
    if np.any(observed > grid):
        raise ValueError("depth clock references a future observation")
    return grid, observed


def select_top_observations(raw_path: Path, depth_clock: pa.Table, *,
                            symbol="BTCUSDC", gap_us=5_000_000):
    """Read only relevant row groups; retain latest <= stale grid, including invalid.

    Timestamp statistics are required for a bounded read.  Missing statistics
    fail explicitly rather than silently loading an arbitrary full raw day.
    Duplicate timestamps are not trade identities and are not deduplicated:
    the last observation in source order is the visible one at that timestamp.
    """
    if gap_us < 0:
        raise ValueError("gap_us must be nonnegative")
    grid, observed = _grid_and_observations(depth_clock)
    targets = grid[(observed <= 0) | (grid - observed > gap_us)]
    metrics = {"gap_grid_rows": len(targets), "row_groups_read": 0,
               "raw_rows_read": 0, "selected_events": 0,
               "selected_invalid_events": 0, "grids_without_prior_top": 0,
               "future_fill_violations": 0}
    if not len(targets):
        return pa.Table.from_pylist([], schema=TOP_SCHEMA), metrics
    parquet = pq.ParquetFile(raw_path)
    if set(TOP_SCHEMA.names) - set(parquet.schema_arrow.names):
        raise ValueError("raw book ticker lacks required observation fields")
    column = parquet.schema_arrow.get_field_index("timestamp")
    bounds = []
    for index in range(parquet.num_row_groups):
        stat = parquet.metadata.row_group(index).column(column).statistics
        if stat is None or not stat.has_min_max:
            raise ValueError("bounded top selection requires timestamp row-group statistics")
        bounds.append((int(stat.min), int(stat.max)))
    if any(lo > hi for lo, hi in bounds) or any(a[1] > b[0] for a, b in zip(bounds, bounds[1:], strict=False)):
        raise ValueError("raw top row-group timestamps regress")
    selected_groups = set()
    # A group's rightmost boundary plus its predecessor covers every as-of
    # query, including invalid last observations and several equal-time groups.
    starts = np.asarray([lo for lo, _ in bounds], dtype=np.int64)
    for i in np.unique(np.searchsorted(starts, targets, side="right") - 1):
        if i >= 0:
            selected_groups.add(int(i))
    if not selected_groups:
        metrics["grids_without_prior_top"] = len(targets)
        return pa.Table.from_pylist([], schema=TOP_SCHEMA), metrics
    table = parquet.read_row_groups(sorted(selected_groups), columns=TOP_SCHEMA.names)
    table = _raw_table(table, symbol)
    metrics["row_groups_read"] = len(selected_groups)
    metrics["raw_rows_read"] = len(table)
    times = table["timestamp"].to_numpy(zero_copy_only=False)
    indices = np.searchsorted(times, targets, side="right") - 1
    metrics["grids_without_prior_top"] = int(np.sum(indices < 0))
    indices = np.unique(indices[indices >= 0])
    selected = _top_table(table.take(pa.array(indices, type=pa.int64())), symbol)
    metrics["selected_events"] = len(selected)
    metrics["selected_invalid_events"] = sum(not _valid_quote(row) for row in selected.to_pylist())
    return selected, metrics


def apply_top_observations(base_bbo: pa.Table, depth_clock: pa.Table,
                           top_events: pa.Table, previous_top_state=None, *,
                           symbol="BTCUSDC", gap_us=5_000_000):
    """Return (BBO, extended clock, continuation, metrics), without altering L2.

    An invalid latest top observation terminates its earlier valid quote.
    Values may remain visible for diagnostics but ``bbo_usable`` is false.
    A newer depth observation can restore validity.  Older depth never makes
    the selected quote's observation clock regress, including across days.
    """
    if gap_us < 0:
        raise ValueError("gap_us must be nonnegative")
    if len(base_bbo) != len(depth_clock) or not base_bbo["timestamp"].equals(depth_clock["timestamp"]):
        raise ValueError("BBO and depth clock grid mismatch")
    if set(BBO_CLOCK_FIELDS) & set(depth_clock.column_names):
        raise ValueError("BBO clock fields already exist; supply the original depth clock")
    grid, observed = _grid_and_observations(depth_clock)
    state = validate_top_state(previous_top_state, symbol=symbol,
                               boundary_us=int(grid[0]) if len(grid) else None)
    events = _top_table(top_events, symbol).to_pylist()
    if state and events and events[0]["timestamp"] < state["last_grid_us"]:
        # A selected predecessor is harmless only if it is the exact already
        # retained event.  Do not replay older unknown updates into new state.
        events = [event for event in events
                  if event["timestamp"] >= state["last_grid_us"]
                  or event == state["latest_top"]]
    latest = state["latest_top"] if state else None
    last_valid = state["last_valid_top"] if state else None
    display = state["last_bbo"] if state else None
    output = [base_bbo[name].to_numpy(zero_copy_only=False).astype(float, copy=True) for name in _BBO]
    base = [arr.copy() for arr in output]
    local_column = (depth_clock["last_provider_local_timestamp_us"].to_pylist()
                    if "last_provider_local_timestamp_us" in depth_clock.column_names else [None] * len(grid))
    obs_out, ages, kinds, usable = [], [], [], []
    cursor = 0
    metrics = {"rows": len(grid), "top_selected_rows": 0, "invalid_top_rows": 0,
               "unknown_rows": 0, "bbo_clock_regressions": 0,
               "future_fill_violations": 0, "max_bbo_age_us": None}
    for i, boundary in enumerate(grid):
        boundary = int(boundary)
        old_clock = display["observed_timestamp_us"] if display else -1
        while cursor < len(events) and events[cursor]["timestamp"] <= boundary:
            event = events[cursor]
            if latest is None or event["timestamp"] >= latest["timestamp"]:
                latest = event
                if _valid_quote(event):
                    last_valid = event
            cursor += 1
        deep = {name: repr(float(base[j][i])) for j, name in enumerate(_QUOTES)}
        deep_valid = observed[i] > 0 and _valid_quote(deep)
        if deep_valid and (display is None or observed[i] > display["observed_timestamp_us"]):
            display = {**deep, "observed_timestamp_us": int(observed[i]),
                       "local_timestamp": local_column[i], "observation_kind": "depth"}
        stale_depth = observed[i] <= 0 or boundary - observed[i] > gap_us
        if (stale_depth and latest is not None and _valid_quote(latest)
                and (display is None or latest["timestamp"] >= display["observed_timestamp_us"])):
            display = {**{name: latest[name] for name in _QUOTES},
                       "observed_timestamp_us": latest["timestamp"],
                       "local_timestamp": latest["local_timestamp"], "observation_kind": "top"}
        blocked = bool(latest is not None and not _valid_quote(latest)
                       and (display is None or latest["timestamp"] >= display["observed_timestamp_us"]))
        invalid_depth = bool(observed[i] > 0 and not deep_valid
                             and (display is None or observed[i] >= display["observed_timestamp_us"]))
        if display is None:
            obs_out.append(None)
            ages.append(None)
            kinds.append("invalid_top" if blocked else "unknown")
            usable.append(False)
            metrics["unknown_rows"] += 1
        else:
            actual = display["observed_timestamp_us"]
            if actual > boundary or actual < old_clock:
                raise ValueError("selected BBO observation clock violates causality")
            obs_out.append(actual)
            ages.append(boundary - actual)
            kinds.append("invalid_top" if blocked else "unknown" if invalid_depth else
                         "source_observed" if actual != old_clock else "carried_forward")
            usable.append(not blocked and not invalid_depth)
            for j, name in enumerate(_QUOTES):
                output[j][i] = float(display[name])
            if display["observation_kind"] == "top":
                metrics["top_selected_rows"] += 1
        metrics["invalid_top_rows"] += int(blocked)
        if invalid_depth and display is not None:
            metrics["unknown_rows"] += 1
    new_bbo = base_bbo
    for name, values in zip(_BBO, output, strict=True):
        index = new_bbo.schema.get_field_index(name)
        new_bbo = new_bbo.set_column(index, new_bbo.schema.field(index), pa.array(values))
    extended = depth_clock
    for name, values, dtype in zip(BBO_CLOCK_FIELDS, (obs_out, ages, kinds, usable),
                                   (pa.int64(), pa.int64(), pa.string(), pa.bool_()), strict=True):
        extended = extended.append_column(name, pa.array(values, type=dtype))
    nonnull_ages = [v for v in ages if v is not None]
    metrics["max_bbo_age_us"] = max(nonnull_ages) if nonnull_ages else None
    next_state = state
    if len(grid):
        next_state = validate_top_state({
            "schema": STATE_SCHEMA, "symbol": symbol, "clock": _CLOCK,
            "last_grid_us": int(grid[-1]), "latest_top": latest,
            "last_valid_top": last_valid, "last_bbo": display,
        }, symbol=symbol)
    return new_bbo, extended, next_state, metrics


def to_daily_book_rows(top_events: pa.Table, *, symbol="BTCUSDC") -> pa.Table:
    """Encode actual top observations as bid/ask pairs, never depth snapshots."""
    from data.daily_raw import DAILY_BOOK_SCHEMA

    rows = []
    for event in _top_table(top_events, symbol).to_pylist():
        for side in ("bid", "ask"):
            row = dict.fromkeys(DAILY_BOOK_SCHEMA.names)
            row.update(exchange=event["exchange"], symbol=event["symbol"],
                       timestamp=event["timestamp"], local_timestamp=event["local_timestamp"],
                       is_snapshot=False, side=side, price=event[f"{side}_price"],
                       amount=event[f"{side}_amount"], stream_id="t0", stream_priority=0,
                       queue_rebase=False, observation_only=True, top_only=True,
                       native_sequence=False, observed_timestamp_us=event["timestamp"],
                       original_timestamp_us=event["timestamp"])
            rows.append(row)
    return pa.Table.from_pylist(rows, schema=DAILY_BOOK_SCHEMA)


def from_daily_book_rows(top_rows: pa.Table, *, symbol="BTCUSDC") -> pa.Table:
    """Decode consecutive top pairs; preserve equal-time, distinct observations."""
    rows = top_rows.to_pylist()
    if len(rows) % 2:
        raise ValueError("top rows must be complete bid/ask observation pairs")
    events = []
    for i in range(0, len(rows), 2):
        bid, ask = rows[i:i + 2]
        for row, side in ((bid, "bid"), (ask, "ask")):
            if (row["side"] != side or row["top_only"] is not True
                    or row["observation_only"] is not True or row["is_snapshot"] is not False
                    or row["native_sequence"] is not False or row["queue_rebase"] is not False
                    or row["stream_id"] != "t0" or row["stream_priority"] != 0
                    or row["observed_timestamp_us"] != row["timestamp"]
                    or row["original_timestamp_us"] != row["timestamp"]):
                raise ValueError("invalid top-only row semantics")
        if any(bid[key] != ask[key] for key in ("exchange", "symbol", "timestamp", "local_timestamp")):
            raise ValueError("top bid/ask pair identity mismatch")
        events.append({"exchange": bid["exchange"], "symbol": bid["symbol"],
                       "timestamp": bid["timestamp"], "local_timestamp": bid["local_timestamp"],
                       "bid_price": bid["price"], "bid_amount": bid["amount"],
                       "ask_price": ask["price"], "ask_amount": ask["amount"]})
    return _top_table(pa.Table.from_pylist(events, schema=TOP_SCHEMA), symbol)


def merge_book_rows(projected_batches: Iterable, top_table: pa.Table, *, batch_size=65536):
    """Stable streaming merge: all depth rows precede top rows at equal time."""
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    times = top_table["timestamp"].to_numpy(zero_copy_only=False)
    if np.any(np.diff(times) < 0):
        raise ValueError("top merge input timestamps regress")
    cursor = 0
    previous = None
    for batch in projected_batches:
        table = batch if isinstance(batch, pa.Table) else pa.Table.from_batches([batch])
        if not table.schema.equals(top_table.schema, check_metadata=False):
            raise ValueError("top/depth merge schema mismatch")
        if not len(table):
            continue
        deep_times = table["timestamp"].to_numpy(zero_copy_only=False)
        if np.any(np.diff(deep_times) < 0) or (previous is not None and deep_times[0] < previous):
            raise ValueError("depth merge input timestamps regress")
        previous = int(deep_times[-1])
        start = 0
        # Equal-time rows at this batch's end may continue in the next batch.
        # Defer that top group until the first strictly newer depth timestamp.
        while cursor < len(times) and times[cursor] < deep_times[-1]:
            ts = times[cursor]
            stop = int(np.searchsorted(deep_times, ts, side="right"))
            if stop > start:
                yield from table.slice(start, stop - start).to_batches(max_chunksize=batch_size)
            end = int(np.searchsorted(times, ts, side="right"))
            yield from top_table.slice(cursor, end - cursor).to_batches(max_chunksize=batch_size)
            cursor = end
            start = stop
        if start < len(table):
            yield from table.slice(start).to_batches(max_chunksize=batch_size)
    if cursor < len(top_table):
        yield from top_table.slice(cursor).to_batches(max_chunksize=batch_size)
