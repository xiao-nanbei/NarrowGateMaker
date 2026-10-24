"""Shared finite-account replay, recovery and complete accounting artifacts.

Research callers own policy and experiment identity; this owner contains no
family imports, scheduler or strategy selection. Frozen releases are unchanged.
"""

import fcntl
import hashlib
import json
import math
import shutil
import time
import uuid
from copy import deepcopy
from pathlib import Path

from narrowgate.runtime.artifact_io import atomic_json as _atomic
from narrowgate.runtime.artifact_io import cell_lock
from narrowgate.runtime.artifact_io import load_json as _load
from narrowgate.runtime.artifact_io import numeric_json as _numeric

DAY_NS = 86400 * 10**9
HOUR_MS = 3_600_000
DAY_MS = 24 * HOUR_MS


def ledger_boundaries(start_ns, end_ns):
    """Account-relative daily slices plus its exact (possibly short) tail."""
    return [*range(start_ns + DAY_NS, end_ns, DAY_NS), end_ns]


def sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for block in iter(lambda: f.read(4 * 1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def simulate_slice(
    prepared, params, predictions, journal, checkpoint=None, cut=None, initialization_lock=None
):
    """The maintained engine owns all account/order/RNG state, never another arm."""
    from models.backtest_tick import simulate_prepared_inputs

    actual = deepcopy(params)
    actual["_l2_journal"] = journal
    lock = None
    try:
        if initialization_lock is not None:
            lock = Path(initialization_lock).open("a")
            fcntl.flock(lock, fcntl.LOCK_EX)
            original = actual.get("_replay_progress_callback")
            released = False
            # Bound a resumed slice's wait for its first operational callback.
            # This changes observer cadence only, never simulated compute/time.
            actual["_replay_progress_interval_events"] = 1000

            def entered_loop(value):
                # Operational callback is excluded by the existing checkpoint
                # binding and stripped at save; no business parameter changes.
                nonlocal released
                if not released:
                    fcntl.flock(lock, fcntl.LOCK_UN)
                    released = True
                if original is not None:
                    original(value)

            actual["_replay_progress_callback"] = entered_loop
        return simulate_prepared_inputs(
            prepared,
            actual,
            prepared_predictions=predictions,
            resume_checkpoint=checkpoint,
            consume_resume_checkpoint=checkpoint is not None,
            checkpoint_at_ts_ms=cut,
        )
    finally:
        if lock is not None:
            fcntl.flock(lock, fcntl.LOCK_UN)
            lock.close()


def diagnostics(result, journal, *, full_calendar=False, decision_observer=None):
    """Keep full event rows for paired opportunity joins; no invented denominators."""
    from collections import Counter

    from execution.chunked_parquet_journal import iter_chunked_parquet_journal

    counts = Counter()
    missing = Counter()
    decisions = 0
    for row in iter_chunked_parquet_journal(journal["manifest"], verify_checksum=True):
        kind = row["event_type"]
        counts[kind] += 1
        if kind == "decision":
            decisions += 1
            p = row["record"]
            for key in (
                "quote_snapshot_market_generation",
                "market_readiness",
                "action",
                "base_price",
                "final_price",
                "base_size",
                "final_size",
            ):
                if key not in p:
                    missing[key] += 1
            if decision_observer is not None:
                decision_observer(p)
    if missing or not journal["independent_delivery_verified"]:
        raise ValueError("incomplete decision-to-order diagnostic delivery")
    risk_names = (
        "max_drawdown",
        "max_inventory",
        "abs_inventory_time_s",
        "sq_inventory_time_s",
        "inventory_lifecycle_count",
        "inventory_lifecycle_closed_count",
        "inventory_lifecycle_open_count",
        "inventory_lifecycle_max_duration_s",
        "inventory_lifecycle_max_adverse_excursion",
    )
    return {
        "schema": "mechanism407_diagnostics.v1" if full_calendar else "mechanism12_diagnostics.v1",
        "events": dict(counts),
        "decisions": decisions,
        "missing_fields": dict(missing),
        "risk": {k: result.get(k) for k in risk_names},
        "risk_missing": [k for k in risk_names if k not in result],
        "common_opportunity_key": (
            "side + quote_snapshot_market_generation; aggregate repeated own-arm decisions, "
            "retain union/missing/intersection, never align own order IDs across arms"
        ),
        "opportunity_scope": (
            "instrumented side callbacks after original warmup; "
            "not every external event is an eligible decision"
        ),
        "censoring": (
            "open lifecycle at account_end is administratively censored; no forced liquidation"
        ),
        "tail_contract": (
            "all 204 original parents including the 24h tail; "
            "frozen paired block diagnostics, previous-use retained; no equivalence claim"
            if full_calendar
            else "report all 12 paired parent outcomes, minimum and empirical lower decile; "
            "no significance/equivalence claim"
        ),
        "production": journal["production"],
    }


def validate_bound_cell(output, ident, validate_params):
    """Shared payload/accounting checks after an owner validates its identity."""
    output = Path(output)
    receipt = _load(output / "completion.json")
    if receipt.get("identity") != ident or receipt.get("economic_complete") is not True:
        raise ValueError("mechanism completion identity differs")
    required = {
        "result.json",
        "accounting.json",
        "metrics.json",
        "effective_params.json",
        "daily-ledger.json",
        "diagnostics.json",
    }
    payloads = receipt["payload_sha256"]
    if not required.issubset(payloads) or not any(x.endswith("/manifest.json") for x in payloads):
        raise ValueError("missing mechanism payloads")
    for name, digest in payloads.items():
        p = output / name
        if not p.resolve().is_relative_to(output.resolve()) or p.is_symlink() or sha(p) != digest:
            raise ValueError("mechanism payload changed: " + name)
    accounting = _load(output / "accounting.json")
    ledger = _load(output / "daily-ledger.json")
    metrics = _load(output / "metrics.json")
    result = _load(output / "result.json")
    params = _load(output / "effective_params.json")
    validate_params(params, ident["arm_id"])
    if (
        accounting.get("economic_complete") is not True
        or ledger["accounting"] != accounting
        or accounting["account_start_ns"] != ident["start_ns"]
        or accounting["account_end_ns"] != ident["end_ns"]
        or result["public_input_contract"]["input_manifest_id"] != ident["input_manifest_id"]
        or receipt["durable_cut_ms"] * 10**6 != ident["end_ns"]
        or params["account_start_ns"] != ident["start_ns"]
        or any(params.get(k) != v for k, v in ident["seed_pair"].items())
        or ledger["input_manifest_id"] != ident["input_manifest_id"]
        or ledger["account_start_ns"] != ident["start_ns"]
        or ledger["account_end_ns"] != ident["end_ns"]
        or metrics["net_pnl"] != accounting["all_in_net_pnl"]
        or len(ledger["rows"]) != len(ledger_boundaries(ident["start_ns"], ident["end_ns"]))
        or ledger["account_reset"] is not False
    ):
        raise ValueError("mechanism accounting binding differs")
    boundaries = [ident["start_ns"], *ledger_boundaries(ident["start_ns"], ident["end_ns"])]
    for i, row in enumerate(ledger["rows"]):
        if (row["start_ns"], row["end_ns"]) != (boundaries[i], boundaries[i + 1]):
            raise ValueError("daily ledger hole")
    for key in ("net_pnl", "volume_btc", "notional_usdc"):
        if not math.isfinite(metrics[key]) or not math.isclose(
            math.fsum(x[key] for x in ledger["rows"]), metrics[key], rel_tol=1e-10, abs_tol=1e-8
        ):
            raise ValueError("ledger totals differ")
    if not math.isclose(
        accounting["terminal_equity"] - accounting["initial_capital"],
        metrics["net_pnl"],
        rel_tol=0,
        abs_tol=1e-8,
    ):
        raise ValueError("equity identity differs")
    for key in ("fees", "funding_cashflow"):
        if not math.isclose(
            math.fsum(row[key] for row in ledger["rows"]),
            accounting[key],
            rel_tol=1e-10,
            abs_tol=1e-8,
        ):
            raise ValueError("daily fee/funding totals differ")
    if not math.isclose(
        accounting["pnl_before_funding"] + accounting["funding_cashflow"],
        metrics["net_pnl"],
        rel_tol=0,
        abs_tol=1e-8,
    ):
        raise ValueError("funding accounting identity differs")
    return receipt


def effective_parameter_record(params):
    """Persist configuration, never a mutable cooldown episode or input array.

    The optional engine hook has one explicit contract projection. Ordinary
    callers retain the original numeric JSON normalization; arbitrary objects
    still fail serialization rather than being silently stringified.
    """
    record = dict(params)
    # Progress is an operational observer, excluded by the replay checkpoint
    # binding as well. It must not become a serialized business parameter.
    record.pop("_replay_progress_callback", None)
    if "_cooldown_rearm_runtime" in record:
        runtime = record["_cooldown_rearm_runtime"]
        contract = getattr(runtime, "contract", None)
        if not isinstance(contract, dict) or contract.get("schema") != "cooldown-rearm-replay-v1":
            raise ValueError("cooldown runtime requires its explicit immutable contract")
        record["_cooldown_rearm_runtime"] = deepcopy(contract)
    if "_quote_price_runtime" in record:
        runtime = record["_quote_price_runtime"]
        contract = getattr(runtime, "contract", None)
        if not isinstance(contract, dict) or contract.get("schema") not in {"quote-price-replay-v1", "quote-price-replay-v2"}:
            raise ValueError("quote-price runtime requires its explicit immutable contract")
        record["_quote_price_runtime"] = deepcopy(contract)
    return json.loads(json.dumps(record, default=_numeric))


def execute_bound_cell(
    prepared,
    *,
    ident,
    params,
    prepared_predictions,
    initial_capital,
    max_mark_age_ns,
    funding,
    output,
    validate_output,
    full_calendar,
    progress=None,
    initialization_lock=None,
    checkpoint_key="mechanism_cell",
    diagnostic_builder=diagnostics,
    retain_completed_checkpoint=False,
    incremental_journal=False,
    durable_interval_ms=None,
    pause_requested=None,
):
    """Unchanged replay/accounting primitive, with an explicit owner boundary.

    The caller must validate identity and full effective parameters first. This
    interface is not a scheduler or an alternative arm registry.
    """
    from models.replay.l2_journal import ReplayL2Journal
    from models.replay.public_accounting import public_equity_ledger, settle_public_replay
    from models.replay.runtime_checkpoint_io import (
        load_trusted_runtime_checkpoint,
        save_runtime_checkpoint,
    )

    if durable_interval_ms is None:
        durable_interval_ms = HOUR_MS
    if type(durable_interval_ms) is not int or durable_interval_ms not in (HOUR_MS, DAY_MS):
        raise ValueError("mechanism durable interval must be one replay hour or day")
    plan = prepared.inputs["bundle"].manifest["plan"]
    if (
        prepared.manifest_id != ident["input_manifest_id"]
        or plan["end_ns"] != ident["end_ns"]
        or params["account_start_ns"] != ident["start_ns"]
        or params.get("initial_live_state")
        or params.get("replay_initial_state_mode", "fresh_start") != "fresh_start"
    ):
        raise ValueError("mechanism input/account binding differs")
    # Snapshot before execution: per-fill mutations belong to checkpoint and
    # diagnostics, not effective configuration. Reuse compares this same form.
    parameter_record = effective_parameter_record(params)
    output = Path(output)
    with cell_lock(output):
        if (output / "identity.json").exists() and _load(output / "identity.json") != ident:
            raise ValueError("foreign mechanism owner")
        _atomic(output / "identity.json", ident)
        if (output / "completion.json").exists():
            validate_output(output)
            if _load(output / "effective_params.json") != parameter_record:
                raise ValueError("completed mechanism parameters differ")
            return {"status": "ALREADY_COMPLETE", "identity": ident}
        checkpoint_path = output / "latest.checkpoint"
        if (output / "progress.json").exists() and not checkpoint_path.exists():
            raise ValueError(
                "recorded account progress has no recovery checkpoint; refusing fresh start"
            )
        checkpoint = (
            load_trusted_runtime_checkpoint(checkpoint_path) if checkpoint_path.exists() else None
        )
        if checkpoint is not None and checkpoint.get(checkpoint_key) != ident:
            raise ValueError("foreign mechanism checkpoint")
        if (
            incremental_journal
            and checkpoint is not None
            and not checkpoint.get("l2_prefix", {}).get("manifest_sha256")
        ):
            raise ValueError(
                "legacy checkpoint requires its original journal release; no implicit migration"
            )
        # This is one independently owned account, not a branching replay.
        # Reuse its append-only sink between hourly memory cuts. Heavy saves
        # remain hourly unless the caller opts into daily durability. Always
        # retain the last disk checkpoint's journal prefix, even while memory
        # advances beyond it; a process failure may still need that prefix.
        journal = None
        if incremental_journal:
            from models.replay.continuous_l2_journal import ContinuousReplayL2Journal

            journal = ContinuousReplayL2Journal(
                output / "journal" / uuid.uuid4().hex, identity=ident
            )
        durable_cut = checkpoint["cut_ts_ms"] if checkpoint else None
        durable_manifest = Path(checkpoint["l2_prefix"]["manifest"]) if checkpoint else None

        def retire_prefix(manifest, *, protected=()):
            if manifest is None or manifest in protected:
                return
            prefix = manifest.parent
            if not prefix.is_relative_to(output / "journal"):
                return
            if prefix != journal.writer.output_dir:
                if not any(p is not None and p.parent == prefix for p in protected):
                    shutil.rmtree(prefix)
            elif manifest.name.startswith("prefix-"):
                manifest.unlink(missing_ok=True)

        cuts = list(
            range(ident["start_ns"] // 10**6 + HOUR_MS, ident["end_ns"] // 10**6, HOUR_MS)
        ) + [None]
        for cut in cuts:
            if cut is not None and checkpoint is not None and cut <= checkpoint["cut_ts_ms"]:
                continue
            old_manifest = Path(checkpoint["l2_prefix"]["manifest"]) if checkpoint else None
            if not incremental_journal:
                journal = ReplayL2Journal(output / "journal" / uuid.uuid4().hex, identity=ident)
            slice_started = time.monotonic()
            result = simulate_slice(
                prepared,
                params,
                prepared_predictions,
                journal,
                checkpoint,
                cut,
                initialization_lock,
            )
            slice_seconds = time.monotonic() - slice_started
            if cut is not None:
                checkpoint = result["_replay_checkpoint"]
                checkpoint[checkpoint_key] = ident
                if checkpoint["cut_ts_ms"] != cut:
                    raise ValueError("unexpected memory cut")
                # Cooperative control is checked only at a supported account cut.
                # A requested stop forces durability; a signal/exit is not a save.
                paused = pause_requested is not None and pause_requested()
                current_manifest = Path(checkpoint["l2_prefix"]["manifest"])
                previous_durable = durable_manifest
                save_seconds = 0.0
                if (
                    paused
                    or durable_cut is None
                    or (cut - ident["start_ns"] // 10**6) % durable_interval_ms == 0
                ):
                    save_started = time.monotonic()
                    save_runtime_checkpoint(checkpoint_path, checkpoint)
                    save_seconds = time.monotonic() - save_started
                    durable_cut, durable_manifest = cut, current_manifest
                protected = (current_manifest, durable_manifest)
                retire_prefix(old_manifest, protected=protected)
                if previous_durable != old_manifest:
                    retire_prefix(previous_durable, protected=protected)
                status = {
                    "status": "PAUSED" if paused else "PARTIAL",
                    "identity": ident,
                    "memory_cut_ms": cut,
                    "durable_cut_ms": durable_cut,
                    "slice_seconds_including_initialization_wait": slice_seconds,
                    "checkpoint_save_seconds": save_seconds,
                    "journal_rows": journal.writer.row_count,
                    "journal_mode": "single_owner_incremental"
                    if incremental_journal
                    else "branching_prefix_copy",
                }
                _atomic(output / "progress.json", status)
                if progress:
                    progress(status)
                if paused:
                    return status
        accounting = settle_public_replay(
            prepared.inputs["bundle"].root,
            result,
            initial_capital=initial_capital,
            max_mark_age_ns=max_mark_age_ns,
            funding=deepcopy(funding),
        )
        ledger = public_equity_ledger(
            prepared.inputs["bundle"].root,
            result,
            ledger_boundaries(ident["start_ns"], ident["end_ns"]),
            initial_capital=initial_capital,
            max_mark_age_ns=max_mark_age_ns,
            funding=deepcopy(funding),
        )
        fills = (
            result["_economic_fill_trace"]
            if result.get("private_fill_visibility_enabled")
            else result["_fill_trace"]
        )
        metrics = {
            "net_pnl": accounting["all_in_net_pnl"],
            "volume_btc": math.fsum(x["fill_qty"] for x in fills),
            "notional_usdc": math.fsum(x["fill_qty"] * x["quote_px"] for x in fills),
        }
        values = {
            "result": result,
            "accounting": accounting,
            "daily-ledger": ledger,
            "effective_params": parameter_record,
            "metrics": metrics,
            "diagnostics": diagnostic_builder(
                result, result["_l2_journal"], full_calendar=full_calendar
            ),
        }
        for name, value in values.items():
            _atomic(output / (name + ".json"), value)
        files = [output / (name + ".json") for name in values]
        files += [p for p in journal.writer.output_dir.rglob("*") if p.is_file()]
        receipt = {
            "identity": ident,
            "economic_complete": True,
            "durable_cut_ms": ident["end_ns"] // 10**6,
            "payload_sha256": {str(p.relative_to(output)): sha(p) for p in files},
        }
        _atomic(output / "completion.json", receipt)
        validate_output(output)
        if not retain_completed_checkpoint:
            checkpoint_path.unlink(missing_ok=True)
            checkpoint_path.with_name(checkpoint_path.name + ".metadata.json").unlink(
                missing_ok=True
            )
            # The final receipt already binds every file in the current
            # writer. Only separate obsolete prefix directories can retire.
            if old_manifest is not None and old_manifest.parent != journal.writer.output_dir:
                retire_prefix(old_manifest)
            if (
                durable_manifest is not None
                and durable_manifest != old_manifest
                and durable_manifest.parent != journal.writer.output_dir
            ):
                retire_prefix(durable_manifest)
        elif old_manifest is not None and old_manifest.parent != journal.writer.output_dir:
            retire_prefix(old_manifest, protected=(durable_manifest,))
        return {"status": "COMPLETE", "identity": ident}
