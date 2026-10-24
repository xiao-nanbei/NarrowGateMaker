"""Local lifecycle over the existing finite-account executor, not a scheduler."""

from __future__ import annotations

import fcntl
import os
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

from narrowgate.runtime.artifact_io import atomic_json, load_json

from .plan import RESOLVED_SCHEMA, account_identity, digest, resolve, source_identity


@contextmanager
def operation_lock(root):
    root.mkdir(parents=True, exist_ok=True)
    with (root / "operation.lock").open("a") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def active(root):
    """An owned lock is authoritative; PID existence alone is not."""
    try:
        with (root / "operation.lock").open("r+") as stream:
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
    except FileNotFoundError:
        pass
    return False


def _stored(root):
    value = load_json(root / "resolved-plan.json")
    if value.get("schema") != RESOLVED_SCHEMA or digest(value["binding"]) != value.get(
        "economic_identity"
    ):
        raise ValueError("resolved plan identity is corrupt")
    if Path(value["locations"]["output"]) != root:
        raise ValueError("run location changed; explicit artifact handoff required")
    return value


def status(root):
    root = Path(root).resolve(strict=True)
    plan = _stored(root)
    live = active(root)
    value = dict(
        run_id=plan["run_id"],
        output=str(root),
        active=live,
        status="RUNNING" if live else "NOT_RUNNING",
        economic_complete=False,
    )
    for name in ("lifecycle", "progress"):
        path = root / f"{name}.json"
        if path.exists():
            value[name] = load_json(path)
    value["pause_requested"] = any(root.glob("pause-*.json"))
    if (root / "completion.json").exists():
        # Marker presence is not full payload/accounting acceptance.
        value["status"] = "COMPLETION_PRESENT"
        value["verification"] = "use runs report for complete payload validation"
    elif not live:
        if (root / "latest.checkpoint").exists():
            value["status"] = (
                "PAUSED" if value.get("progress", {}).get("status") == "PAUSED" else "RECOVERABLE"
            )
        elif (root / "progress.json").exists():
            value["status"] = "RECOVERY_MISSING"
        else:
            value["status"] = "NOT_STARTED_OR_FAILED_BEFORE_FIRST_CUT"
    return value


def request_pause(root):
    root = Path(root).resolve(strict=True)
    plan = _stored(root)
    if (root / "completion.json").exists() or not active(root):
        return status(root)
    request = dict(
        run_id=plan["run_id"], economic_identity=plan["economic_identity"], requested_at=time.time()
    )
    atomic_json(root / f"pause-{uuid.uuid4().hex}.json", request)
    return dict(
        status="PAUSE_REQUESTED",
        run_id=plan["run_id"],
        note="not paused yet; saves at the next supported cut, or completes first",
    )


def _validator(plan):
    from .account import effective_parameter_record, validate_bound_cell

    expected = effective_parameter_record(plan["binding"]["params"])
    ident = account_identity(plan)

    def validate(root):
        def parameters(actual, arm):
            if arm != ident["arm_id"] or actual != expected:
                raise ValueError("run effective parameters differ from resolved plan")

        return validate_bound_cell(root, ident, parameters)

    return validate


def report(root):
    root = Path(root).resolve(strict=True)
    plan = _stored(root)
    if not (root / "completion.json").exists():
        return dict(
            status(root), metrics=None, accounting=None, note="incomplete account; no terminal PnL"
        )
    receipt = _validator(plan)(root)
    metrics = load_json(root / "metrics.json")
    volume, notional = metrics["volume_btc"], metrics["notional_usdc"]
    return dict(
        status="COMPLETE",
        run_id=plan["run_id"],
        identity=receipt["identity"],
        economic_complete=True,
        economic_admission=False,
        metrics=dict(
            metrics,
            pnl_per_btc=None if volume == 0 else metrics["net_pnl"] / volume,
            pnl_bps=None if notional == 0 else 10_000 * metrics["net_pnl"] / notional,
        ),
        accounting=load_json(root / "accounting.json"),
        artifacts={name: str(root / name) for name in receipt["payload_sha256"]},
        limitations=plan["limitations"],
    )


def _execute(plan, root):
    from models.backtest_tick import XMARKET_REPLAY_FEATURE_COLUMNS, prepare_public_inputs
    from models.replay.public_input import PreparedPublicPredictions

    from .account import DAY_MS, diagnostics, execute_bound_cell

    if source_identity() != plan["binding"]["release_identity"]:
        raise ValueError(
            "runtime source changed; use the bound release, not implicit checkpoint conversion"
        )
    b = plan["binding"]
    ident = account_identity(plan)
    atomic_json(root / "lifecycle.json", dict(status="PREPARING", pid=os.getpid(), at=time.time()))
    prepared = prepare_public_inputs(
        plan["locations"]["bundle"], tick_size=b["params"]["tick_size"]
    )
    if prepared.manifest_id != b["input_manifest_id"]:
        raise ValueError("run input manifest changed")
    predictions = None
    if plan["locations"]["model_dir"] is not None:
        from strategy.signal import SignalEngine

        engine = SignalEngine.from_public_models(
            Path(plan["locations"]["model_dir"]), symbol="BTCUSDC"
        )
        predictions = PreparedPublicPredictions.create(
            prepared, engine, XMARKET_REPLAY_FEATURE_COLUMNS
        )

    def paused():
        requests = list(root.glob("pause-*.json"))
        for request in requests:
            if load_json(request).get("economic_identity") != plan["economic_identity"]:
                raise ValueError("foreign pause request")
        return bool(requests)

    def generic_diagnostics(result, journal, *, full_calendar):
        value = diagnostics(result, journal, full_calendar=full_calendar)
        value["schema"] = "narrowgate.account-diagnostics.v1"
        value["tail_contract"] = "explicit plan account; no sample-size or profitability admission"
        return value

    atomic_json(root / "lifecycle.json", dict(status="RUNNING", pid=os.getpid(), at=time.time()))
    result = execute_bound_cell(
        prepared,
        ident=ident,
        params=b["params"],
        prepared_predictions=predictions,
        initial_capital=b["accounting"]["initial_capital"],
        max_mark_age_ns=b["accounting"]["max_mark_age_ns"],
        funding=b["funding"],
        output=root,
        validate_output=_validator(plan),
        full_calendar=False,
        diagnostic_builder=generic_diagnostics,
        incremental_journal=True,
        durable_interval_ms=DAY_MS,
        pause_requested=paused,
        retain_completed_checkpoint=True,
        checkpoint_key="resolved_run_plan",
    )
    atomic_json(
        root / "lifecycle.json", dict(status=result["status"], pid=os.getpid(), at=time.time())
    )
    return result


def run(plan_path):
    plan = resolve(plan_path)
    root = Path(plan["locations"]["output"])
    with operation_lock(root):
        if (root / "resolved-plan.json").exists():
            previous = _stored(root)
            if previous != plan:
                raise ValueError("output already belongs to a different resolved plan")
            if (root / "completion.json").exists():
                return report(root)
            raise ValueError("run already exists; use replay resume, never implicit fresh start")
        if any(p.name != "operation.lock" for p in root.iterdir()):
            raise ValueError("new run output is not empty")
        atomic_json(root / "resolved-plan.json", plan)
        try:
            return _execute(plan, root)
        except Exception as exc:
            atomic_json(
                root / "lifecycle.json", dict(status="FAILED", error=str(exc), at=time.time())
            )
            raise


def resume(root):
    root = Path(root).resolve(strict=True)
    with operation_lock(root):
        plan = _stored(root)
        if (root / "completion.json").exists():
            return report(root)
        if not (root / "latest.checkpoint").exists():
            raise ValueError("no durable recovery checkpoint; refusing fresh start")
        acknowledged_requests = list(root.glob("pause-*.json"))
        # Re-read bound input/model/funding through the original plan. A new
        # path/algorithm is a handoff, not an implicit compatible resume.
        if resolve(plan["source_plan"]) != plan:
            raise ValueError("plan or bound artifacts changed since admission")
        # Acknowledge before input preparation; a pause arriving during
        # preparation must still be honored at the next legal cut.
        for request in acknowledged_requests:
            if load_json(request).get("economic_identity") != plan["economic_identity"]:
                raise ValueError("foreign pause request")
            request.unlink()
        try:
            return _execute(plan, root)
        except Exception as exc:
            atomic_json(
                root / "lifecycle.json", dict(status="FAILED", error=str(exc), at=time.time())
            )
            raise
