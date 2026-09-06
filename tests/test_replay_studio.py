"""Control/worker boundaries, using the existing synthetic replay artifacts only."""

import csv
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from urllib.error import HTTPError, URLError

import pytest

pytest.importorskip("fastapi", reason="install .[studio,dev] for the optional Studio service")
from fastapi.testclient import TestClient

from narrowgate import studio
from narrowgate.replay_demo import DEFAULT_REFERENCE_DIR


@pytest.fixture
def store(tmp_path):
    return studio.Store(tmp_path / "control")


def create(store, arms=None):
    return store.create(
        {
            "name": "Test",
            "runner": "replay-demo",
            "dataset": "synthetic-demo",
            "arms": arms or ["B0"],
        },
        "request-1",
    )


def claim(store, worker="node-1", session="session-1"):
    store.register(worker, session)
    return store.claim(worker, session)


def artifacts(status="completed"):
    files = {
        name: (DEFAULT_REFERENCE_DIR / name).read_text()
        for name in ("summary.json", "trace.jsonl", "receipt.json")
    }
    files.update(
        {
            "stdout.log": "reference verified\n",
            "stderr.log": "",
            "environment.json": '{"python":"3.12","returncode":0}',
        }
    )
    return {"files": files, "status": status}


def test_idempotency_survives_control_restart(store):
    first = create(store, ["B0", "C1"])
    restarted = studio.Store(store.root)
    assert create(restarted, ["B0", "C1"]) == first
    with pytest.raises(studio.Conflict, match="different request"):
        create(restarted, ["B0"])
    assert len(store.jobs()) == 2


def test_concurrent_claims_have_one_owner_each(store):
    create(store, ["B0", "C1"])
    with ThreadPoolExecutor(max_workers=3) as pool:
        rows = list(pool.map(lambda i: claim(store, f"node-{i}", f"session-{i}"), range(3)))
    assert sum(row is not None for row in rows) == 2
    assert len({row["id"] for row in rows if row}) == 2
    row = next(row for row in rows if row)
    assert store.claim(row["worker_id"], f"session-{row['worker_id'].split('-')[-1]}") == row


def test_queue_cancel_never_runs_and_running_cancel_waits_for_ack(store):
    jobs = create(store, ["B0", "C1"])["jobs"]
    store.cancel(jobs[0]["id"])
    running = claim(store)
    assert running["id"] == jobs[1]["id"]
    assert store.cancel(running["id"])["status"] == "cancel_requested"
    assert store.heartbeat(running["id"], "node-1", "session-1")["cancel"]
    with pytest.raises(studio.Conflict, match="cannot publish success"):
        store.publish(running["id"], "node-1", "session-1", artifacts())
    done = store.publish(running["id"], "node-1", "session-1", artifacts("canceled"))
    assert done["status"] == "canceled"


def test_expired_worker_is_lost_not_requeued(store):
    create(store)
    running = claim(store)
    with store.connect() as db:
        db.execute("UPDATE jobs SET updated_at=0")
    store.expire()
    assert store.job(running["id"])["status"] == "lost"
    assert claim(store, "node-2", "session-2") is None
    assert not store.heartbeat(running["id"], "node-1", "session-1")["cancel"]
    assert store.job(running["id"])["status"] == "running"
    with pytest.raises(studio.Conflict, match="worker id is still owned"):
        store.register("node-1", "new-session")


@pytest.mark.parametrize(
    "missing",
    ["stdout.log", "stderr.log", "environment.json", "summary.json", "trace.jsonl", "receipt.json"],
)
def test_completion_requires_accessible_outputs_and_logs(store, missing):
    create(store)
    running = claim(store)
    payload = artifacts()
    del payload["files"][missing]
    with pytest.raises(ValueError):
        store.publish(running["id"], "node-1", "session-1", payload)
    assert store.job(running["id"])["status"] == "running"


def test_corrupt_and_cross_attempt_output_cannot_replace_results(store):
    create(store)
    running = claim(store)
    with pytest.raises(studio.Conflict):
        store.publish(running["id"], "node-1", "wrong-session", artifacts())
    payload = artifacts()
    payload["files"]["trace.jsonl"] += "{}\n"
    with pytest.raises(ValueError, match="reference mismatch"):
        store.publish(running["id"], "node-1", "session-1", payload)
    good = artifacts()
    result = store.publish(running["id"], "node-1", "session-1", good)
    assert store.publish(running["id"], "node-1", "session-1", good) == result
    good["files"]["stdout.log"] = "overwritten"
    with pytest.raises(studio.Conflict, match="cannot be replaced"):
        store.publish(running["id"], "node-1", "session-1", good)


@pytest.mark.parametrize("changes", [{"status": []}, {"error": {"message": "bad"}}])
def test_malformed_publication_is_a_client_error(store, changes):
    create(store)
    running = claim(store)
    client = TestClient(studio.create_app(store.root))
    response = client.post(
        f"/api/workers/node-1/jobs/{running['id']}/publish",
        json={**artifacts(), "session": "session-1", **changes},
    )
    assert response.status_code == 400
    assert store.job(running["id"])["status"] == "running"


def test_disk_failure_does_not_report_completion(store, monkeypatch):
    create(store)
    running = claim(store)
    monkeypatch.setattr(studio.os, "fsync", lambda fd: (_ for _ in ()).throw(OSError("disk full")))
    with pytest.raises(OSError, match="disk full"):
        store.publish(running["id"], "node-1", "session-1", artifacts())
    assert store.job(running["id"])["status"] == "running"


def test_api_filters_private_runners_cross_origin_and_shell(store):
    client = TestClient(studio.create_app(store.root))
    assert client.get("/api/runners").json()["items"][0]["id"] == "replay-demo"
    request = {"name": "test", "runner": "replay-demo", "dataset": "synthetic-demo", "arms": ["B0"]}
    for key, value in (("runner", "live"), ("dataset", "sealed_holdout"), ("command", "uname")):
        response = client.post(
            "/api/experiments", json={**request, key: value}, headers={"Idempotency-Key": "id-1"}
        )
        assert response.status_code == 400
    assert (
        client.post(
            "/api/experiments", json=request, headers={"Origin": "https://untrusted.invalid"}
        ).status_code
        == 403
    )
    assert client.get("/api/jobs", headers={"Host": "untrusted.invalid"}).status_code == 403
    assert client.post("/api/experiments", json=[]).status_code == 400
    assert client.get("/api/jobs/not-a-job").status_code == 404


def test_api_report_uses_canonical_values_only_after_publication(store):
    job_id = create(store)["jobs"][0]["id"]
    client = TestClient(studio.create_app(store.root))
    assert client.get(f"/api/jobs/{job_id}/report").status_code == 409
    claim(store)
    store.publish(job_id, "node-1", "session-1", artifacts())
    report = client.get(f"/api/jobs/{job_id}/report").json()
    assert report["summary"] == json.loads(artifacts()["files"]["summary.json"])
    assert report["classification"] == "synthetic_non_economic"
    assert client.get(f"/api/jobs/{job_id}/logs").json()["stdout"] == "reference verified\n"


def test_api_optional_bearer_authentication(store):
    client = TestClient(studio.create_app(store.root, "test-only-token"))
    assert client.get("/api/jobs").status_code == 401
    assert (
        client.get("/api/jobs", headers={"Authorization": "Bearer test-only-token"}).status_code
        == 200
    )


@pytest.fixture
def b0_source(tmp_path):
    """Small input-schema fixture, not real research data or a replica execution."""
    root = tmp_path / "private-source"
    output = root / "local_fixture"
    output.mkdir(parents=True)
    days = ["2025-01-01", "2025-01-02"]
    segment_id = "2025-01-01_2025-01-02"
    amounts = {
        "trading_pnl_after_fees_usdc": -2.0,
        "funding_cashflow_usdc": 0.25,
        "net_pnl_usdc": -1.75,
        "fill_fee_cost_usdc": 0.1,
        "fills": 4,
        "campaigns": 2,
        "buy_fills": 2,
        "sell_fills": 2,
        "closed_campaigns": 1,
        "open_campaigns": 1,
    }
    verification = {
        "all_segments_complete": True,
        "full_fill_trace_reconciled": True,
        "funding_cashflows_reconciled": True,
        "campaign_values_reconciled_with_csv_rounding": True,
        "queue_lookup_count": 4,
        "queue_exact_count": 2,
        "queue_known_zero_count": 1,
        "queue_missing_count": 1,
        "native_events_consumed": 12,
        "native_events_rejected": 2,
        "native_gap_invalid_sequence_time_reversal_counts": 0,
        "host_comparison_days": days,
    }
    source = {
        "visibility": "local_only_do_not_publish",
        "arm": "baseline",
        "source_commit": "fixture",
        "unique_utc_days": 2,
        "continuous_segments": 1,
        "dates": days,
        "strategy": {"config_sha256": "fixture"},
        "verification": verification,
        "totals": amounts,
        "segments": [
            {**amounts, "segment": segment_id, "days": 2, "selected_output": "local_fixture/result"}
        ],
        "accounting_basis": str(root / "must-not-appear-in-api"),
        "limitations": [str(root / "must-not-appear-in-api")],
    }
    summary = root / "baseline_summary.json"
    summary.write_text(json.dumps(source))
    (root / "input_plan.json").write_text(
        json.dumps(
            {
                "days": days,
                "source_commit": "fixture",
                "phase": "baseline_and_mechanics_development",
                "economic_release": False,
                "live_actions": False,
                "candidate_training_started": False,
                "segments": [{"id": segment_id, "days": days, "preferred_host": "azure"}],
            }
        )
    )
    (output / "result.json").write_text(
        json.dumps(
            {
                "days": days,
                "arms": ["baseline"],
                "config_sha256": "fixture",
                "accounting_window": "continuous_segment",
                "native_exchange_book_mode": "strict",
                "native_exchange_book_warmup_hours": 24,
                "native_exchange_book_root": str(root),
                "native_exchange_book_identities": {days[0]: {"strict_complete": True}},
            }
        )
    )
    daily = {
        "arm": "baseline",
        "day": days[0],
        "window_end_day": days[-1],
        "window_day_count": 2,
        "accounting_window": "continuous_segment",
        "economic_pnl_complete": True,
        "exchange_book_queue_mode": "strict",
        "exchange_book_queue_scope": "strategy_independent_native_snapshot_delta_exchange_time_v1",
        "queue_l2_cancel_ahead_enabled": False,
        "exchange_book_events_consumed": 12,
        "exchange_book_events_accepted": 10,
        "exchange_book_events_rejected": 2,
        "exchange_book_queue_lookup_count": 4,
        "exchange_book_queue_exact_count": 2,
        "exchange_book_queue_known_zero_count": 1,
        "exchange_book_queue_missing_count": 1,
        "exchange_book_source_gap_events": 0,
        "exchange_book_invalid_sequence_messages": 0,
        "exchange_book_sequence_gaps": 0,
        "exchange_book_message_time_reversals": 0,
        "replay_pnl": -2,
        "funding_cashflow_usdc": 0.25,
        "replay_net_pnl": -1.75,
        "fills_total": 4,
        "campaigns": 2,
        "fills_bid_buy": 2,
        "fills_ask_sell": 2,
        "closed_campaigns": 1,
        "open_campaigns": 1,
    }
    with (output / "result.daily.csv").open("w") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(daily))
        writer.writeheader()
        writer.writerow(daily)
    for suffix in ("campaign_labels", "fill_trace", "funding"):
        (output / f"result.{suffix}.csv").write_text("fixture\n1\n")
    # Unselected interrupted/duplicate outputs must never be discovered or imported.
    (root / "unselected.partial").write_text("incomplete")
    return summary


def test_b0_import_is_private_idempotent_and_never_creates_or_executes_jobs(
    store, b0_source, monkeypatch
):
    monkeypatch.setattr(studio.subprocess, "Popen", lambda *a, **k: pytest.fail("replay started"))
    report = store.import_b0(b0_source)
    assert studio.Store(store.root).import_b0(b0_source) == report
    assert store.jobs() == []
    assert report["summary"]["net_pnl"] == -1.75  # fees already deducted; funding added once
    assert report["summary"]["fees_already_included"] is True
    assert len(report["segments"]) == 1
    assert report["segments"][0]["day_count"] == 2  # do not manufacture two daily PnL rows
    assert report["segments"][0]["source"] == "local"  # actual selected source, not preferred host
    assert report["verification"]["queue_missing_count"] == 1
    assert str(b0_source.parent) not in json.dumps(report)
    client = TestClient(studio.create_app(store.root))
    assert client.get("/api/results").json()["items"][0]["id"] == report["id"]
    assert client.get(f"/api/results/{report['id']}").json() == report
    assert client.post("/api/results", json={"summary": str(b0_source)}).status_code == 405
    assert client.get("/api/results/missing").status_code == 404
    assert client.get("/api/runners").json()["items"][0]["id"] == "replay-demo"
    locked = TestClient(studio.create_app(store.root, "test-only-token"))
    assert locked.get("/api/results").status_code == 401


@pytest.mark.parametrize(
    "defect", ["missing", "partial", "traversal", "overlap", "pnl", "nan", "incomplete"]
)
def test_b0_import_fails_closed_without_mutating_results(store, b0_source, defect):
    source = json.loads(b0_source.read_text())
    if defect == "missing":
        (b0_source.parent / "local_fixture/result.fill_trace.csv").unlink()
    elif defect in {"partial", "traversal"}:
        source["segments"][0]["selected_output"] = (
            "local_fixture/result.partial" if defect == "partial" else "local_fixture/../../result"
        )
    elif defect == "overlap":
        source["segments"].append(source["segments"][0])
        source["continuous_segments"] = 2
    elif defect in {"pnl", "nan"}:
        source["totals"]["net_pnl_usdc"] = 123 if defect == "pnl" else float("nan")
    elif defect == "incomplete":
        source["verification"]["all_segments_complete"] = False
    b0_source.write_text(json.dumps(source))
    with pytest.raises(ValueError):
        store.import_b0(b0_source)
    assert store.results() == []
    assert store.jobs() == []


def test_b0_import_does_not_hash_raw_files_or_gate_research_progress(store, b0_source, monkeypatch):
    from pathlib import Path

    plan_path = b0_source.parent / "input_plan.json"
    plan = json.loads(plan_path.read_text())
    plan.update(
        phase="subsequent_training",
        economic_release=True,
        live_actions=True,
        candidate_training_started=True,
    )
    plan_path.write_text(json.dumps(plan))
    read_bytes, open_file = Path.read_bytes, Path.open
    raw_suffixes = (".fill_trace.csv", ".campaign_labels.csv", ".funding.csv")

    def no_raw_read(path, *args, **kwargs):
        assert not str(path).endswith(raw_suffixes), "raw artifacts must not be read"
        return read_bytes(path, *args, **kwargs)

    def no_raw_open(path, *args, **kwargs):
        assert not str(path).endswith(raw_suffixes), "raw artifacts must not be opened"
        return open_file(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_bytes", no_raw_read)
    monkeypatch.setattr(Path, "open", no_raw_open)
    path = b0_source.parent / "local_fixture/result.daily.csv"
    path.write_text(path.read_text().replace(",strict,", ",disabled,"))
    report = store.import_b0(b0_source)
    assert report["segments"][0]["queue_mode"] == "non_strict"
    assert store.jobs() == []


def test_b0_reimport_uses_summary_identity_and_requires_private_storage(store, b0_source):
    report = store.import_b0(b0_source)
    (b0_source.parent / "local_fixture/result.fill_trace.csv").write_text("fixture\n2\n")
    assert store.import_b0(b0_source) == report  # not a second raw-artifact qualification
    path = b0_source.parent / "local_fixture/result.daily.csv"
    path.write_text(path.read_text().replace(",strict,", ",disabled,"))
    with pytest.raises(studio.Conflict, match="display fields changed"):
        store.import_b0(b0_source)
    assert store.result(report["id"]) == report
    store.root.chmod(0o755)
    with pytest.raises(ValueError, match="owner-only"):
        store.import_b0(b0_source)


def test_events_have_durable_order_and_do_not_leak_worker_session(store):
    job_id = create(store)["jobs"][0]["id"]
    claim(store)
    store.cancel(job_id)
    with studio.Store(store.root).connect() as db:
        events = list(db.execute("SELECT id,data FROM events ORDER BY id"))
    assert [json.loads(r["data"])["status"] for r in events] == [
        "queued",
        "running",
        "cancel_requested",
    ]
    assert all("session" not in json.loads(r["data"]) for r in events)
    assert [r["id"] for r in events] == sorted({r["id"] for r in events})


def test_worker_executes_real_cli_and_publishes_before_success(store, tmp_path):
    create(store)
    job = claim(store)

    class DirectClient:
        def post(self, route, body):
            if route.endswith("/heartbeat"):
                return store.heartbeat(job["id"], "node-1", body["session"])
            assert route.endswith("/publish")
            return store.publish(job["id"], "node-1", body["session"], body)

    studio.execute_demo(DirectClient(), "node-1", "session-1", job, tmp_path, lambda: False)
    assert store.job(job["id"])["status"] == "completed"
    assert (
        store.read_artifacts(job["id"])["trace.jsonl"]
        == (DEFAULT_REFERENCE_DIR / "trace.jsonl").read_text()
    )
    assert (tmp_path / job["id"] / "publication.json").is_file()


def test_worker_kills_child_on_control_exception(store, tmp_path, monkeypatch):
    create(store)
    job = claim(store)
    children = []
    popen = subprocess.Popen

    def long_child(*args, **kwargs):
        child = popen(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            **{key: value for key, value in kwargs.items() if key != "cwd"},
        )
        children.append(child)
        return child

    class BrokenClient:
        def post(self, route, body):
            raise RuntimeError("control protocol error")

    monkeypatch.setattr(studio.subprocess, "Popen", long_child)
    with pytest.raises(RuntimeError, match="control protocol"):
        studio.execute_demo(BrokenClient(), "node-1", "session-1", job, tmp_path, lambda: False)
    assert children[0].poll() is not None


@pytest.mark.parametrize(
    "url", ["http://example.invalid", "https://example.invalid", "file:///etc/passwd"]
)
def test_worker_requires_loopback_tunnel(url):
    with pytest.raises(ValueError, match="loopback"):
        studio.Client(url)


@pytest.fixture
def control_client(store):
    """Exercise the HTTP response contract without a socket or external service."""
    with TestClient(studio.create_app(store.root)) as api:

        class Client:
            def post(self, route, body):
                response = api.post(route, json=body)
                if response.status_code >= 400:
                    raise HTTPError(
                        f"http://testserver{route}",
                        response.status_code,
                        response.text,
                        None,
                        None,
                    )
                return response.json()

        yield Client()


def worker_args(root):
    return SimpleNamespace(
        work_dir=root, worker_id="node-1", url="http://127.0.0.1:8080", once=True
    )


def persisted_worker(root):
    root.mkdir()
    identity = {"worker_id": "node-1", "session": "session-1"}
    studio.atomic_text(root / "worker.json", studio.dumps(identity))
    return identity


def test_worker_honors_cancel_between_runner_exit_and_publication(store, control_client, tmp_path):
    create(store)
    job = claim(store)
    published = []

    class CancelAfterExit:
        def post(self, route, body):
            if route.endswith("/publish"):
                published.append(body["status"])
                if len(published) == 1:
                    assert body["status"] == "completed"
                    assert json.loads(body["files"]["environment.json"])["returncode"] == 0
                    store.cancel(job["id"])
            return control_client.post(route, body)

    studio.execute_demo(CancelAfterExit(), "node-1", "session-1", job, tmp_path, lambda: False)

    assert published == ["completed", "canceled"]
    assert store.job(job["id"])["status"] == "canceled"
    output = store.read_artifacts(job["id"])
    assert output["trace.jsonl"] == artifacts()["files"]["trace.jsonl"]
    assert json.loads(output["environment.json"])["returncode"] == 0
    outbox = json.loads((tmp_path / job["id"] / "publication.json").read_text())
    assert outbox["status"] == "canceled"
    assert outbox["files"] == output


@pytest.mark.parametrize("expired", [False, True])
def test_restart_publishes_persisted_outbox_without_rerunning_demo(
    store, control_client, tmp_path, monkeypatch, expired
):
    create(store, ["B0", "C1"])
    job = claim(store)
    root = tmp_path / "worker"
    identity = persisted_worker(root)
    directory = root / job["id"]
    directory.mkdir()
    payload = {**artifacts(), "session": identity["session"]}
    outbox = directory / "publication.json"
    studio.atomic_text(outbox, studio.dumps(payload))
    original_outbox = outbox.read_bytes()
    if expired:
        with store.connect() as db:
            db.execute("UPDATE jobs SET updated_at=0 WHERE id=?", (job["id"],))
        store.expire()

    monkeypatch.setattr(studio, "Client", lambda *_args: control_client)
    monkeypatch.setattr(studio.signal, "signal", lambda *_args: None)
    monkeypatch.setattr(
        studio, "execute_demo", lambda *_args: pytest.fail("must not rerun an archived attempt")
    )
    assert studio.worker(worker_args(root)) == 0

    restarted = studio.Store(store.root)
    assert restarted.job(job["id"])["status"] == "completed"
    assert restarted.read_artifacts(job["id"]) == payload["files"]
    assert outbox.read_bytes() == original_outbox
    assert json.loads((root / "worker.json").read_text()) == identity
    assert sorted(row["status"] for row in restarted.jobs()) == ["completed", "queued"]


@pytest.mark.parametrize("lost_response", ["register", "claim"])
def test_worker_recovers_lost_control_response_without_duplicate_claim_or_execution(
    store, control_client, tmp_path, monkeypatch, lost_response
):
    create(store, ["B0", "C1"])
    calls, claims, executed = [], [], []
    interrupted = False

    class LoseOneResponse:
        def post(self, route, body):
            nonlocal interrupted
            response = control_client.post(route, body)
            action = route.rsplit("/", 1)[-1]
            calls.append((action, body["session"]))
            if action == "claim" and response["job"]:
                claims.append(response["job"]["id"])
            if action == lost_response and not interrupted:
                interrupted = True
                raise URLError("response lost after the control transaction committed")
            return response

    def complete_once(_client, worker_id, session, job, _root, _stopping):
        executed.append(job["id"])
        studio.Store(store.root).publish(job["id"], worker_id, session, artifacts())

    monkeypatch.setattr(studio, "Client", lambda *_args: LoseOneResponse())
    monkeypatch.setattr(studio.signal, "signal", lambda *_args: None)
    monkeypatch.setattr(studio.time, "sleep", lambda *_args: None)
    monkeypatch.setattr(studio, "execute_demo", complete_once)
    root = tmp_path / "worker"
    assert studio.worker(worker_args(root)) == 0

    assert interrupted
    assert len(executed) == 1
    assert set(claims) == set(executed)
    assert len(claims) == (2 if lost_response == "claim" else 1)
    identity = json.loads((root / "worker.json").read_text())
    assert {session for _action, session in calls} == {identity["session"]}
    assert sorted(row["status"] for row in store.jobs()) == ["completed", "queued"]
    with store.connect() as db:
        transitions = [
            json.loads(row["data"])["status"]
            for row in db.execute("SELECT data FROM events ORDER BY id")
        ]
    assert transitions.count("running") == 1


def test_cancel_survives_expiry_and_control_restart(store):
    create(store)
    job = claim(store)
    store.cancel(job["id"])
    with store.connect() as db:
        db.execute("UPDATE jobs SET updated_at=0 WHERE id=?", (job["id"],))
    store.expire()
    restarted = studio.Store(store.root)

    assert restarted.job(job["id"])["status"] == "lost"
    assert restarted.claim("node-1", "session-1")["id"] == job["id"]
    assert restarted.heartbeat(job["id"], "node-1", "session-1") == {
        "status": "cancel_requested",
        "cancel": True,
    }
    with pytest.raises(studio.Conflict, match="cannot publish success"):
        restarted.publish(job["id"], "node-1", "session-1", artifacts())
    assert (
        restarted.publish(job["id"], "node-1", "session-1", artifacts("canceled"))["status"]
        == "canceled"
    )


def test_restart_does_not_rerun_uncertain_execution_directory(
    store, control_client, tmp_path, monkeypatch
):
    create(store)
    job = claim(store)
    root = tmp_path / "worker"
    persisted_worker(root)
    directory = root / job["id"]
    directory.mkdir()
    monkeypatch.setattr(studio, "Client", lambda *_args: control_client)
    monkeypatch.setattr(studio.signal, "signal", lambda *_args: None)
    monkeypatch.setattr(
        studio, "execute_demo", lambda *_args: pytest.fail("an uncertain attempt cannot rerun")
    )

    with pytest.raises(studio.Conflict, match="execution is uncertain"):
        studio.worker(worker_args(root))
    assert store.job(job["id"])["status"] == "running"
    assert not (directory / "publication.json").exists()


def test_publication_cancel_recovery_retries_transient_heartbeat_failure(
    store, control_client, tmp_path, monkeypatch
):
    create(store)
    job = claim(store)
    store.cancel(job["id"])
    payload = {**artifacts(), "session": "session-1"}
    studio.atomic_text(tmp_path / "publication.json", studio.dumps(payload))
    heartbeat_calls = 0

    class LoseHeartbeat:
        def post(self, route, body):
            nonlocal heartbeat_calls
            if route.endswith("/heartbeat"):
                heartbeat_calls += 1
                if heartbeat_calls == 1:
                    raise URLError("control temporarily unavailable during cancel resolution")
            return control_client.post(route, body)

    monkeypatch.setattr(studio.time, "sleep", lambda *_args: None)
    studio.publish_outbox(
        LoseHeartbeat(), f"/api/workers/node-1/jobs/{job['id']}", payload, tmp_path
    )
    assert heartbeat_calls == 2
    assert store.job(job["id"])["status"] == "canceled"
    assert json.loads((tmp_path / "publication.json").read_text())["status"] == "canceled"


def test_stop_child_reaps_exit_racing_with_signal(monkeypatch):
    waits = []
    process = SimpleNamespace(
        pid=123, poll=lambda: None, wait=lambda **kwargs: waits.append(kwargs)
    )

    def already_exited(*_args):
        raise ProcessLookupError("child exited between poll and signal")

    monkeypatch.setattr(studio.os, "killpg", already_exited)
    studio.stop_child(process)
    assert waits == [{"timeout": 5}]
