"""Registered offline plans use real subprocesses, not demo-reference validation."""

import copy
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from narrowgate import studio
from narrowgate import studio_execution as execution


@pytest.fixture
def manifest(tmp_path, monkeypatch):
    monkeypatch.setattr(execution, "memory_available_gib", lambda: 8)
    script = tmp_path / "audit.py"
    script.write_text(
        "import pathlib, sys\n"
        "p=pathlib.Path(sys.argv[1]); (p/'audit.csv').write_text('day,count\\n2026-01-01,1\\n')\n"
        "print('output at ' + str(p))\n"
    )
    raw = tmp_path / "input.csv"
    raw.write_text("id,value\n1,2\n")
    data = {
        "visibility": "local_only_do_not_publish",
        "resources": [
            {
                "id": "mac",
                "label": "Training workstation",
                "kind": "local",
                "roles": {
                    "training": "preferred",
                    "replay": "allowed",
                    "data_processing": "allowed",
                },
            },
            {
                "id": "lan",
                "label": "Replay host",
                "kind": "lan",
                "roles": {
                    "training": "disabled",
                    "replay": "allowed",
                    "data_processing": "allowed",
                },
            },
        ],
        "plans": [
            {
                "id": "audit",
                "revision": "r1",
                "label": "Small raw audit",
                "role": "data_processing",
                "live": False,
                "enabled": True,
                "preferred_resources": ["lan"],
                "targets": {},
            }
        ],
    }
    for rid in ("mac", "lan"):
        output = tmp_path / f"output-{rid}"
        data["plans"][0]["targets"][rid] = {
            "argv": [sys.executable, str(script), str(output)],
            "cwd": str(tmp_path),
            "env": {"OMP_NUM_THREADS": "1"},
            "required_files": [str(raw)],
            "output_dir": str(output),
            "required_outputs": ["audit.csv"],
            "summary_files": [],
            "memory_gib": 0.1,
            "max_seconds": 10,
        }
    path = tmp_path / "execution.private.json"
    path.write_text(json.dumps(data))
    path.chmod(0o600)
    return path


def rewrite(path, mutate):
    data = json.loads(path.read_text())
    mutate(data)
    path.write_text(json.dumps(data))


def registered(store, rid="lan", worker_id=None, catalog=None):
    worker_id = worker_id or f"{rid}-worker"
    store.register(worker_id, "session", execution.registration(catalog or store.execution, rid))
    return worker_id


def submitted(store, key="request", resource="auto", plan_id="audit"):
    return store.create_execution({"plan_id": plan_id, "resource_id": resource}, key)["jobs"][0]


@pytest.fixture
def store(tmp_path, manifest):
    return studio.Store(tmp_path / "control", manifest)


class DirectClient:
    def __init__(self, store, worker_id="lan-worker"):
        self.store, self.worker_id = store, worker_id

    def post(self, route, payload):
        job_id = route.split("/jobs/")[1].split("/")[0]
        if route.endswith("/heartbeat"):
            return self.store.heartbeat(job_id, self.worker_id, payload["session"])
        return self.store.publish(job_id, self.worker_id, payload["session"], payload)


def run_registered(store, tmp_path):
    registered(store)
    submitted(store)
    job = store.claim("lan-worker", "session")
    worker_root = tmp_path / "worker"
    worker_root.mkdir()
    execution.execute(
        DirectClient(store),
        "lan-worker",
        "session",
        job,
        worker_root,
        lambda: False,
        store.execution,
        "lan",
    )
    return store.job(job["id"]), worker_root


def test_only_registered_ids_and_idempotency_no_duplicate_attempt(store, manifest):
    client = TestClient(studio.create_app(store.root, execution_manifest=manifest))
    body = {"plan_id": "audit", "resource_id": "auto"}
    for extra in ("argv", "cwd", "env", "parameters", "output_dir"):
        assert (
            client.post(
                "/api/executions",
                json={**body, extra: "anything"},
                headers={"Idempotency-Key": "request"},
            ).status_code
            == 400
        )
    first = client.post("/api/executions", json=body, headers={"Idempotency-Key": "request"})
    assert first.status_code == 200
    assert (
        client.post("/api/executions", json=body, headers={"Idempotency-Key": "request"}).json()
        == first.json()
    )
    assert (
        client.post(
            "/api/executions", json=body, headers={"Idempotency-Key": "another"}
        ).status_code
        == 409
    )
    catalog = client.get("/api/execution-plans").json()
    assert catalog["items"][0]["attempt"] == {
        "job_id": first.json()["jobs"][0]["id"],
        "status": "queued",
    }
    assert str(manifest.parent) not in json.dumps(catalog)
    assert "argv" not in json.dumps(catalog)


@pytest.mark.parametrize(
    "defect", ["public", "permission", "live", "lan_training", "shell", "credential"]
)
def test_manifest_rejects_unregistered_execution_boundaries(manifest, defect):
    if defect == "permission":
        manifest.chmod(0o644)
    else:

        def change(data):
            plan = data["plans"][0]
            if defect == "public":
                data["visibility"] = "public"
            elif defect == "live":
                plan["live"] = True
            elif defect == "lan_training":
                plan["role"] = "training"
                data["resources"][1]["roles"]["training"] = "allowed"
            elif defect == "shell":
                plan["targets"]["lan"]["argv"] = [sys.executable, "-c", "print(1)"]
            elif defect == "credential":
                plan["targets"]["lan"]["env"]["API_KEY"] = "not-a-real-key"

        rewrite(manifest, change)
    with pytest.raises(ValueError):
        execution.Catalog(manifest)


def test_unavailable_lan_stays_queued_without_automatic_mac_fallback(store):
    registered(store, "mac")
    job = submitted(store)
    assert store.claim("mac-worker", "session") is None
    assert "no_auto_local_fallback" in store.job(job["id"])["queue_reason"]
    assert store.job(job["id"])["status"] == "queued"
    registered(store)
    assert store.claim("lan-worker", "session")["id"] == job["id"]


@pytest.mark.parametrize("field", ["argv", "required_files", "env", "revision"])
def test_configuration_drift_is_not_claimed(store, manifest, field):
    submitted(store)

    def change(data):
        plan = data["plans"][0]
        if field == "revision":
            plan["revision"] = "r2"
        elif field == "env":
            plan["targets"]["lan"]["env"]["OMP_NUM_THREADS"] = "2"
        else:
            plan["targets"]["lan"][field].append(
                "--different" if field == "argv" else "/missing/file.csv"
            )

    rewrite(manifest, change)
    worker_catalog = execution.Catalog(manifest)
    registered(store, catalog=worker_catalog)
    assert store.claim("lan-worker", "session") is None
    with store.connect() as db:
        nodes = execution.worker_view(db, studio.LEASE_SECONDS)
    assert nodes[0]["plans"][0]["ready"] is False
    assert nodes[0]["plans"][0]["reason"] == "registered_plan_configuration_changed"


def test_changed_control_configuration_retains_queued_attempt(store, manifest):
    job = submitted(store)
    registered(store)
    rewrite(manifest, lambda data: data["plans"][0]["targets"]["lan"]["argv"].append("--different"))
    restarted = studio.Store(store.root, manifest)
    assert restarted.claim("lan-worker", "session") is None
    assert restarted.job(job["id"])["queue_reason"] == "registered_plan_configuration_changed"


@pytest.mark.parametrize("changed", ["revision", "signature"])
def test_claim_is_rechecked_before_popen(store, tmp_path, monkeypatch, changed):
    registered(store)
    submitted(store)
    job = store.claim("lan-worker", "session")
    if changed == "revision":
        job["revision"] = "other"
    else:
        job["target_signature"] = "f" * 64
    monkeypatch.setattr(execution.platform, "platform", lambda: "test-platform")
    monkeypatch.setattr(
        studio.subprocess, "Popen", lambda *a, **k: pytest.fail("configuration drift executed")
    )
    execution.execute(
        DirectClient(store),
        "lan-worker",
        "session",
        job,
        tmp_path,
        lambda: False,
        store.execution,
        "lan",
    )
    assert store.job(job["id"])["status"] == "failed"
    assert not Path(store.execution.plans["audit"]["targets"]["lan"]["output_dir"]).exists()


def test_one_physical_resource_slot_across_worker_ids(store, manifest):
    rewrite(
        manifest,
        lambda data: data["plans"].append({**copy.deepcopy(data["plans"][0]), "id": "audit-2"}),
    )
    store = studio.Store(store.root, manifest)
    registered(store, worker_id="lan-one")
    registered(store, worker_id="lan-two")
    submitted(store)
    submitted(store, key="other", plan_id="audit-2")
    first = store.claim("lan-one", "session")
    assert first is not None
    assert store.claim("lan-two", "session") is None
    with store.connect() as db:
        db.execute("UPDATE jobs SET updated_at=0 WHERE id=?", (first["id"],))
    store.expire()
    assert store.job(first["id"])["status"] == "lost"
    assert store.claim("lan-two", "session") is None
    assert studio.Store(store.root, manifest).claim("lan-one", "session")["id"] == first["id"]


def test_distinct_resources_can_claim_independent_whole_plans(store, manifest):
    rewrite(
        manifest,
        lambda data: data["plans"].append({**copy.deepcopy(data["plans"][0]), "id": "audit-2"}),
    )
    store = studio.Store(store.root, manifest)
    registered(store, "mac")
    registered(store)
    submitted(store, resource="lan")
    submitted(store, key="other", resource="mac", plan_id="audit-2")
    assert store.claim("lan-worker", "session")["resource_id"] == "lan"
    assert store.claim("mac-worker", "session")["resource_id"] == "mac"


def test_demo_and_execution_workers_cannot_steal_each_others_jobs(store):
    registered(store)
    store.register("demo", "demo-session")
    real = submitted(store)
    assert store.claim("demo", "demo-session") is None
    demo = store.create(
        {"name": "Demo", "runner": studio.RUNNER, "dataset": studio.DATASET, "arms": ["B0"]},
        "demo-request",
    )["jobs"][0]
    assert store.claim("lan-worker", "session")["id"] == real["id"]
    assert store.claim("demo", "demo-session")["id"] == demo["id"]


def test_real_completion_required_outputs_public_report_and_private_locators(
    store, tmp_path, manifest, monkeypatch
):
    monkeypatch.setattr(
        studio, "validate_demo_artifacts", lambda *_: pytest.fail("real job used demo validation")
    )
    job, root = run_registered(store, tmp_path)
    assert job["status"] == "completed"
    assert (root / job["id"] / "publication.json").is_file()
    assert (root / job["id"] / "stdout.log").is_file()
    client = TestClient(studio.create_app(store.root, execution_manifest=manifest))
    report = client.get(f"/api/jobs/{job['id']}/report")
    assert report.status_code == 200
    assert report.json()["classification"] == "operator_registered_offline"
    assert report.json()["summary"] == {}
    assert report.json()["artifacts"] == [{"name": "audit.csv", "size_bytes": 23}]
    assert "cwd" not in report.text and str(tmp_path) not in report.text
    logs = client.get(f"/api/jobs/{job['id']}/logs")
    assert str(tmp_path) not in logs.text and "private locator" in logs.text
    assert client.get(f"/api/owner/jobs/{job['id']}/locators").status_code == 403
    owner = TestClient(studio.create_app(store.root, "owner-token", execution_manifest=manifest))
    assert owner.get(f"/api/owner/jobs/{job['id']}/locators").status_code == 401
    assert (
        owner.get(
            f"/api/owner/jobs/{job['id']}/locators", headers={"Authorization": "Bearer owner-token"}
        )
        .json()["output_dir"]
        .endswith("output-lan")
    )
    with pytest.raises(studio.Conflict, match="already has an attempt"):
        submitted(store, key="never-repeat")


def test_exit_zero_with_missing_output_is_failed(store, tmp_path):
    Path(store.execution.plans["audit"]["targets"]["lan"]["argv"][1]).write_text(
        "print('done without result')\n"
    )
    job, _ = run_registered(store, tmp_path)
    assert job["status"] == "failed"
    assert "required_result_file" in job["error"]
    assert json.loads(store.read_artifacts(job["id"])["environment.json"])["returncode"] == 0


def test_preflight_inputs_memory_output_and_external_process(manifest, monkeypatch):
    target = execution.Catalog(manifest).plans["audit"]["targets"]["lan"]
    assert execution.preflight(target) == (True, None)
    monkeypatch.setattr(execution, "memory_available_gib", lambda: 0.01)
    assert execution.preflight(target)[1] == "insufficient_available_memory"
    monkeypatch.setattr(execution, "memory_available_gib", lambda: 8)
    target["exclusive_process_contains"] = ["specific-registered-job.py", "--some-fixed-day"]
    monkeypatch.setattr(
        execution.subprocess,
        "check_output",
        lambda *a, **k: "123 python specific-registered-job.py --some-fixed-day",
    )
    assert execution.preflight(target)[1] == "registered_external_process_already_running"
    target.pop("exclusive_process_contains")
    Path(target["output_dir"]).mkdir()
    assert execution.preflight(target)[1] == "fixed_output_already_exists_no_overwrite_or_resume"


def test_actual_worker_entrypoint_uses_registered_adapter(store, tmp_path, manifest, monkeypatch):
    submitted(store)
    client = TestClient(studio.create_app(store.root, execution_manifest=manifest))

    class HTTPBridge:
        def post(self, route, body):
            response = client.post(route, json=body)
            assert response.status_code == 200, response.text
            return response.json()

    monkeypatch.setattr(studio, "Client", lambda *_: HTTPBridge())
    monkeypatch.setattr(studio.signal, "signal", lambda *_: None)
    monkeypatch.setattr(studio, "execute_demo", lambda *_: pytest.fail("real worker started demo"))
    args = SimpleNamespace(
        url="http://127.0.0.1:8080",
        worker_id="lan-worker",
        work_dir=tmp_path / "worker",
        once=True,
        execution_manifest=manifest,
        resource_id="lan",
    )
    assert studio.worker(args) == 0
    assert store.jobs()[0]["status"] == "completed"
    node = client.get("/api/nodes").json()["items"][0]
    assert node["classification"] == "offline_execution_worker" and node["resource_id"] == "lan"
    assert node["capabilities"] == [execution.RUNNER]
    assert "signature" not in json.dumps(node) and str(tmp_path) not in json.dumps(node)


def test_control_connection_failure_reaps_child_and_retains_durable_failed_outbox(
    store, tmp_path, monkeypatch
):
    registered(store)
    submitted(store)
    job = store.claim("lan-worker", "session")
    script = Path(store.execution.plans["audit"]["targets"]["lan"]["argv"][1])
    script.write_text("import time\ntime.sleep(30)\n")
    children = []
    original = studio.subprocess.Popen

    def tracked(*args, **kwargs):
        child = original(*args, **kwargs)
        children.append(child)
        return child

    class Broken:
        def post(self, *_):
            raise RuntimeError("control protocol failure")

    monkeypatch.setattr(studio.subprocess, "Popen", tracked)
    with pytest.raises(RuntimeError, match="control protocol"):
        execution.execute(
            Broken(), "lan-worker", "session", job, tmp_path, lambda: False, store.execution, "lan"
        )
    assert children and children[0].poll() is not None
    publication = json.loads((tmp_path / job["id"] / "publication.json").read_text())
    assert publication["status"] == "failed"
    assert (tmp_path / job["id"] / "stdout.log").is_file()
