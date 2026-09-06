import assert from "node:assert/strict";
import test from "node:test";
import {
  getExecutionPlans,
  getRegisteredExecutionReport,
  submitExecution,
} from "./api.ts";
import {
  canQueuePlan,
  executionTargetText,
  isExecutionWorker,
  isRegisteredJob,
  isSyntheticJob,
} from "./executionPresentation.ts";

const plan = (changes = {}) => ({
  id: "registered-plan",
  revision: "v1",
  label: "已登记离线检查",
  role: "data_processing",
  enabled: true,
  attempt: null,
  eligible_resources: [
    {
      id: "lan",
      label: "研究计算机",
      eligible: true,
      online: false,
      ready: false,
      reason: "worker_not_connected",
    },
    {
      id: "other",
      label: "其他资源",
      eligible: false,
      online: true,
      ready: true,
      reason: "role_not_allowed",
    },
  ],
  requirements: { continuous_plan: true, required_output_count: 1 },
  warnings: ["No automatic provisioning"],
  ...changes,
});

test("registered plans may queue for an offline worker, never for a forbidden resource", () => {
  assert.equal(canQueuePlan(plan(), "lan"), true);
  assert.equal(canQueuePlan(plan(), "auto"), true);
  assert.equal(canQueuePlan(plan(), "other"), false);
  assert.equal(canQueuePlan(plan(), "missing"), false);
  assert.equal(canQueuePlan(plan({ enabled: false }), "lan"), false);
  assert.equal(canQueuePlan(undefined, "auto"), false);
  assert.equal(canQueuePlan(plan({ eligible_resources: [] }), "auto"), false);
});

test("an existing plan revision never permits a duplicate submission, including failed/lost attempts", () => {
  for (const status of [
    "queued",
    "running",
    "completed",
    "failed",
    "lost",
    "canceled",
  ]) {
    const existing = plan({ attempt: { job_id: "existing-job", status } });
    assert.equal(canQueuePlan(existing, "auto"), false);
    assert.equal(canQueuePlan(existing, "lan"), false);
  }
});

test("plan execution readiness is not relabeled as physical-host reachability", () => {
  assert.match(executionTargetText(plan(), "lan"), /worker/);
  assert.match(executionTargetText(plan(), "lan"), /不据此判定主机关机/);
  assert.match(executionTargetText(plan(), "auto"), /不自动改派至本机/);
  assert.match(executionTargetText(plan(), "other"), /不在/);
});

test("real job and worker classifications cannot enter synthetic report/comparison paths", () => {
  const real = {
    id: "real",
    status: "completed",
    classification: "operator_registered_offline",
  };
  const synthetic = {
    id: "demo",
    status: "completed",
    classification: "synthetic_non_economic",
  };
  assert.equal(isRegisteredJob(real), true);
  assert.equal(isSyntheticJob(real), false);
  assert.equal(isSyntheticJob(synthetic), true);
  assert.equal(isRegisteredJob(synthetic), false);
  assert.equal(isSyntheticJob({ id: "unknown", status: "completed" }), false);
  assert.equal(
    isExecutionWorker({
      id: "worker",
      classification: "offline_execution_worker",
    }),
    true,
  );
  assert.equal(
    isExecutionWorker({ id: "demo", classification: "synthetic_demo_worker" }),
    false,
  );
});

test("execution submission sends only plan/resource identifiers and preserves retry key", async (t) => {
  const requests = [];
  t.mock.method(globalThis, "fetch", async (url, request) => {
    requests.push({
      url,
      method: request.method,
      body: JSON.parse(request.body),
      key: request.headers["Idempotency-Key"],
    });
    return new Response(
      JSON.stringify({ id: "existing", jobs: [{ id: "job" }] }),
    );
  });
  await submitExecution("registered-plan", "lan", "one-attempt");
  await submitExecution("registered-plan", "lan", "one-attempt");
  assert.deepEqual(requests[0], {
    url: "/api/executions",
    method: "POST",
    body: { plan_id: "registered-plan", resource_id: "lan" },
    key: "one-attempt",
  });
  assert.deepEqual(requests[1], requests[0]);
});

test("plan catalog preserves disabled/unknown readiness instead of inventing executable targets", async (t) => {
  let payload = {
    items: [plan()],
    limitations: ["Existing complete plans only"],
  };
  t.mock.method(
    globalThis,
    "fetch",
    async () => new Response(JSON.stringify(payload)),
  );
  assert.equal(
    (await getExecutionPlans()).items[0].eligible_resources[0].ready,
    false,
  );
  payload = {
    ...payload,
    items: [
      plan({ eligible_resources: [{ id: "lan", online: true, ready: true }] }),
    ],
  };
  await assert.rejects(getExecutionPlans(), /计划目录/);
});

test("registered execution report keeps empty summaries and refuses synthetic reports", async (t) => {
  let payload = {
    schema_version: "registered_execution_report.v1",
    classification: "operator_registered_offline",
    plan_id: "registered-plan",
    revision: "v1",
    resource_id: "lan",
    role: "data_processing",
    summary: {},
    artifacts: [{ name: "checks.json", size_bytes: 21 }],
    environment: {
      runner: "operator-registered-offline",
      returncode: 0,
      elapsed_seconds: 1.2,
      resource_id: "lan",
      python: "3.12",
      platform: "Linux",
    },
    limitations: [],
  };
  t.mock.method(
    globalThis,
    "fetch",
    async () => new Response(JSON.stringify(payload)),
  );
  const result = await getRegisteredExecutionReport("job");
  assert.deepEqual(result.summary, {});
  assert.equal("trace" in result, false);
  payload = { ...payload, classification: "synthetic_non_economic" };
  await assert.rejects(getRegisteredExecutionReport("job"), /离线执行报告/);
});
