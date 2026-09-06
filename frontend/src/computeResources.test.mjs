import assert from "node:assert/strict";
import test from "node:test";
import { getComputeResources } from "./api.ts";
import {
  RESOURCE_STATE,
  resourceCounts,
  resourceRole,
  schedulerLabel,
  workerLabel,
} from "./resourcePresentation.ts";

const resource = (changes = {}) => ({
  id: "local",
  label: "本地研究主机",
  kind: "local",
  state: "online",
  checked_at: "2026-09-07T00:00:00Z",
  last_error: null,
  hardware: {
    cpu_name: "Example CPU",
    architecture: "arm64",
    vcpu: 8,
    memory_gib: 16,
  },
  capacity: { running_nodes: null, target_nodes: null },
  roles: {
    training: "preferred",
    replay: "allowed",
    data_processing: "allowed",
  },
  scheduler: {
    mode: "external_observer",
    can_submit: false,
    reason: "Existing runner only",
  },
  jobs: [
    {
      id: "paired-ec",
      label: "Paired EC",
      status: "running",
      arm: "EC",
      updated_at: "2026-09-07T00:00:00Z",
    },
  ],
  worker_ids: ["synthetic-a", "synthetic-b"],
  notes: [],
  ...changes,
});

test("resource counts never substitute synthetic worker processes for physical resources", () => {
  const resources = [
    resource(),
    resource({ id: "lan", kind: "lan", jobs: [], worker_ids: [] }),
    resource({
      id: "cloud",
      kind: "azure",
      state: "scaled_to_zero",
      jobs: [],
      capacity: { running_nodes: 0, target_nodes: 0 },
    }),
  ];
  assert.deepEqual(resourceCounts(resources), {
    registered: 3,
    online: 2,
    scaledToZero: 1,
    activeJobs: 1,
  });
  assert.deepEqual(resourceCounts([]), {
    registered: 0,
    online: 0,
    scaledToZero: 0,
    activeJobs: 0,
  });
});

test("stale or unknown historical observations do not count as currently running work", () => {
  for (const state of ["stale", "unknown", "offline", "scaled_to_zero"]) {
    const counts = resourceCounts([resource({ state })]);
    assert.equal(counts.online, 0);
    assert.equal(counts.activeJobs, 0);
    assert.notEqual(RESOURCE_STATE[state].label, "在线");
  }
  assert.match(RESOURCE_STATE.scaled_to_zero.label, /0/);
  assert.notEqual(
    RESOURCE_STATE.scaled_to_zero.label,
    RESOURCE_STATE.offline.label,
  );
});

test("friendly worker labels map two processes to one host without inventing host associations", () => {
  const resources = [resource()];
  for (const id of ["synthetic-a", "synthetic-b"]) {
    assert.equal(
      workerLabel({ id }, resources, 0),
      "本地研究主机 · 合成演示进程",
    );
  }
  assert.match(workerLabel({ id: "unknown-demo" }, resources, 2), /未绑定主机/);
  assert.doesNotMatch(
    workerLabel({ id: "unknown-demo" }, resources, 2),
    /unknown-demo/,
  );
});

test("training assignment and scheduler integration remain distinct from reachability", () => {
  assert.equal(resourceRole("preferred"), "优先使用");
  assert.equal(resourceRole("disabled"), "不分配");
  assert.equal(resourceRole("unrecorded"), "未登记");
  assert.match(schedulerLabel(resource()), /仅状态接入/);
  assert.match(
    schedulerLabel(
      resource({ scheduler: { mode: "not_connected", can_submit: false } }),
    ),
    /未接入/,
  );
  assert.match(
    schedulerLabel(
      resource({ scheduler: { mode: "studio_worker", can_submit: false } }),
    ),
    /不可提交/,
  );
});

test("compute resource API preserves zero cloud nodes and unknown hardware", async (t) => {
  const payload = {
    schema_version: "compute_resources.v1",
    observed_at: "2026-09-07T00:00:00Z",
    items: [
      resource({
        id: "cloud",
        kind: "azure",
        state: "scaled_to_zero",
        hardware: {
          cpu_name: null,
          architecture: null,
          vcpu: null,
          memory_gib: null,
        },
        capacity: { running_nodes: 0, target_nodes: 0 },
        jobs: [],
      }),
    ],
    limitations: ["Read-only external status"],
  };
  t.mock.method(globalThis, "fetch", async (path) => {
    assert.equal(path, "/api/compute-resources");
    return new Response(JSON.stringify(payload));
  });
  const result = await getComputeResources();
  assert.equal(result.items[0].capacity.running_nodes, 0);
  assert.equal(result.items[0].hardware.vcpu, null);
  assert.equal(result.items[0].scheduler.can_submit, false);
});

test("compute resource API refuses worker-only and malformed resource records", async (t) => {
  let payload = { items: [{ id: "demo-a", online: true }] };
  t.mock.method(
    globalThis,
    "fetch",
    async () => new Response(JSON.stringify(payload)),
  );
  await assert.rejects(getComputeResources(), /计算资源目录/);
  for (const change of [
    { state: "looks_online" },
    { scheduler: { mode: "not_connected" } },
    { kind: "synthetic_worker" },
  ]) {
    payload = {
      schema_version: "compute_resources.v1",
      observed_at: null,
      items: [resource(change)],
      limitations: [],
    };
    await assert.rejects(getComputeResources(), /计算资源目录/);
  }
  payload = { ...payload, items: [resource(), resource()] };
  await assert.rejects(getComputeResources(), /计算资源目录/);
});
