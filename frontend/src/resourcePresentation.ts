import type { ComputeResource, Worker } from "./api.ts";

export const RESOURCE_KIND: Record<ComputeResource["kind"], string> = {
  local: "本机计算主机",
  lan: "局域网计算主机",
  azure: "Azure 云计算资源",
};

export const RESOURCE_STATE: Record<
  ComputeResource["state"],
  { label: string; style: string }
> = {
  online: { label: "在线", style: "status-completed" },
  offline: { label: "探测不可达", style: "status-lost" },
  unknown: { label: "状态未确认", style: "" },
  stale: { label: "探测已过期", style: "status-lost" },
  scaled_to_zero: { label: "已缩到 0 · 未运行", style: "status-queued" },
};

export function resourceCounts(resources: ComputeResource[]) {
  return {
    registered: resources.length,
    online: resources.filter((resource) => resource.state === "online").length,
    scaledToZero: resources.filter(
      (resource) => resource.state === "scaled_to_zero",
    ).length,
    activeJobs: resources
      .filter((resource) => resource.state === "online")
      .flatMap((resource) => resource.jobs)
      .filter((job) => ["running", "archiving"].includes(job.status)).length,
  };
}

export function resourceRole(role: string): string {
  return (
    {
      preferred: "优先使用",
      allowed: "允许",
      disabled: "不分配",
      unknown: "未登记",
    }[role] ?? "未登记"
  );
}

export function schedulerLabel(resource: ComputeResource): string {
  if (resource.scheduler.mode === "external_observer")
    return "外部研究任务 · 仅状态接入";
  if (resource.scheduler.mode === "studio_worker")
    return resource.scheduler.can_submit
      ? "Studio worker 已接入 · 能力以登记为准"
      : "Studio worker 已登记 · 不可提交";
  return "未接入 Studio 调度";
}

export function workerLabel(
  worker: Worker,
  resources: ComputeResource[],
  index: number,
): string {
  const resource = resources.find((item) =>
    item.worker_ids.includes(worker.id),
  );
  return resource
    ? `${resource.label} · 合成演示进程`
    : `未绑定主机的合成演示进程 ${index + 1}`;
}

export function researchJobStatus(status: string): string {
  return (
    {
      queued: "排队中",
      not_started: "尚未开始",
      waiting: "等待中",
      running: "运行中",
      archiving: "归档中",
      completed: "已完成",
      succeeded: "已完成",
      failed: "失败",
      canceled: "已取消",
      stopped: "已停止",
      unknown: "状态未知",
    }[status] ?? status
  );
}
