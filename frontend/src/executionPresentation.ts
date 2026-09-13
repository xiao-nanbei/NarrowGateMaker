import type { ExecutionPlan, Job, Worker } from "./api.ts";

export const PLAN_ROLE = {
  training: "模型训练",
  replay: "完整回放",
  data_processing: "数据处理",
};

export const isSyntheticJob = (job: Job) =>
  job.classification === "synthetic_non_economic";
export const isRegisteredJob = (job: Job) =>
  job.classification === "operator_registered_offline";
export const isExecutionWorker = (worker: Worker) =>
  worker.classification === "offline_execution_worker";

export function canQueuePlan(
  plan: ExecutionPlan | undefined,
  resourceId: string,
): boolean {
  return Boolean(
    plan?.enabled &&
      !plan.attempt &&
      plan.eligible_resources.some(
        (resource) =>
          resource.eligible &&
          (resourceId === "auto" || resource.id === resourceId),
      ),
  );
}

export function executionTargetText(
  plan: ExecutionPlan,
  resourceId: string,
): string {
  if (resourceId === "auto")
    return "按登记计划的资源优先顺序等待，不自动改派至本机。";
  const resource = plan.eligible_resources.find(
    (item) => item.id === resourceId,
  );
  if (!resource?.eligible) return "所选资源不在此计划的允许范围内。";
  if (!resource.online)
    return "此资源尚无在线离线执行 worker：只进入持久队列，不开机、不扩容；不据此判定主机关机。";
  if (!resource.ready)
    return "执行 worker 已连接，但执行条件未满足：排队等待，不更改计划或输入。";
  return "执行条件最近检查已满足；worker 领取前仍检查资源与计划，可因忙碌排队。";
}
