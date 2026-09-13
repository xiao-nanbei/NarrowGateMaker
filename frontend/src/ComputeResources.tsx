import { display, stamp } from "./api";
import type { ComputeResource, ComputeResources, Worker } from "./api";
import type { ExecutionPlanCatalog } from "./api";
import { ExecutionPlans } from "./ExecutionPlans";
import { isExecutionWorker } from "./executionPresentation";
import {
  RESOURCE_KIND,
  RESOURCE_STATE,
  researchJobStatus,
  resourceCounts,
  resourceRole,
  schedulerLabel,
  workerLabel,
} from "./resourcePresentation";

const known = (value: string | number | null, unit = "") =>
  value === null
    ? "未记录"
    : `${typeof value === "number" ? new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 2 }).format(value) : value}${unit}`;

function ResourceCard({
  resource,
  workers,
}: {
  resource: ComputeResource;
  workers: Worker[];
}) {
  const state = RESOURCE_STATE[resource.state];
  const current = resource.state === "online";
  const executors = workers.filter(
    (worker) => isExecutionWorker(worker) && worker.resource_id === resource.id,
  );
  return (
    <article className="node-card resource-card">
      <header>
        <span className="node-icon">
          {resource.kind === "azure" ? "☁" : "▦"}
        </span>
        <div className="resource-title">
          <h3>{resource.label}</h3>
          <p>{RESOURCE_KIND[resource.kind]}</p>
        </div>
        <span className={`status ${state.style}`}>
          <i />
          {state.label}
        </span>
      </header>
      <dl>
        <div>
          <dt>状态核验 / UTC</dt>
          <dd>{stamp(resource.checked_at, true)}</dd>
        </div>
        <div>
          <dt>CPU / 架构</dt>
          <dd>
            {known(resource.hardware.cpu_name)}
            <br />
            {known(resource.hardware.architecture)}
          </dd>
        </div>
        <div>
          <dt>计算规格</dt>
          <dd>
            {known(resource.hardware.vcpu, " vCPU")} ·{" "}
            {known(resource.hardware.memory_gib, " GiB")}
          </dd>
        </div>
        {resource.kind === "azure" && (
          <div>
            <dt>节点数：当前 / 目标</dt>
            <dd>
              {known(resource.capacity.running_nodes)} /{" "}
              {known(resource.capacity.target_nodes)}
            </dd>
          </div>
        )}
        <div>
          <dt>已登记分工</dt>
          <dd>
            训练：{resourceRole(resource.roles.training)}
            <br />
            回测：{resourceRole(resource.roles.replay)}
            <br />
            数据处理：{resourceRole(resource.roles.data_processing)}
          </dd>
        </div>
        <div>
          <dt>执行接入</dt>
          <dd>{schedulerLabel(resource)}</dd>
        </div>
      </dl>
      <p className="resource-notes">{resource.scheduler.reason}</p>
      <div className="resource-jobs">
        <h4>离线执行 worker · 与资源探测独立</h4>
        {!executors.length ? (
          <p>
            没有此资源的离线执行 worker 登记；仅 SSH /
            资源探测在线不能领取任务。
          </p>
        ) : (
          executors.map((worker, index) => (
            <div className="resource-worker" key={worker.id}>
              <div className="resource-job">
                <strong>执行进程 {index + 1}</strong>
                <span
                  className={`status ${worker.online === true ? "status-completed" : worker.online === false ? "status-lost" : ""}`}
                >
                  {worker.online === true
                    ? "心跳有效"
                    : worker.online === false
                      ? "心跳过期"
                      : "心跳未知"}
                </span>
              </div>
              <p>最近心跳 {stamp(worker.last_seen, true)} UTC</p>
              {worker.plans?.map((plan) => (
                <p key={`${plan.id}:${plan.revision}`}>
                  <span className="mono">{plan.id}</span> ·{" "}
                  {plan.ready ? "执行条件已满足" : "等待执行条件"}
                  {plan.reason ? ` · ${plan.reason}` : ""}
                </p>
              ))}
              {!worker.plans?.length && <p>尚无已登记计划就绪信息。</p>}
            </div>
          ))
        )}
      </div>
      {resource.last_error && (
        <p className="notice error">最近探测：{resource.last_error}</p>
      )}
      <div className="resource-jobs">
        <h4>真实研究作业 · 独立于合成队列</h4>
        {resource.jobs.length ? (
          <>
            {!current && (
              <p>以下为已有任务记录，不据此推断节点或作业当前仍在运行。</p>
            )}
            {resource.jobs.map((job) => (
              <div className="resource-job" key={job.id}>
                <div>
                  {job.label}
                  {job.arm && <span className="small-tag">{job.arm}</span>}
                  <span className="mono">
                    更新 {stamp(job.updated_at, true)} UTC
                  </span>
                </div>
                <span
                  className={`status ${current ? `status-${job.status}` : ""}`}
                >
                  {!current && "上次记录："}
                  {researchJobStatus(job.status)}
                </span>
              </div>
            ))}
          </>
        ) : (
          <p>
            {resource.state === "scaled_to_zero"
              ? "环境保留，当前没有分配计算节点。"
              : "没有已接入的真实作业记录；不据此断言主机完全空闲。"}
          </p>
        )}
      </div>
      {resource.notes.length > 0 && (
        <ul className="resource-notes">
          {resource.notes.map((note, index) => (
            <li key={index}>{note}</li>
          ))}
        </ul>
      )}
    </article>
  );
}

export function ComputeResourcesView({
  report,
  workers,
  error,
  loading,
  plans,
  plansError,
  onCreated,
}: {
  report: ComputeResources | null;
  workers: Worker[];
  error: string;
  loading: boolean;
  plans: ExecutionPlanCatalog | null;
  plansError: string;
  onCreated: (id?: string) => void;
}) {
  const resources = report?.items ?? [];
  const counts = resourceCounts(resources);
  const demos = workers.filter(
    (worker) => worker.classification === "synthetic_demo_worker",
  );
  return (
    <section className="panel compute-panel">
      <div className="section-heading">
        <div>
          <span className="eyebrow">
            COMPUTE RESOURCES / RESEARCH WORKLOADS
          </span>
          <h2>真实计算资源与研究作业</h2>
        </div>
        <span className="muted">
          资源状态与 worker 心跳分开记录 · 不自动开机或扩容
        </span>
      </div>
      {report && (
        <div className="compute-summary">
          <span>
            <strong>{counts.registered}</strong>处已登记资源
          </span>
          <span>
            <strong>{counts.online}</strong>处当前在线
          </span>
          <span>
            <strong>{counts.scaledToZero}</strong>处已缩到 0
          </span>
          <span>
            <strong>{counts.activeJobs}</strong>个当前观测运行作业
          </span>
          <span>目录读取 {stamp(report.observed_at, true)} UTC</span>
        </div>
      )}
      {error && (
        <div className="notice error" role="alert">
          计算资源状态读取失败：{error}。未沿用旧的在线状态，也未用合成 worker
          替代。
        </div>
      )}
      {!report && !error && (
        <div className="empty" role="status">
          {loading ? "正在读取计算资源…" : "尚未取得计算资源目录。"}
        </div>
      )}
      {report && !resources.length && (
        <div className="empty">
          <h3>尚未登记真实计算资源</h3>
          <p>
            服务端配置主机或云资源后会显示在这里；合成进程登记不会自动成为物理主机。
          </p>
        </div>
      )}
      <ExecutionPlans
        catalog={plans}
        error={plansError}
        onCreated={onCreated}
      />
      <div className="nodes-grid">
        {resources.map((resource) => (
          <ResourceCard
            key={resource.id}
            resource={resource}
            workers={workers}
          />
        ))}
      </div>
      {report?.limitations.length ? (
        <div className="notice">
          {report.limitations.map((text, index) => (
            <p key={index}>{text}</p>
          ))}
        </div>
      ) : null}
      <details className="worker-diagnostics">
        <summary>合成演示进程登记 · {demos.length} 个（不是主机数量）</summary>
        <p>
          下列心跳只表示 demo worker
          进程。进程退出后登记仍可能保留，心跳过期不等于所在主机或云节点掉线。
        </p>
        <div className="nodes-grid">
          {demos.map((worker, index) => (
            <article className="node-card" key={worker.id}>
              <header>
                <h3>{workerLabel(worker, resources, index)}</h3>
                <span
                  className={`status ${worker.online ? "status-completed" : "status-lost"}`}
                >
                  {worker.online === true
                    ? "进程心跳有效"
                    : worker.online === false
                      ? "进程心跳已过期"
                      : "进程心跳未知"}
                </span>
              </header>
              <dl>
                <div>
                  <dt>最近心跳 / UTC</dt>
                  <dd>{stamp(worker.last_seen, true)}</dd>
                </div>
                <div>
                  <dt>合成执行能力</dt>
                  <dd>
                    {worker.capabilities?.length
                      ? worker.capabilities.map(display).join(" · ")
                      : "未登记"}
                  </dd>
                </div>
              </dl>
            </article>
          ))}
        </div>
        {!demos.length && <p>当前没有合成 worker 登记。</p>}
      </details>
    </section>
  );
}
