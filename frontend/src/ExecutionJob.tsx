import { display, stamp } from "./api";
import type { Job, Logs, RegisteredExecutionReport } from "./api";
import { PLAN_ROLE } from "./executionPresentation";
import { researchJobStatus } from "./resourcePresentation";

export function ExecutionJob({
  job,
  report,
  logs,
  loading,
  error,
  onCancel,
}: {
  job: Job;
  report: RegisteredExecutionReport | null;
  logs: Logs | null;
  loading: boolean;
  error: string;
  onCancel: () => void;
}) {
  const active = [
    "queued",
    "running",
    "archiving",
    "cancel_requested",
    "lost",
  ].includes(job.status);
  return (
    <section className="panel result-panel">
      <div className="result-header">
        <div>
          <span className="eyebrow">REGISTERED OFFLINE EXECUTION</span>
          <h2>{job.name || job.plan_id || job.id}</h2>
          <span className="muted mono">{job.id}</span>
        </div>
        <div className="result-actions">
          <span className={`status status-${job.status}`}>
            {job.status === "cancel_requested"
              ? "等待取消"
              : job.status === "lost"
                ? "执行进程失联"
                : researchJobStatus(job.status)}
          </span>
          {active && (
            <button
              className="button secondary small"
              disabled={job.status === "cancel_requested"}
              onClick={onCancel}
            >
              {job.status === "cancel_requested"
                ? "等待取消"
                : job.status === "queued"
                  ? "取消此排队任务"
                  : "请求取消执行"}
            </button>
          )}
        </div>
      </div>
      <div className="result-body">
        <div className="report-label">
          <span className="tag real-tag">已登记离线计划 · 非合成示例</span>
          <span className="muted">执行与归档完成不等于策略通过经济验证。</span>
        </div>
        {["lost", "cancel_requested"].includes(job.status) && (
          <div className="notice">
            取消请求不等于进程已停止；须由原 worker
            接收并回报终态。失联期间不改派或重跑此计划。
          </div>
        )}
        <dl>
          <div>
            <dt>计划 / 版本</dt>
            <dd>
              {job.plan_id ?? report?.plan_id ?? "未记录"} /{" "}
              {job.revision ?? report?.revision ?? "未记录"}
            </dd>
          </div>
          <div>
            <dt>创建 / UTC</dt>
            <dd>{stamp(job.created_at, true)}</dd>
          </div>
          <div>
            <dt>资源分配</dt>
            <dd>
              {job.resource_id ??
                (job.requested_resource_id === "auto"
                  ? "按计划顺序等待"
                  : job.requested_resource_id) ??
                "未分配"}
            </dd>
          </div>
        </dl>
        {job.queue_reason && (
          <p className="notice">
            等待原因：{job.queue_reason}。不会自动扩容
            Azure、改写计划或将任务转为合成回放。
          </p>
        )}
        {error && (
          <p className="notice error" role="alert">
            {error}
          </p>
        )}
        {job.error != null && (
          <p className="notice error">{display(job.error)}</p>
        )}
        {report ? (
          <>
            <h3>已归档执行摘要 · {PLAN_ROLE[report.role] ?? report.role}</h3>
            <p className="muted">
              仅显示登记计划指定的小型摘要，不制造 orders、trace、PnL、Sharpe
              或策略排名。完整原始输出保留在执行节点的持久工作目录。
            </p>
            {Object.keys(report.summary).length ? (
              <pre>{JSON.stringify(report.summary, null, 2)}</pre>
            ) : (
              <p className="notice">
                此计划没有登记可显示的小型 JSON 摘要；这不是零收益或空结果。
              </p>
            )}
            <h3>归档产物</h3>
            <ul className="execution-artifact-list">
              {report.artifacts.map((artifact) => (
                <li key={artifact.name}>
                  <span className="mono">{artifact.name}</span> ·{" "}
                  {display(artifact.size_bytes)} bytes
                </li>
              ))}
            </ul>
            <details>
              <summary>执行环境与耗时</summary>
              <pre>{JSON.stringify(report.environment, null, 2)}</pre>
            </details>
            <ul className="limitations">
              {report.limitations.map((limitation, index) => (
                <li key={index}>{limitation}</li>
              ))}
            </ul>
          </>
        ) : (
          <p className="notice">
            {loading
              ? "正在读取已发布的离线执行报告。"
              : job.status === "lost"
                ? "原执行进程失联，任务不会自动重跑；等待同一执行身份恢复。"
                : active
                  ? "任务尚未完成归档。当前仅展示持久队列状态，不读取中间经济结果。"
                  : "当前没有完成报告；失败或取消的诊断信息见下方终态日志。"}
          </p>
        )}
        <h3>终态归档日志</h3>
        <p className="muted">
          页面只显示服务端保存的脱敏、有界日志；不是实时输出。完整原文留在原
          worker 持久目录。
        </p>
        <div className="log-grid">
          <section>
            <h4>STDOUT</h4>
            <pre>{logs?.stdout || "尚无已发布 STDOUT。"}</pre>
          </section>
          <section>
            <h4>STDERR</h4>
            <pre>{logs?.stderr || "尚无已发布 STDERR。"}</pre>
          </section>
        </div>
      </div>
    </section>
  );
}
