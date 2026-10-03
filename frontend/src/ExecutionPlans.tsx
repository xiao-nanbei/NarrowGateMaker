import { useRef, useState } from "react";
import type { FormEvent } from "react";
import { requestId, submitExecution } from "./api";
import type { ExecutionPlanCatalog } from "./api";
import { researchJobStatus } from "./resourcePresentation";
import {
  PLAN_ROLE,
  canQueuePlan,
  executionTargetText,
} from "./executionPresentation";

export function ExecutionPlans({
  catalog,
  error,
  onCreated,
}: {
  catalog: ExecutionPlanCatalog | null;
  error: string;
  onCreated: (id?: string) => void;
}) {
  const [selected, setSelected] = useState("");
  const [target, setTarget] = useState("auto");
  const [busy, setBusy] = useState(false);
  const [submitError, setSubmitError] = useState("");
  const request = useRef<{ identity: string; key: string } | null>(null);
  const plan = catalog?.items.find((item) => item.id === selected);
  const allowed = canQueuePlan(plan, target);
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    if (!plan || !allowed || busy) return;
    const identity = JSON.stringify([plan.id, plan.revision, target]);
    if (request.current?.identity !== identity)
      request.current = { identity, key: requestId() };
    setBusy(true);
    setSubmitError("");
    try {
      const result = await submitExecution(
        plan.id,
        target,
        request.current.key,
      );
      onCreated(result.jobs[0]?.id);
    } catch (e) {
      setSubmitError(e instanceof Error ? e.message : "提交失败");
    } finally {
      setBusy(false);
    }
  };
  return (
    <section className="execution-plans">
      <div className="section-heading">
        <div>
          <span className="eyebrow">REGISTERED OFFLINE EXECUTION</span>
          <h3>从已登记计划排队执行</h3>
        </div>
        <span className="tag real-tag">不接管现有外部任务</span>
      </div>
      <p className="muted">
        只选择计划和资源；代码、输入、参数、输出与资源顺序由 owner
        预先登记。不会重跑已有 B0，除非另行登记并明确提交相应计划。
      </p>
      {error && (
        <div className="notice error" role="alert">
          计划目录不可用：{error}
        </div>
      )}
      {!catalog?.items.length && !error ? (
        <div className="notice">
          当前没有已登记的离线执行计划。资源在线不等于已有可提交的计划。
        </div>
      ) : (
        <form onSubmit={(event) => void submit(event)}>
          <div className="execution-plan-fields">
            <label>
              离线执行计划
              <select
                aria-label="离线执行计划"
                value={selected}
                disabled={busy}
                onChange={(event) => {
                  setSelected(event.target.value);
                  setTarget("auto");
                  setSubmitError("");
                }}
              >
                <option value="">选择一项已登记计划</option>
                {catalog?.items.map((item) => (
                  <option
                    key={item.id}
                    value={item.id}
                    disabled={!item.enabled && !item.attempt}
                  >
                    {item.label} · {PLAN_ROLE[item.role]}
                    {item.enabled ? "" : " · 未启用"}
                    {item.attempt ? " · 已有任务" : ""}
                  </option>
                ))}
              </select>
            </label>
            <label>
              目标计算资源
              <select
                aria-label="目标计算资源"
                value={target}
                disabled={!plan || busy || Boolean(plan.attempt)}
                onChange={(event) => {
                  setTarget(event.target.value);
                  setSubmitError("");
                }}
              >
                <option value="auto">遵循计划的资源优先顺序</option>
                {plan?.eligible_resources.map((resource) => (
                  <option
                    key={resource.id}
                    value={resource.id}
                    disabled={!resource.eligible}
                  >
                    {resource.label} ·{" "}
                    {!resource.eligible
                      ? "不允许"
                      : !resource.online
                        ? "执行 worker 未连接，仅排队"
                        : resource.ready
                          ? "条件已满足"
                          : "条件未满足，仅排队"}
                  </option>
                ))}
              </select>
            </label>
          </div>
          {plan && (
            <>
              <p className="mono execution-plan-identity">
                计划 {plan.id} · 版本 {plan.revision} · 必需输出{" "}
                {plan.requirements.required_output_count} 项
              </p>
              {plan.attempt ? (
                <div className="notice">
                  此计划／版本已有任务：{researchJobStatus(plan.attempt.status)}
                  。 不重复提交，也不自动重跑失败或失联的执行。
                  <p className="mono">{plan.attempt.job_id}</p>
                  <button
                    type="button"
                    className="button secondary small"
                    onClick={() => onCreated(plan.attempt?.job_id)}
                  >
                    查看已有任务 →
                  </button>
                </div>
              ) : (
                <div className="notice">
                  {executionTargetText(plan, target)} 完整计划不可拆分为跨主机
                  fresh-start 日期。排队不代表立即运行，Azure 缩到 0
                  时不会由此扩容。
                </div>
              )}
              <div className="execution-resource-reasons">
                {plan.eligible_resources.map((resource) => (
                  <p key={resource.id}>
                    <strong>{resource.label}</strong>：
                    {resource.reason ||
                      (resource.eligible
                        ? "在计划允许范围内"
                        : "不在计划允许范围内")}
                  </p>
                ))}
              </div>
              {plan.warnings.length > 0 && (
                <ul className="limitations">
                  {plan.warnings.map((warning, index) => (
                    <li key={index}>{warning}</li>
                  ))}
                </ul>
              )}
            </>
          )}
          {submitError && (
            <div className="notice error" role="alert">
              {submitError}。相同选择重试保留同一提交标识，避免重复创建。
            </div>
          )}
          <button className="button primary" disabled={!allowed || busy}>
            {busy
              ? "正在排队…"
              : plan?.attempt
                ? "此计划已有任务，不重复提交"
                : "排队执行已登记计划 →"}
          </button>
        </form>
      )}
      {catalog?.limitations.length ? (
        <ul className="limitations">
          {catalog.limitations.map((item, index) => (
            <li key={index}>{item}</li>
          ))}
        </ul>
      ) : null}
    </section>
  );
}
