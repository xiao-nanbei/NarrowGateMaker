import { useEffect, useState } from "react";
import { api, display, isCompleted } from "./api";
import type { Job, ResearchResult } from "./api";
import { isRegisteredJob } from "./executionPresentation";

export function ResearchResultView({ result }: { result: ResearchResult }) {
  return (
    <section>
      <h3>研究结果 · {result.arm ?? "未记录组别"}</h3>
      <p>
        {result.economic_complete
          ? "完整经济记账已报告（不代表策略通过）"
          : "经济信息未完整；不能宣称完整净收益"}
      </p>
      <dl>
        <div>
          <dt>实验 / 输入</dt>
          <dd>
            {result.experiment_id ?? "未知"} /{" "}
            {result.input_manifest_id ?? "未知"}
          </dd>
        </div>
        <div>
          <dt>模型 / 评估口径</dt>
          <dd>
            {result.model_id ?? "未知"} /{" "}
            {result.evaluation_contract_id ?? "未知"}
          </dd>
        </div>
      </dl>
      <table>
        <tbody>
          {Object.entries(result.metrics).map(([key, value]) => (
            <tr key={key}>
              <th>{key}</th>
              <td>{value === null ? "未知" : display(value)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="muted">
        币种：{result.currency ?? "未知"}
        。当前摘要不提供完整订单事件或精确绑定的复盘行情；不从成交反推缺失事件。
      </p>
    </section>
  );
}

type Comparison = {
  compatible: boolean;
  differences: string[];
  economic_comparison_complete: boolean;
  metrics: {
    metric: string;
    left: number | null;
    right: number | null;
    delta: number | null;
  }[];
};

export function ResearchCompare({ jobs }: { jobs: Job[] }) {
  const [left, setLeft] = useState("");
  const [right, setRight] = useState("");
  const [report, setReport] = useState<Comparison | null>(null);
  const [error, setError] = useState("");
  const completed = jobs.filter(
    (job) => isRegisteredJob(job) && isCompleted(job),
  );
  useEffect(() => {
    setReport(null);
    setError("");
    if (!left || !right || left === right) return;
    const controller = new AbortController();
    api<Comparison>(
      `/research-comparison?left=${encodeURIComponent(left)}&right=${encodeURIComponent(right)}`,
      { signal: controller.signal },
    )
      .then((value) => {
        if (!controller.signal.aborted) setReport(value);
      })
      .catch((e) => {
        if (!controller.signal.aborted) setError(String(e));
      });
    return () => controller.abort();
  }, [left, right]);
  return (
    <section className="panel compare">
      <h2>真实研究结果比较</h2>
      <p>
        选择两项已完成的登记任务。仅比较同实验、同输入、同分片、同延迟和记账口径的已知指标；未知不是零。
      </p>
      <div className="compare-selects">
        <label>
          参考任务
          <select value={left} onChange={(e) => setLeft(e.target.value)}>
            <option value="">请选择</option>
            {completed
              .filter((j) => j.id !== right)
              .map((j) => (
                <option key={j.id} value={j.id}>
                  {j.name || j.plan_id} · {j.id.slice(0, 8)}
                </option>
              ))}
          </select>
        </label>
        <label>
          候选任务
          <select value={right} onChange={(e) => setRight(e.target.value)}>
            <option value="">请选择</option>
            {completed
              .filter((j) => j.id !== left)
              .map((j) => (
                <option key={j.id} value={j.id}>
                  {j.name || j.plan_id} · {j.id.slice(0, 8)}
                </option>
              ))}
          </select>
        </label>
      </div>
      {completed.length < 2 && (
        <p className="notice">
          尚不足两项已完成登记任务，不会为填充页面重跑历史实验。
        </p>
      )}
      {error && (
        <p role="alert" className="notice error">
          {error}
        </p>
      )}
      {report && (
        <>
          {!report.compatible && (
            <p className="notice">
              口径不一致或未记录，禁止计算差值：{report.differences.join(", ")}
            </p>
          )}
          {!report.economic_comparison_complete && (
            <p className="notice">完整净收益比较尚不成立；仍可查看已有指标。</p>
          )}
          <table>
            <thead>
              <tr>
                <th>指标</th>
                <th>参考</th>
                <th>候选</th>
                <th>候选 − 参考</th>
              </tr>
            </thead>
            <tbody>
              {report.metrics.map((row) => (
                <tr key={row.metric}>
                  <th>{row.metric}</th>
                  {[row.left, row.right, row.delta].map((value, i) => (
                    <td key={i}>
                      {value === null ? "未知 / 不可比" : display(value)}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </section>
  );
}
