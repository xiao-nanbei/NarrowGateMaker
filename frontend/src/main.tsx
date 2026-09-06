import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { createRoot } from "react-dom/client";
import { TraceCharts } from "./TraceCharts";
import {
  api,
  display,
  field,
  getReport,
  isCompleted,
  record,
  requestId,
  setSessionToken,
  stamp,
} from "./api";
import type { Job, Logs, Report, Row, Runner, Worker } from "./api";
import "./style.css";

const STATUS: Record<string, string> = {
  queued: "排队中",
  running: "运行中",
  cancel_requested: "等待取消",
  archiving: "归档中",
  completed: "已完成",
  failed: "失败",
  canceled: "已取消",
  lost: "失联",
};
const ACTIVE = new Set(["queued", "running", "cancel_requested", "archiving"]);
const METRICS = [
  ["terminal.terminal_pnl_usdc", "终值 PnL", "USDC"],
  ["terminal.cumulative_fees_usdc", "累计手续费", "USDC"],
  ["terminal.inventory_btc", "终值库存", "BTC"],
  ["denominators.fill_opportunities.fill_events", "成交事件", "次"],
] as const;
const titleOf = (job: Job) => job.name || job.experiment_id || job.id;
const errorText = (error: unknown) =>
  error instanceof Error ? error.message : display(error);

function Status({ value }: { value: string }) {
  const status = value.toLowerCase();
  return (
    <span className={`status status-${status}`}>
      <i />
      {STATUS[status] ?? value}
    </span>
  );
}
function Empty({
  title,
  children,
}: {
  title: string;
  children?: React.ReactNode;
}) {
  return (
    <div className="empty">
      <div className="empty-mark">▥</div>
      <h3>{title}</h3>
      <p>{children}</p>
    </div>
  );
}
function Notice({
  children,
  error = false,
}: {
  children: React.ReactNode;
  error?: boolean;
}) {
  return (
    <div
      className={error ? "notice error" : "notice"}
      role={error ? "alert" : undefined}
    >
      {children}
    </div>
  );
}
function Metrics({ summary }: { summary: Row }) {
  return (
    <div className="metrics">
      {METRICS.map(([key, label, unit]) => (
        <div className="metric" key={key}>
          <span>{label}</span>
          <strong>{display(field(summary, key))}</strong>
          <small>{unit} · 合成示例</small>
        </div>
      ))}
    </div>
  );
}

function CreateExperiment({
  runner,
  onCreated,
  onClose,
}: {
  runner?: Runner;
  onCreated: (id?: string) => void;
  onClose: () => void;
}) {
  const [name, setName] = useState("合成回放 · 双臂联调");
  const [paired, setPaired] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const request = useRef<{ body: string; key: string } | null>(null);
  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    if (busy || !name.trim() || !runner?.available) return;
    const body = JSON.stringify({
      name: name.trim(),
      runner: "replay-demo",
      dataset: "synthetic-demo",
      arms: paired ? ["B0", "C1"] : ["B0"],
    });
    if (!request.current || request.current.body !== body)
      request.current = { body, key: requestId() };
    setBusy(true);
    setError("");
    try {
      const result = await api<{ id: string; jobs: Job[] }>("/experiments", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Idempotency-Key": request.current.key,
        },
        body,
      });
      onCreated(result.jobs[0]?.id);
      onClose();
    } catch (e) {
      setError(errorText(e));
    } finally {
      setBusy(false);
    }
  };
  return (
    <div
      className="modal-shade"
      onClick={(e) => {
        if (e.target === e.currentTarget && !busy) onClose();
      }}
    >
      <section
        className="modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="create-title"
      >
        <header>
          <div>
            <span className="eyebrow">NEW EXPERIMENT</span>
            <h2 id="create-title">新建回放实验</h2>
          </div>
          <button
            className="icon-button"
            onClick={onClose}
            disabled={busy}
            aria-label="关闭"
          >
            ×
          </button>
        </header>
        <form onSubmit={submit}>
          <label>
            实验名称
            <input
              autoFocus
              value={name}
              onChange={(e) => setName(e.target.value)}
              maxLength={100}
              required
            />
          </label>
          <label>
            执行器
            <select value="replay-demo" disabled>
              <option value="replay-demo">replay-demo · 合成示例</option>
            </select>
          </label>
          <label>
            数据集
            <select value="synthetic-demo" disabled>
              <option value="synthetic-demo">
                synthetic-demo · 内置固定事件带
              </option>
            </select>
          </label>
          <label className="check">
            <input
              type="checkbox"
              checked={paired}
              onChange={(e) => setPaired(e.target.checked)}
            />
            创建 B0 + C1 两个演示臂
          </label>
          <Notice>
            这里只验证任务流转与报告查看。B0 / C1 是演示标签，执行相同合成
            fixture，不代表真实基线、候选策略或经济增益。
          </Notice>
          <div className="disabled-adapters">
            <span>尚未接入</span>
            <button type="button" disabled>
              真实 F01
            </button>
            <button type="button" disabled>
              current B0
            </button>
            <button type="button" disabled>
              E / C 标签研究
            </button>
          </div>
          {!runner?.available && (
            <Notice error>
              replay-demo 当前不可用；请先检查服务端执行器。
            </Notice>
          )}
          {error && (
            <Notice error>
              {error}。同一输入重试将复用提交标识，避免重复创建。
            </Notice>
          )}
          <footer>
            <button
              type="button"
              className="button secondary"
              onClick={onClose}
              disabled={busy}
            >
              取消
            </button>
            <button
              className="button primary"
              disabled={busy || !name.trim() || !runner?.available}
            >
              {busy ? "正在提交…" : "创建实验 →"}
            </button>
          </footer>
        </form>
      </section>
    </div>
  );
}

function ResultView({
  job,
  report,
  logs,
  loading,
  error,
  onCancel,
}: {
  job: Job;
  report: Report | null;
  logs: Logs | null;
  loading: boolean;
  error: string;
  onCancel: () => void;
}) {
  const [tab, setTab] = useState("overview");
  const [selected, setSelected] = useState<Row | null>(null);
  const [order, setOrder] = useState<string | null>(null);
  const [campaign, setCampaign] = useState<Row | null>(null);
  const [eventType, setEventType] = useState("all");
  useEffect(() => {
    setTab("overview");
    setSelected(null);
    setOrder(null);
    setCampaign(null);
    setEventType("all");
  }, [job.id]);
  const trace = report?.trace ?? [];
  const visible = useMemo(
    () =>
      trace.filter((row) => {
        if (eventType !== "all" && row.event !== eventType) return false;
        if (
          order &&
          row.order_id !== order &&
          !(
            Array.isArray(row.fills) &&
            row.fills.some((fill) => record(fill).order_id === order)
          ) &&
          !(
            Array.isArray(row.opportunities) &&
            row.opportunities.some((item) => record(item).order_id === order)
          )
        )
          return false;
        if (
          campaign &&
          (Number(row.ts_ms) < Number(campaign.start_ts_ms) ||
            Number(row.ts_ms) > Number(campaign.end_ts_ms))
        )
          return false;
        return true;
      }),
    [trace, order, campaign, eventType],
  );
  const summary = report?.summary ?? {};
  const orders = Array.isArray(summary.orders)
    ? summary.orders.map(record)
    : [];
  const campaigns = Array.isArray(field(summary, "campaign.closed"))
    ? (field(summary, "campaign.closed") as unknown[]).map(record)
    : [];
  const tabs = [
    ["overview", "结果概览"],
    ["events", "事件时间线"],
    ["orders", "订单"],
    ["campaigns", "Campaign"],
    ["logs", "日志 / 错误"],
  ];
  return (
    <section className="panel result-panel">
      <div className="result-header">
        <div>
          <span className="eyebrow">SELECTED RUN</span>
          <h2>
            {titleOf(job)} <span className="arm">{display(job.arm)}</span>
          </h2>
          <span className="muted mono">{job.id}</span>
        </div>
        <div className="result-actions">
          <Status value={job.status} />
          {ACTIVE.has(job.status.toLowerCase()) && (
            <button
              className="button secondary small"
              disabled={job.status.toLowerCase() === "cancel_requested"}
              onClick={onCancel}
            >
              {job.status.toLowerCase() === "cancel_requested"
                ? "等待取消"
                : "取消任务"}
            </button>
          )}
        </div>
      </div>
      <nav className="tabs" aria-label="结果栏目">
        {tabs.map(([key, label]) => (
          <button
            key={key}
            className={tab === key ? "active" : ""}
            onClick={() => setTab(key)}
          >
            {label}
          </button>
        ))}
      </nav>
      {error && <Notice error>{error}</Notice>}
      {job.error != null && <Notice error>{display(job.error)}</Notice>}
      {tab === "logs" ? (
        <div className="log-grid">
          <Log title="STDOUT" text={logs?.stdout} />
          <Log title="STDERR" text={logs?.stderr} />
        </div>
      ) : loading && !report ? (
        <Empty title="正在读取报告">
          从服务端加载已保存的输出，不在浏览器重算。
        </Empty>
      ) : !report ? (
        <Empty
          title={
            job.status === "canceled"
              ? "任务已取消"
              : job.status === "failed"
                ? "任务失败"
                : job.status === "lost"
                  ? "等待原执行节点恢复"
                  : isCompleted(job)
                    ? "报告尚不可用"
                    : "等待任务完成"
          }
        >
          {job.status === "canceled" || job.status === "failed"
            ? "此任务没有完成报告；已归档的诊断信息可在「日志 / 错误」查看。排队取消不会启动执行器。"
            : job.status === "lost"
              ? "节点失联不会自动重新执行，以免同一任务重复计算。"
              : "运行和归档结束后显示结果与日志。当前版本不提供运行中日志流。"}
        </Empty>
      ) : (
        <>
          {tab === "overview" && (
            <div className="result-body">
              <div className="report-label">
                <span className="tag">SYNTHETIC / NON-ECONOMIC</span>
                <span className="muted">
                  直接呈现 runner 原始报告 · 不做浏览器端损益重算
                </span>
              </div>
              <Metrics summary={summary} />
              <TraceCharts
                trace={trace}
                onSelect={(row) => {
                  setOrder(null);
                  setCampaign(null);
                  setEventType("all");
                  setSelected(row);
                  setTab("events");
                }}
              />
              <div className="summary-grid">
                <section>
                  <h3>终值与账本</h3>
                  <dl>
                    {[
                      ["现金 / USDC", "terminal.cash_usdc"],
                      [
                        "标记价格 / USDC·BTC⁻¹",
                        "terminal.mark_price_usdc_per_btc",
                      ],
                      ["连续 PnL / USDC", "accounting.continuous_pnl_usdc"],
                      ["已关闭 Campaign", "accounting.campaigns_closed"],
                      [
                        "Campaign 加和误差 / USDC",
                        "accounting.campaign_value_additivity_error_usdc",
                      ],
                    ].map(([label, key]) => (
                      <div key={key}>
                        <dt>{label}</dt>
                        <dd>{display(field(summary, key))}</dd>
                      </div>
                    ))}
                  </dl>
                </section>
                <section>
                  <h3>回放分母</h3>
                  <dl>
                    {[
                      ["事件带记录", "denominators.events.tape_records"],
                      ["报单", "denominators.orders.submitted"],
                      ["已撤订单", "denominators.orders.canceled"],
                      [
                        "终点活跃订单",
                        "denominators.orders.active_at_terminal",
                      ],
                      [
                        "成交量 / BTC",
                        "denominators.fill_opportunities.filled_quantity_btc",
                      ],
                    ].map(([label, key]) => (
                      <div key={key}>
                        <dt>{label}</dt>
                        <dd>{display(field(summary, key))}</dd>
                      </div>
                    ))}
                  </dl>
                </section>
              </div>
              <h3>证据边界</h3>
              <ul className="limitations">
                {(report.limitations?.length
                  ? report.limitations
                  : [
                      "合成回放仅用于工具与机制演示，不能作为真实行情收益、live 对齐或策略晋级证据。",
                    ]
                ).map((item, index) => (
                  <li key={index}>{display(item)}</li>
                ))}
              </ul>
              <details>
                <summary>原始报告 summary</summary>
                <pre>{JSON.stringify(summary, null, 2)}</pre>
              </details>
            </div>
          )}
          {tab === "events" && (
            <div className="result-body">
              <div className="trace-controls">
                <label>
                  事件
                  <select
                    value={eventType}
                    onChange={(e) => setEventType(e.target.value)}
                  >
                    <option value="all">全部事件</option>
                    {Array.from(
                      new Set(trace.map((row) => display(row.event))),
                    ).map((type) => (
                      <option key={type}>{type}</option>
                    ))}
                  </select>
                </label>
                <span className="muted">
                  {visible.length} / {trace.length} 条 · UTC
                </span>
                {order && (
                  <button
                    className="filter-chip"
                    onClick={() => setOrder(null)}
                  >
                    订单 {order} ×
                  </button>
                )}
                {campaign && (
                  <button
                    className="filter-chip"
                    onClick={() => setCampaign(null)}
                  >
                    Campaign {display(campaign.campaign_id)} ×
                  </button>
                )}
              </div>
              <div
                className="event-strip"
                aria-label="按原始 trace 顺序排列的事件"
              >
                {visible.slice(0, 200).map((row, index) => (
                  <button
                    key={`${row.seq}-${index}`}
                    className={`event-dot event-${display(row.event)} ${selected === row ? "selected" : ""}`}
                    title={`#${display(row.seq)} ${display(row.event)} ${stamp(row.ts_ms)}`}
                    aria-label={`查看事件 ${display(row.seq)} ${display(row.event)}`}
                    onClick={() => setSelected(row)}
                  />
                ))}
              </div>
              <div className="trace-layout">
                <div className="table-scroll">
                  <table>
                    <thead>
                      <tr>
                        <th>Seq</th>
                        <th>时间 / UTC</th>
                        <th>事件</th>
                        <th>方向</th>
                        <th>价格</th>
                        <th>订单</th>
                      </tr>
                    </thead>
                    <tbody>
                      {visible.map((row, index) => (
                        <tr
                          key={`${row.seq}-${index}`}
                          className={selected === row ? "selected-row" : ""}
                        >
                          <td>
                            <button
                              className="text-button mono"
                              onClick={() => setSelected(row)}
                            >
                              #{display(row.seq)}
                            </button>
                          </td>
                          <td className="mono">{stamp(row.ts_ms)}</td>
                          <td>{display(row.event)}</td>
                          <td className={row.side === "BUY" ? "buy" : "sell"}>
                            {display(row.side ?? row.passive_side)}
                          </td>
                          <td className="mono">
                            {display(row.price ?? row.mark_price)}
                          </td>
                          <td className="mono">{display(row.order_id)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {visible.length === 0 && (
                    <Empty title="没有匹配事件">
                      清除订单或 Campaign 筛选后再试。
                    </Empty>
                  )}
                </div>
                <aside className="event-detail">
                  <h3>事件详情</h3>
                  {selected ? (
                    <pre>{JSON.stringify(selected, null, 2)}</pre>
                  ) : (
                    <p className="muted">
                      点击事件编号或上方事件点，查看原始字段及成交明细。
                    </p>
                  )}
                </aside>
              </div>
            </div>
          )}
          {tab === "orders" && (
            <div className="result-body">
              <p className="muted">
                点击订单 ID，联动查看其提交、成交机会和终态事件。
              </p>
              <div className="table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>订单 ID</th>
                      <th>方向</th>
                      <th>状态</th>
                      <th>价格</th>
                      <th>数量 / BTC</th>
                      <th>已成交 / BTC</th>
                      <th>终点前排量 / BTC</th>
                    </tr>
                  </thead>
                  <tbody>
                    {orders.map((row, index) => (
                      <tr key={index}>
                        <td>
                          <button
                            className="text-button mono"
                            onClick={() => {
                              setOrder(String(row.order_id));
                              setCampaign(null);
                              setEventType("all");
                              setSelected(null);
                              setTab("events");
                            }}
                          >
                            {display(row.order_id)}
                          </button>
                        </td>
                        <td className={row.side === "BUY" ? "buy" : "sell"}>
                          {display(row.side)}
                        </td>
                        <td>{display(row.status)}</td>
                        <td>{display(row.price)}</td>
                        <td>{display(row.quantity_btc)}</td>
                        <td>{display(row.filled_quantity_btc)}</td>
                        <td>{display(row.queue_ahead_terminal_btc)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {!orders.length && <Empty title="报告中没有订单记录" />}
            </div>
          )}
          {tab === "campaigns" && (
            <div className="result-body">
              <p className="muted">
                Campaign 金额直接来自原始账本；点击 ID 查看对应起止区间的事件。
              </p>
              <div className="table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>Campaign</th>
                      <th>方向</th>
                      <th>开始 / UTC</th>
                      <th>结束 / UTC</th>
                      <th>最大库存 / BTC</th>
                      <th>终值 / USDC</th>
                    </tr>
                  </thead>
                  <tbody>
                    {campaigns.map((row, index) => (
                      <tr key={index}>
                        <td>
                          <button
                            className="text-button mono"
                            onClick={() => {
                              setCampaign(row);
                              setOrder(null);
                              setEventType("all");
                              setSelected(null);
                              setTab("events");
                            }}
                          >
                            {display(row.campaign_id)}
                          </button>
                        </td>
                        <td>{display(row.side)}</td>
                        <td className="mono">{stamp(row.start_ts_ms)}</td>
                        <td className="mono">{stamp(row.end_ts_ms)}</td>
                        <td>{display(row.peak_abs_inventory_btc)}</td>
                        <td>{display(row.terminal_value_usdc)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {!campaigns.length && <Empty title="没有已关闭的 Campaign" />}
              <p className="muted">
                终点存在未关闭 Campaign：
                {display(field(summary, "campaign.open_at_terminal"))}
              </p>
            </div>
          )}
        </>
      )}
    </section>
  );
}
function Log({ title, text }: { title: string; text?: string | null }) {
  return (
    <section className="log-panel">
      <header>
        {title}
        <span>{text ? "服务端原文" : "暂无已归档输出"}</span>
      </header>
      <pre>
        {text ||
          "服务端尚无已发布日志。当前版本仅提供终态归档日志；运行中日志保留在 worker。"}
      </pre>
    </section>
  );
}

function SessionAuth({
  onApply,
  onClose,
}: {
  onApply: (token: string) => void;
  onClose: () => void;
}) {
  const [token, setToken] = useState("");
  return (
    <div className="modal-shade">
      <section
        className="modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="auth-title"
      >
        <header>
          <div>
            <span className="eyebrow">SESSION ACCESS</span>
            <h2 id="auth-title">服务访问凭据</h2>
          </div>
          <button className="icon-button" onClick={onClose} aria-label="关闭">
            ×
          </button>
        </header>
        <form
          onSubmit={(event) => {
            event.preventDefault();
            onApply(token);
          }}
        >
          <label>
            可选 Bearer token
            <input
              type="password"
              autoFocus
              autoComplete="off"
              value={token}
              onChange={(event) => setToken(event.target.value)}
              placeholder="默认 loopback demo 无需填写"
            />
          </label>
          <Notice>
            仅保存在当前页面内存，不写入 URL、localStorage
            或文件。刷新页面后清除。启用 token 时使用认证轮询，不将凭据传给 SSE
            URL。
          </Notice>
          <footer>
            <button
              type="button"
              className="button secondary"
              onClick={() => onApply("")}
            >
              清除凭据
            </button>
            <button className="button primary">应用到当前页面</button>
          </footer>
        </form>
      </section>
    </div>
  );
}

function Compare({ jobs }: { jobs: Job[] }) {
  const completed = jobs.filter(isCompleted);
  const [left, setLeft] = useState("");
  const [right, setRight] = useState("");
  const [reports, setReports] = useState<[Report, Report] | null>(null);
  const [error, setError] = useState("");
  useEffect(() => {
    setReports(null);
    setError("");
    if (!left || !right || left === right) return;
    const controller = new AbortController();
    Promise.all([
      getReport(left, controller.signal),
      getReport(right, controller.signal),
    ])
      .then((values) => setReports(values))
      .catch((e) => {
        if (!controller.signal.aborted) setError(errorText(e));
      });
    return () => controller.abort();
  }, [left, right]);
  const choices = (
    value: string,
    setter: (value: string) => void,
    other: string,
    label: string,
  ) => (
    <label>
      {label}
      <select value={value} onChange={(e) => setter(e.target.value)}>
        <option value="">选择已完成任务</option>
        {completed
          .filter((job) => job.id !== other)
          .map((job) => (
            <option key={job.id} value={job.id}>
              {titleOf(job)} · {display(job.arm)} · {job.id.slice(0, 8)}
            </option>
          ))}
      </select>
    </label>
  );
  return (
    <section className="panel compare">
      <div className="section-heading">
        <div>
          <span className="eyebrow">SIDE BY SIDE</span>
          <h2>结果比较</h2>
        </div>
        <span className="tag">仅 COMPLETE 合成报告</span>
      </div>
      <div className="compare-selects">
        {choices(left, setLeft, right, "参考任务")}
        {choices(right, setRight, left, "对照任务")}
      </div>
      <Notice>
        并排读取两个已完成的 demo 输出，不计算新的
        PnL，也不将标签差异解释为策略收益。
      </Notice>
      {error && <Notice error>{error}</Notice>}
      {reports ? (
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>原始报告字段</th>
                <th>参考任务</th>
                <th>对照任务</th>
              </tr>
            </thead>
            <tbody>
              {[
                ...METRICS,
                ["denominators.orders.submitted", "报单数量", "次"],
                ["accounting.campaigns_closed", "已关闭 Campaign", "个"],
              ].map(([key, label, unit]) => (
                <tr key={key}>
                  <td>
                    {label} <span className="muted">/ {unit}</span>
                  </td>
                  <td className="mono">
                    {display(field(reports[0].summary, key))}
                  </td>
                  <td className="mono">
                    {display(field(reports[1].summary, key))}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <Empty
          title={
            completed.length < 2
              ? "还需要两个已完成任务"
              : left && right
                ? "正在读取比较报告"
                : "选择两个已完成任务"
          }
        >
          只有归档完成的结果才进入比较；缺失指标显示为「—」。
        </Empty>
      )}
    </section>
  );
}

function App() {
  const [jobs, setJobs] = useState<Job[]>([]);
  const [workers, setWorkers] = useState<Worker[]>([]);
  const [runners, setRunners] = useState<Runner[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [report, setReport] = useState<Report | null>(null);
  const [logs, setLogs] = useState<Logs | null>(null);
  const [reportError, setReportError] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const [loadingReport, setLoadingReport] = useState(false);
  const [create, setCreate] = useState(false);
  const [section, setSection] = useState("runs");
  const [filter, setFilter] = useState("all");
  const [search, setSearch] = useState("");
  const [connected, setConnected] = useState(false);
  const [authOpen, setAuthOpen] = useState(false);
  const [authVersion, setAuthVersion] = useState(0);
  const [authenticated, setAuthenticated] = useState(false);
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null);
  const refreshBusy = useRef(false);
  const selected = jobs.find((job) => job.id === selectedId) ?? null;
  const refresh = useCallback(async () => {
    if (refreshBusy.current) return;
    refreshBusy.current = true;
    try {
      const [j, n, r] = await Promise.all([
        api<{ items: Job[] }>("/jobs"),
        api<{ items: Worker[] }>("/nodes"),
        api<{ items: Runner[] }>("/runners"),
      ]);
      setJobs(j.items);
      setWorkers(n.items);
      setRunners(r.items);
      setError("");
      setLastRefresh(new Date());
    } catch (e) {
      setError(errorText(e));
    } finally {
      setLoading(false);
      refreshBusy.current = false;
    }
  }, []);
  useEffect(() => {
    void refresh();
    const interval = window.setInterval(() => void refresh(), 4000);
    if (authenticated) {
      setConnected(false);
      return () => window.clearInterval(interval);
    }
    const events = new EventSource("/api/events");
    events.onopen = () => setConnected(true);
    events.onerror = () => setConnected(false);
    events.onmessage = () => void refresh();
    events.addEventListener("job_changed", () => void refresh());
    return () => {
      window.clearInterval(interval);
      events.close();
    };
  }, [refresh, authVersion, authenticated]);
  useEffect(() => {
    if (!selectedId && jobs.length) setSelectedId(jobs[0].id);
  }, [jobs, selectedId]);
  useEffect(() => {
    setReport(null);
    setLogs(null);
    setReportError("");
  }, [selectedId]);
  useEffect(() => {
    if (!selected) return;
    const controller = new AbortController();
    const id = selected.id;
    setLoadingReport(isCompleted(selected));
    if (isCompleted(selected))
      getReport(id, controller.signal)
        .then((value) => {
          setReport(value);
          setReportError("");
        })
        .catch((e) => {
          if (!controller.signal.aborted) setReportError(errorText(e));
        })
        .finally(() => {
          if (!controller.signal.aborted) setLoadingReport(false);
        });
    api<Logs>(`/jobs/${encodeURIComponent(id)}/logs`, {
      signal: controller.signal,
    })
      .then(setLogs)
      .catch((e) => {
        if (!controller.signal.aborted) setReportError(errorText(e));
      });
    return () => controller.abort();
  }, [selected?.id, selected?.status, selected?.updated_at, authVersion]);
  const visible = jobs.filter(
    (job) =>
      (filter === "all" || job.status.toLowerCase() === filter) &&
      `${titleOf(job)} ${job.id} ${job.arm ?? ""}`
        .toLowerCase()
        .includes(search.toLowerCase()),
  );
  const cancel = async () => {
    if (!selected) return;
    try {
      await api(`/jobs/${encodeURIComponent(selected.id)}/cancel`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "{}",
      });
      await refresh();
    } catch (e) {
      setError(errorText(e));
    }
  };
  return (
    <div className="app-shell">
      <aside className="sidebar">
        <a className="brand" href="./">
          <span className="brand-icon">
            N<span>G</span>
          </span>
          <span>
            NarrowGate<small>REPLAY STUDIO</small>
          </span>
        </a>
        <div className="workspace-label">RESEARCH WORKSPACE</div>
        <nav>
          {[
            ["runs", "▤", "回放任务"],
            ["compare", "⇄", "结果比较"],
            ["nodes", "▦", "计算节点"],
          ].map(([key, icon, label]) => (
            <button
              key={key}
              className={section === key ? "active" : ""}
              onClick={() => setSection(key)}
            >
              <span>{icon}</span>
              {label}
              <small>
                {key === "runs"
                  ? jobs.length
                  : key === "nodes"
                    ? workers.length
                    : ""}
              </small>
            </button>
          ))}
        </nav>
        <div className="sidebar-note">
          <span className="sidebar-note-dot" />
          <strong>离线研究，边界清晰</strong>
          <p>
            任务与结果集中查看。当前仅开放合成 replay-demo；不连接交易下单接口。
          </p>
        </div>
        <footer>
          <span>NG / STUDIO 01</span>
          <span className="mono">LOCAL CONTROL PLANE</span>
        </footer>
      </aside>
      <main>
        <header className="topbar">
          <div className="breadcrumbs">
            工作空间 <span>/</span>{" "}
            {section === "runs"
              ? "回放任务"
              : section === "compare"
                ? "结果比较"
                : "计算节点"}
          </div>
          <div className="connection">
            <i className={connected ? "live" : ""} />
            {connected ? "实时推送" : "定时刷新"}
            <span className="mono">
              {lastRefresh
                ? `${stamp(lastRefresh.toISOString())} UTC`
                : "正在连接"}
            </span>
            <button
              className="icon-button"
              aria-label="刷新"
              title="刷新"
              onClick={() => void refresh()}
            >
              ↻
            </button>
          </div>
        </header>
        <div className="content">
          <div className="page-heading">
            <div>
              <span className="eyebrow">
                REMOTE REPLAY / RESEARCH WORKBENCH
              </span>
              <h1>
                {section === "runs"
                  ? "让每一次回放，都可追溯。"
                  : section === "compare"
                    ? "先看同一口径，再做比较。"
                    : "算力在哪里，任务就在哪里。"}
              </h1>
              <p>
                管理执行、检查事件路径，回到原始报告。没有预置收益，也没有隐含实盘授权。
              </p>
            </div>
            <button className="button primary" onClick={() => setCreate(true)}>
              ＋ 新建实验
            </button>
          </div>
          <div className="boundary-strip">
            <span className="tag">演示能力已接入</span>
            <span>replay-demo / synthetic-demo</span>
            <span className="muted">
              真实 F01、current B0 与 E/C：适配器未接入，提交关闭
            </span>
          </div>
          <div className="auth-bar">
            <button className="text-button" onClick={() => setAuthOpen(true)}>
              {authenticated ? "已应用会话凭据 · 修改" : "访问凭据"}
            </button>
            <span>可选 · 仅当前页面</span>
          </div>
          {error && (
            <Notice error>
              无法更新工作台：{error}。显示的可能是上次成功读取的状态。
            </Notice>
          )}
          {authOpen && (
            <SessionAuth
              onClose={() => setAuthOpen(false)}
              onApply={(token) => {
                setSessionToken(token);
                setAuthenticated(Boolean(token.trim()));
                setAuthVersion((value) => value + 1);
                setAuthOpen(false);
              }}
            />
          )}
          {section === "runs" && (
            <>
              <div className="run-counters">
                <span>
                  <strong>
                    {
                      jobs.filter((job) => ACTIVE.has(job.status.toLowerCase()))
                        .length
                    }
                  </strong>
                  进行中
                </span>
                <span>
                  <strong>{jobs.filter(isCompleted).length}</strong>已完成
                </span>
                <span>
                  <strong>
                    {
                      jobs.filter((job) =>
                        ["failed", "lost"].includes(job.status.toLowerCase()),
                      ).length
                    }
                  </strong>
                  需处理
                </span>
                <span>
                  <strong>{workers.length}</strong>已登记节点
                </span>
                <span className="counter-caption">
                  状态来自服务端 · UTC 时间
                </span>
              </div>
              <section className="panel jobs-panel">
                <div className="section-heading">
                  <h2>
                    任务队列 <span className="count">{jobs.length}</span>
                  </h2>
                  <div className="table-tools">
                    <input
                      aria-label="搜索任务"
                      placeholder="搜索名称、任务 ID…"
                      value={search}
                      onChange={(e) => setSearch(e.target.value)}
                    />
                    <select
                      aria-label="筛选状态"
                      value={filter}
                      onChange={(e) => setFilter(e.target.value)}
                    >
                      <option value="all">全部状态</option>
                      {Object.entries(STATUS).map(([value, label]) => (
                        <option value={value} key={value}>
                          {label}
                        </option>
                      ))}
                    </select>
                  </div>
                </div>
                <div className="table-scroll">
                  <table>
                    <thead>
                      <tr>
                        <th>实验 / 任务</th>
                        <th>Arm</th>
                        <th>状态</th>
                        <th>执行节点</th>
                        <th>创建时间 / UTC</th>
                        <th />
                      </tr>
                    </thead>
                    <tbody>
                      {visible.map((job) => (
                        <tr
                          key={job.id}
                          className={
                            selectedId === job.id ? "selected-row" : ""
                          }
                        >
                          <td>
                            <button
                              className="job-link"
                              onClick={() => setSelectedId(job.id)}
                            >
                              {titleOf(job)}
                              <small>{job.id}</small>
                            </button>
                          </td>
                          <td>
                            <span className="arm">{display(job.arm)}</span>
                          </td>
                          <td>
                            <Status value={job.status} />
                          </td>
                          <td className="mono">{display(job.worker_id)}</td>
                          <td className="mono">
                            {stamp(job.created_at, true)}
                          </td>
                          <td>
                            <button
                              className="icon-button"
                              aria-label={`查看任务 ${job.id}`}
                              onClick={() => setSelectedId(job.id)}
                            >
                              ↗
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                {!visible.length && (
                  <Empty
                    title={
                      loading
                        ? "正在加载任务"
                        : jobs.length
                          ? "没有匹配任务"
                          : "从第一项回放开始"
                    }
                  >
                    {jobs.length
                      ? "调整筛选条件以查看任务。"
                      : "新建一个合成实验，验证执行、归档与结果检查的完整流程。"}
                  </Empty>
                )}
              </section>
              {selected && (
                <ResultView
                  job={selected}
                  report={report}
                  logs={logs}
                  loading={loadingReport}
                  error={reportError}
                  onCancel={() => void cancel()}
                />
              )}
            </>
          )}
          {section === "compare" && <Compare jobs={jobs} />}
          {section === "nodes" && (
            <section className="panel">
              <div className="section-heading">
                <div>
                  <span className="eyebrow">WORKER REGISTRY</span>
                  <h2>计算节点</h2>
                </div>
                <span className="muted">可用性采用服务端心跳判断</span>
              </div>
              <div className="nodes-grid">
                {workers.map((worker) => (
                  <article className="node-card" key={worker.id}>
                    <header>
                      <span className="node-icon">▦</span>
                      <h3>{worker.id}</h3>
                      <span
                        className={`status ${worker.online === true ? "status-completed" : worker.online === false ? "status-lost" : ""}`}
                      >
                        <i />
                        {worker.online === true
                          ? "在线"
                          : worker.online === false
                            ? "心跳超时"
                            : "状态未知"}
                      </span>
                    </header>
                    <dl>
                      <div>
                        <dt>最近心跳 / UTC</dt>
                        <dd>{stamp(worker.last_seen, true)}</dd>
                      </div>
                      <div>
                        <dt>执行能力</dt>
                        <dd>
                          {worker.capabilities?.length
                            ? worker.capabilities.map((item) => (
                                <span className="small-tag" key={display(item)}>
                                  {display(item)}
                                </span>
                              ))
                            : "—"}
                        </dd>
                      </div>
                      <div>
                        <dt>已登记数据集</dt>
                        <dd>
                          {worker.datasets?.length
                            ? worker.datasets.map((item) => (
                                <span className="small-tag" key={display(item)}>
                                  {display(item)}
                                </span>
                              ))
                            : "—"}
                        </dd>
                      </div>
                    </dl>
                  </article>
                ))}
              </div>
              {!workers.length && (
                <Empty title="尚无已登记节点">
                  按服务端说明启动 worker
                  后，心跳与能力将显示在这里。界面不会自行创建云资源。
                </Empty>
              )}
            </section>
          )}
          <footer className="page-footer">
            <span>NarrowGate · Observe the path, not just the number.</span>
            <span>原始报告驱动 · 缺失值不补零</span>
          </footer>
        </div>
      </main>
      {create && (
        <CreateExperiment
          runner={runners.find((r) => r.id === "replay-demo")}
          onClose={() => setCreate(false)}
          onCreated={(id) => {
            if (id) setSelectedId(id);
            setSection("runs");
            void refresh();
          }}
        />
      )}
    </div>
  );
}

createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
