import { useEffect, useMemo, useRef, useState } from "react";
import {
  api,
  getQualityCatalog,
  getQualityReport,
  refreshQualityInventory,
  stamp,
} from "./api";
import type {
  QualityCatalog,
  QualityExport,
  QualityReport,
  QualityRefresh,
  QualitySource,
  QualityTask,
} from "./api";
import {
  filterQualityDays,
  qualityAudit,
  qualityReason,
  qualityReplica,
  qualityTask,
  utcDays,
} from "./traceSeries";

const LABELS: Record<string, string> = {
  present: "登记源已有文件",
  missing: "登记源明确缺失",
  unknown: "源文件尚未观察",
  passed: "通过",
  failed: "失败",
  unchecked: "未执行检查",
  partial: "部分检查",
  not_applicable: "不适用",
  verified: "副本已核验",
  present_unverified: "副本待核验",
  stale: "副本记录已过期",
};
const TASKS = {
  candles: "K 线",
  feature_input: "特征输入",
  modeled_replay: "模型回放",
  strict_replay: "严格队列回放",
  funding_pnl: "资金费核算",
} as const;
const message = (error: unknown) =>
  error instanceof Error ? error.message : "读取失败";
const dayString = (ms: number) => new Date(ms).toISOString().slice(0, 10);
function State({ value, label }: { value: string; label?: string }) {
  return (
    <span className={`dq-state dq-state-${value}`}>
      {label ?? LABELS[value] ?? value}
    </span>
  );
}

function SourceDetails({
  source,
  node,
}: {
  source: QualitySource;
  node: string;
}) {
  const known = (value: number | null, suffix = "") =>
    value === null
      ? "原报告未记录"
      : `${value.toLocaleString("en-US")}${suffix}`;
  const audit = qualityAudit(source);
  const replica = qualityReplica(source);
  const stage = {
    raw: "原始数据",
    processed: "处理后产物",
    registered: "登记产物（未声明处理阶段）",
  }[source.stage ?? "registered"];
  const raw = source.stage === "raw";
  const counts = source.replica.file_counts;
  return (
    <details className="quality-source">
      <summary>
        <strong>{source.label}</strong>
        <span className="mono">
          {source.exchange} / {source.market} / {source.symbol} /{" "}
          {source.data_type}
        </span>
        <State
          value={source.availability}
          label={
            raw
              ? undefined
              : {
                  present: "产物已生成",
                  missing: "产物未找到 / 待生成",
                  unknown: "产物尚未观察",
                }[source.availability]
          }
        />
        {!raw && <State value={audit.state} label={audit.label} />}
        {raw && counts && (
          <span className="tag">
            文件 {counts.present} / {counts.expected ?? "未固定数量"}
          </span>
        )}
        <span className="muted">
          <State value={replica.state} label={replica.label} />
        </span>
      </summary>
      <div className="quality-source-body">
        <p className="muted">
          {source.source} · {source.version} · {source.dataset_id}
        </p>
        <div className="quality-layers">
          <section>
            <h4>1 · 当前数据 / 产物</h4>
            <p>{stage}</p>
            <State value={source.availability} />
            <p className="muted">
              文件观察：
              {source.observed_at
                ? `${stamp(source.observed_at, true)} UTC`
                : "尚无当前观察时间"}
            </p>
            <State value={audit.state} label={audit.label} />
            <p>{audit.reason}</p>
          </section>
          <section>
            <h4>2 · 所选节点副本</h4>
            <p className="mono">{node}</p>
            <State value={replica.state} label={replica.label} />
            <p>{replica.reason}</p>
            <p className="muted">
              副本核验：
              {source.replica.last_checked_at
                ? `${stamp(source.replica.last_checked_at, true)} UTC`
                : "尚无该节点核验时间"}
            </p>
            <p className="muted">
              节点观察：
              {{
                online: "在线记录",
                offline: "不可达记录",
                unknown: "尚未观察",
              }[source.replica.node_status] ?? "尚无节点观察"}
              ；节点在线不代表副本已核验。
            </p>
          </section>
        </div>
        <h4 className="quality-subtitle">
          3 ·{" "}
          {raw ? "原始源检查（不代表处理产物已生成）" : "当前各用途的适用范围"}
        </h4>
        <div className="quality-usability">
          {Object.entries(TASKS).map(([key, label]) => {
            const current = qualityTask(source, key as keyof typeof TASKS);
            return (
              <div key={key}>
                <strong>{label}</strong>
                <State value={current.state} label={current.label} />
                <p>{current.reason}</p>
              </div>
            );
          })}
        </div>
        <p className="muted">
          严格队列条件只约束对应执行场景。缺少 native sequence 不自动否定 K
          线、特征或训练用途；训练仍需各自的特征、标签与因果时序检查。
        </p>
        <h4 className="quality-subtitle">4 · 原检查报告（保留历史范围）</h4>
        <p>
          <State value={source.check_status} /> ·{" "}
          {source.check_scope || "原报告未记录检查范围"}
        </p>
        <dl className="quality-facts">
          <div>
            <dt>原报告记录数</dt>
            <dd>{known(source.records)}</dd>
          </div>
          <div>
            <dt>登记文件总字节数</dt>
            <dd>{known(source.size_bytes, " B")}</dd>
          </div>
          <div>
            <dt>已报告覆盖率</dt>
            <dd>
              {source.coverage_ratio === null
                ? "原报告未记录"
                : `${(source.coverage_ratio * 100).toFixed(3)}%`}
            </dd>
          </div>
          <div>
            <dt>已报告最大间隔</dt>
            <dd>{known(source.max_gap_ms, " ms")}</dd>
          </div>
          <div>
            <dt>原质量检查时间 / UTC</dt>
            <dd>
              {source.checked_at
                ? stamp(source.checked_at, true)
                : "原报告未记录"}
            </dd>
          </div>
        </dl>
        <p className="quality-evidence">
          原报告：{source.evidence_label || "尚未登记来源"}
        </p>
        <ul className="limitations">
          {(source.reasons.length
            ? source.reasons
            : ["来源未记录详细原因；不据此推定无问题。"]
          ).map((reason, i) => (
            <li key={i}>{qualityReason(reason)}</li>
          ))}
        </ul>
        {source.intervals.length ? (
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>开始 / UTC</th>
                  <th>结束 / UTC</th>
                  <th>状态</th>
                  <th>类型</th>
                  <th>原因</th>
                </tr>
              </thead>
              <tbody>
                {source.intervals.map((interval, i) => (
                  <tr key={i}>
                    <td className="mono">{stamp(interval.start_ms, true)}</td>
                    <td className="mono">{stamp(interval.end_ms, true)}</td>
                    <td>{interval.status}</td>
                    <td>{interval.kind}</td>
                    <td>{interval.reason}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="muted">未提供区间级审计明细；不是“没有缺口”的证明。</p>
        )}
      </div>
    </details>
  );
}

export function DataQuality({
  authVersion,
  onReviewDay,
}: {
  authVersion: number;
  onReviewDay: (day: string) => void;
}) {
  const [catalog, setCatalog] = useState<QualityCatalog | null>(null);
  const [report, setReport] = useState<QualityReport | null>(null);
  const [start, setStart] = useState(dayString(Date.now() - 29 * 86400000));
  const [end, setEnd] = useState(dayString(Date.now()));
  const [node, setNode] = useState("local");
  const [source, setSource] = useState("");
  const [market, setMarket] = useState("");
  const [symbol, setSymbol] = useState("");
  const [dataset, setDataset] = useState("");
  const [stage, setStage] = useState<"raw" | "processed" | "registered">(
    "processed",
  );
  const [page, setPage] = useState(0);
  const [task, setTask] = useState<QualityTask | "">("");
  const [problem, setProblem] = useState(false);
  const [missingReplica, setMissingReplica] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [exporting, setExporting] = useState(false);
  const [refresh, setRefresh] = useState(0);
  const [refreshingInventory, setRefreshingInventory] = useState(false);
  const [inventoryError, setInventoryError] = useState("");
  const [inventoryRefresh, setInventoryRefresh] = useState<{
    range: string;
    result: QualityRefresh["refresh"];
  } | null>(null);
  const refreshController = useRef<AbortController | null>(null);
  const days = useMemo(() => utcDays(start, end), [start, end]);
  const valid = days.length > 0 && days.length <= 730;
  const query = useMemo(
    () =>
      new URLSearchParams({
        start_day: start,
        end_day: end,
        node,
        ...(dataset ? { dataset_id: dataset } : {}),
      }),
    [start, end, node, dataset],
  );
  useEffect(() => {
    setInventoryRefresh(null);
    setInventoryError("");
    return () => refreshController.current?.abort();
  }, [authVersion]);
  useEffect(() => {
    const controller = new AbortController();
    getQualityCatalog(controller.signal)
      .then((value) => {
        if (!controller.signal.aborted) {
          setCatalog(value);
          setNode((current) =>
            value.nodes.some((n) => n.id === current)
              ? current
              : (value.nodes[0]?.id ?? "local"),
          );
        }
      })
      .catch((e) => {
        if (!controller.signal.aborted) setError(message(e));
      });
    return () => controller.abort();
  }, [authVersion, refresh]);
  useEffect(() => {
    setReport(null);
    if (!valid) {
      setLoading(false);
      return;
    }
    const controller = new AbortController();
    setLoading(true);
    setError("");
    getQualityReport(query, controller.signal)
      .then((value) => {
        if (!controller.signal.aborted) setReport(value);
      })
      .catch((e) => {
        if (!controller.signal.aborted) setError(message(e));
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
  }, [query, valid, authVersion, refresh]);
  const matches = (entry: {
    source: string;
    market: string;
    symbol: string;
    stage?: string;
  }) =>
    (entry.stage ?? "registered") === stage &&
    (!source || entry.source === source) &&
    (!market || entry.market === market) &&
    (!symbol || entry.symbol === symbol);
  const items = filterQualityDays(report?.items ?? [], {
    source,
    market,
    symbol,
    datasetId: dataset,
    stage,
    task,
    problemOnly: problem,
    missingReplicaOnly: missingReplica,
  });
  useEffect(
    () => setPage(0),
    [
      start,
      end,
      source,
      market,
      symbol,
      dataset,
      stage,
      task,
      problem,
      missingReplica,
      node,
    ],
  );
  const visibleDays = items.slice(page * 20, (page + 1) * 20);
  const lastPage = Math.max(0, Math.ceil(items.length / 20) - 1);
  const coverage = new Map<
    string,
    { label: string; present: number; missing: number; unknown: number }
  >();
  for (const day of items)
    for (const entry of day.sources) {
      const row = coverage.get(entry.dataset_id) ?? {
        label: entry.label,
        present: 0,
        missing: 0,
        unknown: 0,
      };
      row[entry.availability] += 1;
      coverage.set(entry.dataset_id, row);
    }
  const refreshInventory = async () => {
    if (!valid || refreshingInventory || node !== "local") return;
    const controller = new AbortController();
    refreshController.current = controller;
    const requestedQuery = new URLSearchParams(query);
    setRefreshingInventory(true);
    setInventoryError("");
    try {
      const result = await refreshQualityInventory(
        requestedQuery,
        controller.signal,
      );
      if (!controller.signal.aborted) {
        setInventoryRefresh({
          range: `${result.start_day} — ${result.end_day}`,
          result: result.refresh,
        });
        // Reload the current filters; the user may have changed them during the request.
        setRefresh((n) => n + 1);
      }
    } catch (e) {
      if (!controller.signal.aborted) setInventoryError(message(e));
    } finally {
      if (refreshController.current === controller) {
        refreshController.current = null;
        setRefreshingInventory(false);
      }
    }
  };
  const exportMissing = async () => {
    setExporting(true);
    setError("");
    try {
      const result = await api<QualityExport>(`/data-quality/export?${query}`);
      const allowed = new Set(
        items.flatMap((day) =>
          day.sources
            .filter((s) => stage !== "raw" || s.availability !== "present")
            .map((s) => `${day.day}:${s.dataset_id}`),
        ),
      );
      const selected = result.items.filter((item) =>
        allowed.has(`${item.day}:${item.dataset_id}`),
      );
      const blob = new Blob(
        [
          JSON.stringify(
            { start_day: start, end_day: end, node, items: selected },
            null,
            2,
          ),
        ],
        { type: "application/json" },
      );
      const url = URL.createObjectURL(blob),
        link = document.createElement("a");
      link.href = url;
      link.download = `data-quality-${start}-${end}.json`;
      link.click();
      window.setTimeout(() => URL.revokeObjectURL(url), 1000);
    } catch (e) {
      setError(message(e));
    } finally {
      setExporting(false);
    }
  };
  const select = (
    label: string,
    value: string,
    setter: (value: string) => void,
    options: string[],
  ) => (
    <label>
      {label}
      <select value={value} onChange={(e) => setter(e.target.value)}>
        <option value="">全部</option>
        {options.map((option) => (
          <option key={option}>{option}</option>
        ))}
      </select>
    </label>
  );
  const options = (key: "source" | "market" | "symbol") =>
    [
      ...new Set(
        (catalog?.datasets ?? [])
          .filter((item) => (item.stage ?? "registered") === stage)
          .map((item) => item[key]),
      ),
    ].sort();
  return (
    <section className="panel quality-panel">
      <div className="result-header">
        <div>
          <span className="eyebrow">
            UTC DATA CALENDAR / REGISTERED INVENTORY
          </span>
          <h2>原始行情与可用产物，分开查看。</h2>
        </div>
        <button
          className="button secondary"
          onClick={() => setRefresh((n) => n + 1)}
        >
          重新读取页面
        </button>
      </div>
      <div className="result-body">
        <div className="quality-actions" role="group" aria-label="数据层">
          {(
            [
              ["raw", "原始行情"],
              ["processed", "训练 / 回测产物"],
              ["registered", "待分类记录"],
            ] as const
          ).map(([value, label]) => (
            <button
              key={value}
              className={`button ${stage === value ? "" : "secondary"}`}
              aria-pressed={stage === value}
              onClick={() => {
                setStage(value);
                setDataset("");
                setSource("");
                setTask("");
              }}
            >
              {label} ·{" "}
              {
                (catalog?.datasets ?? []).filter(
                  (d) => (d.stage ?? "registered") === value,
                ).length
              }
            </button>
          ))}
          <button
            className="button secondary"
            disabled={!catalog?.calendar}
            onClick={() => {
              if (catalog?.calendar) {
                setStart(catalog.calendar.start_day);
                setEnd(catalog.calendar.end_day);
              }
            }}
          >
            查看全部登记日期
          </button>
        </div>
        <div className="notice">
          {stage === "raw"
            ? "本栏检查购买 / 下载的原始文件是否在。一个供应商某日缺文件，不等于其他供应商也缺；没有产物审计，不会在这里把源文件判为坏数据。"
            : "本栏只列处理后产物。产物缺失可能只是尚未生成，不等于原始行情缺失；已有文件也不自动代表所有模型的训练特征和标签齐全。"}
          连续回测可以跨日维护状态，无须按盈利或旧研究名单挑日；需要保留具体输入、用途和补齐区间。
          沿用快照生成的网格应记录原快照年龄；丢失增量期间的盘口未知，不能记成已确认零变化。
        </div>
        <div className="notice">
          本页只展示当前选入目录的数据，不是每个回测的必需输入清单。参考行情与执行盘口需求不同；未启用的数据应在需要时再纳入。
          原始数据、处理后产物、用途检查与机器副本分开记录。未观察副本不等于缺失；有文件不等于完成处理后检查。某秒没有成交不等于丢数据，ffill
          后连续也不是无丢包证明。当前质量记录未绑定旧 B0
          结果，不能作为该结果的历史质量证明。
        </div>
        <div className="quality-filters">
          <label>
            开始 / UTC
            <input
              type="date"
              value={start}
              onChange={(e) => setStart(e.target.value)}
            />
          </label>
          <label>
            结束 / UTC
            <input
              type="date"
              value={end}
              onChange={(e) => setEnd(e.target.value)}
            />
          </label>
          <label>
            节点
            <select value={node} onChange={(e) => setNode(e.target.value)}>
              {(catalog?.nodes.length
                ? catalog.nodes
                : [{ id: "local", status: "unknown", last_seen: null }]
              ).map((n) => (
                <option key={n.id} value={n.id}>
                  {n.id} · {n.status}
                </option>
              ))}
            </select>
          </label>
          {select(
            "来源",
            source,
            (value) => {
              setSource(value);
              setDataset("");
            },
            options("source"),
          )}
          {select(
            "市场",
            market,
            (value) => {
              setMarket(value);
              setDataset("");
            },
            options("market"),
          )}
          {select(
            "币对",
            symbol,
            (value) => {
              setSymbol(value);
              setDataset("");
            },
            options("symbol"),
          )}
          <label>
            数据集
            <select
              value={dataset}
              onChange={(e) => setDataset(e.target.value)}
            >
              <option value="">全部数据集</option>
              {(catalog?.datasets ?? []).filter(matches).map((item) => (
                <option key={item.id} value={item.id}>
                  {item.label}
                </option>
              ))}
            </select>
          </label>
        </div>
        <div className="quality-actions">
          <label>
            待处理判断用途
            <select
              value={task}
              onChange={(e) => setTask(e.target.value as QualityTask | "")}
            >
              <option value="">全部检查（含节点副本）</option>
              {Object.entries(TASKS).map(([key, label]) => (
                <option key={key} value={key}>
                  {label}
                </option>
              ))}
            </select>
          </label>
          <label className="check">
            <input
              type="checkbox"
              checked={problem}
              onChange={(e) => setProblem(e.target.checked)}
            />
            仅待处理 / 复核日
          </label>
          <label className="check">
            <input
              type="checkbox"
              checked={missingReplica}
              onChange={(e) => setMissingReplica(e.target.checked)}
            />
            仅明确缺失副本（未知不算缺失）
          </label>
          <button
            className="button secondary small"
            disabled={!report || exporting}
            onClick={() => void exportMissing()}
          >
            {exporting ? "正在导出…" : "导出补数 / 核验清单"}
          </button>
          <button
            className="button secondary small"
            disabled={
              !valid ||
              refreshingInventory ||
              !catalog?.datasets.length ||
              node !== "local"
            }
            title={
              node === "local"
                ? "仅观察已登记本机文件元数据"
                : "远端副本保持只读；选择 local 后刷新本机清单"
            }
            onClick={() => void refreshInventory()}
          >
            {refreshingInventory ? "正在观察登记目录…" : "刷新本机登记清单"}
          </button>
        </div>
        <p className="muted">
          选择具体用途时，待处理日只依据该用途的当前检查结论；不适用不是通过，也不算该用途的待处理项。节点副本仍单独显示。
          刷新只观察所选日期段和数据集（未选则全部已登记数据集）的本机文件存在性、大小等元数据。
          不扫描未登记目录、不重新计算
          SHA、不启动下载、质量计算或回测；远端副本不会被本机刷新为已核验。
          {node !== "local" &&
            " 当前选中远端副本：刷新按钮未启用，可切回 local 观察本机清单。"}
        </p>
        {inventoryRefresh && (
          <div className="notice" role="status">
            {inventoryRefresh.range}：
            {inventoryRefresh.result.status === "refreshed"
              ? "登记清单观察已刷新"
              : "此范围尚无可刷新的登记清单"}
            。{qualityReason(inventoryRefresh.result.reason)}
            {inventoryRefresh.result.observed_at &&
              ` 观察时间 ${stamp(inventoryRefresh.result.observed_at, true)} UTC。`}
          </div>
        )}
        {inventoryError && (
          <div className="notice error" role="alert">
            清单刷新未完成：{inventoryError}。未启动其他扫描或质量计算。
          </div>
        )}
        {!valid && (
          <div className="notice error" role="alert">
            请选择有序、有效且不超过 730 天的 UTC 日期范围。
          </div>
        )}
        {error && (
          <div className="notice error" role="alert">
            {error}。未用默认通过状态替代。
          </div>
        )}
        <p className="muted quality-count">
          {loading
            ? "正在读取元数据…"
            : `${items.length} / ${report?.items.length ?? 0} 日 · 请求 ${days.length} 日（含未下载头尾日）`}{" "}
          · 目录更新时间 {stamp(catalog?.updated_at, true)} UTC
        </p>
        {!loading && !error && report && items.length === 0 && (
          <div className="empty">
            <h3>当前数据层与筛选没有记录</h3>
            <p>清除待复核日 / 副本筛选或选择其他来源。空结果不是质量通过。</p>
          </div>
        )}
        <div className="quality-calendar">
          {coverage.size > 0 && (
            <div className="table-scroll">
              <table>
                <caption>当前层 / 筛选范围的文件覆盖（不是质量评级）</caption>
                <thead>
                  <tr>
                    <th>数据集</th>
                    <th>{stage === "raw" ? "原始文件组在" : "产物已生成"}</th>
                    <th>
                      {stage === "raw" ? "登记路径缺文件" : "未生成 / 未找到"}
                    </th>
                    <th>未观察 / 未登记</th>
                  </tr>
                </thead>
                <tbody>
                  {[...coverage].map(([id, row]) => (
                    <tr key={id}>
                      <td>{row.label}</td>
                      <td>{row.present}</td>
                      <td>{row.missing}</td>
                      <td>{row.unknown}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          {visibleDays.map((day) => (
            <article className="quality-day" key={day.day}>
              <header>
                <div>
                  <strong className="mono">{day.day}</strong>
                  <span>
                    {new Date(`${day.day}T00:00:00Z`).toLocaleDateString(
                      "zh-CN",
                      { weekday: "short", timeZone: "UTC" },
                    )}
                  </span>
                  {day.ongoing && <span className="tag">UTC 日进行中</span>}
                </div>
                <button
                  className="text-button"
                  onClick={() => onReviewDay(day.day)}
                >
                  转到交易复盘 →
                </button>
              </header>
              {day.sources.length ? (
                day.sources.map((entry) => (
                  <SourceDetails
                    key={entry.dataset_id}
                    source={entry}
                    node={node}
                  />
                ))
              ) : (
                <p className="muted">
                  当前来源筛选尚无清单记录；未观察文件，不推定存在或通过。
                </p>
              )}
            </article>
          ))}
        </div>
        {items.length > 20 && (
          <div className="quality-actions">
            <button
              className="button secondary"
              disabled={page === 0}
              onClick={() => setPage((p) => p - 1)}
            >
              上一页
            </button>
            <span>
              第 {page + 1} / {lastPage + 1} 页 · 每页 20
              日（导出仍包含整个筛选范围）
            </span>
            <button
              className="button secondary"
              disabled={page >= lastPage}
              onClick={() => setPage((p) => p + 1)}
            >
              下一页
            </button>
          </div>
        )}
        {report?.limitations.length ? (
          <ul className="limitations">
            {report.limitations.map((limit, i) => (
              <li key={i}>{limit}</li>
            ))}
          </ul>
        ) : null}
      </div>
    </section>
  );
}
