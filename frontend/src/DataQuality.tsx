import { useEffect, useMemo, useState } from "react";
import { api, getQualityCatalog, getQualityReport, stamp } from "./api";
import type {
  QualityCatalog,
  QualityExport,
  QualityReport,
  QualitySource,
} from "./api";
import { filterQualityDays, utcDays } from "./traceSeries";

const LABELS: Record<string, string> = {
  present: "有文件",
  missing: "缺失",
  unknown: "未知",
  passed: "通过",
  failed: "失败",
  unchecked: "未检查",
  partial: "部分检查",
  not_applicable: "不适用",
  verified: "副本已核验",
  present_unverified: "副本待核验",
  stale: "副本记录已过期",
};
const TASKS = {
  candles: "K 线",
  modeled_replay: "模型回放",
  strict_replay: "严格回放",
  funding_pnl: "资金费核算",
} as const;
const message = (error: unknown) =>
  error instanceof Error ? error.message : "读取失败";
const dayString = (ms: number) => new Date(ms).toISOString().slice(0, 10);
function State({ value }: { value: string }) {
  return (
    <span className={`dq-state dq-state-${value}`}>
      {LABELS[value] ?? value}
    </span>
  );
}

function SourceDetails({ source }: { source: QualitySource }) {
  const known = (value: number | null, suffix = "") =>
    value === null ? "未知" : `${value.toLocaleString("en-US")}${suffix}`;
  return (
    <details className="quality-source">
      <summary>
        <strong>{source.label}</strong>
        <span className="mono">
          {source.exchange} / {source.market} / {source.symbol} /{" "}
          {source.data_type}
        </span>
        <State value={source.availability} />
        <State value={source.check_status} />
        <span className="muted">
          副本 <State value={source.replica.status} />
        </span>
      </summary>
      <div className="quality-source-body">
        <p className="muted">
          {source.source} · {source.version} · {source.dataset_id}
        </p>
        <div className="quality-usability">
          {Object.entries(TASKS).map(([key, label]) => (
            <div key={key}>
              <span>{label}</span>
              <State value={source.task_usability[key as keyof typeof TASKS]} />
            </div>
          ))}
        </div>
        <dl className="quality-facts">
          <div>
            <dt>记录数</dt>
            <dd>{known(source.records)}</dd>
          </div>
          <div>
            <dt>文件字节数</dt>
            <dd>{known(source.size_bytes, " B")}</dd>
          </div>
          <div>
            <dt>已报告覆盖率</dt>
            <dd>
              {source.coverage_ratio === null
                ? "未知"
                : `${(source.coverage_ratio * 100).toFixed(3)}%`}
            </dd>
          </div>
          <div>
            <dt>已报告最大间隔</dt>
            <dd>{known(source.max_gap_ms, " ms")}</dd>
          </div>
          <div>
            <dt>质量检查时间 / UTC</dt>
            <dd>{stamp(source.checked_at, true)}</dd>
          </div>
          <div>
            <dt>副本核验时间 / UTC</dt>
            <dd>{stamp(source.replica.last_checked_at, true)}</dd>
          </div>
          <div>
            <dt>检查范围</dt>
            <dd>{source.check_scope || "未知"}</dd>
          </div>
          <div>
            <dt>节点状态</dt>
            <dd>{source.replica.node_status || "未知"}</dd>
          </div>
        </dl>
        <p className="quality-evidence">
          证据：{source.evidence_label || "未提供"}
        </p>
        <ul className="limitations">
          {(source.reasons.length
            ? source.reasons
            : ["来源未记录详细原因；不据此推定无问题。"]
          ).map((reason, i) => (
            <li key={i}>{reason}</li>
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
  const [problem, setProblem] = useState(false);
  const [missingReplica, setMissingReplica] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [exporting, setExporting] = useState(false);
  const [refresh, setRefresh] = useState(0);
  const days = useMemo(() => utcDays(start, end), [start, end]);
  const valid = days.length > 0 && days.length <= 366;
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
  const matches = (entry: { source: string; market: string; symbol: string }) =>
    (!source || entry.source === source) &&
    (!market || entry.market === market) &&
    (!symbol || entry.symbol === symbol);
  const items = filterQualityDays(report?.items ?? [], {
    source,
    market,
    symbol,
    datasetId: dataset,
    problemOnly: problem,
    missingReplicaOnly: missingReplica,
  });
  const exportMissing = async () => {
    setExporting(true);
    setError("");
    try {
      const result = await api<QualityExport>(`/data-quality/export?${query}`);
      const allowed = new Set(
        items.flatMap((day) =>
          day.sources.map((s) => `${day.day}:${s.dataset_id}`),
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
    [...new Set((catalog?.datasets ?? []).map((item) => item[key]))].sort();
  return (
    <section className="panel quality-panel">
      <div className="result-header">
        <div>
          <span className="eyebrow">UTC DATA CALENDAR / READ ONLY</span>
          <h2>先看数据是否在，再看能否使用。</h2>
        </div>
        <button
          className="button secondary"
          onClick={() => setRefresh((n) => n + 1)}
        >
          刷新元数据
        </button>
      </div>
      <div className="result-body">
        <div className="notice">
          本页只展示当前选入目录的数据，不是每个回测的必需输入清单。参考行情与执行盘口需求不同；未启用的数据应在需要时再纳入。
          数据可用性、质量检查、任务适用性与节点副本分开记录。离线或未核验节点显示未知；某秒没有成交不等于数据缺失。当前质量记录未绑定旧
          B0 结果，不能作为该结果的历史质量证明。
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
          <label className="check">
            <input
              type="checkbox"
              checked={problem}
              onChange={(e) => setProblem(e.target.checked)}
            />
            仅问题日
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
        </div>
        {!valid && (
          <div className="notice error" role="alert">
            请选择有序、有效且不超过 366 天的 UTC 日期范围。
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
            <h3>当前筛选没有记录</h3>
            <p>清除问题日 / 副本筛选或选择其他来源。空结果不是质量通过。</p>
          </div>
        )}
        <div className="quality-calendar">
          {items.map((day) => (
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
                  <SourceDetails key={entry.dataset_id} source={entry} />
                ))
              ) : (
                <p className="muted">
                  当前来源筛选无记录；可用性未知，不推定通过。
                </p>
              )}
            </article>
          ))}
        </div>
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
