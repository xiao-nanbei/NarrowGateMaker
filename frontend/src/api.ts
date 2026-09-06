export type Row = Record<string, unknown>;
export type Job = {
  id: string;
  experiment_id?: string | null;
  name?: string | null;
  arm?: string | null;
  status: string;
  worker_id?: string | null;
  created_at?: string | number | null;
  updated_at?: string | number | null;
  error?: unknown;
};
export type Runner = {
  id: string;
  label: string;
  classification: string;
  available: boolean;
};
export type Worker = {
  id: string;
  last_seen?: string | number | null;
  online?: boolean;
  capabilities?: unknown[];
  datasets?: unknown[];
};
export type ComputeResource = {
  id: string;
  label: string;
  kind: "local" | "lan" | "azure";
  state: "online" | "offline" | "unknown" | "stale" | "scaled_to_zero";
  checked_at: string | null;
  last_error: string | null;
  hardware: {
    cpu_name: string | null;
    vcpu: number | null;
    memory_gib: number | null;
    architecture: string | null;
  };
  capacity: { running_nodes: number | null; target_nodes: number | null };
  roles: {
    training: "preferred" | "allowed" | "disabled" | "unknown";
    replay: "allowed" | "disabled" | "unknown";
    data_processing: "allowed" | "disabled" | "unknown";
  };
  scheduler: {
    mode: "studio_worker" | "external_observer" | "not_connected";
    can_submit: boolean;
    reason: string;
  };
  jobs: {
    id: string;
    label: string;
    status: string;
    updated_at: string | null;
    arm: string | null;
  }[];
  worker_ids: string[];
  notes: string[];
};
export type ComputeResources = {
  schema_version: "compute_resources.v1";
  observed_at: string | null;
  items: ComputeResource[];
  limitations: string[];
};

export async function getComputeResources(
  signal?: AbortSignal,
): Promise<ComputeResources> {
  const report = await api<ComputeResources>("/compute-resources", { signal });
  if (
    report.schema_version !== "compute_resources.v1" ||
    !Array.isArray(report.items) ||
    !Array.isArray(report.limitations) ||
    new Set(report.items.map((item) => item.id)).size !== report.items.length ||
    report.items.some(
      (item) =>
        typeof item.id !== "string" ||
        typeof item.label !== "string" ||
        !["local", "lan", "azure"].includes(item.kind) ||
        !["online", "offline", "unknown", "stale", "scaled_to_zero"].includes(
          item.state,
        ) ||
        !item.hardware ||
        !item.capacity ||
        !item.roles ||
        !item.scheduler ||
        typeof item.scheduler.can_submit !== "boolean" ||
        !Array.isArray(item.jobs) ||
        !Array.isArray(item.worker_ids) ||
        !Array.isArray(item.notes),
    )
  )
    throw new Error("计算资源目录格式不受支持；未将 worker 心跳当作主机状态。");
  return report;
}
export type Report = {
  schema_version: string;
  classification: string;
  summary: Row;
  trace: Row[];
  limitations: unknown[];
};
export type Logs = {
  stdout: string | null;
  stderr: string | null;
  scope?: string;
};
export type BaselineResult = {
  id: string;
  name: string;
  classification: "real_market_baseline_read_only";
  imported_at: string | number;
  coverage_days: number;
  segment_count: number;
};
export type BaselineReport = {
  id: string;
  name: string;
  classification: "real_market_baseline_read_only";
  summary: {
    coverage_days: number;
    segment_count: number;
    trading_pnl: number;
    funding_pnl: number;
    net_pnl: number;
    fees_already_included: true;
    fee_cost: number;
    filled_orders: number;
    buy_fills: number;
    sell_fills: number;
    campaign_count: number;
    closed_campaigns: number;
    open_campaigns: number;
  };
  segments: {
    index: number;
    start_day: string;
    end_day: string;
    day_count: number;
    source: "local" | "azure";
    trading_pnl: number;
    funding_pnl: number;
    net_pnl: number;
    filled_orders: number;
    campaign_count: number;
    queue_mode: "strict" | "non_strict";
  }[];
  verification: {
    overlap_days: string[];
    passed: boolean;
    description: string;
    queue_lookup_count: number;
    queue_exact_count: number;
    queue_known_zero_count: number;
    queue_missing_count: number;
    native_events_consumed: number;
    native_events_rejected: number;
    native_gap_invalid_sequence_time_reversal_counts: number;
  };
  limitations: string[];
};

export type QualityDataset = {
  id: string;
  source: string;
  exchange: string;
  market: string;
  symbol: string;
  data_type: string;
  version: string;
  label: string;
};
export type QualitySource = Omit<QualityDataset, "id"> & {
  dataset_id: string;
  availability: "present" | "missing" | "unknown";
  check_status: "passed" | "failed" | "unchecked" | "partial";
  check_scope: string;
  task_usability: Record<
    "candles" | "modeled_replay" | "strict_replay" | "funding_pnl",
    "passed" | "failed" | "unknown" | "not_applicable"
  >;
  records: number | null;
  size_bytes: number | null;
  checked_at: string | null;
  coverage_ratio: number | null;
  max_gap_ms: number | null;
  reasons: string[];
  intervals: {
    start_ms: number;
    end_ms: number;
    status: "gap" | "invalid" | "valid" | "unknown";
    kind: string;
    reason: string;
  }[];
  replica: {
    status: "verified" | "present_unverified" | "missing" | "unknown" | "stale";
    last_checked_at: string | null;
    node_status: string;
  };
  evidence_label: string;
};
export type QualityDay = {
  day: string;
  ongoing: boolean;
  problem: boolean;
  sources: QualitySource[];
};
export type QualityCatalog = {
  datasets: QualityDataset[];
  nodes: { id: string; status: string; last_seen: string | null }[];
  updated_at: string | null;
};
export type QualityReport = {
  start_day: string;
  end_day: string;
  node: string;
  items: QualityDay[];
  limitations: string[];
};
export type QualityExport = {
  items: ({
    day: string;
    dataset_id?: string;
    start_ms: number;
    end_ms: number;
    reason: string;
    recommended_action: string;
  } & Row)[];
};

export async function getQualityCatalog(
  signal?: AbortSignal,
): Promise<QualityCatalog> {
  const value = await api<QualityCatalog>("/data-quality/catalog", { signal });
  if (!Array.isArray(value.datasets) || !Array.isArray(value.nodes))
    throw new Error("质量目录格式不受支持。");
  return value;
}

export async function getQualityReport(
  query: URLSearchParams,
  signal?: AbortSignal,
): Promise<QualityReport> {
  const value = await api<QualityReport>(`/data-quality?${query}`, { signal });
  const start = Date.parse(`${query.get("start_day")}T00:00:00Z`);
  const end = Date.parse(`${query.get("end_day")}T00:00:00Z`);
  if (
    value.start_day !== query.get("start_day") ||
    value.end_day !== query.get("end_day") ||
    value.node !== query.get("node") ||
    !Array.isArray(value.items) ||
    value.items.length !== (end - start) / 86400000 + 1 ||
    !Array.isArray(value.limitations) ||
    value.items.some(
      (day, index) =>
        day.day !==
          new Date(start + index * 86400000).toISOString().slice(0, 10) ||
        !Array.isArray(day.sources) ||
        day.sources.some(
          (source) =>
            !source.task_usability ||
            !source.replica ||
            !Array.isArray(source.intervals) ||
            !Array.isArray(source.reasons) ||
            Object.values(source.task_usability).some(
              (state) =>
                !["passed", "failed", "unknown", "not_applicable"].includes(
                  state,
                ),
            ) ||
            !["present", "missing", "unknown"].includes(source.availability) ||
            !["passed", "failed", "unchecked", "partial"].includes(
              source.check_status,
            ),
        ),
    )
  ) {
    throw new Error("质量记录格式不受支持；不能将缺失字段视为通过。");
  }
  return value;
}

export type MarketSource = {
  kind: string;
  location: string;
  connected: boolean;
};
export type MarketMetadata = {
  result_id: string;
  symbol: string;
  classification: "simulated_historical_fills";
  status: "available" | "unavailable";
  reason: string | null;
  source: MarketSource;
  segments: {
    index: number;
    start_day: string;
    end_day: string;
    days: string[];
    source: "local" | "azure";
  }[];
  intervals_s: number[];
  max_candles: number;
  max_window_ms: number;
  order_lifecycle: "partial_fill_snapshots_only";
  pnl: "unavailable";
};
export type MarketCandle = {
  time_ms: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  source_rows: number;
};
export type MarketCandles = {
  result_id: string;
  status: "available" | "unavailable";
  reason: string | null;
  source: MarketSource;
  interval_s: number;
  start_ms: number;
  end_ms: number;
  items: MarketCandle[];
  count: number;
  truncated: boolean;
  gaps: number;
};
export type MarketFill = {
  id: string;
  segment_index: number;
  fill_sequence: number;
  fill_ts_ms: number;
  visible_ts_ms: number | null;
  side: "BUY" | "SELL";
  price: number;
  fill_trade_price?: number | null;
  quantity: number;
  order_id: string | null;
  source_order_id: string | null;
  inventory_before: number | null;
  inventory_after: number | null;
  fee: number | null;
  fee_asset: string | null;
  campaign_id: string | null;
  campaign_id_at_submit?: string | null;
};
export type MarketFills = {
  result_id: string;
  status: "available" | "unavailable";
  reason: string | null;
  classification: "simulated_historical_fills";
  start_ms: number;
  end_ms: number;
  items: MarketFill[];
  count: number;
  next_cursor: string | null;
  truncated: boolean;
};
export type MarketOrder = {
  result_id: string;
  id: string;
  status: string;
  classification: "simulated_historical_fills";
  scope: "fill_trace_snapshots_only";
  source_order_id: string | null;
  segment_index: number;
  side: string;
  price: number | null;
  quantity: number | null;
  submit_ts_ms: number | null;
  activate_ts_ms: number | null;
  new_ack_ts_ms: number | null;
  cancel_request_ts_ms: number | null;
  cancel_effective_ts_ms: number | null;
  cancel_ack_ts_ms: number | null;
  filled_quantity: number | null;
  fill_count: number;
  fills: MarketFill[];
  truncated?: boolean;
  lifecycle_complete: false;
  pnl: null;
};

const marketRoute = (id: string, kind: string) =>
  `/results/${encodeURIComponent(id)}/${kind}`;
const finite = (value: unknown) =>
  typeof value === "number" && Number.isFinite(value);
export async function getMarketMetadata(
  id: string,
  signal?: AbortSignal,
): Promise<MarketMetadata> {
  const value = await api<MarketMetadata>(marketRoute(id, "market"), {
    signal,
  });
  if (
    value.result_id !== id ||
    value.classification !== "simulated_historical_fills" ||
    !Array.isArray(value.segments) ||
    !value.source ||
    !Array.isArray(value.intervals_s)
  )
    throw new Error("市场复盘接口分类或格式不受支持。");
  return value;
}
export async function getMarketCandles(
  id: string,
  query: URLSearchParams,
  signal?: AbortSignal,
): Promise<MarketCandles> {
  const value = await api<MarketCandles>(
    `${marketRoute(id, "candles")}?${query}`,
    { signal },
  );
  if (
    value.result_id !== id ||
    value.start_ms !== Number(query.get("start_ms")) ||
    value.end_ms !== Number(query.get("end_ms")) ||
    !Array.isArray(value.items) ||
    value.count !== value.items.length ||
    value.items.some(
      (c, index) =>
        ![c.time_ms, c.open, c.high, c.low, c.close].every(finite) ||
        c.time_ms < value.start_ms ||
        c.time_ms >= value.end_ms ||
        (index > 0 && c.time_ms <= value.items[index - 1].time_ms) ||
        c.low > Math.min(c.open, c.close) ||
        c.high < Math.max(c.open, c.close),
    )
  )
    throw new Error("市场 OHLC 格式不受支持；不由模拟成交构造 K 线。");
  return value;
}
export async function getMarketFills(
  id: string,
  query: URLSearchParams,
  signal?: AbortSignal,
): Promise<MarketFills> {
  const value = await api<MarketFills>(`${marketRoute(id, "fills")}?${query}`, {
    signal,
  });
  if (
    value.result_id !== id ||
    value.start_ms !== Number(query.get("start_ms")) ||
    value.end_ms !== Number(query.get("end_ms")) ||
    value.classification !== "simulated_historical_fills" ||
    !Array.isArray(value.items) ||
    value.count !== value.items.length ||
    new Set(value.items.map((f) => f.id)).size !== value.items.length ||
    value.items.some(
      (f) =>
        f.fill_ts_ms < value.start_ms ||
        f.fill_ts_ms >= value.end_ms ||
        typeof f.id !== "string" ||
        !["BUY", "SELL"].includes(f.side) ||
        ![f.fill_ts_ms, f.price, f.quantity].every(finite),
    )
  )
    throw new Error("模拟成交格式不受支持；未混用合成 trace。");
  return value;
}
export async function getMarketOrder(
  id: string,
  orderId: string,
  signal?: AbortSignal,
): Promise<MarketOrder> {
  const value = await api<MarketOrder>(
    marketRoute(id, `orders/${encodeURIComponent(orderId)}`),
    { signal },
  );
  if (
    value.result_id !== id ||
    value.id !== orderId ||
    value.classification !== "simulated_historical_fills" ||
    value.scope !== "fill_trace_snapshots_only" ||
    value.lifecycle_complete !== false
  )
    throw new Error("订单快照格式不受支持；不推断完整挂单状态。");
  return value;
}

let sessionToken = "";
export function setSessionToken(value: string): void {
  sessionToken = value.trim();
}

export async function api<T>(path: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`/api${path}`, {
    ...options,
    headers: {
      Accept: "application/json",
      ...(sessionToken ? { Authorization: `Bearer ${sessionToken}` } : {}),
      ...options?.headers,
    },
  });
  if (!response.ok) {
    let detail = "";
    try {
      const body = (await response.json()) as Row;
      detail =
        typeof body.detail === "string"
          ? body.detail
          : JSON.stringify(body.detail ?? body.error ?? "");
    } catch {
      /* A non-JSON error still retains its HTTP status. */
    }
    throw new Error(
      `${response.status} ${response.statusText}${detail ? ` · ${detail}` : ""}`,
    );
  }
  return response.json() as Promise<T>;
}

export async function getReport(
  id: string,
  signal?: AbortSignal,
): Promise<Report> {
  const report = await api<Report>(`/jobs/${encodeURIComponent(id)}/report`, {
    signal,
  });
  if (
    report.schema_version !== "backtest_report.v1" ||
    !Array.isArray(report.trace) ||
    !report.summary
  ) {
    throw new Error("报告格式不受支持；请查看原始日志。");
  }
  if (report.classification !== "synthetic_non_economic") {
    throw new Error(
      "合成任务视图仅接纳 synthetic_non_economic 报告；真实 B0 结果请在独立只读栏目查看。",
    );
  }
  return report;
}

export async function getBaselineResults(): Promise<BaselineResult[]> {
  const result = await api<{ items: BaselineResult[] }>("/results");
  if (
    !Array.isArray(result.items) ||
    result.items.some(
      (item) =>
        item.classification !== "real_market_baseline_read_only" ||
        typeof item.id !== "string" ||
        typeof item.name !== "string" ||
        !Number.isInteger(item.coverage_days) ||
        !Number.isInteger(item.segment_count),
    )
  ) {
    throw new Error("真实结果目录格式不受支持，未将其作为合成任务接纳。");
  }
  return result.items;
}

export async function getBaselineReport(
  id: string,
  signal?: AbortSignal,
): Promise<BaselineReport> {
  const report = await api<BaselineReport>(
    `/results/${encodeURIComponent(id)}`,
    {
      signal,
    },
  );
  const numeric = (value: unknown): value is number =>
    typeof value === "number" && Number.isFinite(value);
  const date = (value: unknown) =>
    typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value);
  if (
    report.id !== id ||
    typeof report.name !== "string" ||
    report.classification !== "real_market_baseline_read_only" ||
    !report.summary ||
    report.summary.fees_already_included !== true ||
    [
      "coverage_days",
      "segment_count",
      "trading_pnl",
      "funding_pnl",
      "net_pnl",
      "fee_cost",
      "filled_orders",
      "buy_fills",
      "sell_fills",
      "campaign_count",
      "closed_campaigns",
      "open_campaigns",
    ].some((key) => !numeric(record(report.summary)[key])) ||
    !Array.isArray(report.segments) ||
    report.segments.length !== report.summary.segment_count ||
    report.segments.some(
      (segment) =>
        !date(segment.start_day) ||
        !date(segment.end_day) ||
        !["local", "azure"].includes(segment.source) ||
        !["strict", "non_strict"].includes(String(segment.queue_mode)) ||
        [
          "index",
          "day_count",
          "trading_pnl",
          "funding_pnl",
          "net_pnl",
          "filled_orders",
          "campaign_count",
        ].some((key) => !numeric(record(segment)[key])),
    ) ||
    !report.verification ||
    typeof report.verification.passed !== "boolean" ||
    typeof report.verification.description !== "string" ||
    !Array.isArray(report.verification.overlap_days) ||
    !report.verification.overlap_days.every(date) ||
    [
      "queue_lookup_count",
      "queue_exact_count",
      "queue_known_zero_count",
      "queue_missing_count",
      "native_events_consumed",
      "native_events_rejected",
      "native_gap_invalid_sequence_time_reversal_counts",
    ].some((key) => !numeric(record(report.verification)[key])) ||
    !Array.isArray(report.limitations) ||
    !report.limitations.every((item) => typeof item === "string")
  ) {
    throw new Error(
      "真实 B0 报告分类、金额口径或分段格式不受支持；已停止展示。",
    );
  }
  return report;
}

export function record(value: unknown): Row {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Row)
    : {};
}
export function field(value: unknown, path: string): unknown {
  return path
    .split(".")
    .reduce<unknown>((current, key) => record(current)[key], value);
}
export function display(value: unknown): string {
  if (value === null || value === undefined || value === "") return "—";
  if (typeof value === "boolean") return value ? "是" : "否";
  return typeof value === "object" ? JSON.stringify(value) : String(value);
}
export function stamp(value: unknown, full = false): string {
  if (value === null || value === undefined || value === "") return "—";
  const date = new Date(
    typeof value === "number"
      ? value < 1e11
        ? value * 1000
        : value
      : String(value),
  );
  if (!Number.isFinite(date.getTime())) return display(value);
  return date
    .toISOString()
    .slice(full ? 0 : 11, full ? 23 : 23)
    .replace("T", " ");
}
export function isCompleted(job: Job): boolean {
  return job.status.toLowerCase() === "completed";
}
export function requestId(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join(
    "",
  );
}
