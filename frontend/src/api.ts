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
