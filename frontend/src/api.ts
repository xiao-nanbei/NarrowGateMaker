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
      "此版本界面仅接纳 synthetic_non_economic 报告，真实研究适配器尚未接入。",
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
