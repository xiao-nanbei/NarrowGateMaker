import type {
  MarketFill,
  QualityDay,
  QualitySource,
  QualityTask,
  Row,
} from "./api";

export type RecordedPoint = {
  timestamp: number;
  value: number;
  original: string | number;
  row: Row;
};

export type RecordedSeries = {
  kind: "inventory" | "pnl";
  label: string;
  unit: string;
  source: string;
  points: RecordedPoint[];
};

function numeric(value: unknown): number | null {
  if (typeof value !== "number" && typeof value !== "string") return null;
  if (typeof value === "string" && !value.trim()) return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function readPoints(trace: Row[], source: string): RecordedPoint[] {
  const points: RecordedPoint[] = [];
  for (const row of trace) {
    const timestamp = numeric(row.ts_ms);
    if (timestamp === null) continue;
    const key = source.startsWith("fills.") ? source.slice(6) : source;
    const records = source.startsWith("fills.")
      ? Array.isArray(row.fills)
        ? row.fills
        : []
      : [row];
    for (const entry of records) {
      if (!entry || typeof entry !== "object" || Array.isArray(entry)) continue;
      const original = (entry as Row)[key];
      const value = numeric(original);
      if (value !== null) {
        points.push({
          timestamp,
          value,
          original: original as string | number,
          row,
        });
      }
    }
  }
  return points;
}

/** Select one already-recorded field per chart; never sum, mark or fill missing state. */
export function recordedSeries(trace: Row[]): RecordedSeries[] {
  const output: RecordedSeries[] = [];
  const definitions = [
    {
      kind: "inventory" as const,
      label: "库存观测",
      unit: "BTC",
      sources: [
        "inventory_btc",
        "inventory_after_btc",
        "fills.inventory_after_btc",
      ],
    },
    {
      kind: "pnl" as const,
      label: "PnL 观测",
      unit: "USDC",
      sources: ["cumulative_pnl_usdc", "terminal_pnl_usdc", "pnl_usdc"],
    },
  ];
  for (const definition of definitions) {
    for (const source of definition.sources) {
      const points = readPoints(trace, source);
      if (points.length >= 2) {
        output.push({ ...definition, source, points });
        break;
      }
    }
  }
  return output;
}

export const CANDLE_INTERVALS = {
  "1s": 1000,
  "5s": 5000,
  "1m": 60000,
  "5m": 300000,
} as const;
export type CandleInterval = keyof typeof CANDLE_INTERVALS;
export type TimeWindow = { start: number; end: number };

/** Inclusive UTC dates, including days for which no source record exists. */
export function utcDays(start: string, end: string): string[] {
  const parse = (day: string) => {
    const ms = Date.parse(`${day}T00:00:00.000Z`);
    return Number.isFinite(ms) &&
      new Date(ms).toISOString().slice(0, 10) === day
      ? ms
      : null;
  };
  const from = parse(start),
    to = parse(end);
  if (from === null || to === null || to < from || to - from > 3660 * 86400000)
    return [];
  return Array.from({ length: (to - from) / 86400000 + 1 }, (_, i) =>
    new Date(from + i * 86400000).toISOString().slice(0, 10),
  );
}

/** Half-open UI windows never fetch outside the selected UTC day. */
export function boundedWindow(
  start: number,
  span: number,
  bounds: TimeWindow,
): TimeWindow {
  const width = Math.min(Math.max(1, span), bounds.end - bounds.start);
  const left = Math.max(bounds.start, Math.min(start, bounds.end - width));
  return { start: left, end: left + width };
}

export function zoomWindow(
  window: TimeWindow,
  factor: number,
  bounds: TimeWindow,
  minimum: number,
  maximum: number,
): TimeWindow {
  const width = Math.min(
    maximum,
    Math.max(minimum, (window.end - window.start) * factor),
  );
  return boundedWindow((window.start + window.end - width) / 2, width, bounds);
}

export function candleBucket(
  timestamp: number,
  interval: CandleInterval,
): number {
  return (
    Math.floor(timestamp / CANDLE_INTERVALS[interval]) *
    CANDLE_INTERVALS[interval]
  );
}

export function alignedWindow(
  window: TimeWindow,
  interval: CandleInterval,
  bounds: TimeWindow,
): TimeWindow {
  const step = CANDLE_INTERVALS[interval];
  return boundedWindow(
    Math.floor(window.start / step) * step,
    Math.max(step, Math.round((window.end - window.start) / step) * step),
    bounds,
  );
}

/** Bucket by physical fill time, retaining every fill and its exact original fields. */
export function groupMarketFills(
  fills: MarketFill[],
  interval: CandleInterval,
): Map<number, MarketFill[]> {
  const groups = new Map<number, MarketFill[]>();
  for (const fill of fills) {
    const bucket = candleBucket(fill.fill_ts_ms, interval);
    groups.set(bucket, [...(groups.get(bucket) ?? []), fill]);
  }
  return groups;
}

/** Inventory belongs to local fill callbacks, never to the earlier exchange fill clock. */
export function visibleInventoryFills(
  fills: MarketFill[],
  window: TimeWindow,
): MarketFill[] {
  return fills
    .filter(
      (fill) =>
        typeof fill.inventory_after === "number" &&
        Number.isFinite(fill.inventory_after) &&
        typeof fill.visible_ts_ms === "number" &&
        Number.isFinite(fill.visible_ts_ms) &&
        fill.visible_ts_ms >= window.start &&
        fill.visible_ts_ms < window.end,
    )
    .sort(
      (a, b) =>
        a.visible_ts_ms! - b.visible_ts_ms! ||
        a.fill_sequence - b.fill_sequence,
    );
}

/** A problem-day filter applies to visible sources, not hidden source failures. */
export function qualityReason(reason: string | undefined): string {
  if (!reason) return "尚未登记具体检查原因。";
  return (
    {
      no_recorded_audit: "尚无该来源／日期的处理后检查记录。",
      historical_audit_not_bound_to_current_files:
        "有历史检查，但尚未关联当前文件；不能沿用历史通过结论。",
      local_files_changed_since_observation:
        "当前文件的元数据与上次观察不同，需要重新关联或检查。",
      recorded_content_audit_current_size_matched:
        "已关联登记版本的原检查，当前仅文件大小匹配；本次没有重新校验内容。",
      inventory_snapshot_only: "仅观察到文件清单，尚未执行该产物的内容检查。",
      remote_replica_not_observed:
        "未观察所选远端的该版本副本；本机刷新不会更新远端核验时间。",
      local_inventory_root_unavailable:
        "登记数据目录当前不可访问，可能未挂载；不据此判定文件缺失。",
      local_file_missing:
        "登记目录可访问，但未找到此日对应文件；先用现有补数／同步流程处理。",
      local_presence_only:
        "本机已观察到文件，待内容核验；存在不等于可用于所有任务。",
      task_not_mapped_in_recorded_audit:
        "已有检查没有给出此用途的结论，需使用相应的处理后检查。",
      source_not_applicable: "此数据源不承担该用途，不是下载失败。",
      quality_refresh_source_not_registered:
        "当前仅有已导入的目录记录，尚未登记可供本机刷新的文件清单。",
    }[reason] ?? reason
  );
}

export function qualityAudit(source: QualitySource): {
  state: string;
  label: string;
  reason: string;
} {
  const applicability = source.audit_applicability;
  if (!applicability)
    return {
      state: "unchecked",
      label: "尚无当前文件复核",
      reason: "旧目录仅保留历史检查；尚无当前文件与该检查的关联记录。",
    };
  const labels: Record<string, [string, string]> = {
    no_audit: ["unchecked", "处理后检查尚未执行"],
    historical_unbound: ["unchecked", "历史检查未关联当前文件"],
    changed_since_observation: ["stale", "文件已变化，待复核"],
    verified_snapshot: ["verified", "已有核验快照已关联"],
    inventory_snapshot_only: ["unchecked", "已有清单，内容待检查"],
    recorded_content_audit_current_size_matched: [
      "partial",
      "已有检查已关联 · 仅大小匹配",
    ],
  };
  const [state, label] = labels[applicability.status] ?? [
    "unchecked",
    "检查关联范围未识别",
  ];
  return { state, label, reason: qualityReason(applicability.reason) };
}

export function qualityTask(source: QualitySource, task: QualityTask) {
  const state = source.current_task_usability?.[task] ?? "unknown";
  return {
    state,
    label: {
      passed: "已有适用检查",
      failed: "此用途未通过",
      unknown: "此用途待复核",
      not_applicable: "不适用此用途",
    }[state],
    reason: source.task_reasons?.[task]
      ? qualityReason(source.task_reasons[task])
      : state === "not_applicable"
        ? "此源未被登记为该用途，不代表文件有质量错误。"
        : "尚无当前用途的原因记录；历史结果不能自动代替当前结论。",
  };
}

export function qualityReplica(source: QualitySource) {
  const state = source.replica.status;
  return {
    state,
    label: {
      verified: "该版本副本已有核验记录",
      present_unverified: "节点上已有文件，待审核",
      missing: "此节点明确缺少副本",
      unknown: "此节点副本未观察",
      stale: "副本观察已过期",
    }[state],
    reason: source.replica.observation_reason
      ? qualityReason(source.replica.observation_reason)
      : state === "unknown"
        ? "尚无所选节点副本的观察记录；不等于文件缺失。"
        : "结论仅适用于此节点、此版本和已记录的核验时间。",
  };
}

export function filterQualityDays(
  days: QualityDay[],
  filters: {
    source: string;
    market: string;
    symbol: string;
    datasetId: string;
    task?: QualityTask | "";
    problemOnly: boolean;
    missingReplicaOnly: boolean;
  },
): QualityDay[] {
  const sourceFilter = Boolean(
    filters.source || filters.market || filters.symbol || filters.datasetId,
  );
  return days.flatMap((day) => {
    const sources = day.sources.filter(
      (source) =>
        (!filters.source || source.source === filters.source) &&
        (!filters.market || source.market === filters.market) &&
        (!filters.symbol || source.symbol === filters.symbol) &&
        (!filters.datasetId || source.dataset_id === filters.datasetId) &&
        (!filters.missingReplicaOnly || source.replica.status === "missing"),
    );
    if ((sourceFilter || filters.missingReplicaOnly) && !sources.length)
      return [];
    const problem =
      !sources.length ||
      sources.some((source) => {
        if (filters.task) {
          const state = qualityTask(source, filters.task).state;
          return state !== "passed" && state !== "not_applicable";
        }
        return (
          source.availability !== "present" ||
          source.check_status !== "passed" ||
          ![
            "verified_snapshot",
            "recorded_content_audit_current_size_matched",
          ].includes(source.audit_applicability?.status ?? "") ||
          Object.values(source.current_task_usability ?? {}).some(
            (state) => state === "failed",
          ) ||
          source.replica.status !== "verified" ||
          source.intervals.some(
            (interval) =>
              interval.status === "gap" || interval.status === "invalid",
          )
        );
      });
    return filters.problemOnly && !problem
      ? []
      : [{ ...day, sources, problem }];
  });
}

export type FillLabelBounds = {
  id: string;
  side: "BUY" | "SELL";
  left: number;
  width: number;
};

/** Thin text only, separately per side; fill points and group counts are untouched. */
export function selectFillLabels(
  labels: FillLabelBounds[],
  priorityIds: string[] = [],
  gap = 6,
): Set<string> {
  const rank = (id: string) => {
    const index = priorityIds.indexOf(id);
    return index < 0 ? priorityIds.length : index;
  };
  const ordered = [...labels].sort(
    (a, b) =>
      rank(a.id) - rank(b.id) || a.left - b.left || a.id.localeCompare(b.id),
  );
  const shown: FillLabelBounds[] = [];
  for (const label of ordered) {
    if (
      !shown.some(
        (other) =>
          other.side === label.side &&
          label.left < other.left + other.width + gap &&
          label.left + label.width + gap > other.left,
      )
    )
      shown.push(label);
  }
  return new Set(shown.map((label) => label.id));
}
