import type { MarketFill, QualityDay, Row } from "./api";

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
export function filterQualityDays(
  days: QualityDay[],
  filters: {
    source: string;
    market: string;
    symbol: string;
    datasetId: string;
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
      sources.some(
        (source) =>
          source.availability !== "present" ||
          source.check_status !== "passed" ||
          source.replica.status !== "verified" ||
          source.intervals.some(
            (interval) =>
              interval.status === "gap" || interval.status === "invalid",
          ),
      );
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
