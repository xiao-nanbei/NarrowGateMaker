import type { Row } from "./api";

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
