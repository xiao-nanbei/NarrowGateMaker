import { useMemo } from "react";
import { display, stamp } from "./api";
import type { Row } from "./api";
import { recordedSeries } from "./traceSeries";
import type { RecordedSeries } from "./traceSeries";

function RecordedChart({
  series,
  onSelect,
}: {
  series: RecordedSeries;
  onSelect: (row: Row) => void;
}) {
  const values = series.points.map((point) => point.value);
  const timestamps = series.points.map((point) => point.timestamp);
  const low = Math.min(...values);
  const high = Math.max(...values);
  const start = Math.min(...timestamps);
  const end = Math.max(...timestamps);
  const x = (timestamp: number) =>
    end === start ? 350 : 65 + ((timestamp - start) / (end - start)) * 550;
  const y = (value: number) =>
    high === low ? 74 : 126 - ((value - low) / (high - low)) * 100;
  const coordinates = series.points
    .map((point) => `${x(point.timestamp)},${y(point.value)}`)
    .join(" ");
  const axis = (value: number) => Number(value.toPrecision(6)).toString();
  return (
    <section className="recorded-chart">
      <header>
        <h3>
          {series.label} <span>/ {series.unit}</span>
        </h3>
        <span>{series.points.length} 个原始观测点</span>
      </header>
      <svg
        viewBox="0 0 650 164"
        role="group"
        aria-label={`${series.label}，仅展示原始 trace 字段 ${series.source}`}
      >
        <line x1="65" x2="615" y1="26" y2="26" className="chart-grid" />
        <line x1="65" x2="615" y1="126" y2="126" className="chart-grid" />
        <text x="52" y="30" textAnchor="end">
          {axis(high)}
        </text>
        {high !== low && (
          <text x="52" y="130" textAnchor="end">
            {axis(low)}
          </text>
        )}
        <text x="65" y="153">
          {stamp(start)}
        </text>
        <text x="615" y="153" textAnchor="end">
          {stamp(end)} UTC
        </text>
        <polyline points={coordinates} className="chart-path" />
        {series.points.map((point, index) => (
          <circle
            key={`${point.row.seq}-${index}`}
            cx={x(point.timestamp)}
            cy={y(point.value)}
            r="4"
            className="chart-point"
            tabIndex={0}
            role="button"
            aria-label={`${series.label} ${point.original} ${series.unit}，查看事件 ${display(point.row.seq)}`}
            onClick={() => onSelect(point.row)}
            onKeyDown={(event) => {
              if (event.key === "Enter" || event.key === " ") {
                event.preventDefault();
                onSelect(point.row);
              }
            }}
          >
            <title>
              #{display(point.row.seq)} · {stamp(point.timestamp)} UTC ·{" "}
              {point.original} {series.unit}
            </title>
          </circle>
        ))}
      </svg>
      <p>
        <code>{series.source}</code> ·
        点击观测点查看原始事件；连线不代表点间状态已知。
      </p>
    </section>
  );
}

export function TraceCharts({
  trace,
  onSelect,
}: {
  trace: Row[];
  onSelect: (row: Row) => void;
}) {
  const series = useMemo(() => recordedSeries(trace), [trace]);
  if (!series.length) return null;
  return (
    <div className="recorded-charts">
      {series.map((item) => (
        <RecordedChart key={item.kind} series={item} onSelect={onSelect} />
      ))}
      {!series.some((item) => item.kind === "pnl") && (
        <p className="chart-unavailable">
          trace 未提供至少两个同口径 PnL
          观测点，不绘制收益曲线。终值仍读取上方原报告。
        </p>
      )}
    </div>
  );
}
