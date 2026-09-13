import { useEffect, useMemo, useRef, useState } from "react";
import {
  display,
  getMarketCandles,
  getMarketFills,
  getMarketMetadata,
  getMarketOrder,
  stamp,
} from "./api";
import type {
  BaselineResult,
  MarketCandles,
  MarketFill,
  MarketFills,
  MarketMetadata,
  MarketOrder,
} from "./api";
import {
  alignedWindow,
  boundedWindow,
  candleBucket,
  CANDLE_INTERVALS,
  groupMarketFills,
  selectFillLabels,
  visibleInventoryFills,
  zoomWindow,
} from "./traceSeries";
import type { CandleInterval, TimeWindow } from "./traceSeries";

const errorMessage = (error: unknown) =>
  error instanceof Error ? error.message : "读取失败";
const exact = (value: unknown) =>
  value === null || value === undefined ? "未记录" : display(value);
const milliseconds = (value: number | null) =>
  value === null ? "未记录" : `${new Date(value).toISOString()} · ${value} ms`;
const MAX_CANDLES = 1000;

function CandleChart({
  candles,
  fills,
  interval,
  window,
  selectedId,
  onSelect,
  onPan,
  onZoom,
}: {
  candles: MarketCandles;
  fills: MarketFill[];
  interval: CandleInterval;
  window: TimeWindow;
  selectedId: string | null;
  onSelect: (fill: MarketFill) => void;
  onPan: (fraction: number) => void;
  onZoom: (factor: number) => void;
}) {
  const drag = useRef<number | null>(null);
  const [dragging, setDragging] = useState(false);
  const [hoveredLabel, setHoveredLabel] = useState<string | null>(null);
  const groups = useMemo(
    () => groupMarketFills(fills, interval),
    [fills, interval],
  );
  const prices = [
    ...candles.items.flatMap((c) => [c.low, c.high]),
    ...fills.map((f) => f.price),
  ];
  const minimum = Math.min(...prices),
    maximum = Math.max(...prices);
  const pad = Math.max((maximum - minimum) * 0.08, maximum * 0.00001, 0.01);
  const low = minimum - pad,
    high = maximum + pad;
  const x = (ts: number) =>
    76 + ((ts - window.start) / (window.end - window.start)) * 836;
  const y = (price: number) => 312 - ((price - low) / (high - low)) * 276;
  const labelId = (fill: MarketFill) =>
    `${candleBucket(fill.fill_ts_ms, interval)}:${fill.side}`;
  const labels = [...groups].flatMap(([bucket, members]) =>
    (["BUY", "SELL"] as const).flatMap((side) => {
      const sameSide = members.filter((fill) => fill.side === side);
      if (!sameSide.length) return [];
      const text = `${side === "BUY" ? "B" : "S"}${sameSide.length > 1 ? ` ×${sameSide.length}` : ""}`;
      const width = Math.max(29, text.length * 6 + 16);
      const px = Math.max(
        96,
        Math.min(930 - width, x(bucket + CANDLE_INTERVALS[interval] / 2)),
      );
      return [
        {
          id: `${bucket}:${side}`,
          side,
          left: px - 18,
          width,
          text,
          px,
          py: Math.max(
            28,
            Math.min(325, y(sameSide[0].price) + (side === "BUY" ? 22 : -22)),
          ),
          first: sameSide[0],
          count: sameSide.length,
          total: members.length,
        },
      ];
    }),
  );
  const selectedFill = fills.find((fill) => fill.id === selectedId);
  const priorityLabels = [
    hoveredLabel,
    selectedFill ? labelId(selectedFill) : null,
  ].filter((id): id is string => id !== null);
  const shownLabels = selectFillLabels(labels, priorityLabels);
  const omittedLabels = labels.length - shownLabels.size;
  const width = Math.max(
    1,
    Math.min(
      16,
      (CANDLE_INTERVALS[interval] / (window.end - window.start)) * 836 * 0.7,
    ),
  );
  const inventory = visibleInventoryFills(fills, window);
  const unknownInventoryClock = fills.filter(
    (f) => f.inventory_after !== null && f.visible_ts_ms === null,
  ).length;
  const inventoryMax = Math.max(
    0.000001,
    ...inventory.map((f) => Math.abs(f.inventory_after!)),
  );
  const iy = (value: number) => 408 - (value / inventoryMax) * 28;
  const axes = [0, 1, 2, 3, 4];
  return (
    <div className={`market-chart ${dragging ? "dragging" : ""}`}>
      <svg
        viewBox="0 0 960 492"
        tabIndex={0}
        role="group"
        aria-label="真实市场 K 线与模拟 BUY SELL 成交；方向键平移，加减键缩放"
        onKeyDown={(event) => {
          if (event.key === "ArrowLeft" || event.key === "ArrowRight") {
            event.preventDefault();
            onPan(event.key === "ArrowLeft" ? -0.5 : 0.5);
          } else if (["+", "=", "-"].includes(event.key)) {
            event.preventDefault();
            onZoom(event.key === "-" ? 2 : 0.5);
          }
        }}
        onPointerDown={(event) => {
          if (event.button !== 0) return;
          drag.current = event.clientX;
          setDragging(true);
          event.currentTarget.setPointerCapture(event.pointerId);
        }}
        onPointerUp={(event) => {
          if (drag.current !== null) {
            const dx = event.clientX - drag.current;
            if (Math.abs(dx) > 8)
              onPan(-dx / event.currentTarget.getBoundingClientRect().width);
          }
          drag.current = null;
          setDragging(false);
        }}
        onPointerCancel={() => {
          drag.current = null;
          setDragging(false);
        }}
      >
        <defs>
          <clipPath id="market-price-clip">
            <rect x="75" y="18" width="838" height="320" />
          </clipPath>
          <clipPath id="market-inventory-clip">
            <rect x="75" y="360" width="838" height="90" />
          </clipPath>
        </defs>
        {axes.map((i) => (
          <g key={i}>
            <line
              className="chart-grid"
              x1="76"
              x2="912"
              y1={36 + i * 69}
              y2={36 + i * 69}
            />
            <text x="65" y={40 + i * 69} textAnchor="end">
              {(high - ((high - low) * i) / 4).toFixed(2)}
            </text>
          </g>
        ))}
        <text x="76" y="16">
          市场价格 / USDC
        </text>
        <g clipPath="url(#market-price-clip)">
          {candles.items.map((c) => (
            <g
              key={c.time_ms}
              className={c.close >= c.open ? "candle-up" : "candle-down"}
            >
              <title>
                {new Date(c.time_ms).toISOString()} · O {c.open} H {c.high} L{" "}
                {c.low} C {c.close} · 源记录 {c.source_rows}
              </title>
              <line
                x1={x(c.time_ms + CANDLE_INTERVALS[interval] / 2)}
                x2={x(c.time_ms + CANDLE_INTERVALS[interval] / 2)}
                y1={y(c.high)}
                y2={y(c.low)}
              />
              <rect
                x={x(c.time_ms + CANDLE_INTERVALS[interval] / 2) - width / 2}
                y={y(Math.max(c.open, c.close))}
                width={width}
                height={Math.max(1, Math.abs(y(c.open) - y(c.close)))}
              />
            </g>
          ))}
          {fills.map((f) => (
            <circle
              key={f.id}
              cx={x(f.fill_ts_ms)}
              cy={y(f.price)}
              r="3"
              className={`fill-point ${f.side === "BUY" ? "fill-buy" : "fill-sell"}`}
              role="button"
              tabIndex={0}
              aria-label={`查看 ${f.side} 成交 ${f.id}，${milliseconds(f.fill_ts_ms)}，价格 ${f.price}`}
              onPointerEnter={() => setHoveredLabel(labelId(f))}
              onPointerLeave={() => setHoveredLabel(null)}
              onFocus={() => setHoveredLabel(labelId(f))}
              onBlur={() => setHoveredLabel(null)}
              onPointerDown={(e) => e.stopPropagation()}
              onClick={() => onSelect(f)}
              onKeyDown={(e) => {
                if (e.key === "Enter" || e.key === " ") {
                  e.preventDefault();
                  onSelect(f);
                }
              }}
            >
              <title>
                {f.side} {milliseconds(f.fill_ts_ms)} · {f.price} × {f.quantity}{" "}
                · {f.id}
              </title>
            </circle>
          ))}
          {labels
            .filter((label) => shownLabels.has(label.id))
            .map((label) => (
              <g
                key={label.id}
                data-label-id={label.id}
                className={`fill-marker ${label.side === "BUY" ? "fill-buy" : "fill-sell"}`}
                role="button"
                tabIndex={0}
                aria-label={`${label.side} ${label.count} 笔，查看该 K 线全部 ${label.total} 笔成交`}
                onPointerEnter={() => setHoveredLabel(label.id)}
                onPointerLeave={() => setHoveredLabel(null)}
                onPointerDown={(e) => e.stopPropagation()}
                onClick={() => onSelect(label.first)}
                onKeyDown={(e) => {
                  if (e.key === "Enter" || e.key === " ") {
                    e.preventDefault();
                    onSelect(label.first);
                  }
                }}
              >
                <rect
                  x={label.left}
                  y={label.py - 10}
                  width={label.width}
                  height="18"
                  rx="4"
                />
                <text x={label.px - 10} y={label.py + 3}>
                  {label.text}
                </text>
              </g>
            ))}
        </g>
        <line className="chart-grid" x1="76" x2="912" y1="350" y2="350" />
        <text x="76" y="365">
          成交后库存 / BTC · 本地可见时间，不推断跨时刻状态
        </text>
        <line className="chart-grid" x1="76" x2="912" y1="408" y2="408" />
        <text x="65" y="411" textAnchor="end">
          0
        </text>
        <g clipPath="url(#market-inventory-clip)">
          {inventory.map((f, index) =>
            index > 0 &&
            f.visible_ts_ms === inventory[index - 1].visible_ts_ms ? (
              <line
                key={`step:${f.id}`}
                className="inventory-step"
                x1={x(f.visible_ts_ms!)}
                x2={x(f.visible_ts_ms!)}
                y1={iy(inventory[index - 1].inventory_after!)}
                y2={iy(f.inventory_after!)}
              />
            ) : null,
          )}
          {inventory.map((f) => (
            <circle
              key={f.id}
              cx={x(f.visible_ts_ms!)}
              cy={iy(f.inventory_after!)}
              r="3"
              className="inventory-point"
              onPointerDown={(e) => e.stopPropagation()}
              onClick={() => onSelect(f)}
            >
              <title>
                本地可见 {milliseconds(f.visible_ts_ms)} · 顺序 #
                {f.fill_sequence} · 库存 {f.inventory_after} BTC
              </title>
            </circle>
          ))}
        </g>
        {!inventory.length && (
          <text x="400" y="414">
            窗口内没有已记录的成交后库存观测
          </text>
        )}
        {[0, 1, 2, 3, 4].map((i) => (
          <text
            key={i}
            x={76 + i * 209}
            y="471"
            textAnchor={i === 0 ? "start" : i === 4 ? "end" : "middle"}
          >
            {stamp(window.start + ((window.end - window.start) * i) / 4)}
            {i === 4 ? " UTC" : ""}
          </text>
        ))}
      </svg>
      <p className="chart-help">
        拖动后松开以载入相邻窗口 · 方向键平移，＋ / － 缩放 · B / S
        仅表示买卖方向，不表示开平仓 · 小圆点保留每笔实际执行价格与时间
        {omittedLabels > 0
          ? ` · 密集窗口省略 ${omittedLabels} 个 B/S 文字标签（成交点未省略）；可放大、悬停或点击成交点`
          : ""}
        {unknownInventoryClock > 0
          ? ` · ${unknownInventoryClock} 笔库存缺本地可见时钟，未绘制`
          : ""}
      </p>
    </div>
  );
}

function FillDetails({
  fill,
  resultId,
}: {
  fill: MarketFill;
  resultId: string;
}) {
  const [order, setOrder] = useState<MarketOrder | null>(null);
  const [error, setError] = useState("");
  useEffect(() => {
    setOrder(null);
    setError("");
    if (!fill.order_id) return;
    const controller = new AbortController();
    getMarketOrder(resultId, fill.order_id, controller.signal)
      .then((value) => {
        if (!controller.signal.aborted) setOrder(value);
      })
      .catch((e) => {
        if (!controller.signal.aborted) setError(errorMessage(e));
      });
    return () => controller.abort();
  }, [resultId, fill.order_id]);
  return (
    <aside className="fill-details">
      <span className="eyebrow">SIMULATED FILL / ORIGINAL VALUES</span>
      <h3>
        <span className={fill.side === "BUY" ? "buy" : "sell"}>
          {fill.side}
        </span>{" "}
        · 成交 #{fill.fill_sequence}
      </h3>
      <dl>
        {[
          ["发生时间 / UTC", milliseconds(fill.fill_ts_ms)],
          ["私有可见时间 / UTC", milliseconds(fill.visible_ts_ms)],
          ["执行价 / USDC", exact(fill.price)],
          ["数量 / BTC", exact(fill.quantity)],
          ["原订单 ID", exact(fill.source_order_id)],
          ["分段内订单标识", exact(fill.order_id)],
          ["成交标识", fill.id],
          ["成交前库存 / BTC", exact(fill.inventory_before)],
          ["成交后库存 / BTC", exact(fill.inventory_after)],
          [
            "已记录费用",
            `${exact(fill.fee)} ${fill.fee_asset ?? "（币种未记录）"}`,
          ],
          ["Campaign", exact(fill.campaign_id)],
          ["提交时 Campaign（非最终归属）", exact(fill.campaign_id_at_submit)],
          ["触发市场成交价（非记账执行价）", exact(fill.fill_trade_price)],
        ].map(([label, value]) => (
          <div key={label}>
            <dt>{label}</dt>
            <dd className="mono">{value}</dd>
          </div>
        ))}
      </dl>
      <div className="notice">
        模拟成交，不是实盘 execution。费用正数为成本、负数为返佣，已在交易 PnL
        中计算，不再次扣除。库存是原始本地成交回调记录，不能冒充更早可见的策略状态。未提供连续
        PnL / 完整有效挂单生命周期，不生成对应曲线或挂单带。
      </div>
      {error && (
        <p className="notice error" role="alert">
          订单快照不可读：{error}
        </p>
      )}
      {order && (
        <details open>
          <summary>订单成交快照 · 生命周期不完整</summary>
          <dl>
            {[
              ["提交", order.submit_ts_ms],
              ["激活", order.activate_ts_ms],
              ["新订单 ACK", order.new_ack_ts_ms],
              ["撤单请求", order.cancel_request_ts_ms],
              ["撤单生效", order.cancel_effective_ts_ms],
              ["撤单 ACK", order.cancel_ack_ts_ms],
            ].map(([label, value]) => (
              <div key={label}>
                <dt>{label} / UTC</dt>
                <dd className="mono">{milliseconds(value as number | null)}</dd>
              </div>
            ))}
          </dl>
          <p className="muted">
            订单记录 {order.fill_count} 笔成交 · 已记录成交量{" "}
            {exact(order.filled_quantity)} BTC；
            {order.truncated ? "订单明细已截断，不代表完整订单成交。" : ""}仅
            fill trace 快照，缺失字段不补造。
          </p>
        </details>
      )}
    </aside>
  );
}

export function TradeReplay({
  results,
  resultId,
  onResultId,
  authVersion,
  requestedDay,
}: {
  results: BaselineResult[];
  resultId: string;
  onResultId: (id: string) => void;
  authVersion: number;
  requestedDay?: string;
}) {
  const [meta, setMeta] = useState<MarketMetadata | null>(null);
  const [segment, setSegment] = useState("");
  const [day, setDay] = useState("");
  const [interval, setInterval] = useState<CandleInterval>("1m");
  const [window, setWindow] = useState<TimeWindow>({ start: 0, end: 0 });
  const [candles, setCandles] = useState<MarketCandles | null>(null);
  const [page, setPage] = useState<MarketFills | null>(null);
  const [fills, setFills] = useState<MarketFill[]>([]);
  const [selected, setSelected] = useState<MarketFill | null>(null);
  const [bucket, setBucket] = useState<number | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const panel = useRef<HTMLElement>(null);
  const [loadingHeight, setLoadingHeight] = useState<number>();
  const generation = useRef(0);
  const bounds = useMemo(() => {
    const start = Date.parse(`${day}T00:00:00Z`);
    return { start, end: start + 86400000 };
  }, [day]);
  const selectedSegment = meta?.segments.find(
    (s) => String(s.index) === segment,
  );
  useEffect(() => {
    setMeta(null);
    setDay("");
    setError("");
    setCandles(null);
    setFills([]);
    if (!resultId) return;
    const controller = new AbortController();
    getMarketMetadata(resultId, controller.signal)
      .then((value) => {
        if (controller.signal.aborted) return;
        setMeta(value);
        const first =
          value.segments.find(
            (s) => requestedDay && s.days.includes(requestedDay),
          ) ?? value.segments[0];
        setSegment(first ? String(first.index) : "");
        setDay(
          first
            ? requestedDay && first.days.includes(requestedDay)
              ? requestedDay
              : first.days[0]
            : "",
        );
      })
      .catch((e) => {
        if (!controller.signal.aborted) setError(errorMessage(e));
      });
    return () => controller.abort();
  }, [resultId, authVersion, requestedDay]);
  useEffect(() => {
    if (day)
      setWindow(
        boundedWindow(bounds.start, CANDLE_INTERVALS[interval] * 240, bounds),
      );
  }, [day, bounds]);
  useEffect(() => {
    const version = ++generation.current;
    setCandles(null);
    setFills([]);
    setPage(null);
    setSelected(null);
    setBucket(null);
    setLoadingMore(false);
    if (
      !meta ||
      !day ||
      !Number.isFinite(bounds.start) ||
      window.start < bounds.start ||
      window.end > bounds.end ||
      window.end <= window.start
    ) {
      setLoading(false);
      return;
    }
    const controller = new AbortController();
    // Clearing the old chart/list must not shrink the document and clamp its
    // scroll offset while the new window is in flight. Retain geometry only.
    setLoadingHeight(panel.current?.getBoundingClientRect().height);
    setLoading(true);
    setError("");
    const query = new URLSearchParams({
      start_ms: String(Math.floor(window.start)),
      end_ms: String(Math.ceil(window.end)),
    });
    Promise.all([
      getMarketCandles(
        resultId,
        new URLSearchParams({
          ...Object.fromEntries(query),
          interval_s: String(CANDLE_INTERVALS[interval] / 1000),
        }),
        controller.signal,
      ).then((value) => {
        if (version === generation.current) setCandles(value);
      }),
      getMarketFills(
        resultId,
        new URLSearchParams({ ...Object.fromEntries(query), limit: "1000" }),
        controller.signal,
      ).then((value) => {
        if (version === generation.current) {
          setPage(value);
          setFills(value.items);
        }
      }),
    ])
      .catch((e) => {
        if (!controller.signal.aborted) setError(errorMessage(e));
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
  }, [resultId, meta, day, bounds, window, interval, authVersion]);
  const maxWindow = Math.min(
    meta?.max_window_ms ?? 86400000,
    Math.min(MAX_CANDLES, meta?.max_candles ?? MAX_CANDLES) *
      CANDLE_INTERVALS[interval],
  );
  const move = (fraction: number) =>
    setWindow((current) =>
      alignedWindow(
        boundedWindow(
          current.start + (current.end - current.start) * fraction,
          current.end - current.start,
          bounds,
        ),
        interval,
        bounds,
      ),
    );
  const zoom = (factor: number) =>
    setWindow((current) =>
      alignedWindow(
        zoomWindow(
          current,
          factor,
          bounds,
          CANDLE_INTERVALS[interval] * 20,
          maxWindow,
        ),
        interval,
        bounds,
      ),
    );
  const visible = fills.filter(
    (f) =>
      String(f.segment_index) === segment &&
      (bucket === null || candleBucket(f.fill_ts_ms, interval) === bucket),
  );
  const select = (fill: MarketFill) => {
    setSelected(fill);
    setBucket(candleBucket(fill.fill_ts_ms, interval));
  };
  const loadMore = async () => {
    if (!page?.next_cursor || loadingMore) return;
    const version = generation.current;
    setLoadingMore(true);
    try {
      const next = await getMarketFills(
        resultId,
        new URLSearchParams({
          start_ms: String(Math.floor(window.start)),
          end_ms: String(Math.ceil(window.end)),
          limit: "1000",
          cursor: page.next_cursor,
        }),
      );
      if (version === generation.current) {
        setPage(next);
        setFills((current) => [...current, ...next.items]);
      }
    } catch (e) {
      if (version === generation.current) setError(errorMessage(e));
    } finally {
      if (version === generation.current) setLoadingMore(false);
    }
  };
  return (
    <section
      ref={panel}
      className="panel trade-replay"
      style={{ minHeight: loading ? loadingHeight : undefined }}
      aria-busy={loading}
    >
      <div className="result-header">
        <div>
          <span className="eyebrow">MARKET CANDLES + SIMULATED FILLS</span>
          <h2>回到每一笔成交发生的位置。</h2>
        </div>
        <span className="tag real-tag">真实行情 · 历史模拟成交 · 只读</span>
      </div>
      <div className="result-body">
        <div className="replay-controls">
          <label>
            已导入真实 B0
            <select
              aria-label="已导入真实 B0"
              value={resultId}
              onChange={(e) => onResultId(e.target.value)}
            >
              <option value="">选择结果</option>
              {results.map((result) => (
                <option key={result.id} value={result.id}>
                  {result.name}
                </option>
              ))}
            </select>
          </label>
          <label>
            连续分段
            <select
              aria-label="连续分段"
              value={segment}
              onChange={(e) => {
                setSegment(e.target.value);
                setDay(
                  meta?.segments.find((s) => String(s.index) === e.target.value)
                    ?.days[0] ?? "",
                );
              }}
            >
              {meta?.segments.map((s) => (
                <option key={s.index} value={s.index}>
                  #{s.index} · {s.start_day} → {s.end_day} · {s.source}
                </option>
              ))}
            </select>
          </label>
          <label>
            日期 / UTC
            <select
              aria-label="日期 / UTC"
              value={day}
              onChange={(e) => setDay(e.target.value)}
            >
              {selectedSegment?.days.map((d) => <option key={d}>{d}</option>)}
            </select>
          </label>
          <label>
            K 线周期
            <select
              aria-label="K 线周期"
              value={interval}
              onChange={(e) => {
                const next = e.target.value as CandleInterval;
                setInterval(next);
                setWindow((current) =>
                  alignedWindow(
                    boundedWindow(
                      (current.start + current.end) / 2 -
                        CANDLE_INTERVALS[next] * 120,
                      CANDLE_INTERVALS[next] * 240,
                      bounds,
                    ),
                    next,
                    bounds,
                  ),
                );
              }}
            >
              {Object.entries(CANDLE_INTERVALS).map(([key, ms]) => (
                <option
                  key={key}
                  value={key}
                  disabled={
                    meta ? !meta.intervals_s.includes(ms / 1000) : false
                  }
                >
                  {key}
                </option>
              ))}
            </select>
          </label>
        </div>
        {requestedDay && (
          <p className="notice">
            来自质量日历：{requestedDay}
            。请核对结果、分段和市场；此跳转不建立当前质量记录与历史回放源的身份绑定。
            {day !== requestedDay
              ? "所选结果不含该日期，当前显示其可用日期。"
              : ""}
          </p>
        )}
        {meta && (
          <div className="notice market-binding-notice">
            <strong>历史市场上下文 · 未验证为原回放精确输入</strong>
            <br />
            {meta.symbol} · 行情源 {meta.source.kind} / {meta.source.location} ·{" "}
            {meta.source.connected
              ? "已登记市场来源（窗口可用性以下方响应为准）"
              : "当前来源未连接 / 不可读取"}{" "}
            · 不代表执行源节点当前在线
          </div>
        )}
        <div className="replay-window-tools">
          <button
            className="button secondary small"
            disabled={!day || loading || window.start <= bounds.start}
            onClick={() => move(-0.5)}
          >
            ← 较早
          </button>
          <button
            className="button secondary small"
            disabled={!day || loading || window.end >= bounds.end}
            onClick={() => move(0.5)}
          >
            较晚 →
          </button>
          <button
            className="button secondary small"
            disabled={
              !day ||
              loading ||
              window.end - window.start <= CANDLE_INTERVALS[interval] * 20
            }
            onClick={() => zoom(0.5)}
          >
            ＋ 放大
          </button>
          <button
            className="button secondary small"
            disabled={
              !day ||
              loading ||
              window.end - window.start >= Math.min(maxWindow, 86400000)
            }
            onClick={() => zoom(2)}
          >
            － 缩小
          </button>
          <span className="mono">
            {day
              ? `${stamp(window.start, true)} → ${stamp(window.end, true)} UTC · 右端不含`
              : "选择结果以载入窗口"}
          </span>
        </div>
        {error && (
          <div className="notice error" role="alert">
            {error}。未用合成行情替代；可更换窗口重试。
          </div>
        )}
        {loading && (
          <div className="notice" role="status">
            正在读取有界行情 / 成交窗口…
          </div>
        )}
        {!resultId && (
          <div className="empty">
            <h3>尚无所选真实 B0</h3>
            <p>请先选择已导入结果。这里不会创建回放任务。</p>
          </div>
        )}
        {meta?.status === "unavailable" && (
          <div className="notice">
            {meta.reason || "此结果未绑定可读取的市场数据。"}
          </div>
        )}
        {candles && (
          <p className="chart-coverage">
            {candles.status === "available" ? (
              <>
                市场 K 线 {candles.count} 根 · 无 bar 秒 {candles.gaps}
                （原因未区分，不直接判定源缺口） ·{" "}
                {candles.truncated ? "K 线已截断，非完整窗口" : "K 线未截断"}
              </>
            ) : (
              "市场 K 线不可用 · 覆盖与无 bar 秒数未知"
            )}{" "}
            · 成交已加载 {fills.length} 笔
            {!page || page.status !== "available"
              ? "（尚未取得成交完整性）"
              : page.next_cursor || page.truncated
                ? "（分页未完整）"
                : "（该请求已完整）"}
          </p>
        )}
        {candles?.items.length ? (
          <CandleChart
            candles={candles}
            fills={fills.filter((f) => String(f.segment_index) === segment)}
            interval={interval}
            window={window}
            selectedId={selected?.id ?? null}
            onSelect={select}
            onPan={move}
            onZoom={zoom}
          />
        ) : (
          !loading &&
          meta && (
            <div className="empty">
              <h3>该窗口没有可绘制的市场 K 线</h3>
              <p>
                {candles?.reason ||
                  "未返回市场 OHLC；成交记录仍可在下方查看。不会由策略成交生成蜡烛或填补空桶。"}
              </p>
            </div>
          )
        )}
        {page?.status === "unavailable" && (
          <div className="notice">
            成交不可用：{page.reason || "未绑定完整可读 trace。"}
          </div>
        )}
        <div className="fill-list-header">
          <h3>模拟成交 · {visible.length} 笔</h3>
          {bucket !== null && (
            <button className="filter-chip" onClick={() => setBucket(null)}>
              K 线 {stamp(bucket)} UTC · 清除筛选 ×
            </button>
          )}
          <span className="muted">同一根 K 线、同一毫秒的多笔成交逐条保留</span>
        </div>
        <div className="market-detail-layout">
          <div
            className="table-scroll fill-table"
            tabIndex={0}
            role="region"
            aria-label="窗口内模拟成交明细"
          >
            <table>
              <thead>
                <tr>
                  <th>发生时间 / UTC</th>
                  <th>方向</th>
                  <th>执行价 / USDC</th>
                  <th>数量 / BTC</th>
                  <th>原订单 ID</th>
                  <th>库存前 → 后 / BTC</th>
                  <th>费用</th>
                </tr>
              </thead>
              <tbody>
                {visible.map((fill) => (
                  <tr
                    key={fill.id}
                    className={selected?.id === fill.id ? "selected-row" : ""}
                  >
                    <td>
                      <button
                        className="text-button mono"
                        onClick={() => setSelected(fill)}
                      >
                        {new Date(fill.fill_ts_ms).toISOString()}
                      </button>
                    </td>
                    <td className={fill.side === "BUY" ? "buy" : "sell"}>
                      {fill.side}
                    </td>
                    <td className="mono">{fill.price}</td>
                    <td className="mono">{fill.quantity}</td>
                    <td className="mono">{exact(fill.source_order_id)}</td>
                    <td className="mono">
                      {exact(fill.inventory_before)} →{" "}
                      {exact(fill.inventory_after)}
                    </td>
                    <td className="mono">
                      {exact(fill.fee)} {fill.fee_asset ?? ""}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            {!visible.length && !loading && (
              <p className="empty">
                窗口 / 筛选内没有已加载成交。零成交不等于市场缺数据。
              </p>
            )}
          </div>
          {selected && (
            <FillDetails
              key={selected.id}
              fill={selected}
              resultId={resultId}
            />
          )}
        </div>
        {(page?.next_cursor || page?.truncated) && (
          <div className="notice">
            成交列表尚未完整；图上仅叠加已加载的 {fills.length} 笔。
            {page.next_cursor ? (
              <button
                className="button secondary small"
                disabled={loadingMore}
                onClick={() => void loadMore()}
              >
                {loadingMore ? "正在加载…" : "继续加载成交"}
              </button>
            ) : (
              "来源已截断，无法由此列表证明完整路径。"
            )}
          </div>
        )}
        <p className="muted replay-provenance">
          没有连续 PnL
          观测，不绘制收益线；只有成交快照，不假设报单后始终有效。更多成交会改变此图的观测覆盖，不改变原始回放结果。
        </p>
      </div>
    </section>
  );
}
