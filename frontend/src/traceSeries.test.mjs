import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import {
  recordedSeries,
  utcDays,
  boundedWindow,
  zoomWindow,
  candleBucket,
  alignedWindow,
  groupMarketFills,
  visibleInventoryFills,
  filterQualityDays,
  selectFillLabels,
  qualityAudit,
  qualityReason,
  qualityReplica,
  qualityTask,
} from "./traceSeries.ts";
import {
  getMarketCandles,
  getMarketFills,
  getQualityReport,
  refreshQualityInventory,
} from "./api.ts";

test("public synthetic fixture plots only its three recorded post-fill inventories", () => {
  const path = new URL(
    "../../narrowgate/fixtures/replay_demo/reference/trace.jsonl",
    import.meta.url,
  );
  const trace = readFileSync(path, "utf8")
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line));
  const series = recordedSeries(trace);
  assert.equal(series.length, 1);
  assert.equal(series[0].source, "fills.inventory_after_btc");
  assert.deepEqual(
    series[0].points.map((point) => point.original),
    ["0.00400000", "0.01000000", "0.00000000"],
  );
  assert.equal(series[0].points[0].row, trace[4]);
});

test("missing fields, empty values and single points never become a curve", () => {
  assert.deepEqual(
    recordedSeries([
      { ts_ms: 1, inventory_btc: null },
      { ts_ms: 2, inventory_btc: "" },
      { ts_ms: 3, inventory_btc: false },
      { ts_ms: 4, inventory_btc: 1 },
    ]),
    [],
  );
});

test("equity is not relabeled PnL and fees are not summed", () => {
  assert.deepEqual(
    recordedSeries([
      { ts_ms: 1, terminal_equity_usdc: 1, fee_usdc: 2 },
      { ts_ms: 2, terminal_equity_usdc: 4, fee_usdc: 3 },
    ]),
    [],
  );
});

test("a series preserves zeros, negative values, precision text and record order", () => {
  const series = recordedSeries([
    { ts_ms: 3, cumulative_pnl_usdc: "0.0000" },
    { ts_ms: 5, cumulative_pnl_usdc: "-0.10000000" },
  ]);
  assert.equal(series[0].source, "cumulative_pnl_usdc");
  assert.deepEqual(
    series[0].points.map((point) => point.original),
    ["0.0000", "-0.10000000"],
  );
});

test("different PnL field semantics are not spliced together", () => {
  assert.deepEqual(
    recordedSeries([
      { ts_ms: 1, cumulative_pnl_usdc: 1 },
      { ts_ms: 2, terminal_pnl_usdc: 3 },
    ]),
    [],
  );
});

test("non-finite values and missing timestamps are excluded, not repaired", () => {
  assert.deepEqual(
    recordedSeries([
      { inventory_btc: 1 },
      { ts_ms: 2, inventory_btc: Infinity },
      { ts_ms: 3, inventory_btc: "bad" },
      { ts_ms: 4, inventory_btc: 0 },
    ]),
    [],
  );
});

test("quality calendar retains absent head, middle and tail days in UTC", () => {
  assert.deepEqual(utcDays("2026-04-29", "2026-05-02"), [
    "2026-04-29",
    "2026-04-30",
    "2026-05-01",
    "2026-05-02",
  ]);
  assert.deepEqual(utcDays("2026-02-30", "2026-03-02"), []);
  assert.deepEqual(utcDays("2026-05-02", "2026-05-01"), []);
});

test("pan and zoom clamp to one day and a bounded candle count", () => {
  const bounds = { start: 100000, end: 200000 };
  assert.deepEqual(boundedWindow(190000, 20000, bounds), {
    start: 180000,
    end: 200000,
  });
  assert.deepEqual(boundedWindow(0, 20000, bounds), {
    start: 100000,
    end: 120000,
  });
  assert.deepEqual(
    zoomWindow({ start: 100000, end: 120000 }, 10, bounds, 1000, 50000),
    { start: 100000, end: 150000 },
  );
  assert.deepEqual(
    zoomWindow({ start: 100000, end: 120000 }, 0.01, bounds, 1000, 50000),
    { start: 109500, end: 110500 },
  );
});

test("candle grouping preserves an exact millisecond boundary", () => {
  assert.equal(candleBucket(1776816059999, "1m"), 1776816000000);
  assert.equal(candleBucket(1776816060000, "1m"), 1776816060000);
  assert.equal(candleBucket(1776816004999, "5s"), 1776816000000);
});

test("arbitrary drag distances still request whole candles within UTC bounds", () => {
  const bounds = { start: 1776816000000, end: 1776902400000 };
  for (const interval of ["1s", "5s", "1m", "5m"]) {
    const step = { "1s": 1000, "5s": 5000, "1m": 60000, "5m": 300000 }[
      interval
    ];
    const aligned = alignedWindow(
      { start: bounds.start + 73234, end: bounds.start + 912345 },
      interval,
      bounds,
    );
    assert.equal(aligned.start % step, 0);
    assert.equal(aligned.end % step, 0);
    assert.ok(aligned.start >= bounds.start && aligned.end <= bounds.end);
  }
});

test("same-candle and same-millisecond fills retain both sides, price and original identity", () => {
  const fills = [
    {
      id: "segment-0-fill-1",
      fill_ts_ms: 1776816000555,
      side: "BUY",
      price: 10,
      inventory_after: null,
    },
    {
      id: "segment-0-fill-2",
      fill_ts_ms: 1776816000555,
      side: "BUY",
      price: 11,
      inventory_after: 0,
    },
    {
      id: "segment-0-fill-3",
      fill_ts_ms: 1776816004999,
      side: "SELL",
      price: 12,
      inventory_after: -1,
    },
  ];
  const grouped = groupMarketFills(fills, "1m");
  assert.equal(grouped.size, 1);
  assert.deepEqual(grouped.get(1776816000000), fills);
  assert.equal(grouped.get(1776816000000)[0], fills[0]);
  assert.equal(fills[0].inventory_after, null);
});

test("inventory uses local visibility order, never an earlier physical fill time", () => {
  const rows = [
    {
      id: "later-sequence",
      fill_sequence: 2,
      fill_ts_ms: 1001,
      visible_ts_ms: 1500,
      inventory_after: 0.002,
    },
    {
      id: "first-sequence",
      fill_sequence: 1,
      fill_ts_ms: 1000,
      visible_ts_ms: 1500,
      inventory_after: 0.001,
    },
    {
      id: "unknown-clock",
      fill_sequence: 3,
      fill_ts_ms: 1002,
      visible_ts_ms: null,
      inventory_after: 0.003,
    },
    {
      id: "visible-after-window",
      fill_sequence: 4,
      fill_ts_ms: 1900,
      visible_ts_ms: 2001,
      inventory_after: 0,
    },
  ];
  assert.deepEqual(
    visibleInventoryFills(rows, { start: 1000, end: 2000 }).map(
      (row) => row.id,
    ),
    ["first-sequence", "later-sequence"],
  );
  assert.equal(rows[0].id, "later-sequence");
});

test("market API rejects fabricated OHLC ranges and wrong window identities", async (t) => {
  const query = new URLSearchParams({
    start_ms: "1000",
    end_ms: "2000",
    interval_s: "1",
  });
  let payload = {
    result_id: "public-fixture",
    start_ms: 1000,
    end_ms: 2000,
    count: 1,
    items: [{ time_ms: 1000, open: 10, high: 9, low: 8, close: 11 }],
  };
  t.mock.method(
    globalThis,
    "fetch",
    async () => new Response(JSON.stringify(payload)),
  );
  await assert.rejects(getMarketCandles("public-fixture", query), /OHLC/);
  payload = { ...payload, start_ms: 0, items: [], count: 0 };
  await assert.rejects(getMarketCandles("public-fixture", query), /OHLC/);
});

test("fill API preserves null accounting and pagination; synthetic results cannot enter", async (t) => {
  const query = new URLSearchParams({ start_ms: "1000", end_ms: "2000" });
  const row = {
    id: "segment-0-fill-1",
    fill_ts_ms: 1555,
    side: "BUY",
    price: 10,
    quantity: 0.001,
    fee: null,
    inventory_after: null,
  };
  let payload = {
    result_id: "public-fixture",
    classification: "simulated_historical_fills",
    start_ms: 1000,
    end_ms: 2000,
    items: [row],
    count: 1,
    next_cursor: "opaque-next",
    truncated: true,
  };
  t.mock.method(
    globalThis,
    "fetch",
    async () => new Response(JSON.stringify(payload)),
  );
  const result = await getMarketFills("public-fixture", query);
  assert.equal(result.items[0].fee, null);
  assert.equal(result.next_cursor, "opaque-next");
  assert.equal(result.truncated, true);
  payload = { ...payload, classification: "synthetic_non_economic" };
  await assert.rejects(getMarketFills("public-fixture", query), /模拟成交/);
  payload = {
    ...payload,
    classification: "simulated_historical_fills",
    items: [row, row],
    count: 2,
  };
  await assert.rejects(getMarketFills("public-fixture", query), /模拟成交/);
});

test("quality API refuses a calendar that silently omits missing head/tail dates", async (t) => {
  const query = new URLSearchParams({
    start_day: "2026-04-20",
    end_day: "2026-04-22",
    node: "local",
  });
  let payload = {
    start_day: "2026-04-20",
    end_day: "2026-04-22",
    node: "local",
    items: [{ day: "2026-04-21", sources: [] }],
    limitations: [],
  };
  t.mock.method(
    globalThis,
    "fetch",
    async () => new Response(JSON.stringify(payload)),
  );
  await assert.rejects(getQualityReport(query), /质量记录/);
  payload = {
    ...payload,
    items: utcDays("2026-04-20", "2026-04-22").map((day) => ({
      day,
      sources: [],
    })),
  };
  assert.equal((await getQualityReport(query)).items.length, 3);
});

const noQualityFilters = {
  source: "",
  market: "",
  symbol: "",
  datasetId: "",
  problemOnly: false,
  missingReplicaOnly: false,
};
const qualityFixture = (changes = {}) => ({
  dataset_id: "good",
  source: "good-source",
  market: "perp",
  symbol: "BTCUSDC",
  availability: "present",
  check_status: "passed",
  audit_applicability: {
    status: "verified_snapshot",
    reason: "Existing recorded audit snapshot",
  },
  replica: { status: "verified" },
  intervals: [],
  ...changes,
});

test("raw and prepared calendars never mix absent audits with missing raw files", () => {
  const raw = qualityFixture({
    dataset_id: "provider",
    stage: "raw",
    check_status: "unchecked",
    replica: { status: "present_unverified" },
    audit_applicability: undefined,
  });
  const product = qualityFixture({
    dataset_id: "bars",
    stage: "processed",
    availability: "missing",
  });
  const days = [{ day: "2026-08-01", sources: [raw, product] }];
  assert.deepEqual(
    filterQualityDays(days, { ...noQualityFilters, stage: "raw" })[0].sources,
    [raw],
  );
  assert.equal(
    filterQualityDays(days, {
      ...noQualityFilters,
      stage: "raw",
      problemOnly: true,
    }).length,
    0,
  );
  assert.deepEqual(
    filterQualityDays(days, {
      ...noQualityFilters,
      stage: "processed",
      problemOnly: true,
    })[0].sources,
    [product],
  );
  assert.equal(
    filterQualityDays(days, { ...noQualityFilters, stage: "registered" })
      .length,
    0,
  );
  assert.equal(days[0].sources.length, 2);
});

test("historical audit success never becomes current task success without its current mapping", () => {
  const historical = qualityFixture({
    audit_applicability: undefined,
    task_usability: { candles: "passed", strict_replay: "passed" },
  });
  assert.equal(qualityTask(historical, "candles").state, "unknown");
  assert.equal(qualityTask(historical, "strict_replay").state, "unknown");
  assert.match(qualityAudit(historical).reason, /历史检查/);
  assert.equal(
    filterQualityDays([{ day: "2026-07-24", sources: [historical] }], {
      ...noQualityFilters,
      problemOnly: true,
    }).length,
    1,
  );
});

test("quality presentation keeps feature usability independent of native queue and funding scope", () => {
  const source = qualityFixture({
    stage: "processed",
    audit_applicability: {
      status: "recorded_content_audit_current_size_matched",
      reason: "recorded_content_audit_current_size_matched",
    },
    current_task_usability: {
      candles: "passed",
      feature_input: "passed",
      modeled_replay: "unknown",
      strict_replay: "unknown",
      funding_pnl: "not_applicable",
    },
    task_reasons: {
      feature_input: "recorded_content_audit_current_size_matched",
      strict_replay: "task_not_mapped_in_recorded_audit",
      funding_pnl: "source_not_applicable",
    },
  });
  assert.equal(qualityTask(source, "feature_input").state, "passed");
  assert.equal(qualityTask(source, "strict_replay").state, "unknown");
  assert.equal(qualityTask(source, "funding_pnl").state, "not_applicable");
  assert.match(qualityTask(source, "funding_pnl").reason, /不是下载失败/);
  assert.match(qualityAudit(source).label, /仅大小匹配/);
  assert.match(qualityAudit(source).reason, /没有重新校验内容/);
});

test("quality unknown reasons distinguish unobserved node, inaccessible mount and unchecked contents", () => {
  const remote = qualityFixture({
    replica: {
      status: "unknown",
      observation_reason: "remote_replica_not_observed",
    },
  });
  assert.match(qualityReplica(remote).label, /未观察/);
  assert.match(qualityReplica(remote).reason, /不会更新远端/);
  assert.match(
    qualityReason("local_inventory_root_unavailable"),
    /不据此判定文件缺失/,
  );
  assert.match(qualityReason("local_presence_only"), /待内容核验/);
  assert.match(qualityReason("no_recorded_audit"), /处理后检查/);
  assert.match(
    qualityAudit(
      qualityFixture({
        audit_applicability: {
          status: "changed_since_observation",
          reason: "local_files_changed_since_observation",
        },
      }),
    ).label,
    /待复核/,
  );
});

test("quality refresh sends only registered selectors, never paths or a replay instruction", async (t) => {
  const requests = [];
  const query = new URLSearchParams({
    start_day: "2026-07-24",
    end_day: "2026-07-24",
    dataset_id: "recorded-bars",
    node: "local",
    path: "/not-transmitted",
    command: "not-transmitted",
  });
  t.mock.method(globalThis, "fetch", async (url, options) => {
    requests.push({
      url,
      method: options.method,
      body: JSON.parse(options.body),
    });
    return new Response(
      JSON.stringify({
        start_day: "2026-07-24",
        end_day: "2026-07-24",
        node: "local",
        items: [{ day: "2026-07-24", sources: [] }],
        limitations: [],
        refresh: {
          status: "refreshed",
          observed_at: "2026-09-07T00:00:00Z",
          scope: "registered_local_metadata_only",
          reason: "local_presence_only",
        },
      }),
    );
  });
  const result = await refreshQualityInventory(query);
  assert.equal(result.refresh.status, "refreshed");
  assert.deepEqual(requests, [
    {
      url: "/api/data-quality/refresh",
      method: "POST",
      body: {
        start_day: "2026-07-24",
        end_day: "2026-07-24",
        dataset_id: "recorded-bars",
        node: "local",
      },
    },
  ]);
});

test("problem days are recalculated after source filters, excluding hidden failures", () => {
  const good = qualityFixture(),
    bad = qualityFixture({
      dataset_id: "bad",
      source: "bad-source",
      check_status: "failed",
    });
  const days = [{ day: "2026-07-24", sources: [good, bad], problem: true }];
  assert.equal(
    filterQualityDays(days, { ...noQualityFilters, problemOnly: true }).length,
    1,
  );
  assert.deepEqual(
    filterQualityDays(days, {
      ...noQualityFilters,
      source: "good-source",
      problemOnly: true,
    }),
    [],
  );
  const visible = filterQualityDays(days, {
    ...noQualityFilters,
    source: "good-source",
  });
  assert.equal(visible[0].problem, false);
  assert.deepEqual(visible[0].sources, [good]);
  assert.equal(days[0].sources.length, 2);
});

test("purpose filtering does not confuse unusable queue or unobserved replica with usable bars", () => {
  const source = qualityFixture({
    replica: { status: "unknown" },
    current_task_usability: {
      candles: "passed",
      strict_replay: "failed",
      funding_pnl: "not_applicable",
    },
  });
  const days = [{ day: "2026-07-24", sources: [source] }];
  const check = (task) =>
    filterQualityDays(days, { ...noQualityFilters, problemOnly: true, task });
  assert.equal(check("candles").length, 0);
  assert.equal(check("strict_replay").length, 1);
  assert.equal(check("feature_input").length, 1);
  assert.equal(check("funding_pnl").length, 0);
  assert.equal(check("").length, 1);
});

test("unfiltered calendar retains empty dates but active source filters do not leave blank days", () => {
  const days = [
    { day: "2026-07-24", sources: [], problem: false },
    { day: "2026-07-25", sources: [qualityFixture()], problem: false },
  ];
  const unfiltered = filterQualityDays(days, noQualityFilters);
  assert.equal(unfiltered.length, 2);
  assert.equal(unfiltered[0].problem, true);
  for (const filter of [
    { source: "absent" },
    { market: "spot" },
    { symbol: "ETHUSDC" },
    { datasetId: "absent" },
  ]) {
    assert.deepEqual(
      filterQualityDays(days, { ...noQualityFilters, ...filter }),
      [],
    );
  }
});

test("recorded gaps and invalid intervals remain problems even when aggregate check says passed", () => {
  for (const status of ["gap", "invalid"]) {
    const days = [
      {
        day: "2026-07-24",
        sources: [qualityFixture({ intervals: [{ status }] })],
        problem: false,
      },
    ];
    assert.equal(
      filterQualityDays(days, { ...noQualityFilters, problemOnly: true })
        .length,
      1,
    );
  }
  const days = [
    {
      day: "2026-07-24",
      sources: [qualityFixture({ intervals: [{ status: "valid" }] })],
      problem: true,
    },
  ];
  assert.deepEqual(
    filterQualityDays(days, { ...noQualityFilters, problemOnly: true }),
    [],
  );
});

test("unknown replicas are problems but never match explicit missing-replica filtering", () => {
  const unknown = qualityFixture({ replica: { status: "unknown" } });
  const missing = qualityFixture({
    dataset_id: "missing",
    replica: { status: "missing" },
  });
  const days = [
    { day: "2026-07-24", sources: [unknown, missing], problem: true },
  ];
  const filtered = filterQualityDays(days, {
    ...noQualityFilters,
    missingReplicaOnly: true,
  });
  assert.deepEqual(filtered[0].sources, [missing]);
  assert.equal(
    filterQualityDays([{ ...days[0], sources: [unknown] }], {
      ...noQualityFilters,
      problemOnly: true,
    }).length,
    1,
  );
});

test("dense fill labels thin only same-side text without changing source coordinates", () => {
  const labels = [
    { id: "buy-a", side: "BUY", left: 100, width: 29 },
    { id: "buy-b", side: "BUY", left: 120, width: 29 },
    { id: "buy-c", side: "BUY", left: 180, width: 29 },
    { id: "sell-b", side: "SELL", left: 120, width: 29 },
  ];
  const original = structuredClone(labels);
  assert.deepEqual([...selectFillLabels(labels)].sort(), [
    "buy-a",
    "buy-c",
    "sell-b",
  ]);
  assert.deepEqual(labels, original);
});

test("hovered and selected groups take priority but do not create colliding labels", () => {
  const labels = [
    { id: "early", side: "BUY", left: 100, width: 29 },
    { id: "selected", side: "BUY", left: 120, width: 29 },
    { id: "hovered", side: "BUY", left: 140, width: 29 },
  ];
  assert.deepEqual([...selectFillLabels(labels, ["selected"])], ["selected"]);
  assert.deepEqual(
    [...selectFillLabels(labels, ["hovered", "selected"])].sort(),
    ["early", "hovered"],
  );
});

test("wider screen spacing after zoom naturally admits more unchanged group labels", () => {
  const labels = Array.from({ length: 142 }, (_, i) => ({
    id: `fill-group-${i}`,
    side: i % 2 ? "BUY" : "SELL",
    left: i * 5,
    width: 29,
  }));
  const before = selectFillLabels(labels);
  const after = selectFillLabels(
    labels.map((label) => ({ ...label, left: label.left * 4 })),
  );
  assert.ok(before.size < labels.length);
  assert.equal(after.size, labels.length);
  assert.equal(labels.length, 142);
});
