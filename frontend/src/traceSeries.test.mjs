import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { recordedSeries } from "./traceSeries.ts";

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
