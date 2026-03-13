const test = require("node:test");
const assert = require("node:assert/strict");

const {
  findLastWhere,
  findLastWithNav,
  findNavOnOrBefore,
  getLatestAum,
  filterHistoryByRange,
  parseChartSeries,
  mergeFundSeries,
  mergeValueSeries,
  extractSeriesFromHistory,
  mergeHistoryIncremental,
  computeReturn,
  buildReturnsRow,
  buildHoldingMap,
  buildCompositionChanges,
} = require("../../lib/calculations");

// ── findLastWhere ───────────────────────────────────────────────────────────

test("findLastWhere finds last matching element", () => {
  const arr = [1, 2, 3, 4, 5];
  assert.equal(findLastWhere(arr, (x) => x % 2 === 0), 4);
});

test("findLastWhere returns null when no match", () => {
  assert.equal(findLastWhere([1, 3, 5], (x) => x % 2 === 0), null);
});

test("findLastWhere handles empty array", () => {
  assert.equal(findLastWhere([], () => true), null);
});

// ── findLastWithNav ─────────────────────────────────────────────────────────

test("findLastWithNav finds last point with nav", () => {
  const history = [
    { time: "2024-01-01", nav: 100 },
    { time: "2024-01-02", nav: 101 },
    { time: "2024-01-03", nav: null },
  ];
  const result = findLastWithNav(history);
  assert.equal(result.time, "2024-01-02");
  assert.equal(result.nav, 101);
});

test("findLastWithNav returns null for empty array", () => {
  assert.equal(findLastWithNav([]), null);
});

test("findLastWithNav returns null when no point has nav", () => {
  assert.equal(
    findLastWithNav([{ time: "2024-01-01", nav: null }]),
    null
  );
});

// ── findNavOnOrBefore ───────────────────────────────────────────────────────

test("findNavOnOrBefore finds nav on exact date", () => {
  const history = [
    { time: "2024-01-01", nav: 100 },
    { time: "2024-01-02", nav: 105 },
  ];
  assert.equal(findNavOnOrBefore(history, "2024-01-02"), 105);
});

test("findNavOnOrBefore finds nav before target date", () => {
  const history = [
    { time: "2024-01-01", nav: 100 },
    { time: "2024-01-03", nav: 110 },
  ];
  assert.equal(findNavOnOrBefore(history, "2024-01-02"), 100);
});

test("findNavOnOrBefore returns null when no nav before target", () => {
  const history = [{ time: "2024-01-05", nav: 100 }];
  assert.equal(findNavOnOrBefore(history, "2024-01-02"), null);
});

// ── getLatestAum ────────────────────────────────────────────────────────────

test("getLatestAum returns last non-null aum", () => {
  const history = [
    { time: "2024-01-01", aum: 1000 },
    { time: "2024-01-02", aum: 2000 },
    { time: "2024-01-03", aum: null },
  ];
  assert.equal(getLatestAum(history), 2000);
});

test("getLatestAum returns 0 for history without aum", () => {
  assert.equal(getLatestAum([{ time: "2024-01-01" }]), 0);
});

// ── filterHistoryByRange ────────────────────────────────────────────────────

test("filterHistoryByRange filters by from and to", () => {
  const history = [
    { time: "2024-01-01" },
    { time: "2024-01-05" },
    { time: "2024-01-10" },
  ];
  const result = filterHistoryByRange(history, "2024-01-02", "2024-01-08");
  assert.equal(result.length, 1);
  assert.equal(result[0].time, "2024-01-05");
});

test("filterHistoryByRange returns all when no bounds", () => {
  const history = [{ time: "2024-01-01" }, { time: "2024-01-02" }];
  assert.equal(filterHistoryByRange(history, null, null).length, 2);
});

// ── parseChartSeries ────────────────────────────────────────────────────────

test("parseChartSeries converts timestamp-value pairs", () => {
  const raw = [
    [1704067200000, 100],
    [1704153600000, 101],
  ];
  const result = parseChartSeries(raw);
  assert.equal(result.length, 2);
  assert.ok(result[0].time);
  assert.equal(result[0].value, 100);
  assert.equal(result[1].value, 101);
});

test("parseChartSeries handles empty input", () => {
  assert.deepEqual(parseChartSeries(null), []);
  assert.deepEqual(parseChartSeries([]), []);
});

test("parseChartSeries skips invalid points", () => {
  const raw = [[1704067200000, 100], "bad", [1704153600000, null]];
  const result = parseChartSeries(raw);
  assert.equal(result.length, 1);
});

// ── mergeFundSeries ─────────────────────────────────────────────────────────

test("mergeFundSeries merges NAV and AUM series", () => {
  const pay = [
    { time: "2024-01-01", value: 100 },
    { time: "2024-01-02", value: 102 },
  ];
  const sca = [
    { time: "2024-01-01", value: 1000 },
    { time: "2024-01-02", value: 1020 },
  ];
  const merged = mergeFundSeries(pay, sca);
  assert.equal(merged.length, 2);
  assert.equal(merged[0].nav, 100);
  assert.equal(merged[0].aum, 1000);
  assert.equal(merged[0].shares, 10);
  assert.equal(merged[1].nav, 102);
  assert.equal(merged[1].aum, 1020);
});

test("mergeFundSeries calculates flow from share changes", () => {
  const pay = [
    { time: "2024-01-01", value: 100 },
    { time: "2024-01-02", value: 100 },
  ];
  const sca = [
    { time: "2024-01-01", value: 1000 },
    { time: "2024-01-02", value: 1200 },
  ];
  const merged = mergeFundSeries(pay, sca);
  assert.equal(merged[0].flow, null); // first point has no previous
  assert.ok(merged[1].flow > 0, "should have positive flow");
});

// ── mergeValueSeries ────────────────────────────────────────────────────────

test("mergeValueSeries merges and deduplicates by date", () => {
  const existing = [{ time: "2024-01-01", value: 100 }];
  const fresh = [
    { time: "2024-01-01", value: 101 },
    { time: "2024-01-02", value: 102 },
  ];
  const result = mergeValueSeries(existing, fresh);
  assert.equal(result.length, 2);
  assert.equal(result[0].value, 101); // fresh overwrites existing
  assert.equal(result[1].value, 102);
});

// ── extractSeriesFromHistory ────────────────────────────────────────────────

test("extractSeriesFromHistory extracts a given key", () => {
  const history = [
    { time: "2024-01-01", nav: 100, aum: 1000 },
    { time: "2024-01-02", nav: 101, aum: null },
  ];
  const navSeries = extractSeriesFromHistory(history, "nav");
  assert.equal(navSeries.length, 2);
  assert.equal(navSeries[0].value, 100);

  const aumSeries = extractSeriesFromHistory(history, "aum");
  assert.equal(aumSeries.length, 1); // null values filtered out
});

// ── computeReturn ───────────────────────────────────────────────────────────

test("computeReturn calculates percentage return for period", () => {
  const history = [
    { time: "2023-01-01", nav: 100 },
    { time: "2024-01-01", nav: 120 },
  ];
  const result = computeReturn(history, "2024-01-01", "1y");
  assert.ok(result != null);
  assert.ok(Math.abs(result - 20) < 0.01, `Expected ~20%, got ${result}`);
});

test("computeReturn returns null for unknown period", () => {
  assert.equal(computeReturn([], "2024-01-01", "10y"), null);
});

test("computeReturn returns null when start NAV is 0 or missing", () => {
  const history = [{ time: "2024-01-01", nav: 100 }];
  assert.equal(computeReturn(history, "2024-01-01", "1y"), null);
});

test("computeReturn ytd calculates from Jan 1", () => {
  const history = [
    { time: "2023-12-31", nav: 100 },
    { time: "2024-01-01", nav: 100 },
    { time: "2024-06-15", nav: 110 },
  ];
  const result = computeReturn(history, "2024-06-15", "ytd");
  assert.ok(result != null);
  assert.ok(Math.abs(result - 10) < 0.01);
});

// ── buildReturnsRow ─────────────────────────────────────────────────────────

test("buildReturnsRow produces full row with returns", () => {
  const fundMeta = {
    id: 123,
    ticker: "TFND",
    fund_id: "1234567890",
    name: "Test Fund",
    management_company: "MC",
    fund_type: "открытый",
    investment_type: "Акции",
    specialization: "Широкого рынка",
  };
  const history = [
    { time: "2023-01-01", nav: 100, aum: 1000 },
    { time: "2023-06-01", nav: 105, aum: 1100 },
    { time: "2024-01-01", nav: 120, aum: 1500 },
  ];
  const row = buildReturnsRow(fundMeta, history);
  assert.equal(row.id, 123);
  assert.equal(row.ticker, "TFND");
  assert.equal(row.name, "Test Fund");
  assert.equal(row.date, "2024-01-01");
  assert.ok(row.nav != null);
  assert.ok(row.aum != null);
  assert.ok(row["1y"] != null);
});

test("buildReturnsRow handles empty history", () => {
  const fundMeta = { id: 1, name: "Empty" };
  const row = buildReturnsRow(fundMeta, []);
  assert.equal(row.date, null);
  assert.equal(row.nav, null);
  assert.equal(row["1y"], null);
});

// ── buildHoldingMap ─────────────────────────────────────────────────────────

test("buildHoldingMap creates map from issuers", () => {
  const items = [
    { name: "Газпром [GAZP]", percent: 15.5 },
    { name: "Сбербанк [SBER]", percent: 10.2 },
  ];
  const map = buildHoldingMap(items);
  assert.equal(map.size, 2);
  assert.ok(map.has("газпром"));
  assert.equal(map.get("газпром").percent, 15.5);
});

// ── buildCompositionChanges ─────────────────────────────────────────────────

test("buildCompositionChanges detects bought and sold", () => {
  const current = [
    { name: "Газпром", percent: 15 },
    { name: "Лукойл", percent: 10 },
  ];
  const previous = [
    { name: "Газпром", percent: 10 },
    { name: "Сбербанк", percent: 8 },
  ];
  const result = buildCompositionChanges(current, previous, "2024-01-01");
  assert.equal(result.previous_date, "2024-01-01");
  assert.ok(result.bought.length > 0, "should have bought items");
  assert.ok(result.sold.length > 0, "should have sold items");

  const gazBought = result.bought.find((r) => r.name === "Газпром");
  assert.ok(gazBought, "Газпром should be in bought (increased)");
  assert.equal(gazBought.delta, 5);

  const sberSold = result.sold.find((r) => r.name === "Сбербанк");
  assert.ok(sberSold, "Сбербанк should be in sold (removed)");
});

test("buildCompositionChanges returns empty for no previous", () => {
  const result = buildCompositionChanges(
    [{ name: "A", percent: 10 }],
    null,
    null
  );
  assert.equal(result.previous_date, null);
  assert.deepEqual(result.bought, []);
  assert.deepEqual(result.sold, []);
});
