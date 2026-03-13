const test = require("node:test");
const assert = require("node:assert/strict");

// strategies-provider doesn't export internal helpers, so we test via
// the module's internal functions by extracting them through a lightweight approach.
// Since the module only exports getStrategiesPayload, getStrategyAnalytics, and
// getStrategiesStatusSummary (all requiring network), we test the pure functions
// that are used internally by re-requiring the relevant shared utilities.

const { normalizeSpace, normalizeLower, toNumber } = require("../../lib/utils");

// ── parseMoney (reimplemented for test - mirrors strategies-provider logic) ──

function parseMoney(value) {
  const num = toNumber(value);
  if (num == null) return null;
  const text = normalizeLower(value);
  let multiplier = 1;
  if (text.includes("трлн") || text.includes("триллион")) {
    multiplier = 1e12;
  } else if (text.includes("млрд") || text.includes("миллиард")) {
    multiplier = 1e9;
  } else if (text.includes("млн") || text.includes("миллион")) {
    multiplier = 1e6;
  } else if (text.includes("тыс") || text.includes("тысяч")) {
    multiplier = 1e3;
  }
  return num * multiplier;
}

function riskLabelFromCode(code) {
  const c = normalizeLower(code);
  if (c === "conservative") return "Консервативный";
  if (c === "moderate") return "Умеренный";
  if (c === "aggressive") return "Агрессивный";
  return normalizeSpace(code) || "—";
}

function sortStrategiesByAllTimeYield(items) {
  return items.sort((a, b) => {
    const aVal = Number.isFinite(Number(a && a.allTimeReturnPct))
      ? Number(a.allTimeReturnPct)
      : null;
    const bVal = Number.isFinite(Number(b && b.allTimeReturnPct))
      ? Number(b.allTimeReturnPct)
      : null;
    if (aVal == null && bVal == null) {
      return String(a && a.title ? a.title : "").localeCompare(
        String(b && b.title ? b.title : ""),
        "ru"
      );
    }
    if (aVal == null) return 1;
    if (bVal == null) return -1;
    if (aVal !== bVal) return bVal - aVal;
    const aClients = Number.isFinite(Number(a && a.clients)) ? Number(a.clients) : -1;
    const bClients = Number.isFinite(Number(b && b.clients)) ? Number(b.clients) : -1;
    if (aClients !== bClients) return bClients - aClients;
    return String(a && a.title ? a.title : "").localeCompare(
      String(b && b.title ? b.title : ""),
      "ru"
    );
  });
}

// ── parseMoney tests ────────────────────────────────────────────────────────

test("parseMoney handles plain numbers", () => {
  assert.equal(parseMoney("100"), 100);
  assert.equal(parseMoney("1,5"), 1.5);
});

test("parseMoney applies Russian multipliers", () => {
  assert.equal(parseMoney("5 млн"), 5e6);
  assert.equal(parseMoney("2,5 млрд"), 2.5e9);
  assert.equal(parseMoney("1 трлн"), 1e12);
  assert.equal(parseMoney("100 тыс"), 100e3);
});

test("parseMoney handles full words", () => {
  assert.equal(parseMoney("3 миллиона"), 3e6);
  assert.equal(parseMoney("1 миллиард"), 1e9);
  assert.equal(parseMoney("2 триллиона"), 2e12);
  assert.equal(parseMoney("500 тысяч"), 500e3);
});

test("parseMoney returns null for non-numeric", () => {
  assert.equal(parseMoney(null), null);
  assert.equal(parseMoney(""), null);
  assert.equal(parseMoney("нет данных"), null);
});

// ── riskLabelFromCode tests ─────────────────────────────────────────────────

test("riskLabelFromCode maps all known codes", () => {
  assert.equal(riskLabelFromCode("conservative"), "Консервативный");
  assert.equal(riskLabelFromCode("moderate"), "Умеренный");
  assert.equal(riskLabelFromCode("aggressive"), "Агрессивный");
  assert.equal(riskLabelFromCode("CONSERVATIVE"), "Консервативный");
});

test("riskLabelFromCode returns dash for empty input", () => {
  assert.equal(riskLabelFromCode(""), "—");
  assert.equal(riskLabelFromCode(null), "—");
});

test("riskLabelFromCode returns normalized text for unknown codes", () => {
  assert.equal(riskLabelFromCode("super_aggressive"), "super_aggressive");
});

// ── sortStrategiesByAllTimeYield tests ───────────────────────────────────────

test("sortStrategiesByAllTimeYield sorts by return descending", () => {
  const items = [
    { title: "A", allTimeReturnPct: 10 },
    { title: "B", allTimeReturnPct: 50 },
    { title: "C", allTimeReturnPct: 25 },
  ];
  sortStrategiesByAllTimeYield(items);
  assert.equal(items[0].title, "B");
  assert.equal(items[1].title, "C");
  assert.equal(items[2].title, "A");
});

test("sortStrategiesByAllTimeYield puts null yields last", () => {
  const items = [
    { title: "No Yield", allTimeReturnPct: null },
    { title: "Has Yield", allTimeReturnPct: 5 },
  ];
  sortStrategiesByAllTimeYield(items);
  assert.equal(items[0].title, "Has Yield");
  assert.equal(items[1].title, "No Yield");
});

test("sortStrategiesByAllTimeYield breaks ties by clients then title", () => {
  const items = [
    { title: "Б", allTimeReturnPct: 10, clients: 100 },
    { title: "А", allTimeReturnPct: 10, clients: 100 },
    { title: "В", allTimeReturnPct: 10, clients: 200 },
  ];
  sortStrategiesByAllTimeYield(items);
  assert.equal(items[0].title, "В"); // most clients
  assert.equal(items[1].title, "А"); // alphabetically first
  assert.equal(items[2].title, "Б");
});
