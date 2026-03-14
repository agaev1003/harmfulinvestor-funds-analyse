const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");

const {
  parseIsoDateParts,
  ageDaysFromIsoDate,
  ageDaysFromTimestamp,
  ruDateToIso,
  isoDateToRu,
  shiftIsoDate,
  normalizeFundType,
  normalizeInvestmentType,
  normalizeHoldingName,
  normalizeHoldingKey,
  parseListPage,
  shouldIncludeFund,
  parseHeaderMeta,
  parseFundMetaFromDetailHtml,
  parseInvestfundsStructureDate,
  parseInvestfundsStructureFromHtml,
} = require("../../lib/parser");

const FIXTURES = path.join(__dirname, "..", "fixtures");

// ── Date helpers ────────────────────────────────────────────────────────────

test("ruDateToIso converts DD.MM.YYYY to YYYY-MM-DD", () => {
  assert.equal(ruDateToIso("15.01.2024"), "2024-01-15");
  assert.equal(ruDateToIso("01.12.2023"), "2023-12-01");
});

test("ruDateToIso returns null for invalid input", () => {
  assert.equal(ruDateToIso(""), null);
  assert.equal(ruDateToIso(null), null);
  assert.equal(ruDateToIso("2024-01-15"), null);
});

test("isoDateToRu converts YYYY-MM-DD to DD.MM.YYYY", () => {
  assert.equal(isoDateToRu("2024-01-15"), "15.01.2024");
  assert.equal(isoDateToRu("2023-12-01"), "01.12.2023");
});

test("isoDateToRu returns null for invalid input", () => {
  assert.equal(isoDateToRu(""), null);
  assert.equal(isoDateToRu("15.01.2024"), null);
});

test("ruDateToIso / isoDateToRu round-trip", () => {
  const ru = "25.03.2024";
  assert.equal(isoDateToRu(ruDateToIso(ru)), ru);
  const iso = "2024-03-25";
  assert.equal(ruDateToIso(isoDateToRu(iso)), iso);
});

test("parseIsoDateParts extracts year, month, day", () => {
  const parts = parseIsoDateParts("2024-03-15");
  assert.deepEqual(parts, { year: 2024, month: 3, day: 15 });
});

test("parseIsoDateParts returns null for invalid dates", () => {
  assert.equal(parseIsoDateParts(""), null);
  assert.equal(parseIsoDateParts("not-a-date"), null);
  assert.equal(parseIsoDateParts(null), null);
});

test("ageDaysFromIsoDate calculates days from date", () => {
  const now = new Date("2024-03-15T12:00:00Z");
  assert.equal(ageDaysFromIsoDate("2024-03-15", now), 0);
  assert.equal(ageDaysFromIsoDate("2024-03-14", now), 1);
  assert.equal(ageDaysFromIsoDate("2024-03-10", now), 5);
});

test("ageDaysFromIsoDate returns null for invalid input", () => {
  assert.equal(ageDaysFromIsoDate(""), null);
  assert.equal(ageDaysFromIsoDate(null), null);
});

test("ageDaysFromTimestamp calculates days from timestamp", () => {
  const nowMs = Date.parse("2024-03-15T12:00:00Z");
  const ts = "2024-03-13T12:00:00Z";
  assert.equal(ageDaysFromTimestamp(ts, nowMs), 2);
});

test("shiftIsoDate shifts by years, months, days", () => {
  assert.equal(shiftIsoDate("2024-03-15", { days: -5 }), "2024-03-10");
  assert.equal(shiftIsoDate("2024-03-15", { months: -1 }), "2024-02-15");
  assert.equal(shiftIsoDate("2024-03-15", { years: -1 }), "2023-03-15");
  assert.equal(shiftIsoDate("2024-01-31", { months: 1 }), "2024-03-02");
});

// ── Fund type normalizers ───────────────────────────────────────────────────

test("normalizeFundType detects all known types", () => {
  assert.equal(normalizeFundType("Биржевой"), "биржевой");
  assert.equal(normalizeFundType("Открытый"), "открытый");
  assert.equal(normalizeFundType("Интервальный"), "открытый");
  assert.equal(normalizeFundType(""), null);
});

test("normalizeInvestmentType detects all known types", () => {
  assert.equal(normalizeInvestmentType("Акции"), "Акции");
  assert.equal(normalizeInvestmentType("Облигации"), "Облигации");
  assert.equal(normalizeInvestmentType("Драгоценные металлы"), "Драгметаллы");
  assert.equal(normalizeInvestmentType("Денежный рынок"), "Денежный");
  assert.equal(normalizeInvestmentType("Смешанный"), "Смешанный");
  assert.equal(normalizeInvestmentType("Не определено"), "Смешанный");
  assert.equal(normalizeInvestmentType(""), "Смешанный");
  assert.equal(normalizeInvestmentType("Золото"), "Драгметаллы");
});

// ── Holding name helpers ────────────────────────────────────────────────────

test("normalizeHoldingName strips bracket suffixes", () => {
  assert.equal(normalizeHoldingName("Газпром [GAZP]"), "Газпром");
  assert.equal(normalizeHoldingName("Сбербанк, акция, [SBER]"), "Сбербанк");
  assert.equal(normalizeHoldingName("Лукойл - Облигации [LKOH-B]"), "Лукойл");
});

test("normalizeHoldingName returns null for empty", () => {
  assert.equal(normalizeHoldingName(""), null);
  assert.equal(normalizeHoldingName(null), null);
});

test("normalizeHoldingKey returns lowercase with ё → е", () => {
  assert.equal(normalizeHoldingKey("Ёлка"), "елка");
  assert.equal(normalizeHoldingKey("ГАЗПРОМ [GAZP]"), "газпром");
});

// ── shouldIncludeFund ───────────────────────────────────────────────────────

test("shouldIncludeFund accepts open and ETF with formed status", () => {
  assert.equal(
    shouldIncludeFund({ fund_type: "открытый", status: "сформирован" }),
    true
  );
  assert.equal(
    shouldIncludeFund({ fund_type: "биржевой", status: "сформирован" }),
    true
  );
});

test("shouldIncludeFund rejects wrong type or status", () => {
  assert.equal(
    shouldIncludeFund({ fund_type: "закрытый", status: "сформирован" }),
    false
  );
  assert.equal(
    shouldIncludeFund({ fund_type: "открытый", status: "ликвидирован" }),
    false
  );
});

// ── parseHeaderMeta ─────────────────────────────────────────────────────────

test("parseHeaderMeta extracts name, mc, fund_id, ticker", () => {
  const result = parseHeaderMeta(
    "Тест Фонд (УК Тест), 1234567890, TFND"
  );
  assert.equal(result.name, "Тест Фонд");
  assert.equal(result.management_company, "УК Тест");
  assert.equal(result.ticker, "TFND");
});

test("parseHeaderMeta handles empty input", () => {
  const result = parseHeaderMeta("");
  assert.equal(result.name, null);
  assert.equal(result.management_company, null);
});

// ── parseListPage (with fixture) ────────────────────────────────────────────

test("parseListPage extracts rows and maxPage from fixture", () => {
  const html = fs.readFileSync(path.join(FIXTURES, "fund-list-page.html"), "utf8");
  const { rows, maxPage } = parseListPage(html);

  assert.equal(maxPage, 3);
  assert.equal(rows.length, 3);

  const first = rows.find((r) => r.id === 12345);
  assert.ok(first, "should find fund 12345");
  assert.equal(first.name, "Тест Фонд Акций");
  assert.equal(first.fund_type, "открытый");
  assert.equal(first.management_company, "УК Тест");
  assert.equal(first.status, "сформирован");
  assert.equal(first.investment_type, "Акции");
  assert.equal(first.nav_date, "2024-01-15");
  assert.equal(first.aum_mln, 1250.5);
  assert.equal(shouldIncludeFund(first), true);

  const second = rows.find((r) => r.id === 67890);
  assert.ok(second, "should find fund 67890");
  assert.equal(second.fund_type, "биржевой");
  assert.equal(second.investment_type, "Облигации");
  assert.equal(shouldIncludeFund(second), true);

  const third = rows.find((r) => r.id === 11111);
  assert.ok(third, "should find fund 11111");
  assert.equal(third.fund_type, "открытый"); // "интервальный" → "открытый"
  assert.equal(third.status, "ликвидирован");
  assert.equal(shouldIncludeFund(third), false);
});

// ── parseFundMetaFromDetailHtml (with fixture) ──────────────────────────────

test("parseFundMetaFromDetailHtml extracts name and MC", () => {
  const html = fs.readFileSync(path.join(FIXTURES, "fund-detail-page.html"), "utf8");
  const meta = parseFundMetaFromDetailHtml(html);
  assert.equal(meta.name, "Тест Фонд Акций");
  assert.equal(meta.management_company, "УК Тест");
});

// ── parseInvestfundsStructureDate ───────────────────────────────────────────

test("parseInvestfundsStructureDate extracts date from title", () => {
  assert.equal(
    parseInvestfundsStructureDate("Структура на 15.01.2024"),
    "2024-01-15"
  );
  assert.equal(parseInvestfundsStructureDate(""), null);
  assert.equal(parseInvestfundsStructureDate("No date here"), null);
});

// ── parseInvestfundsStructureFromHtml (with fixture) ────────────────────────

test("parseInvestfundsStructureFromHtml extracts issuers", () => {
  const html = fs.readFileSync(path.join(FIXTURES, "fund-detail-page.html"), "utf8");
  const result = parseInvestfundsStructureFromHtml(html);
  assert.ok(result, "should return a result");
  assert.equal(result.structure_date, "2024-01-15");
  assert.ok(result.issuers.length >= 3, "should have at least 3 issuers");

  const gazprom = result.issuers.find((i) => i.name === "Газпром");
  assert.ok(gazprom, "should find Газпром");
  assert.equal(gazprom.percent, 15.5);

  const sber = result.issuers.find((i) => i.name === "Сбербанк");
  assert.ok(sber, "should find Сбербанк (stripped suffix)");
  assert.equal(sber.percent, 12.3);

  const lukoil = result.issuers.find((i) => i.name === "Лукойл");
  assert.ok(lukoil, "should find Лукойл (stripped suffix)");
  assert.equal(lukoil.percent, 8.7);
});

test("parseInvestfundsStructureFromHtml fallback without data-modul", () => {
  const html = fs.readFileSync(
    path.join(FIXTURES, "fund-detail-no-datamodul.html"),
    "utf8"
  );
  const result = parseInvestfundsStructureFromHtml(html);
  assert.ok(result, "should return a result via fallback");
  assert.equal(result.structure_date, "2026-02-20");
  assert.equal(result.issuers.length, 3);

  const vtb = result.issuers.find((i) => i.name.includes("Банк ВТБ"));
  assert.ok(vtb, "should find Банк ВТБ");
  assert.equal(vtb.percent, 9.15);
});
