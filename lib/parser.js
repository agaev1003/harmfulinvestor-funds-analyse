const cheerio = require("cheerio");
const { normalizeSpace, normalizeLower, csvLikeText, toNumber, round } = require("./utils");

// ── Date helpers ────────────────────────────────────────────────────────────

const DATE_FMT_MSK = new Intl.DateTimeFormat("sv-SE", {
  timeZone: "Europe/Moscow",
});
const HOUR_FMT_MSK = new Intl.DateTimeFormat("en-GB", {
  timeZone: "Europe/Moscow",
  hour: "2-digit",
  hour12: false,
});

function parseIsoDateParts(isoDate) {
  const match = String(isoDate || "").trim().match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) {
    return null;
  }
  return { year, month, day };
}

function ageDaysFromIsoDate(isoDate, now = new Date()) {
  const parts = parseIsoDateParts(isoDate);
  if (!parts) return null;
  const fromUtc = Date.UTC(parts.year, parts.month - 1, parts.day);
  const nowUtc = Date.UTC(
    now.getUTCFullYear(),
    now.getUTCMonth(),
    now.getUTCDate()
  );
  return Math.max(0, Math.floor((nowUtc - fromUtc) / 86_400_000));
}

function ageDaysFromTimestamp(value, nowMs = Date.now()) {
  const ts = Date.parse(String(value || ""));
  if (!Number.isFinite(ts)) return null;
  return Math.max(0, Math.floor((nowMs - ts) / 86_400_000));
}

function ruDateToIso(value) {
  const normalized = normalizeSpace(value);
  const match = normalized.match(/^(\d{2})\.(\d{2})\.(\d{4})$/);
  if (!match) return null;
  return `${match[3]}-${match[2]}-${match[1]}`;
}

function isoDateToRu(value) {
  const normalized = normalizeSpace(value);
  const match = normalized.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;
  return `${match[3]}.${match[2]}.${match[1]}`;
}

function isoDateToUtcDate(value) {
  return new Date(`${value}T00:00:00Z`);
}

function shiftIsoDate(value, { years = 0, months = 0, days = 0 } = {}) {
  const d = isoDateToUtcDate(value);
  if (years) d.setUTCFullYear(d.getUTCFullYear() + years);
  if (months) d.setUTCMonth(d.getUTCMonth() + months);
  if (days) d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

function mskDate(value = new Date()) {
  return DATE_FMT_MSK.format(value);
}

function mskHour(value = new Date()) {
  const raw = HOUR_FMT_MSK.format(value);
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : null;
}

function isGeneratedAfterMskHour(isoTs, hourMsk) {
  const ts = Date.parse(String(isoTs || ""));
  if (!Number.isFinite(ts)) return false;
  const d = new Date(ts);
  const h = mskHour(d);
  return h != null && h >= hourMsk;
}

function toIsoFromMs(ms) {
  const n = Number(ms);
  if (!Number.isFinite(n)) return null;
  return DATE_FMT_MSK.format(new Date(n));
}

function datasetTimestampMs(dataset) {
  const ts = Date.parse(dataset && dataset.generatedAt ? dataset.generatedAt : "");
  return Number.isFinite(ts) ? ts : Date.now();
}

// ── Fund type / investment normalizers ──────────────────────────────────────

function normalizeFundType(value) {
  const v = normalizeLower(value);
  if (v.includes("бирж")) return "биржевой";
  if (v.includes("открыт")) return "открытый";
  if (v.includes("интерв")) return "открытый";
  return v || null;
}

function normalizeInvestmentType(value) {
  const v = normalizeLower(value);
  if (!v || v.includes("не определ")) return "Смешанный";
  if (v.includes("акци")) return "Акции";
  if (v.includes("облига")) return "Облигации";
  if (v.includes("драг") || v.includes("металл") || v.includes("золото")) {
    return "Драгметаллы";
  }
  if (v.includes("денеж")) return "Денежный";
  if (v.includes("смеш")) return "Смешанный";
  return "Смешанный";
}

// ── Holding name helpers ────────────────────────────────────────────────────

function normalizeHoldingName(value) {
  let v = csvLikeText(value);
  if (!v) return null;
  v = v.replace(/\s*-\s*Облигации\s*\[.*$/i, "");
  v = v.replace(/\s*,\s*акция\s*,?\s*\[.*$/i, "");
  v = v.replace(/\s*,\s*акции\s*,?\s*\[.*$/i, "");
  v = v.replace(/\s*\[[^\]]+\]\s*$/g, "");
  return csvLikeText(v) || null;
}

function normalizeHoldingKey(value) {
  const normalized = normalizeHoldingName(value);
  return normalized ? normalized.toLowerCase().replace(/ё/g, "е") : null;
}

// ── investfunds.ru HTML parsers ─────────────────────────────────────────────

function parseMaxPage($) {
  let maxPage = 1;
  $("a.js_pagination[href*='page=']").each((_, el) => {
    const href = $(el).attr("href") || "";
    const match = href.match(/page=(\d+)/);
    if (match) {
      const page = Number(match[1]);
      if (Number.isFinite(page) && page > maxPage) maxPage = page;
    }
  });
  return maxPage;
}

function parseListPage(html) {
  const $ = cheerio.load(html);
  const maxPage = parseMaxPage($);

  const fixedByIndex = new Map();
  $("tr[class*='field_fixed_']").each((_, row) => {
    const className = $(row).attr("class") || "";
    const match = className.match(/field_fixed_(\d+)/);
    if (!match) return;
    const rowIndex = match[1];
    const link = $(row).find("td.field_name a[href*='/funds/']").first();
    const href = link.attr("href") || "";
    const idMatch = href.match(/\/funds\/(\d+)\//);
    if (!idMatch) return;

    const fundId = Number(idMatch[1]);
    const fundName = csvLikeText(link.text());
    const fundTypeRaw = csvLikeText($(row).find("td.field_name .blue").first().text());

    fixedByIndex.set(rowIndex, {
      id: fundId,
      name: fundName,
      detail_path: href,
      fund_type_raw: fundTypeRaw,
    });
  });

  const scrollByIndex = new Map();
  $("tr[class*='field_scroll_']").each((_, row) => {
    const className = $(row).attr("class") || "";
    const match = className.match(/field_scroll_(\d+)/);
    if (!match) return;
    const rowIndex = match[1];
    const get = (selector) =>
      csvLikeText($(row).find(selector).first().text());

    scrollByIndex.set(rowIndex, {
      management_company: get("td.field_funds_comp_name .js_td_width"),
      status_raw: get("td.field_funds_statuses_name .js_td_width"),
      category_raw: get("td.field_funds_categories_title .js_td_width"),
      investment_object_raw: get("td.field_funds_object_name .js_td_width"),
      specialization_raw: get(
        "td.field_funds_investing_directions_name .js_td_width"
      ),
      nav_date_raw: get("td.field_funds_nav_date .js_td_width"),
      aum_mln_raw: get("td.field_nav .js_td_width"),
    });
  });

  const rows = [];
  for (const [idx, fixed] of fixedByIndex.entries()) {
    const scroll = scrollByIndex.get(idx) || {};
    rows.push({
      ...fixed,
      ...scroll,
      fund_type: normalizeFundType(fixed.fund_type_raw),
      status: normalizeLower(scroll.status_raw),
      investment_type: normalizeInvestmentType(scroll.investment_object_raw),
      specialization:
        csvLikeText(scroll.specialization_raw) || csvLikeText(scroll.category_raw),
      nav_date: ruDateToIso(scroll.nav_date_raw),
      aum_mln: toNumber(scroll.aum_mln_raw),
    });
  }

  return { rows, maxPage };
}

function shouldIncludeFund(row) {
  const typeOk = row.fund_type === "открытый" || row.fund_type === "биржевой";
  const statusOk = row.status === "сформирован";
  return typeOk && statusOk;
}

function parseHeaderMeta(header) {
  const normalized = csvLikeText(header);
  if (!normalized) {
    return { name: null, management_company: null, fund_id: null, ticker: null };
  }

  const nameMatch = normalized.match(/^(.*?)\s*\(([^)]+)\)/);
  let name = null;
  let managementCompany = null;
  if (nameMatch) {
    name = csvLikeText(nameMatch[1]);
    managementCompany = csvLikeText(nameMatch[2]);
  }

  const segments = normalized.split(",").map((s) => csvLikeText(s));
  let fundId = null;
  let ticker = null;

  for (const segment of segments) {
    if (!fundId && /^\d[\dA-ZА-ЯЁa-zа-яё\-./\s]{2,}$/.test(segment)) {
      fundId = segment.replace(/\s+/g, "");
      continue;
    }
    if (!ticker && /^[A-Z0-9.-]{2,20}$/.test(segment)) {
      ticker = segment;
    }
  }

  return {
    name: name || null,
    management_company: managementCompany || null,
    fund_id: fundId,
    ticker: ticker || null,
  };
}

function parseDetailInfoMap($) {
  const info = new Map();
  $("[data-modul='info'] .item").each((_, el) => {
    const key = normalizeLower($(el).find(".name").first().text());
    const val = csvLikeText($(el).find(".value").first().text());
    if (key) info.set(key, val || null);
  });
  return info;
}

function pickInfoValue(infoMap, keys) {
  for (const key of keys) {
    const value = infoMap.get(key);
    if (value) return value;
  }
  return null;
}

function parseFundMetaFromDetailHtml(html) {
  const $ = cheerio.load(html);
  const header = csvLikeText($(".widget_info_ttl").first().text());
  const parsed = parseHeaderMeta(header);
  const infoMap = parseDetailInfoMap($);

  const nameFromH1 = csvLikeText($("h1").first().text());
  const name = parsed.name || nameFromH1 || null;

  const managementCompany =
    parsed.management_company ||
    pickInfoValue(infoMap, ["управляющая компания", "управляющая компания:"]) ||
    csvLikeText($(".wdgt_img_logo img").first().attr("alt")) ||
    null;

  return {
    name,
    management_company: managementCompany,
  };
}

function parseInvestfundsStructureDate(rawText) {
  const text = csvLikeText(rawText);
  if (!text) return null;
  const match = text.match(/(\d{2}\.\d{2}\.\d{4})/);
  if (!match) return null;
  return ruDateToIso(match[1]) || null;
}

function findStructureBlock($) {
  // Primary: original data-modul attribute
  const primary = $("[data-modul='structure']").first();
  if (primary.length) return primary;

  // Fallback: find any element containing "Структура фонда" heading
  // and locate its parent container with a table
  let fallback = null;
  $("*").each((_, el) => {
    if (fallback) return;
    const text = $(el).text().trim();
    if (/^Структура\s+фонда\s+на\s+\d{2}\.\d{2}\.\d{4}/.test(text)) {
      // Walk up to find a container that has a table
      let parent = $(el).parent();
      for (let depth = 0; depth < 6 && parent.length; depth++) {
        if (parent.find("table tbody tr").length > 0) {
          fallback = parent;
          return;
        }
        parent = parent.parent();
      }
    }
  });
  return fallback || $(); // return empty cheerio object if not found
}

function parseInvestfundsStructureFromHtml(html) {
  const $ = cheerio.load(html);
  const block = findStructureBlock($);
  if (!block.length) return null;

  // Try original heading selector, then fall back to searching block text
  let structureDate = parseInvestfundsStructureDate(
    block.find(".middle_ttl").first().text()
  );
  if (!structureDate) {
    // Scan all text nodes in the block for a date pattern
    const blockText = block.text();
    const dateMatch = blockText.match(/Структура\s+фонда\s+на\s+(\d{2}\.\d{2}\.\d{4})/);
    if (dateMatch) {
      structureDate = ruDateToIso(dateMatch[1]);
    }
  }

  const issuers = [];
  block.find("table tbody tr").each((_, row) => {
    const tds = $(row).find("td");
    if (tds.length < 2) return;
    const nameRaw = csvLikeText($(tds[0]).text());
    const percent = toNumber($(tds[1]).text());
    const name = normalizeHoldingName(nameRaw);
    if (!name || percent == null) return;
    issuers.push({
      name,
      percent: round(percent, 2),
    });
  });

  if (!issuers.length) return null;
  return {
    structure_date: structureDate,
    issuers,
  };
}

module.exports = {
  // Date helpers
  parseIsoDateParts,
  ageDaysFromIsoDate,
  ageDaysFromTimestamp,
  ruDateToIso,
  isoDateToRu,
  isoDateToUtcDate,
  shiftIsoDate,
  mskDate,
  mskHour,
  isGeneratedAfterMskHour,
  toIsoFromMs,
  datasetTimestampMs,
  // Fund normalizers
  normalizeFundType,
  normalizeInvestmentType,
  // Holding helpers
  normalizeHoldingName,
  normalizeHoldingKey,
  // HTML parsers
  parseMaxPage,
  parseListPage,
  shouldIncludeFund,
  parseHeaderMeta,
  parseDetailInfoMap,
  pickInfoValue,
  parseFundMetaFromDetailHtml,
  parseInvestfundsStructureDate,
  parseInvestfundsStructureFromHtml,
};
