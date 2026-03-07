const fs = require("fs/promises");
const path = require("path");
const cheerio = require("cheerio");
const zlib = require("zlib");

const IS_RENDER = Boolean(process.env.RENDER);
const BASE_URL = process.env.DATA_SOURCE_BASE || "https://investfunds.ru";
const CACHE_FILE =
  process.env.DATA_CACHE_FILE ||
  path.join(__dirname, ".cache", "fund-dashboard-data.json");
const DATASET_SEED_GZ_FILE =
  process.env.DATASET_SEED_GZ_FILE ||
  path.join(__dirname, "seed-cache", "fund-dashboard-data.json.gz");
const DATASET_VERSION = Number(process.env.DATASET_VERSION || 6);

const CACHE_TTL_MS = Number(
  process.env.DATA_CACHE_TTL_MS ||
    (IS_RENDER ? 24 * 60 * 60 * 1000 : 12 * 60 * 60 * 1000)
);
const REQUEST_TIMEOUT_MS = Number(process.env.DATA_REQUEST_TIMEOUT_MS || 25_000);
const COMPOSITION_REQUEST_TIMEOUT_MS = Number(
  process.env.COMPOSITION_REQUEST_TIMEOUT_MS || (IS_RENDER ? 12_000 : 18_000)
);
const COMPOSITION_REQUEST_RETRIES = Number(
  process.env.COMPOSITION_REQUEST_RETRIES || (IS_RENDER ? 0 : 1)
);
const SERIES_REQUEST_TIMEOUT_MS = Number(
  process.env.SERIES_REQUEST_TIMEOUT_MS || (IS_RENDER ? 12_000 : 18_000)
);
const SERIES_REQUEST_RETRIES = Number(
  process.env.SERIES_REQUEST_RETRIES || (IS_RENDER ? 0 : 1)
);
const SERIES_SINGLE_FALLBACK_CONCURRENCY = Number(
  process.env.SERIES_SINGLE_FALLBACK_CONCURRENCY || (IS_RENDER ? 6 : 10)
);
const MAX_LIST_PAGES = Number(process.env.MAX_LIST_PAGES || 75);
const LIST_CONCURRENCY = Number(process.env.LIST_CONCURRENCY || (IS_RENDER ? 6 : 8));
const DETAIL_CONCURRENCY = Number(
  process.env.DETAIL_CONCURRENCY || (IS_RENDER ? 10 : 16)
);
const HISTORY_BATCH_SIZE = Number(process.env.HISTORY_BATCH_SIZE || (IS_RENDER ? 32 : 48));
const HISTORY_BATCH_CONCURRENCY = Number(
  process.env.HISTORY_BATCH_CONCURRENCY || (IS_RENDER ? 4 : 6)
);
const HISTORY_LOOKBACK_DAYS = Number(process.env.HISTORY_LOOKBACK_DAYS || 14);
const TOP_GROUP_COUNT = Number(process.env.TOP_GROUP_COUNT || 9);
const COMPOSITION_CACHE_FILE =
  process.env.COMPOSITION_CACHE_FILE ||
  path.join(__dirname, ".cache", "fund-compositions.json");
const COMPOSITION_SEED_GZ_FILE =
  process.env.COMPOSITION_SEED_GZ_FILE ||
  path.join(__dirname, "seed-cache", "fund-compositions.json.gz");
const COMPOSITION_CACHE_TTL_MS = Number(
  process.env.COMPOSITION_CACHE_TTL_MS ||
    (IS_RENDER ? 48 * 60 * 60 * 1000 : 24 * 60 * 60 * 1000)
);
const COMPOSITION_CONCURRENCY = Number(
  process.env.COMPOSITION_CONCURRENCY || (IS_RENDER ? 5 : 8)
);
const COMPOSITION_DATASET_VERSION = Number(
  process.env.COMPOSITION_DATASET_VERSION || 4
);
const STALE_MARK_DAYS = Number(process.env.STALE_MARK_DAYS || 45);
const COMPOSITION_DELTA_MAX_PER_RUN = Number(
  process.env.COMPOSITION_DELTA_MAX_PER_RUN || (IS_RENDER ? 90 : 160)
);
const COMPOSITION_DELTA_MAX_FORCE_RUN = Number(
  process.env.COMPOSITION_DELTA_MAX_FORCE_RUN || (IS_RENDER ? 180 : 320)
);
const COMPOSITION_ITEM_REFRESH_DAYS = Number(
  process.env.COMPOSITION_ITEM_REFRESH_DAYS || 10
);
const COMPOSITION_MISSING_RECHECK_DAYS = Number(
  process.env.COMPOSITION_MISSING_RECHECK_DAYS || 4
);
const RANKING_SCAN_TTL_MS = Number(
  process.env.RANKING_SCAN_TTL_MS || (IS_RENDER ? 6 * 60 * 60 * 1000 : 60 * 60 * 1000)
);
const DAILY_MAIN_REFRESH_HOUR_MSK = Number(
  process.env.DAILY_MAIN_REFRESH_HOUR_MSK ||
    process.env.DAILY_REFRESH_HOUR_MSK ||
    12
);
const DAILY_COMPOSITION_REFRESH_HOUR_MSK = Number(
  process.env.DAILY_COMPOSITION_REFRESH_HOUR_MSK ||
    process.env.DAILY_REFRESH_HOUR_MSK ||
    12
);

const DATE_FMT_MSK = new Intl.DateTimeFormat("sv-SE", {
  timeZone: "Europe/Moscow",
});
const HOUR_FMT_MSK = new Intl.DateTimeFormat("en-GB", {
  timeZone: "Europe/Moscow",
  hour: "2-digit",
  hour12: false,
});

const state = {
  dataset: null,
  loadedAt: 0,
  diskLoaded: false,
  buildPromise: null,
  datasetAutoRefreshDate: null,
  compositions: null,
  compositionsLoadedAt: 0,
  compositionsDiskLoaded: false,
  compositionsBuildPromise: null,
  compositionsAutoRefreshDate: null,
  compositionsBuildState: null,
  compositionsLastRun: null,
  compositionsLastError: null,
  marketBuildState: null,
  marketLastRun: null,
  marketLastError: null,
  rankingIds: null,
  rankingIdsLoadedAt: 0,
};

function datasetTimestampMs(dataset) {
  const ts = Date.parse(dataset && dataset.generatedAt ? dataset.generatedAt : "");
  return Number.isFinite(ts) ? ts : Date.now();
}

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

async function readSeedJsonGz(filePath) {
  try {
    const compressed = await fs.readFile(filePath);
    const raw = await new Promise((resolve, reject) => {
      zlib.gunzip(compressed, (error, out) => {
        if (error) reject(error);
        else resolve(out);
      });
    });
    return JSON.parse(raw.toString("utf8"));
  } catch {
    return null;
  }
}

async function persistCacheSnapshot(filePath, data) {
  try {
    await fs.mkdir(path.dirname(filePath), { recursive: true });
    await fs.writeFile(filePath, JSON.stringify(data), "utf8");
  } catch {
    // Non-fatal: runtime can still operate from in-memory data.
  }
}

function normalizeSpace(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function normalizeLower(value) {
  return normalizeSpace(value).toLowerCase();
}

function toNumber(value) {
  if (value == null) return null;
  const prepared = String(value)
    .replace(/\u00A0/g, " ")
    .replace(/\s+/g, "")
    .replace(/,/g, ".")
    .replace(/[^0-9.+-]/g, "");
  if (!prepared || prepared === "-" || prepared === "+" || prepared === ".") {
    return null;
  }
  const num = Number(prepared);
  return Number.isFinite(num) ? num : null;
}

function round(value, digits = 2) {
  if (!Number.isFinite(value)) return null;
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
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

function normalizeFundType(value) {
  const v = normalizeLower(value);
  if (v.includes("бирж")) return "биржевой";
  if (v.includes("открыт")) return "открытый";
  // Friend dashboard includes интервальные фонды in total universe (317).
  // We map them to ОПИФ to keep the two-type UI consistent.
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

function toIsoFromMs(ms) {
  const n = Number(ms);
  if (!Number.isFinite(n)) return null;
  return DATE_FMT_MSK.format(new Date(n));
}

function csvLikeText(value) {
  return normalizeSpace(value).replace(/\s+/g, " ").trim();
}

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

async function fetchWithTimeout(url, options = {}) {
  const timeoutMs =
    Number.isFinite(Number(options.timeoutMs)) && Number(options.timeoutMs) > 0
      ? Number(options.timeoutMs)
      : REQUEST_TIMEOUT_MS;
  const fetchOptions = { ...options };
  delete fetchOptions.timeoutMs;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      ...fetchOptions,
      signal: controller.signal,
      headers: {
        "user-agent": "fund-dashboard-local/1.0",
        accept: "text/html,application/json;q=0.9,*/*;q=0.8",
        ...(fetchOptions.headers || {}),
      },
    });
    return response;
  } finally {
    clearTimeout(timer);
  }
}

async function fetchText(url, retries = 2, requestOptions = {}) {
  let lastErr;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      const response = await fetchWithTimeout(url, requestOptions);
      if (!response.ok) {
        throw new Error(`HTTP ${response.status} for ${url}`);
      }
      return await response.text();
    } catch (error) {
      lastErr = error;
      if (attempt < retries) {
        await new Promise((resolve) => setTimeout(resolve, 400 * (attempt + 1)));
      }
    }
  }
  throw lastErr;
}

async function fetchJson(url, retries = 2, requestOptions = {}) {
  let lastErr;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      const response = await fetchWithTimeout(url, {
        ...requestOptions,
        headers: {
          accept: "application/json,text/javascript,*/*;q=0.9",
          ...((requestOptions && requestOptions.headers) || {}),
        },
      });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status} for ${url}`);
      }
      const text = await response.text();
      if (!text.trim()) return null;
      return JSON.parse(text);
    } catch (error) {
      lastErr = error;
      if (attempt < retries) {
        await new Promise((resolve) => setTimeout(resolve, 400 * (attempt + 1)));
      }
    }
  }
  throw lastErr;
}

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

function parseInvestfundsStructureDate(rawText) {
  const text = csvLikeText(rawText);
  if (!text) return null;
  const match = text.match(/(\d{2}\.\d{2}\.\d{4})/);
  if (!match) return null;
  return ruDateToIso(match[1]) || null;
}

function parseInvestfundsStructureFromHtml(html) {
  const $ = cheerio.load(html);
  const block = $("[data-modul='structure']").first();
  if (!block.length) return null;

  const structureDate = parseInvestfundsStructureDate(
    block.find(".middle_ttl").first().text()
  );

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

async function loadPreviousCompositionDataset() {
  try {
    const raw = await fs.readFile(COMPOSITION_CACHE_FILE, "utf8");
    const parsed = JSON.parse(raw);
    const items =
      parsed && Array.isArray(parsed.items)
        ? parsed.items.filter((item) => item && item.id != null)
        : [];
    const checksById =
      parsed && parsed.checksById && typeof parsed.checksById === "object"
        ? parsed.checksById
        : {};
    return { items, checksById };
  } catch {
    return { items: [], checksById: {} };
  }
}

async function fetchRankingFundIds({ force = false } = {}) {
  const hasFreshCache =
    !force &&
    Array.isArray(state.rankingIds) &&
    Date.now() - state.rankingIdsLoadedAt <= RANKING_SCAN_TTL_MS;
  if (hasFreshCache) return state.rankingIds;

  try {
    const html = await fetchText(`${BASE_URL}/fund-rankings/fund-yield/`);
    const ids = [...html.matchAll(/\/funds\/(\d+)\/?/g)]
      .map((m) => Number(m[1]))
      .filter((id) => Number.isFinite(id));
    state.rankingIds = [...new Set(ids)];
    state.rankingIdsLoadedAt = Date.now();
    return state.rankingIds;
  } catch (error) {
    console.warn(
      `[composition] Ranking scan failed: ${String(error.message || error)}`
    );
    if (Array.isArray(state.rankingIds)) return state.rankingIds;
    return [];
  }
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

function buildHoldingMap(items) {
  const map = new Map();
  for (const item of items || []) {
    const key = normalizeHoldingKey(item && item.name);
    const percent = toNumber(item && item.percent);
    if (!key || percent == null) continue;
    map.set(key, {
      name: normalizeHoldingName(item.name) || item.name,
      percent,
    });
  }
  return map;
}

function buildCompositionChanges(currentIssuers, previousIssuers, previousDate) {
  if (!previousDate || !Array.isArray(previousIssuers) || !previousIssuers.length) {
    return {
      previous_date: null,
      bought: [],
      sold: [],
    };
  }

  const currentMap = buildHoldingMap(currentIssuers);
  const previousMap = buildHoldingMap(previousIssuers);
  const keys = new Set([...currentMap.keys(), ...previousMap.keys()]);

  const bought = [];
  const sold = [];

  for (const key of keys) {
    const curr = currentMap.get(key);
    const prev = previousMap.get(key);
    const currentPercent = curr ? Number(curr.percent) : 0;
    const previousPercent = prev ? Number(prev.percent) : 0;
    const delta = round(currentPercent - previousPercent, 2);
    if (delta == null || Math.abs(delta) < 0.01) continue;

    const row = {
      name: (curr && curr.name) || (prev && prev.name) || key,
      delta: Math.abs(delta),
      current: round(currentPercent, 2),
      previous: round(previousPercent, 2),
    };
    if (delta > 0) bought.push(row);
    else sold.push(row);
  }

  bought.sort((a, b) => b.delta - a.delta);
  sold.sort((a, b) => b.delta - a.delta);

  return {
    previous_date: previousDate || null,
    bought,
    sold,
  };
}

function shouldRefreshCompositionCandidate(
  candidateId,
  previousById,
  checksById,
  { nowMs, forceRefresh }
) {
  const id = String(candidateId);
  const previous = previousById.get(id) || null;
  const check = checksById[id] || null;
  if (!check && !previous) return true;

  const checkedAt = (check && check.checked_at) || (previous && previous.checked_at);
  const checkedAge = ageDaysFromTimestamp(checkedAt, nowMs);

  // If never checked properly, refresh first.
  if (checkedAge == null) return true;

  if (forceRefresh) {
    // In forced mode we still do delta refresh, but with larger quota and faster recheck thresholds.
    if (check && check.has_structure === false) {
      return checkedAge >= Math.max(1, Math.floor(COMPOSITION_MISSING_RECHECK_DAYS / 2));
    }
    return checkedAge >= Math.max(1, Math.floor(COMPOSITION_ITEM_REFRESH_DAYS / 2));
  }

  if (check && check.has_structure === false) {
    return checkedAge >= COMPOSITION_MISSING_RECHECK_DAYS;
  }

  const structureDate =
    (check && check.structure_date) || (previous && previous.structure_date) || null;
  const structureAge = ageDaysFromIsoDate(structureDate);
  if (structureAge != null && structureAge >= COMPOSITION_ITEM_REFRESH_DAYS) {
    return true;
  }

  return checkedAge >= COMPOSITION_ITEM_REFRESH_DAYS;
}

function pickCompositionRefreshCandidates(
  candidates,
  previousById,
  checksById,
  { forceRefresh = false } = {}
) {
  if (!Array.isArray(candidates) || !candidates.length) return [];
  const nowMs = Date.now();
  const maxToRefresh = forceRefresh
    ? COMPOSITION_DELTA_MAX_FORCE_RUN
    : COMPOSITION_DELTA_MAX_PER_RUN;

  const ranked = [];
  for (const candidate of candidates) {
    const id = String(candidate.id);
    if (
      !shouldRefreshCompositionCandidate(id, previousById, checksById, {
        nowMs,
        forceRefresh,
      })
    ) {
      continue;
    }

    const previous = previousById.get(id) || null;
    const check = checksById[id] || null;
    const checkedAt = (check && check.checked_at) || (previous && previous.checked_at);
    const checkedAge = ageDaysFromTimestamp(checkedAt, nowMs);
    const structureDate =
      (check && check.structure_date) || (previous && previous.structure_date) || null;
    const structureAge = ageDaysFromIsoDate(structureDate);

    let score = 0;
    if (!check && !previous) {
      score = 1_000_000;
    } else if (check && check.has_structure === false) {
      score = 800_000 + (checkedAge || 0);
    } else {
      score = 600_000 + Math.max(checkedAge || 0, structureAge || 0);
    }

    ranked.push({ candidate, score });
  }

  ranked.sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    return String(a.candidate.id).localeCompare(String(b.candidate.id), "en");
  });

  return ranked.slice(0, Math.max(1, maxToRefresh)).map((entry) => entry.candidate);
}

async function buildCompositionsDataset({ forceRefresh = false } = {}) {
  const startedAt = Date.now();
  const runStats = {
    startedAt: new Date().toISOString(),
    mode: forceRefresh ? "forced-delta" : "delta",
    candidateTotal: 0,
    processed: 0,
    successCount: 0,
    noStructureCount: 0,
    failedCount: 0,
    errorSamples: [],
  };
  state.compositionsBuildState = runStats;
  state.compositionsLastError = null;

  const finalizeRun = ({ refreshedCount = 0 } = {}) => {
    const durationSec = Math.max(
      0,
      Math.round(((Date.now() - startedAt) / 1000) * 10) / 10
    );
    const snapshot = {
      startedAt: runStats.startedAt,
      completedAt: new Date().toISOString(),
      durationSec,
      mode: runStats.mode,
      candidateTotal: runStats.candidateTotal,
      processed: runStats.processed,
      successCount: runStats.successCount,
      noStructureCount: runStats.noStructureCount,
      failedCount: runStats.failedCount,
      refreshedCount,
      errorSamples: runStats.errorSamples.slice(0, 8),
    };
    state.compositionsLastRun = snapshot;
    return snapshot;
  };

  console.log(
    `[composition] Build started... mode=${forceRefresh ? "forced-delta" : "delta"}`
  );
  try {
    // Use current dataset snapshot without forcing market rebuild;
    // this keeps composition refresh lightweight on Render.
    await ensureDiskLoaded();
    const ds = state.dataset || (await getDataset());
    const rankingIds = await fetchRankingFundIds();
    const candidatesById = new Map(
      ds.funds.map((fund) => [
        String(fund.id),
        {
          id: Number(fund.id),
          name: fund.name || null,
          management_company: fund.management_company || null,
        },
      ])
    );
    for (const id of rankingIds) {
      const key = String(id);
      if (candidatesById.has(key)) continue;
      candidatesById.set(key, {
        id,
        name: null,
        management_company: null,
      });
    }
    const candidates = [...candidatesById.values()];

    const previousDataset = await loadPreviousCompositionDataset();
    const previousItems = previousDataset.items;
    const previousChecksById =
      previousDataset.checksById && typeof previousDataset.checksById === "object"
        ? previousDataset.checksById
        : {};
    const previousById = new Map(
      previousItems
        .filter((item) => item && item.id != null)
        .map((item) => [String(item.id), item])
    );

    const checksById = { ...previousChecksById };
    const refreshCandidates = pickCompositionRefreshCandidates(
      candidates,
      previousById,
      previousChecksById,
      { forceRefresh }
    );
    runStats.candidateTotal = refreshCandidates.length;

    if (!refreshCandidates.length && previousItems.length) {
      console.log("[composition] Delta build skipped: no due candidates.");
      const runSnapshot = finalizeRun({ refreshedCount: 0 });
      return {
        version: COMPOSITION_DATASET_VERSION,
        source: `${BASE_URL} + /fund-rankings/fund-yield/`,
        // We still store current check time even if no candidates were due,
        // so UI shows "last checked" instead of stale generation timestamp.
        generatedAt: new Date().toISOString(),
        fundUniverse: candidates.length,
        refreshedCount: 0,
        checksById,
        buildStats: runSnapshot,
        items: previousItems
          .slice()
          .sort((a, b) =>
            String(a.name || "").localeCompare(String(b.name || ""), "ru")
          ),
      };
    }

    console.log(
      `[composition] Delta candidates: ${refreshCandidates.length}/${candidates.length}`
    );

    const withStructure = await mapLimit(
      refreshCandidates,
      COMPOSITION_CONCURRENCY,
      async (candidate, idx) => {
        const candidateId = String(candidate.id);
        const previous = previousById.get(candidateId) || null;
        const checkedAt = new Date().toISOString();
        try {
          const html = await fetchText(
            `${BASE_URL}/funds/${candidate.id}/`,
            COMPOSITION_REQUEST_RETRIES,
            { timeoutMs: COMPOSITION_REQUEST_TIMEOUT_MS }
          );
          const structure = parseInvestfundsStructureFromHtml(html);
          if (!structure || !structure.issuers.length) {
            runStats.noStructureCount += 1;
            checksById[candidateId] = {
              checked_at: checkedAt,
              has_structure: false,
              structure_date: null,
            };
            return previous || null;
          }
          const parsedMeta = parseFundMetaFromDetailHtml(html);
          const changes = buildCompositionChanges(
            structure.issuers,
            previous && Array.isArray(previous.issuers) ? previous.issuers : [],
            previous ? previous.structure_date : null
          );
          checksById[candidateId] = {
            checked_at: checkedAt,
            has_structure: true,
            structure_date: structure.structure_date || null,
          };
          runStats.successCount += 1;

          return {
            id: String(candidate.id),
            source_id: candidate.id,
            name: candidate.name || parsedMeta.name || `Фонд ${candidate.id}`,
            management_company:
              candidate.management_company || parsedMeta.management_company || null,
            structure_date: structure.structure_date || null,
            checked_at: checkedAt,
            issuers: structure.issuers,
            changes,
          };
        } catch (error) {
          runStats.failedCount += 1;
          if (runStats.errorSamples.length < 8) {
            runStats.errorSamples.push({
              id: candidateId,
              message: String(error.message || error).slice(0, 240),
            });
          }
          console.warn(
            `[composition] Structure parse failed for ${candidate.id}: ${String(
              error.message || error
            )}`
          );
          checksById[candidateId] = {
            checked_at: checkedAt,
            has_structure:
              previous && Array.isArray(previous.issuers) && previous.issuers.length > 0,
            structure_date: previous ? previous.structure_date || null : null,
          };
          return previous || null;
        } finally {
          runStats.processed += 1;
          if ((idx + 1) % 20 === 0 || idx + 1 === refreshCandidates.length) {
            console.log(
              `[composition] Parsed ${Math.min(
                idx + 1,
                refreshCandidates.length
              )}/${refreshCandidates.length}`
            );
          }
        }
      },
      { continueOnError: true }
    );

    const refreshedById = new Map();
    for (const item of withStructure) {
      if (!item || item.id == null) continue;
      const id = String(item.id);
      refreshedById.set(id, item);
      if (!checksById[id]) {
        checksById[id] = {
          checked_at: item.checked_at || new Date().toISOString(),
          has_structure: Array.isArray(item.issuers) && item.issuers.length > 0,
          structure_date: item.structure_date || null,
        };
      }
    }

    const knownCandidateIds = new Set(candidates.map((c) => String(c.id)));
    const mergedById = new Map();

    // Keep previous snapshot as baseline.
    for (const [id, item] of previousById.entries()) {
      if (!item) continue;
      mergedById.set(id, item);
    }

    // Apply refreshed rows on top.
    for (const [id, item] of refreshedById.entries()) {
      mergedById.set(id, item);
    }

    // Remove items that are definitely no longer in active universe and ranking scan.
    for (const id of [...mergedById.keys()]) {
      if (!knownCandidateIds.has(id)) {
        mergedById.delete(id);
      }
    }

    const items = [...mergedById.values()]
      .filter((item) => item && Array.isArray(item.issuers) && item.issuers.length > 0)
      .sort((a, b) => String(a.name || "").localeCompare(String(b.name || ""), "ru"));

    const runSnapshot = finalizeRun({ refreshedCount: refreshCandidates.length });
    const dataset = {
      version: COMPOSITION_DATASET_VERSION,
      source: `${BASE_URL} + /fund-rankings/fund-yield/`,
      generatedAt: new Date().toISOString(),
      fundUniverse: candidates.length,
      refreshedCount: refreshCandidates.length,
      checksById,
      buildStats: runSnapshot,
      items,
    };

    await fs.mkdir(path.dirname(COMPOSITION_CACHE_FILE), { recursive: true });
    await fs.writeFile(COMPOSITION_CACHE_FILE, JSON.stringify(dataset), "utf8");

    const sec = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(
      `[composition] Build finished in ${sec}s. Refreshed: ${refreshCandidates.length}. Items: ${items.length}. Failed: ${runStats.failedCount}. Cache: ${COMPOSITION_CACHE_FILE}`
    );
    return dataset;
  } catch (error) {
    state.compositionsLastError = String(error.message || error);
    throw error;
  } finally {
    if (state.compositionsBuildState === runStats) {
      state.compositionsBuildState = null;
    }
  }
}

async function mapLimit(items, limit, worker, { continueOnError = false } = {}) {
  if (!items.length) return [];
  const capped = Math.max(1, Math.min(limit, items.length));
  const results = new Array(items.length);
  let cursor = 0;
  let fatalError = null;

  const workers = Array.from({ length: capped }, async () => {
    while (true) {
      if (fatalError) break;
      const i = cursor;
      cursor += 1;
      if (i >= items.length) break;
      try {
        results[i] = await worker(items[i], i);
      } catch (error) {
        if (continueOnError) {
          results[i] = null;
          continue;
        }
        fatalError = error;
        break;
      }
    }
  });

  await Promise.all(workers);
  if (fatalError) throw fatalError;
  return results;
}

function chunkArray(items, chunkSize) {
  const size = Math.max(1, Number(chunkSize) || 1);
  const chunks = [];
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }
  return chunks;
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

function pickInfoValue(infoMap, keys) {
  for (const key of keys) {
    const value = infoMap.get(key);
    if (value) return value;
  }
  return null;
}

function buildFallbackFundMeta(row) {
  return {
    id: row.id,
    source_id: row.id,
    ticker: null,
    fund_id: null,
    name: row.name || null,
    isin: null,
    management_company: row.management_company || null,
    fund_type: row.fund_type || "открытый",
    investment_type: row.investment_type || "Смешанный",
    specialization: row.specialization || row.category_raw || null,
    universe_source: row.universe_source || "main",
    cbonds_id: row.id,
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

async function fetchFundDetails(row) {
  const url = `${BASE_URL}/funds/${row.id}/`;
  const html = await fetchText(url);
  const $ = cheerio.load(html);

  const header = csvLikeText($(".widget_info_ttl").first().text());
  const parsed = parseHeaderMeta(header);
  const infoMap = parseDetailInfoMap($);

  const ticker =
    pickInfoValue(infoMap, ["тикер", "биржевой тикер"]) || parsed.ticker || null;
  const isin = pickInfoValue(infoMap, ["isin", "код isin"]) || null;
  const fundId =
    pickInfoValue(infoMap, [
      "номер регистрации",
      "регистрационный номер",
      "номер регистрации правил",
    ]) ||
    parsed.fund_id ||
    null;

  const managementCompany =
    row.management_company ||
    parsed.management_company ||
    csvLikeText($(".wdgt_img_logo img").first().attr("alt")) ||
    null;

  const name = row.name || parsed.name || null;

  const specialization =
    row.specialization ||
    infoMap.get("инвестиционная стратегия") ||
    infoMap.get("специализация") ||
    row.category_raw ||
    null;

  const fundTypeFromInfo = normalizeFundType(
    pickInfoValue(infoMap, ["тип фонда", "тип пиф", "вид фонда", "тип"]) || ""
  );
  const investmentTypeFromInfo = normalizeInvestmentType(
    pickInfoValue(infoMap, [
      "объект инвестирования",
      "тип активов",
      "инвестиционная стратегия",
      "категория",
      "категория фонда",
    ]) || ""
  );

  return {
    id: row.id,
    source_id: row.id,
    ticker,
    fund_id: fundId,
    name,
    isin,
    management_company: managementCompany,
    fund_type: row.fund_type || fundTypeFromInfo || "открытый",
    investment_type: row.investment_type || investmentTypeFromInfo || "Смешанный",
    specialization,
    universe_source: row.universe_source || "main",
    cbonds_id: row.id,
  };
}

function parseChartSeries(rawSeries) {
  const map = new Map();
  for (const point of rawSeries || []) {
    if (!Array.isArray(point) || point.length < 2) continue;
    const date = toIsoFromMs(point[0]);
    const value = toNumber(point[1]);
    if (!date || value == null) continue;
    map.set(date, value);
  }
  return [...map.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([time, value]) => ({ time, value }));
}

function buildChartUrl(fundIds, dataKey, dateFrom = "01.01.1990") {
  const routeFundId = fundIds[0];
  const params = new URLSearchParams();
  params.set("action", "chartData");
  params.set("data_key", dataKey);
  params.set("date_from", dateFrom);
  params.set("currencyId", "1");
  for (const id of fundIds) {
    params.append("ids[]", String(id));
  }
  return `${BASE_URL}/funds/${routeFundId}/?${params.toString()}`;
}

async function fetchFundSeriesBatch(
  fundIds,
  dataKey,
  { dateFrom = "01.01.1990" } = {}
) {
  const out = {};
  for (const id of fundIds) {
    out[String(id)] = [];
  }

  if (!fundIds.length) return out;

  const url = buildChartUrl(fundIds, dataKey, dateFrom);
  const payload = await fetchJson(
    url,
    SERIES_REQUEST_RETRIES,
    { timeoutMs: SERIES_REQUEST_TIMEOUT_MS }
  );
  const blocks = Array.isArray(payload)
    ? payload
    : payload && Array.isArray(payload.data)
      ? [payload]
      : [];

  if (blocks.length !== fundIds.length) {
    console.warn(
      `[data] Batch mismatch for ${dataKey}: requested=${fundIds.length}, got=${blocks.length}. Fallback to single requests.`
    );
    await mapLimit(
      fundIds,
      Math.max(1, Math.min(SERIES_SINGLE_FALLBACK_CONCURRENCY, fundIds.length)),
      async (fundId) => {
        try {
          const singlePayload = await fetchJson(
            buildChartUrl([fundId], dataKey, dateFrom),
            SERIES_REQUEST_RETRIES,
            { timeoutMs: SERIES_REQUEST_TIMEOUT_MS }
          );
          const singleBlocks = Array.isArray(singlePayload)
            ? singlePayload
            : singlePayload && Array.isArray(singlePayload.data)
              ? [singlePayload]
              : [];
          const singleRaw = singleBlocks[0] && Array.isArray(singleBlocks[0].data)
            ? singleBlocks[0].data
            : [];
          out[String(fundId)] = parseChartSeries(singleRaw);
        } catch (error) {
          console.warn(
            `[data] Single series fetch failed for ${dataKey}/${fundId}: ${String(
              error.message || error
            )}`
          );
          out[String(fundId)] = [];
        }
      },
      { continueOnError: true }
    );
    return out;
  }

  for (let i = 0; i < fundIds.length; i += 1) {
    const fundId = fundIds[i];
    const block = blocks[i];
    const rawSeries = block && Array.isArray(block.data) ? block.data : [];
    out[String(fundId)] = parseChartSeries(rawSeries);
  }
  return out;
}

function mergeFundSeries(paySeries, scaSeries) {
  const byDate = new Map();
  for (const p of paySeries) {
    if (!byDate.has(p.time)) byDate.set(p.time, { time: p.time });
    byDate.get(p.time).nav = p.value;
  }
  for (const p of scaSeries) {
    if (!byDate.has(p.time)) byDate.set(p.time, { time: p.time });
    byDate.get(p.time).aum = p.value;
  }

  const rows = [...byDate.values()].sort((a, b) => a.time.localeCompare(b.time));

  let prevShares = null;
  let prevAum = null;
  const merged = [];

  for (const row of rows) {
    const nav = row.nav != null ? Number(row.nav) : null;
    const aum = row.aum != null ? Number(row.aum) : null;

    let shares = null;
    if (nav != null && nav > 0 && aum != null) {
      shares = aum / nav;
    }

    let flow = null;
    if (shares != null && prevShares != null && nav != null) {
      flow = (shares - prevShares) * nav;
    } else if (aum != null && prevAum != null) {
      flow = aum - prevAum;
    }

    merged.push({
      time: row.time,
      nav: nav != null ? round(nav, 6) : null,
      aum: aum != null ? round(aum, 2) : null,
      shares: shares != null ? round(shares, 2) : null,
      flow: flow != null ? round(flow, 2) : null,
    });

    if (shares != null) prevShares = shares;
    if (aum != null) prevAum = aum;
  }

  return merged;
}

function mergeValueSeries(existingSeries, freshSeries) {
  const byDate = new Map();
  for (const point of existingSeries || []) {
    if (!point || !point.time || point.value == null) continue;
    byDate.set(point.time, point.value);
  }
  for (const point of freshSeries || []) {
    if (!point || !point.time || point.value == null) continue;
    byDate.set(point.time, point.value);
  }
  return [...byDate.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([time, value]) => ({ time, value }));
}

function extractSeriesFromHistory(history, key) {
  if (!Array.isArray(history) || !history.length) return [];
  return history
    .filter((point) => point && point.time && point[key] != null)
    .map((point) => ({ time: point.time, value: point[key] }));
}

function mergeHistoryIncremental(previousHistory, freshPaySeries, freshScaSeries) {
  if (!Array.isArray(previousHistory) || !previousHistory.length) {
    return mergeFundSeries(freshPaySeries, freshScaSeries);
  }
  const previousPay = extractSeriesFromHistory(previousHistory, "nav");
  const previousSca = extractSeriesFromHistory(previousHistory, "aum");
  const paySeries = mergeValueSeries(previousPay, freshPaySeries);
  const scaSeries = mergeValueSeries(previousSca, freshScaSeries);
  return mergeFundSeries(paySeries, scaSeries);
}

function historyLastDate(history) {
  if (!Array.isArray(history) || !history.length) return null;
  return history[history.length - 1].time || null;
}

function batchDateFrom(batchFundIds, previousHistoriesById) {
  let earliestIso = null;
  for (const fundId of batchFundIds) {
    const history = previousHistoriesById[String(fundId)] || [];
    const lastDate = historyLastDate(history);
    if (!lastDate || !/^\d{4}-\d{2}-\d{2}$/.test(lastDate)) return "01.01.1990";
    const lookbackDate = shiftIsoDate(lastDate, { days: -HISTORY_LOOKBACK_DAYS });
    if (!earliestIso || lookbackDate < earliestIso) earliestIso = lookbackDate;
  }
  return isoDateToRu(earliestIso) || "01.01.1990";
}

function findLastWithNav(history) {
  for (let i = history.length - 1; i >= 0; i -= 1) {
    if (history[i].nav != null) return history[i];
  }
  return null;
}

function findNavOnOrBefore(history, targetIsoDate) {
  for (let i = history.length - 1; i >= 0; i -= 1) {
    const point = history[i];
    if (point.time <= targetIsoDate && point.nav != null) return point.nav;
  }
  return null;
}

function computeReturn(history, lastDate, period) {
  let targetDate = null;
  switch (period) {
    case "1m":
      targetDate = shiftIsoDate(lastDate, { months: -1 });
      break;
    case "3m":
      targetDate = shiftIsoDate(lastDate, { months: -3 });
      break;
    case "6m":
      targetDate = shiftIsoDate(lastDate, { months: -6 });
      break;
    case "ytd": {
      const year = isoDateToUtcDate(lastDate).getUTCFullYear();
      targetDate = `${year}-01-01`;
      break;
    }
    case "1y":
      targetDate = shiftIsoDate(lastDate, { years: -1 });
      break;
    case "3y":
      targetDate = shiftIsoDate(lastDate, { years: -3 });
      break;
    case "5y":
      targetDate = shiftIsoDate(lastDate, { years: -5 });
      break;
    default:
      return null;
  }

  const endNav = findNavOnOrBefore(history, lastDate);
  const startNav = findNavOnOrBefore(history, targetDate);
  if (endNav == null || startNav == null || startNav <= 0) return null;
  return round(((endNav / startNav) - 1) * 100, 4);
}

function buildReturnsRow(fundMeta, history) {
  const last = findLastWithNav(history);
  const lastAum = [...history].reverse().find((p) => p.aum != null) || null;
  const lastDate = (last || lastAum || {}).time || null;

  if (!lastDate) {
    return {
      id: fundMeta.id,
      ticker: fundMeta.ticker,
      fund_id: fundMeta.fund_id,
      name: fundMeta.name,
      mc: fundMeta.management_company,
      fund_type: fundMeta.fund_type,
      inv_type: fundMeta.investment_type,
      spec: fundMeta.specialization,
      nav: null,
      aum: null,
      date: null,
      "1m": null,
      "3m": null,
      "6m": null,
      ytd: null,
      "1y": null,
      "3y": null,
      "5y": null,
    };
  }

  const latestNav = findNavOnOrBefore(history, lastDate);
  const latestAumPoint = [...history].reverse().find((p) => p.time <= lastDate && p.aum != null);
  const latestAum = latestAumPoint ? latestAumPoint.aum : null;

  return {
    id: fundMeta.id,
    ticker: fundMeta.ticker,
    fund_id: fundMeta.fund_id,
    name: fundMeta.name,
    mc: fundMeta.management_company,
    fund_type: fundMeta.fund_type,
    inv_type: fundMeta.investment_type,
    spec: fundMeta.specialization,
    nav: latestNav != null ? round(latestNav, 6) : null,
    aum: latestAum != null ? round(latestAum, 2) : null,
    date: lastDate,
    "1m": computeReturn(history, lastDate, "1m"),
    "3m": computeReturn(history, lastDate, "3m"),
    "6m": computeReturn(history, lastDate, "6m"),
    ytd: computeReturn(history, lastDate, "ytd"),
    "1y": computeReturn(history, lastDate, "1y"),
    "3y": computeReturn(history, lastDate, "3y"),
    "5y": computeReturn(history, lastDate, "5y"),
  };
}

function getLatestAum(history) {
  for (let i = history.length - 1; i >= 0; i -= 1) {
    if (history[i].aum != null) return history[i].aum;
  }
  return 0;
}

function buildTimeseriesForGroups(funds, historiesById, getGroupName) {
  const dateSet = new Set();
  for (const fund of funds) {
    const history = historiesById[String(fund.id)] || [];
    for (const p of history) {
      if (p.aum != null || p.flow != null) dateSet.add(p.time);
    }
  }

  const dates = [...dateSet].sort((a, b) => a.localeCompare(b));
  const index = new Map(dates.map((d, i) => [d, i]));
  const data = {};

  for (const fund of funds) {
    const history = historiesById[String(fund.id)] || [];
    const groupName = getGroupName(fund);
    if (!groupName) continue;

    if (!data[groupName]) {
      data[groupName] = {
        aum: new Array(dates.length).fill(0),
        flow: new Array(dates.length).fill(0),
      };
    }

    const aumByDate = new Map();
    for (const point of history) {
      if (point.aum != null) aumByDate.set(point.time, point.aum);
    }
    let lastAum = null;
    for (let i = 0; i < dates.length; i += 1) {
      const date = dates[i];
      if (aumByDate.has(date)) lastAum = aumByDate.get(date);
      if (lastAum != null) data[groupName].aum[i] += lastAum;
    }

    for (const point of history) {
      const idx = index.get(point.time);
      if (idx == null) continue;
      if (point.flow != null) data[groupName].flow[idx] += point.flow;
    }
  }

  return { dates, data };
}

function parseFundTypeQuery(raw) {
  const q = normalizeLower(raw || "all");
  if (!q || q === "all") return new Set(["открытый", "биржевой"]);
  const set = new Set();
  if (q.includes("opif")) set.add("открытый");
  if (q.includes("bpif")) set.add("биржевой");
  if (!set.size) {
    set.add("открытый");
    set.add("биржевой");
  }
  return set;
}

function filterByFundType(funds, rawFundType) {
  const allowed = parseFundTypeQuery(rawFundType);
  return funds.filter((f) => allowed.has(f.fund_type));
}

function isPrimaryFund(fund) {
  return String(fund && fund.universe_source ? fund.universe_source : "main") !==
    "ranking-extra";
}

function pickTopGroupsByAum(funds, historiesById, groupKeySelector, topN = TOP_GROUP_COUNT) {
  const sums = new Map();
  for (const fund of funds) {
    const key = groupKeySelector(fund);
    if (!key) continue;
    const latest = getLatestAum(historiesById[String(fund.id)] || []);
    sums.set(key, (sums.get(key) || 0) + latest);
  }
  return [...sums.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, topN)
    .map(([key]) => key);
}

async function buildDataset() {
  const startedAt = Date.now();
  const runStats = {
    startedAt: new Date().toISOString(),
    stage: "list-first-page",
    listPages: 0,
    listPagesParsed: 0,
    sourceFunds: 0,
    selectedFunds: 0,
    rankingAdded: 0,
    candidates: 0,
    detailsTotal: 0,
    detailsProcessed: 0,
    detailFetchedCount: 0,
    detailReusedCount: 0,
    detailFailedCount: 0,
    historyBatchesTotal: 0,
    historyBatchesDone: 0,
    historyBatchFailed: 0,
    historyFundsTotal: 0,
    historyFundsDone: 0,
  };
  state.marketBuildState = runStats;
  state.marketLastError = null;

  const finalizeRun = () => {
    const durationSec = Math.max(
      0,
      Math.round(((Date.now() - startedAt) / 1000) * 10) / 10
    );
    const snapshot = {
      startedAt: runStats.startedAt,
      completedAt: new Date().toISOString(),
      durationSec,
      stage: runStats.stage,
      listPages: runStats.listPages,
      listPagesParsed: runStats.listPagesParsed,
      sourceFunds: runStats.sourceFunds,
      selectedFunds: runStats.selectedFunds,
      rankingAdded: runStats.rankingAdded,
      candidates: runStats.candidates,
      detailsTotal: runStats.detailsTotal,
      detailsProcessed: runStats.detailsProcessed,
      detailFetchedCount: runStats.detailFetchedCount,
      detailReusedCount: runStats.detailReusedCount,
      detailFailedCount: runStats.detailFailedCount,
      historyBatchesTotal: runStats.historyBatchesTotal,
      historyBatchesDone: runStats.historyBatchesDone,
      historyBatchFailed: runStats.historyBatchFailed,
      historyFundsTotal: runStats.historyFundsTotal,
      historyFundsDone: runStats.historyFundsDone,
    };
    state.marketLastRun = snapshot;
    return snapshot;
  };

  console.log("[data] Build started...");
  try {
    const firstHtml = await fetchText(`${BASE_URL}/funds/?showID=54&limit=50&page=1`);
    const firstParsed = parseListPage(firstHtml);
    runStats.listPagesParsed = 1;
    const maxPage = Math.min(firstParsed.maxPage || 1, MAX_LIST_PAGES);
    runStats.listPages = maxPage;
    runStats.stage = "list-pages";
    console.log(`[data] List pages: ${maxPage}`);

    const rawRows = [...firstParsed.rows];
    const pages = Array.from({ length: Math.max(0, maxPage - 1) }, (_, i) => i + 2);
    if (pages.length) {
      const pageRows = await mapLimit(
        pages,
        LIST_CONCURRENCY,
        async (page, i) => {
          const html = await fetchText(
            `${BASE_URL}/funds/?showID=54&limit=50&page=${page}`
          );
          const parsed = parseListPage(html);
          runStats.listPagesParsed += 1;
          if ((i + 1) % 10 === 0 || i + 1 === pages.length) {
            console.log(`[data] Parsed list page ${page}/${maxPage}`);
          }
          return parsed.rows;
        }
      );
      for (const rows of pageRows) {
        if (Array.isArray(rows)) rawRows.push(...rows);
      }
    }

    const byId = new Map();
    for (const row of rawRows) {
      if (!row.id) continue;
      if (!byId.has(row.id)) byId.set(row.id, row);
    }
    const uniqueRows = [...byId.values()];
    runStats.sourceFunds = uniqueRows.length;
    console.log(`[data] Funds in source list: ${uniqueRows.length}`);

    const selectedRows = uniqueRows
      .filter(shouldIncludeFund)
      .map((row) => ({ ...row, universe_source: "main" }));
    runStats.selectedFunds = selectedRows.length;
    console.log(`[data] Selected open/exchange + formed: ${selectedRows.length}`);

    const rankingIds = await fetchRankingFundIds();
    const selectedById = new Map(selectedRows.map((row) => [String(row.id), row]));
    let rankingAdded = 0;
    for (const id of rankingIds) {
      const key = String(id);
      if (selectedById.has(key)) continue;
      selectedById.set(key, {
        id,
        name: null,
        detail_path: `/funds/${id}/`,
        fund_type_raw: null,
        management_company: null,
        status_raw: "сформирован",
        category_raw: null,
        investment_object_raw: null,
        specialization_raw: null,
        nav_date_raw: null,
        aum_mln_raw: null,
        fund_type: "открытый",
        status: "сформирован",
        investment_type: "Смешанный",
        specialization: null,
        universe_source: "ranking-extra",
        nav_date: null,
        aum_mln: null,
      });
      rankingAdded += 1;
    }
    runStats.rankingAdded = rankingAdded;
    const candidateRows = [...selectedById.values()];
    runStats.candidates = candidateRows.length;
    runStats.detailsTotal = candidateRows.length;
    runStats.stage = "details";
    console.log(`[data] Added from ranking scan: ${rankingAdded}`);
    console.log(`[data] Total candidates for details: ${candidateRows.length}`);

    const previousFunds =
      state.dataset && Array.isArray(state.dataset.funds) ? state.dataset.funds : [];
    const previousHistoriesById =
      state.dataset && state.dataset.historiesById ? state.dataset.historiesById : {};
    const previousById = new Map(previousFunds.map((f) => [f.id, f]));

    const detailed = await mapLimit(
      candidateRows,
      DETAIL_CONCURRENCY,
      async (row, i) => {
        try {
          const previous = previousById.get(row.id);
          if (previous && (previous.ticker || previous.fund_id || previous.isin)) {
            runStats.detailReusedCount += 1;
            return {
              ...buildFallbackFundMeta(row),
              ticker: previous.ticker || null,
              fund_id: previous.fund_id || null,
              name: row.name || previous.name || null,
              isin: previous.isin || null,
              management_company:
                row.management_company || previous.management_company || null,
              specialization:
                row.specialization || previous.specialization || row.category_raw || null,
            };
          }

          let meta = null;
          try {
            meta = await fetchFundDetails(row);
            runStats.detailFetchedCount += 1;
          } catch (error) {
            runStats.detailFailedCount += 1;
            console.warn(
              `[data] Fund card parse failed for ${row.id}: ${String(
                error.message || error
              )}`
            );
            meta = buildFallbackFundMeta(row);
          }
          if ((i + 1) % 25 === 0 || i + 1 === candidateRows.length) {
            console.log(`[data] Parsed fund card ${i + 1}/${candidateRows.length}`);
          }
          return meta;
        } finally {
          runStats.detailsProcessed += 1;
        }
      }
    );

    const funds = detailed.filter(Boolean);
    runStats.detailsTotal = funds.length;
    console.log(`[data] Detailed funds: ${funds.length}`);
    console.log(
      `[data] Fund cards fetched: ${runStats.detailFetchedCount}, reused from cache: ${runStats.detailReusedCount}, failed: ${runStats.detailFailedCount}`
    );

    const historiesById = {};
    const fundById = new Map(funds.map((fund) => [fund.id, fund]));
    const fundIdsWithHistory = [];
    const fundIdsWithoutHistory = [];
    for (const fund of funds) {
      const prevHistory = previousHistoriesById[String(fund.id)] || [];
      if (prevHistory.length) {
        fundIdsWithHistory.push(fund.id);
      } else {
        fundIdsWithoutHistory.push(fund.id);
      }
    }
    const batches = [
      ...chunkArray(fundIdsWithHistory, HISTORY_BATCH_SIZE),
      ...chunkArray(fundIdsWithoutHistory, HISTORY_BATCH_SIZE),
    ];
    runStats.stage = "histories";
    runStats.historyBatchesTotal = batches.length;
    runStats.historyFundsTotal = funds.length;
    console.log(
      `[data] History mode: incremental=${fundIdsWithHistory.length}, full=${fundIdsWithoutHistory.length}`
    );
    let loadedFunds = 0;

    await mapLimit(
      batches,
      HISTORY_BATCH_CONCURRENCY,
      async (batchFundIds, batchIndex) => {
        try {
          const dateFrom = batchDateFrom(batchFundIds, previousHistoriesById);
          const [payById, scaById] = await Promise.all([
            fetchFundSeriesBatch(batchFundIds, "pay", { dateFrom }),
            fetchFundSeriesBatch(batchFundIds, "sca", { dateFrom }),
          ]);

          for (const fundId of batchFundIds) {
            const paySeries = payById[String(fundId)] || [];
            const scaSeries = scaById[String(fundId)] || [];
            const previousHistory = previousHistoriesById[String(fundId)] || [];
            const history = mergeHistoryIncremental(previousHistory, paySeries, scaSeries);
            historiesById[String(fundId)] = history;

            const fund = fundById.get(fundId);
            if (!fund) continue;

            const last = findLastWithNav(history);
            const lastAumPoint = [...history].reverse().find((p) => p.aum != null) || null;
            fund.latest_nav = last ? last.nav : null;
            fund.latest_aum = lastAumPoint ? lastAumPoint.aum : null;
            fund.latest_date = (last || lastAumPoint || {}).time || null;
          }
        } catch (error) {
          runStats.historyBatchFailed += 1;
          console.warn(
            `[data] History batch failed (${batchIndex + 1}/${batches.length}): ${String(
              error.message || error
            )}`
          );
          // Keep previous history for this batch to avoid full-build failure.
          for (const fundId of batchFundIds) {
            const previousHistory = previousHistoriesById[String(fundId)] || [];
            historiesById[String(fundId)] = previousHistory;
            const fund = fundById.get(fundId);
            if (!fund) continue;
            const last = findLastWithNav(previousHistory);
            const lastAumPoint =
              [...previousHistory].reverse().find((p) => p.aum != null) || null;
            fund.latest_nav = last ? last.nav : null;
            fund.latest_aum = lastAumPoint ? lastAumPoint.aum : null;
            fund.latest_date = (last || lastAumPoint || {}).time || null;
          }
        } finally {
          runStats.historyBatchesDone += 1;
          runStats.historyFundsDone += batchFundIds.length;
          loadedFunds += batchFundIds.length;
          console.log(
            `[data] Loaded history batch ${batchIndex + 1}/${batches.length} (${Math.min(
              loadedFunds,
              funds.length
            )}/${funds.length})`
          );
        }
      },
      { continueOnError: true }
    );

    runStats.stage = "returns";
    const returns = funds.map((fund) =>
      buildReturnsRow(fund, historiesById[String(fund.id)] || [])
    );

    const runSnapshot = finalizeRun();
    const dataset = {
      version: DATASET_VERSION,
      source: BASE_URL,
      generatedAt: new Date().toISOString(),
      funds,
      returns,
      historiesById,
      buildStats: runSnapshot,
    };

    await fs.mkdir(path.dirname(CACHE_FILE), { recursive: true });
    await fs.writeFile(CACHE_FILE, JSON.stringify(dataset), "utf8");

    const sec = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(
      `[data] Build finished in ${sec}s. Funds=${funds.length}. Detail failed=${runStats.detailFailedCount}. History failed batches=${runStats.historyBatchFailed}. Cache: ${CACHE_FILE}`
    );
    return dataset;
  } catch (error) {
    state.marketLastError = String(error.message || error);
    throw error;
  } finally {
    if (state.marketBuildState === runStats) {
      state.marketBuildState = null;
    }
  }
}

function isValidMainDataset(data) {
  return Boolean(
    data &&
      Number(data.version) === DATASET_VERSION &&
      Array.isArray(data.funds) &&
      Array.isArray(data.returns) &&
      data.historiesById &&
      typeof data.historiesById === "object"
  );
}

async function ensureDiskLoaded() {
  if (state.diskLoaded) return;
  state.diskLoaded = true;
  try {
    const raw = await fs.readFile(CACHE_FILE, "utf8");
    const data = JSON.parse(raw);
    if (isValidMainDataset(data)) {
      state.dataset = data;
      state.loadedAt = datasetTimestampMs(data);
      console.log(`[data] Loaded cache from disk (${CACHE_FILE})`);
    }
  } catch {
    // Cache file may not exist on first run
  }

  if (!state.dataset) {
    const seedData = await readSeedJsonGz(DATASET_SEED_GZ_FILE);
    if (isValidMainDataset(seedData)) {
      state.dataset = seedData;
      state.loadedAt = datasetTimestampMs(seedData);
      console.log(`[data] Loaded seed cache (${DATASET_SEED_GZ_FILE})`);
      persistCacheSnapshot(CACHE_FILE, seedData);
    }
  }
}

async function getDataset() {
  await ensureDiskLoaded();

  const startDatasetBuild = ({ resetDailyMarkOnFail = null } = {}) => {
    if (state.buildPromise) return state.buildPromise;
    state.buildPromise = buildDataset()
      .then((data) => {
        state.dataset = data;
        state.loadedAt = datasetTimestampMs(data);
        return data;
      })
      .catch((error) => {
        if (
          resetDailyMarkOnFail &&
          state.datasetAutoRefreshDate === resetDailyMarkOnFail
        ) {
          state.datasetAutoRefreshDate = null;
        }
        throw error;
      })
      .finally(() => {
        state.buildPromise = null;
      });
    return state.buildPromise;
  };

  const maybeRunDailyNoonRefresh = () => {
    if (DAILY_MAIN_REFRESH_HOUR_MSK < 0) return;
    if (!state.dataset) return;
    const todayMsk = mskDate();
    const nowHourMsk = mskHour();
    if (nowHourMsk == null || nowHourMsk < DAILY_MAIN_REFRESH_HOUR_MSK) return;
    if (state.datasetAutoRefreshDate === todayMsk) return;

    const generatedTodayMsk =
      state.dataset.generatedAt &&
      mskDate(new Date(state.dataset.generatedAt)) === todayMsk;
    if (
      generatedTodayMsk &&
      isGeneratedAfterMskHour(state.dataset.generatedAt, DAILY_MAIN_REFRESH_HOUR_MSK)
    ) {
      state.datasetAutoRefreshDate = todayMsk;
      return;
    }

    state.datasetAutoRefreshDate = todayMsk;
    startDatasetBuild({ resetDailyMarkOnFail: todayMsk }).catch((error) => {
      console.warn(
        `[data] Daily auto-refresh failed: ${String(error.message || error)}`
      );
    });
    console.log(
      `[data] Daily auto-refresh started (MSK ${DAILY_MAIN_REFRESH_HOUR_MSK}:00)`
    );
  };

  if (state.dataset) {
    const isStale = Date.now() - state.loadedAt > CACHE_TTL_MS;
    if (isStale) startDatasetBuild();
    maybeRunDailyNoonRefresh();
    return state.dataset;
  }

  return startDatasetBuild();
}

function isValidCompositionDataset(data) {
  return Boolean(
    data &&
      Number(data.version) === COMPOSITION_DATASET_VERSION &&
      Array.isArray(data.items)
  );
}

async function ensureCompositionDiskLoaded() {
  if (state.compositionsDiskLoaded) return;
  state.compositionsDiskLoaded = true;
  try {
    const raw = await fs.readFile(COMPOSITION_CACHE_FILE, "utf8");
    const data = JSON.parse(raw);
    if (isValidCompositionDataset(data)) {
      state.compositions = data;
      state.compositionsLoadedAt = datasetTimestampMs(data);
      console.log(
        `[composition] Loaded cache from disk (${COMPOSITION_CACHE_FILE})`
      );
    }
  } catch {
    // Cache file may not exist on first run
  }

  if (!state.compositions) {
    const seedData = await readSeedJsonGz(COMPOSITION_SEED_GZ_FILE);
    if (isValidCompositionDataset(seedData)) {
      state.compositions = seedData;
      state.compositionsLoadedAt = datasetTimestampMs(seedData);
      console.log(
        `[composition] Loaded seed cache (${COMPOSITION_SEED_GZ_FILE})`
      );
      persistCacheSnapshot(COMPOSITION_CACHE_FILE, seedData);
    }
  }
}

async function getCompositionsDataset({ forceRefresh = false } = {}) {
  await ensureCompositionDiskLoaded();

  const startCompositionsBuild = ({
    force = false,
    resetDailyMarkOnFail = null,
  } = {}) => {
    if (state.compositionsBuildPromise) return state.compositionsBuildPromise;
    state.compositionsBuildPromise = buildCompositionsDataset({ forceRefresh: force })
      .then((data) => {
        state.compositions = data;
        state.compositionsLoadedAt = datasetTimestampMs(data);
        return data;
      })
      .catch((error) => {
        if (
          resetDailyMarkOnFail &&
          state.compositionsAutoRefreshDate === resetDailyMarkOnFail
        ) {
          state.compositionsAutoRefreshDate = null;
        }
        throw error;
      })
      .finally(() => {
        state.compositionsBuildPromise = null;
      });
    return state.compositionsBuildPromise;
  };

  if (forceRefresh) {
    // Stale-while-revalidate mode:
    // if we already have snapshot, return it immediately and refresh in background.
    if (state.compositions) {
      startCompositionsBuild({ force: true });
      return state.compositions;
    }
    return startCompositionsBuild({ force: true });
  }

  const maybeRunDailyNoonRefresh = () => {
    if (DAILY_COMPOSITION_REFRESH_HOUR_MSK < 0) return;
    if (!state.compositions) return;
    const todayMsk = mskDate();
    const nowHourMsk = mskHour();
    if (nowHourMsk == null || nowHourMsk < DAILY_COMPOSITION_REFRESH_HOUR_MSK) return;
    if (state.compositionsAutoRefreshDate === todayMsk) return;

    const generatedTodayMsk =
      state.compositions.generatedAt &&
      mskDate(new Date(state.compositions.generatedAt)) === todayMsk;
    if (
      generatedTodayMsk &&
      isGeneratedAfterMskHour(
        state.compositions.generatedAt,
        DAILY_COMPOSITION_REFRESH_HOUR_MSK
      )
    ) {
      state.compositionsAutoRefreshDate = todayMsk;
      return;
    }

    state.compositionsAutoRefreshDate = todayMsk;
    startCompositionsBuild({
      // Daily run should stay lightweight (regular delta mode).
      // Forced mode is still available via manual refresh button.
      force: false,
      resetDailyMarkOnFail: todayMsk,
    }).catch((error) => {
      console.warn(
        `[composition] Daily auto-refresh failed: ${String(error.message || error)}`
      );
    });
    console.log(
      `[composition] Daily auto-refresh started (MSK ${DAILY_COMPOSITION_REFRESH_HOUR_MSK}:00)`
    );
  };

  if (state.compositions) {
    const isStale = Date.now() - state.compositionsLoadedAt > COMPOSITION_CACHE_TTL_MS;
    if (isStale) startCompositionsBuild();
    maybeRunDailyNoonRefresh();
    return state.compositions;
  }

  return startCompositionsBuild();
}

function filterHistoryByRange(history, from, to) {
  if (!from && !to) return history;
  return history.filter((p) => {
    if (from && p.time < from) return false;
    if (to && p.time > to) return false;
    return true;
  });
}

async function getFunds() {
  const ds = await getDataset();
  return ds.funds.map((f) => ({
    id: f.id,
    ticker: f.ticker || null,
    fund_id: f.fund_id || null,
    name: f.name,
    isin: f.isin || null,
    management_company: f.management_company || null,
    fund_type: f.fund_type,
    investment_type: f.investment_type,
    specialization: f.specialization || null,
    cbonds_id: f.cbonds_id || f.id,
  }));
}

async function getReturns() {
  const ds = await getDataset();
  return ds.returns;
}

async function getFundCompositions(forceRefresh = false) {
  const ds = await getCompositionsDataset({ forceRefresh });
  return ds.items;
}

function buildCompositionsMeta(ds) {
  const items = ds && Array.isArray(ds.items) ? ds.items : [];
  const now = new Date();
  let staleCount = 0;
  let maxAgeDays = null;
  let latestStructureDate = null;

  for (const item of items) {
    const date = item && item.structure_date ? String(item.structure_date) : null;
    if (!date) continue;
    const ageDays = ageDaysFromIsoDate(date, now);
    if (ageDays == null) continue;
    if (ageDays >= STALE_MARK_DAYS) staleCount += 1;
    if (maxAgeDays == null || ageDays > maxAgeDays) maxAgeDays = ageDays;
    if (!latestStructureDate || date > latestStructureDate) latestStructureDate = date;
  }

  const dsBuildStats =
    ds && ds.buildStats && typeof ds.buildStats === "object" ? ds.buildStats : null;
  const buildState =
    state.compositionsBuildState && typeof state.compositionsBuildState === "object"
      ? state.compositionsBuildState
      : null;
  const lastRun =
    state.compositionsLastRun && typeof state.compositionsLastRun === "object"
      ? state.compositionsLastRun
      : dsBuildStats;

  const progress = buildState
    ? {
        startedAt: buildState.startedAt || null,
        mode: buildState.mode || null,
        candidateTotal: Number(buildState.candidateTotal) || 0,
        processed: Number(buildState.processed) || 0,
        successCount: Number(buildState.successCount) || 0,
        noStructureCount: Number(buildState.noStructureCount) || 0,
        failedCount: Number(buildState.failedCount) || 0,
        errorSamples: Array.isArray(buildState.errorSamples)
          ? buildState.errorSamples.slice(0, 8)
          : [],
      }
    : null;

  const failedFunds = progress
    ? progress.failedCount
    : lastRun && Number.isFinite(Number(lastRun.failedCount))
      ? Number(lastRun.failedCount)
      : 0;

  return {
    generatedAt: ds && ds.generatedAt ? ds.generatedAt : null,
    fundUniverse:
      ds && Number.isFinite(Number(ds.fundUniverse)) ? Number(ds.fundUniverse) : null,
    itemsCount: items.length,
    refreshedCount:
      ds && Number.isFinite(Number(ds.refreshedCount)) ? Number(ds.refreshedCount) : null,
    staleMarkDays: STALE_MARK_DAYS,
    staleCount,
    maxAgeDays,
    latestStructureDate: latestStructureDate || null,
    refreshing: Boolean(state.compositionsBuildPromise),
    progress,
    failedFunds,
    lastError: state.compositionsLastError || null,
    lastRun:
      lastRun && typeof lastRun === "object"
        ? {
            startedAt: lastRun.startedAt || null,
            completedAt: lastRun.completedAt || null,
            durationSec:
              Number.isFinite(Number(lastRun.durationSec)) && Number(lastRun.durationSec) >= 0
                ? Number(lastRun.durationSec)
                : null,
            mode: lastRun.mode || null,
            candidateTotal: Number(lastRun.candidateTotal) || 0,
            processed: Number(lastRun.processed) || 0,
            successCount: Number(lastRun.successCount) || 0,
            noStructureCount: Number(lastRun.noStructureCount) || 0,
            failedCount: Number(lastRun.failedCount) || 0,
            refreshedCount: Number(lastRun.refreshedCount) || 0,
            errorSamples: Array.isArray(lastRun.errorSamples)
              ? lastRun.errorSamples.slice(0, 8)
              : [],
          }
        : null,
  };
}

async function getCompositionsPayload({ forceRefresh = false } = {}) {
  const ds = await getCompositionsDataset({ forceRefresh });
  return {
    items: Array.isArray(ds.items) ? ds.items : [],
    meta: buildCompositionsMeta(ds),
  };
}

async function getStatusSummary() {
  const [mainDs, compDs] = await Promise.all([
    getDataset(),
    getCompositionsDataset({ forceRefresh: false }),
  ]);

  const mainGeneratedAt = mainDs && mainDs.generatedAt ? mainDs.generatedAt : null;
  const mainAgeDays = ageDaysFromTimestamp(mainGeneratedAt);
  const marketBuildState =
    state.marketBuildState && typeof state.marketBuildState === "object"
      ? state.marketBuildState
      : null;
  const marketLastRun =
    state.marketLastRun && typeof state.marketLastRun === "object"
      ? state.marketLastRun
      : mainDs && mainDs.buildStats && typeof mainDs.buildStats === "object"
        ? mainDs.buildStats
        : null;

  return {
    ok: true,
    source: mainDs && mainDs.source ? mainDs.source : BASE_URL,
    funds:
      mainDs && Array.isArray(mainDs.funds) ? mainDs.funds.length : 0,
    generatedAt: mainGeneratedAt,
    generatedAgeDays: mainAgeDays,
    staleMarkDays: STALE_MARK_DAYS,
    refreshing: {
      market: Boolean(state.buildPromise),
      compositions: Boolean(state.compositionsBuildPromise),
    },
    market: {
      generatedAt: mainGeneratedAt,
      generatedAgeDays: mainAgeDays,
      refreshing: Boolean(state.buildPromise),
      progress: marketBuildState
        ? {
            stage: marketBuildState.stage || null,
            detailsTotal: Number(marketBuildState.detailsTotal) || 0,
            detailsProcessed: Number(marketBuildState.detailsProcessed) || 0,
            detailFailedCount: Number(marketBuildState.detailFailedCount) || 0,
            historyBatchesTotal: Number(marketBuildState.historyBatchesTotal) || 0,
            historyBatchesDone: Number(marketBuildState.historyBatchesDone) || 0,
            historyBatchFailed: Number(marketBuildState.historyBatchFailed) || 0,
            historyFundsTotal: Number(marketBuildState.historyFundsTotal) || 0,
            historyFundsDone: Number(marketBuildState.historyFundsDone) || 0,
          }
        : null,
      failedFunds: marketBuildState
        ? Number(marketBuildState.detailFailedCount) || 0
        : marketLastRun
          ? Number(marketLastRun.detailFailedCount) || 0
          : 0,
      failedHistoryBatches: marketBuildState
        ? Number(marketBuildState.historyBatchFailed) || 0
        : marketLastRun
          ? Number(marketLastRun.historyBatchFailed) || 0
          : 0,
      lastError: state.marketLastError || null,
      lastRun: marketLastRun || null,
    },
    compositions: buildCompositionsMeta(compDs),
  };
}

async function getNavByFundId(fundId, from, to) {
  const ds = await getDataset();
  const history = ds.historiesById[String(fundId)];
  if (!history) return null;
  return filterHistoryByRange(history, from, to);
}

async function getMarketData(rawFundType) {
  const ds = await getDataset();
  const primaryFunds = ds.funds.filter(isPrimaryFund);
  const filteredFunds = filterByFundType(primaryFunds, rawFundType);

  const byType = buildTimeseriesForGroups(
    filteredFunds,
    ds.historiesById,
    (f) => f.investment_type || "Смешанный"
  );

  const topMCs = pickTopGroupsByAum(
    filteredFunds,
    ds.historiesById,
    (f) => f.management_company || "Прочие"
  );
  const topSet = new Set(topMCs);

  const byMC = buildTimeseriesForGroups(
    filteredFunds,
    ds.historiesById,
    (f) => {
      const mc = f.management_company || "Прочие";
      return topSet.has(mc) ? mc : "Прочие";
    }
  );

  return { byType, byMC, topMCs };
}

async function getMCDetail(mcName, rawFundType) {
  const ds = await getDataset();
  const selectedMc = normalizeSpace(mcName || "");
  if (!selectedMc) {
    return { byType: { dates: [], data: {} }, byFund: { dates: [], data: {} }, topFundNames: [] };
  }

  const primaryFunds = ds.funds.filter(isPrimaryFund);
  const filteredByType = filterByFundType(primaryFunds, rawFundType);
  const mcFunds = filteredByType.filter(
    (f) => normalizeSpace(f.management_company) === selectedMc
  );

  const byType = buildTimeseriesForGroups(
    mcFunds,
    ds.historiesById,
    (f) => f.investment_type || "Смешанный"
  );

  const topFundNames = [...mcFunds]
    .map((f) => ({
      name: f.name,
      aum: getLatestAum(ds.historiesById[String(f.id)] || []),
    }))
    .sort((a, b) => b.aum - a.aum)
    .slice(0, TOP_GROUP_COUNT)
    .map((x) => x.name);

  const topSet = new Set(topFundNames);
  const byFund = buildTimeseriesForGroups(
    mcFunds,
    ds.historiesById,
    (f) => (topSet.has(f.name) ? f.name : "Прочие")
  );

  return { byType, byFund, topFundNames };
}

module.exports = {
  getDataset,
  getFunds,
  getReturns,
  getFundCompositions,
  getCompositionsPayload,
  getNavByFundId,
  getMarketData,
  getMCDetail,
  getStatusSummary,
};
