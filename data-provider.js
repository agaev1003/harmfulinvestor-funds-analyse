const fs = require("fs/promises");
const path = require("path");
const cheerio = require("cheerio");
const zlib = require("zlib");

// ── Shared library imports ──────────────────────────────────────────────────
const {
  normalizeSpace,
  normalizeLower,
  csvLikeText,
  toNumber,
  round,
  clearTimerSafe,
  withTimeout,
  persistCacheSnapshot,
} = require("./lib/utils");
const { fetchText, fetchJson } = require("./lib/http-client");
const mapLimit = require("./lib/map-limit");
const {
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
  normalizeFundType,
  normalizeInvestmentType,
  normalizeHoldingName,
  normalizeHoldingKey,
  parseListPage,
  shouldIncludeFund,
  parseHeaderMeta,
  parseDetailInfoMap,
  pickInfoValue,
  parseFundMetaFromDetailHtml,
  parseInvestfundsStructureFromHtml,
} = require("./lib/parser");
const {
  findLastWhere,
  findLastWithNav,
  findNavOnOrBefore,
  getLatestAum,
  filterHistoryByRange,
  parseChartSeries,
  mergeFundSeries,
  mergeHistoryIncremental,
  computeReturn,
  buildReturnsRow,
  buildCompositionChanges,
  buildTimeseriesForGroups,
} = require("./lib/calculations");

// ── Configuration ───────────────────────────────────────────────────────────

const IS_RENDER = Boolean(process.env.RENDER);
const BASE_URL = process.env.DATA_SOURCE_BASE || "https://investfunds.ru";
const IS_APP_CONTAINER_DIR = path.resolve(__dirname) === "/app";
const DEFAULT_CACHE_DIR =
  process.env.DATA_CACHE_DIR ||
  (IS_APP_CONTAINER_DIR
    ? path.join(process.env.TMPDIR || "/tmp", "fund-dashboard-cache")
    : path.join(__dirname, ".cache"));
const CACHE_FILE =
  process.env.DATA_CACHE_FILE ||
  path.join(DEFAULT_CACHE_DIR, "fund-dashboard-data.json");
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
const DETAIL_REQUEST_TIMEOUT_MS = Number(
  process.env.DETAIL_REQUEST_TIMEOUT_MS || (IS_RENDER ? 12_000 : 18_000)
);
const DETAIL_REQUEST_RETRIES = Number(
  process.env.DETAIL_REQUEST_RETRIES || (IS_RENDER ? 0 : 1)
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
  path.join(DEFAULT_CACHE_DIR, "fund-compositions.json");
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
    10
);
const DAILY_COMPOSITION_REFRESH_HOUR_MSK = Number(
  process.env.DAILY_COMPOSITION_REFRESH_HOUR_MSK ||
    process.env.DAILY_REFRESH_HOUR_MSK ||
    10
);
const BUILD_FAIL_BACKOFF_MS = Number(
  process.env.BUILD_FAIL_BACKOFF_MS || (IS_RENDER ? 5 * 60 * 1000 : 60 * 1000)
);
const MAIN_BUILD_TIMEOUT_MS = Number(
  process.env.MAIN_BUILD_TIMEOUT_MS || (IS_RENDER ? 10 * 60 * 1000 : 15 * 60 * 1000)
);
const COMPOSITION_BUILD_TIMEOUT_MS = Number(
  process.env.COMPOSITION_BUILD_TIMEOUT_MS ||
    (IS_RENDER ? 8 * 60 * 1000 : 12 * 60 * 1000)
);

// ── State ───────────────────────────────────────────────────────────────────

const state = {
  dataset: null,
  loadedAt: 0,
  diskLoaded: false,
  diskLoadPromise: null,
  buildPromise: null,
  marketBuildRunId: 0,
  marketBuildWatchdog: null,
  datasetAutoRefreshDate: null,
  compositions: null,
  compositionsLoadedAt: 0,
  compositionsDiskLoaded: false,
  compositionsDiskLoadPromise: null,
  compositionsBuildPromise: null,
  compositionsBuildRunId: 0,
  compositionsBuildWatchdog: null,
  compositionsAutoRefreshDate: null,
  compositionsBuildState: null,
  compositionsLastRun: null,
  compositionsLastError: null,
  marketBuildState: null,
  marketLastRun: null,
  marketLastError: null,
  marketNextRetryAt: 0,
  rankingIds: null,
  rankingIdsLoadedAt: 0,
  compositionsNextRetryAt: 0,
};

// ── Seed / disk cache helpers ───────────────────────────────────────────────

function getLatestMarketDataDate(dataset) {
  const funds = dataset && Array.isArray(dataset.funds) ? dataset.funds : [];
  let latest = null;
  for (const fund of funds) {
    const date = String(fund && fund.latest_date ? fund.latest_date : "").trim();
    if (!date) continue;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) continue;
    if (!latest || date > latest) latest = date;
  }
  return latest;
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

// ── Composition candidate logic ─────────────────────────────────────────────

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

  if (checkedAge == null) return true;

  if (forceRefresh) {
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

// ── Composition build ───────────────────────────────────────────────────────

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
    await ensureDiskLoaded();
    // Use existing dataset directly to avoid blocking on a stuck market build.
    if (!state.dataset) {
      await getDataset();
    }
    const ds = state.dataset;
    if (!ds || !Array.isArray(ds.funds)) {
      throw new Error("Нет данных по рынку для построения составов");
    }
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

    for (const [id, item] of previousById.entries()) {
      if (!item) continue;
      mergedById.set(id, item);
    }

    for (const [id, item] of refreshedById.entries()) {
      mergedById.set(id, item);
    }

    for (const id of [...mergedById.keys()]) {
      if (!knownCandidateIds.has(id)) {
        mergedById.delete(id);
      }
    }

    const items = [...mergedById.values()]
      .filter((item) => item && Array.isArray(item.issuers) && item.issuers.length > 0)
      .sort((a, b) => String(a.name || "").localeCompare(String(b.name || ""), "ru"));

    const catastrophicRefreshFailure =
      refreshCandidates.length > 0 &&
      runStats.failedCount >= refreshCandidates.length &&
      runStats.successCount === 0 &&
      runStats.noStructureCount === 0;

    if (catastrophicRefreshFailure) {
      const sampleMessage =
        runStats.errorSamples.length > 0 && runStats.errorSamples[0].message
          ? `: ${runStats.errorSamples[0].message}`
          : "";
      throw new Error(
        `Не удалось обновить составы: все ${refreshCandidates.length} запросов завершились ошибкой${sampleMessage}`
      );
    }

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

    await persistCacheSnapshot(COMPOSITION_CACHE_FILE, dataset);

    const sec = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(
      `[composition] Build finished in ${sec}s. Refreshed: ${refreshCandidates.length}. Items: ${items.length}. Failed: ${runStats.failedCount}. Cache: ${COMPOSITION_CACHE_FILE}`
    );
    return dataset;
  } catch (error) {
    state.compositionsLastError = String(error.message || error);
    finalizeRun({ refreshedCount: 0 });
    throw error;
  } finally {
    if (state.compositionsBuildState === runStats) {
      state.compositionsBuildState = null;
    }
  }
}

// ── Chunk helper ────────────────────────────────────────────────────────────

function chunkArray(items, chunkSize) {
  const size = Math.max(1, Number(chunkSize) || 1);
  const chunks = [];
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }
  return chunks;
}

// ── Fund detail fetching ────────────────────────────────────────────────────

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

async function fetchFundDetails(row) {
  const url = `${BASE_URL}/funds/${row.id}/`;
  const html = await fetchText(
    url,
    DETAIL_REQUEST_RETRIES,
    { timeoutMs: DETAIL_REQUEST_TIMEOUT_MS }
  );
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

// ── Chart URL and series batch fetch ────────────────────────────────────────

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

// ── Fund type helpers ───────────────────────────────────────────────────────

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

// ── Main dataset build ──────────────────────────────────────────────────────

async function buildDataset() {
  const startedAt = Date.now();
  const runStats = {
    startedAt: new Date().toISOString(),
    stage: "list-first-page",
    listPages: 0,
    listPagesParsed: 0,
    listPagesFailed: 0,
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
      listPagesFailed: runStats.listPagesFailed,
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
          try {
            const html = await fetchText(
              `${BASE_URL}/funds/?showID=54&limit=50&page=${page}`
            );
            const parsed = parseListPage(html);
            runStats.listPagesParsed += 1;
            if ((i + 1) % 10 === 0 || i + 1 === pages.length) {
              console.log(`[data] Parsed list page ${page}/${maxPage}`);
            }
            return parsed.rows;
          } catch (error) {
            runStats.listPagesFailed += 1;
            console.warn(
              `[data] List page fetch failed ${page}/${maxPage}: ${String(
                error.message || error
              )}`
            );
            return [];
          }
        },
        { continueOnError: true }
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
            // P11 fix: use findLastWhere instead of [...history].reverse().find()
            const lastAumPoint = findLastWhere(history, (p) => p.aum != null);
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
          for (const fundId of batchFundIds) {
            const previousHistory = previousHistoriesById[String(fundId)] || [];
            historiesById[String(fundId)] = previousHistory;
            const fund = fundById.get(fundId);
            if (!fund) continue;
            const last = findLastWithNav(previousHistory);
            const lastAumPoint = findLastWhere(previousHistory, (p) => p.aum != null);
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

    await persistCacheSnapshot(CACHE_FILE, dataset);

    const sec = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(
      `[data] Build finished in ${sec}s. Funds=${funds.length}. List page failed=${runStats.listPagesFailed}. Detail failed=${runStats.detailFailedCount}. History failed batches=${runStats.historyBatchFailed}. Cache: ${CACHE_FILE}`
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

// ── Dataset validation & disk loading ───────────────────────────────────────

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
  if (state.diskLoadPromise) {
    await state.diskLoadPromise;
    return;
  }

  state.diskLoadPromise = (async () => {
    try {
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
    } finally {
      state.diskLoaded = true;
      state.diskLoadPromise = null;
    }
  })();

  await state.diskLoadPromise;
}

// ── getDataset (main entry) ─────────────────────────────────────────────────

async function getDataset() {
  await ensureDiskLoaded();

  const startDatasetBuild = ({ resetDailyMarkOnFail = null } = {}) => {
    if (
      state.dataset &&
      state.marketNextRetryAt &&
      Date.now() < state.marketNextRetryAt
    ) {
      return Promise.resolve(state.dataset);
    }
    if (state.buildPromise) {
      return withTimeout(state.buildPromise, MAIN_BUILD_TIMEOUT_MS, "main build");
    }
    const runId = Number(state.marketBuildRunId || 0) + 1;
    state.marketBuildRunId = runId;
    let watchdog = null;
    const buildPromise = buildDataset()
      .then((data) => {
        if (state.marketBuildRunId === runId) {
          state.dataset = data;
          state.loadedAt = datasetTimestampMs(data);
          state.marketNextRetryAt = 0;
          state.marketLastError = null;
        }
        return data;
      })
      .catch((error) => {
        if (state.marketBuildRunId === runId) {
          if (
            resetDailyMarkOnFail &&
            state.datasetAutoRefreshDate === resetDailyMarkOnFail
          ) {
            state.datasetAutoRefreshDate = null;
          }
          state.marketNextRetryAt = Date.now() + Math.max(10_000, BUILD_FAIL_BACKOFF_MS);
        }
        throw error;
      })
      .finally(() => {
        if (state.marketBuildWatchdog === watchdog) {
          clearTimerSafe(watchdog);
          state.marketBuildWatchdog = null;
        }
        if (state.buildPromise === buildPromise) {
          state.buildPromise = null;
        }
      });
    watchdog = setTimeout(() => {
      if (state.marketBuildRunId !== runId) return;
      if (state.buildPromise !== buildPromise) return;
      state.marketLastError = `main build watchdog timeout after ${MAIN_BUILD_TIMEOUT_MS}ms`;
      state.marketNextRetryAt = Date.now() + Math.max(10_000, BUILD_FAIL_BACKOFF_MS);
      // Prevent infinite restart loop: bump loadedAt so data is not immediately stale.
      if (state.dataset) {
        state.loadedAt = Date.now();
      }
      if (
        resetDailyMarkOnFail &&
        state.datasetAutoRefreshDate === resetDailyMarkOnFail
      ) {
        state.datasetAutoRefreshDate = null;
      }
      state.marketBuildState = null;
      state.buildPromise = null;
      console.warn(
        `[data] Main build watchdog released stuck build after ${MAIN_BUILD_TIMEOUT_MS}ms`
      );
    }, MAIN_BUILD_TIMEOUT_MS + 1_000);
    if (typeof watchdog.unref === "function") watchdog.unref();
    if (state.marketBuildWatchdog) clearTimerSafe(state.marketBuildWatchdog);
    state.marketBuildWatchdog = watchdog;
    state.buildPromise = buildPromise;
    return withTimeout(buildPromise, MAIN_BUILD_TIMEOUT_MS, "main build");
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
    if (isStale) {
      startDatasetBuild().catch((error) => {
        console.warn(`[data] Background refresh failed: ${String(error.message || error)}`);
      });
    }
    maybeRunDailyNoonRefresh();
    return state.dataset;
  }

  return startDatasetBuild();
}

// ── Compositions dataset loading ────────────────────────────────────────────

function isValidCompositionDataset(data) {
  return Boolean(
    data &&
      Number(data.version) === COMPOSITION_DATASET_VERSION &&
      Array.isArray(data.items)
  );
}

async function ensureCompositionDiskLoaded() {
  if (state.compositionsDiskLoaded) return;
  if (state.compositionsDiskLoadPromise) {
    await state.compositionsDiskLoadPromise;
    return;
  }

  state.compositionsDiskLoadPromise = (async () => {
    try {
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
    } finally {
      state.compositionsDiskLoaded = true;
      state.compositionsDiskLoadPromise = null;
    }
  })();

  await state.compositionsDiskLoadPromise;
}

async function getCompositionsDataset({ forceRefresh = false } = {}) {
  await ensureCompositionDiskLoaded();

  const startCompositionsBuild = ({
    force = false,
    resetDailyMarkOnFail = null,
  } = {}) => {
    if (
      !force &&
      state.compositions &&
      state.compositionsNextRetryAt &&
      Date.now() < state.compositionsNextRetryAt
    ) {
      return Promise.resolve(state.compositions);
    }
    if (state.compositionsBuildPromise) {
      return withTimeout(
        state.compositionsBuildPromise,
        COMPOSITION_BUILD_TIMEOUT_MS,
        "composition build"
      );
    }
    const runId = Number(state.compositionsBuildRunId || 0) + 1;
    state.compositionsBuildRunId = runId;
    let watchdog = null;
    const buildPromise = buildCompositionsDataset({ forceRefresh: force })
      .then((data) => {
        if (state.compositionsBuildRunId === runId) {
          state.compositions = data;
          state.compositionsLoadedAt = datasetTimestampMs(data);
          state.compositionsNextRetryAt = 0;
          state.compositionsLastError = null;
        }
        return data;
      })
      .catch((error) => {
        if (state.compositionsBuildRunId === runId) {
          if (
            resetDailyMarkOnFail &&
            state.compositionsAutoRefreshDate === resetDailyMarkOnFail
          ) {
            state.compositionsAutoRefreshDate = null;
          }
          if (!force) {
            state.compositionsNextRetryAt =
              Date.now() + Math.max(10_000, BUILD_FAIL_BACKOFF_MS);
          }
        }
        throw error;
      })
      .finally(() => {
        if (state.compositionsBuildWatchdog === watchdog) {
          clearTimerSafe(watchdog);
          state.compositionsBuildWatchdog = null;
        }
        if (state.compositionsBuildPromise === buildPromise) {
          state.compositionsBuildPromise = null;
        }
      });
    watchdog = setTimeout(() => {
      if (state.compositionsBuildRunId !== runId) return;
      if (state.compositionsBuildPromise !== buildPromise) return;
      state.compositionsLastError =
        `composition build watchdog timeout after ${COMPOSITION_BUILD_TIMEOUT_MS}ms`;
      if (!force) {
        state.compositionsNextRetryAt =
          Date.now() + Math.max(10_000, BUILD_FAIL_BACKOFF_MS);
      }
      // Prevent infinite restart loop: bump loadedAt so data is not immediately stale.
      if (state.compositions) {
        state.compositionsLoadedAt = Date.now();
      }
      if (
        resetDailyMarkOnFail &&
        state.compositionsAutoRefreshDate === resetDailyMarkOnFail
      ) {
        state.compositionsAutoRefreshDate = null;
      }
      state.compositionsBuildState = null;
      state.compositionsBuildPromise = null;
      console.warn(
        `[composition] Build watchdog released stuck build after ${COMPOSITION_BUILD_TIMEOUT_MS}ms`
      );
    }, COMPOSITION_BUILD_TIMEOUT_MS + 1_000);
    if (typeof watchdog.unref === "function") watchdog.unref();
    if (state.compositionsBuildWatchdog) clearTimerSafe(state.compositionsBuildWatchdog);
    state.compositionsBuildWatchdog = watchdog;
    state.compositionsBuildPromise = buildPromise;
    return withTimeout(buildPromise, COMPOSITION_BUILD_TIMEOUT_MS, "composition build");
  };

  if (forceRefresh) {
    if (state.compositions) {
      startCompositionsBuild({ force: true }).catch((error) => {
        console.warn(
          `[composition] Forced background refresh failed: ${String(error.message || error)}`
        );
      });
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
    if (isStale) {
      startCompositionsBuild().catch((error) => {
        console.warn(
          `[composition] Background refresh failed: ${String(error.message || error)}`
        );
      });
    }
    maybeRunDailyNoonRefresh();
    return state.compositions;
  }

  return startCompositionsBuild();
}

// ── Non-blocking dataset accessor ───────────────────────────────────────────

/**
 * Returns cached dataset if available (from disk/seed/memory).
 * Triggers a background build if missing but never blocks on it.
 * Throws only when no cached data exists at all.
 */
async function getDatasetNonBlocking() {
  await ensureDiskLoaded();
  if (state.dataset) {
    // Kick off a background refresh if stale, but don't wait
    getDataset().catch(() => {});
    return state.dataset;
  }
  // No cached data — must wait for initial build
  return getDataset();
}

// ── Public API functions ────────────────────────────────────────────────────

async function getFunds() {
  const ds = await getDatasetNonBlocking();
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
  const ds = await getDatasetNonBlocking();
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
    const ageD = ageDaysFromIsoDate(date, now);
    if (ageD == null) continue;
    if (ageD >= STALE_MARK_DAYS) staleCount += 1;
    if (maxAgeDays == null || ageD > maxAgeDays) maxAgeDays = ageD;
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
    nextRetryAt:
      state.compositionsNextRetryAt && state.compositionsNextRetryAt > Date.now()
        ? new Date(state.compositionsNextRetryAt).toISOString()
        : null,
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
  // Non-blocking: return seed/disk data immediately if available
  await ensureCompositionDiskLoaded();
  if (state.compositions && !forceRefresh) {
    // Kick off background refresh if stale, but don't wait
    getCompositionsDataset({ forceRefresh: false }).catch(() => {});
    return {
      items: Array.isArray(state.compositions.items) ? state.compositions.items : [],
      meta: buildCompositionsMeta(state.compositions),
    };
  }
  const ds = await getCompositionsDataset({ forceRefresh });
  return {
    items: Array.isArray(ds.items) ? ds.items : [],
    meta: buildCompositionsMeta(ds),
  };
}

async function getStatusSummary() {
  // Use already-loaded state to avoid blocking on slow builds.
  await ensureDiskLoaded();
  await ensureCompositionDiskLoaded();
  const mainDs = state.dataset || { funds: [], generatedAt: null };
  const compDs = state.compositions || { items: [], generatedAt: null };

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
  const marketLatestDataDate = getLatestMarketDataDate(mainDs);
  const marketLatestDataAgeDays = ageDaysFromIsoDate(marketLatestDataDate);

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
      latestDataDate: marketLatestDataDate,
      latestDataAgeDays: marketLatestDataAgeDays,
      refreshing: Boolean(state.buildPromise),
      progress: marketBuildState
        ? {
            stage: marketBuildState.stage || null,
            listPages: Number(marketBuildState.listPages) || 0,
            listPagesParsed: Number(marketBuildState.listPagesParsed) || 0,
            listPagesFailed: Number(marketBuildState.listPagesFailed) || 0,
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
      failedListPages: marketBuildState
        ? Number(marketBuildState.listPagesFailed) || 0
        : marketLastRun
          ? Number(marketLastRun.listPagesFailed) || 0
          : 0,
      lastError: state.marketLastError || null,
      nextRetryAt:
        state.marketNextRetryAt && state.marketNextRetryAt > Date.now()
          ? new Date(state.marketNextRetryAt).toISOString()
          : null,
      lastRun: marketLastRun || null,
    },
    compositions: buildCompositionsMeta(compDs),
  };
}

async function getNavByFundId(fundId, from, to) {
  const ds = await getDatasetNonBlocking();
  const history = ds.historiesById[String(fundId)];
  if (!history) return null;
  return filterHistoryByRange(history, from, to);
}

async function getMarketData(rawFundType) {
  const ds = await getDatasetNonBlocking();
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
  const ds = await getDatasetNonBlocking();
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

// ── Proactive daily scheduler ────────────────────────────────────────────────
// Runs every 5 minutes and triggers refresh at the configured MSK hour,
// even if no HTTP requests come in.

const SCHEDULER_INTERVAL_MS = 5 * 60 * 1000;

function startDailyScheduler() {
  const tick = async () => {
    try {
      const nowHourMsk = mskHour();
      const todayMsk = mskDate();
      if (nowHourMsk == null) return;

      // Market data refresh
      if (
        DAILY_MAIN_REFRESH_HOUR_MSK >= 0 &&
        nowHourMsk >= DAILY_MAIN_REFRESH_HOUR_MSK &&
        state.datasetAutoRefreshDate !== todayMsk
      ) {
        await ensureDiskLoaded();
        if (state.dataset) {
          console.log(`[scheduler] Triggering daily market refresh (${todayMsk} ${nowHourMsk}:xx MSK)`);
          getDataset().catch((err) => {
            console.warn(`[scheduler] Market refresh failed: ${String(err.message || err)}`);
          });
        }
      }

      // Compositions refresh
      if (
        DAILY_COMPOSITION_REFRESH_HOUR_MSK >= 0 &&
        nowHourMsk >= DAILY_COMPOSITION_REFRESH_HOUR_MSK &&
        state.compositionsAutoRefreshDate !== todayMsk
      ) {
        await ensureCompositionDiskLoaded();
        if (state.compositions) {
          console.log(`[scheduler] Triggering daily compositions refresh (${todayMsk} ${nowHourMsk}:xx MSK)`);
          getCompositionsDataset({ forceRefresh: false }).catch((err) => {
            console.warn(`[scheduler] Compositions refresh failed: ${String(err.message || err)}`);
          });
        }
      }
    } catch (err) {
      console.warn(`[scheduler] Tick error: ${String(err.message || err)}`);
    }
  };

  const timer = setInterval(tick, SCHEDULER_INTERVAL_MS);
  timer.unref(); // Don't prevent process exit
  console.log(
    `[scheduler] Daily auto-refresh scheduled at ${DAILY_MAIN_REFRESH_HOUR_MSK}:00 MSK (market) / ${DAILY_COMPOSITION_REFRESH_HOUR_MSK}:00 MSK (compositions)`
  );
}

startDailyScheduler();

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
