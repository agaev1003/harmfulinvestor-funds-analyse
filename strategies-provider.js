const fs = require("fs/promises");
const path = require("path");
const {
  normalizeSpace,
  normalizeLower,
  toNumber,
  clearTimerSafe,
  persistCacheSnapshot,
} = require("./lib/utils");
const mapLimit = require("./lib/map-limit");

const STRATEGY_RETRY_DELAY_BASE_MS = 350;

const IS_RENDER = Boolean(process.env.RENDER);
const IS_APP_CONTAINER_DIR = path.resolve(__dirname) === "/app";
const DEFAULT_CACHE_DIR =
  process.env.DATA_CACHE_DIR ||
  (IS_APP_CONTAINER_DIR
    ? path.join(process.env.TMPDIR || "/tmp", "fund-dashboard-cache")
    : path.join(__dirname, ".cache"));

const STRATEGY_CACHE_FILE =
  process.env.STRATEGY_CACHE_FILE ||
  path.join(DEFAULT_CACHE_DIR, "strategies-dashboard-data.json");

const STRATEGY_CACHE_TTL_MS = Number(
  process.env.STRATEGY_CACHE_TTL_MS || (IS_RENDER ? 15 * 60 * 1000 : 10 * 60 * 1000)
);
const STRATEGY_ANALYTICS_CACHE_TTL_MS = Number(
  process.env.STRATEGY_ANALYTICS_CACHE_TTL_MS || 10 * 60 * 1000
);
const STRATEGY_REFRESH_TIMEOUT_MS = Number(
  process.env.STRATEGY_REFRESH_TIMEOUT_MS || (IS_RENDER ? 4 * 60 * 1000 : 6 * 60 * 1000)
);
const STRATEGY_REQUEST_TIMEOUT_MS = Number(
  process.env.STRATEGY_REQUEST_TIMEOUT_MS || (IS_RENDER ? 18_000 : 20_000)
);
const STRATEGY_REQUEST_RETRIES = Number(
  process.env.STRATEGY_REQUEST_RETRIES || (IS_RENDER ? 1 : 2)
);
const STRATEGY_DETAIL_CONCURRENCY = Number(
  process.env.STRATEGY_DETAIL_CONCURRENCY || (IS_RENDER ? 6 : 10)
);
const STRATEGY_CATALOG_LIMIT = Number(process.env.STRATEGY_CATALOG_LIMIT || 100);
const STRATEGY_MAX_CATALOG_PAGES = Number(process.env.STRATEGY_MAX_CATALOG_PAGES || 80);
const STRATEGY_SCAN_ALL_TABS =
  process.env.STRATEGY_SCAN_ALL_TABS != null
    ? !["0", "false", "no"].includes(
        String(process.env.STRATEGY_SCAN_ALL_TABS).trim().toLowerCase()
      )
    : true;
const ANALYTICS_CACHE_MAX_ENTRIES = Number(
  process.env.STRATEGY_ANALYTICS_CACHE_MAX_ENTRIES || 500
);

const TRACKING_API_BASE =
  process.env.TRACKING_API_BASE ||
  "https://www.tbank.ru/api/invest-gw/tracking/api/v1";
const TRACKING_REFERER =
  process.env.TRACKING_REFERER || "https://www.tbank.ru/invest/strategies/";
const TRACKING_APP_VERSION = process.env.TRACKING_APP_VERSION || "1.596.0";
const TRACKING_USER_AGENT =
  process.env.TRACKING_USER_AGENT ||
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36";

const state = {
  snapshot: null,
  loadedAt: 0,
  diskLoaded: false,
  diskLoadPromise: null,
  refreshPromise: null,
  refreshRunId: 0,
  refreshWatchdog: null,
  lastError: null,
  lastRun: null,
  analyticsCache: new Map(),
};

function randomHex(size) {
  let out = "";
  while (out.length < size) {
    out += Math.floor(Math.random() * 16).toString(16);
  }
  return out.slice(0, size);
}

function buildTrackingHeaders(extraHeaders = {}) {
  const traceId = randomHex(32);
  const spanId = randomHex(16);
  return {
    accept: "application/json, text/plain, */*",
    "x-app-name": "invest",
    "x-app-version": TRACKING_APP_VERSION,
    "x-platform": "web",
    "x-b3-traceid": traceId,
    "x-b3-spanid": spanId,
    traceparent: `00-${traceId}-${spanId}-01`,
    referer: TRACKING_REFERER,
    "user-agent": TRACKING_USER_AGENT,
    ...extraHeaders,
  };
}

async function fetchWithTimeout(url, options = {}) {
  const timeoutMs =
    Number.isFinite(Number(options.timeoutMs)) && Number(options.timeoutMs) > 0
      ? Number(options.timeoutMs)
      : STRATEGY_REQUEST_TIMEOUT_MS;
  const fetchOptions = { ...options };
  delete fetchOptions.timeoutMs;

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, {
      ...fetchOptions,
      signal: controller.signal,
      headers: buildTrackingHeaders(fetchOptions.headers || {}),
    });
  } finally {
    clearTimeout(timer);
  }
}

async function fetchJson(url, retries = STRATEGY_REQUEST_RETRIES, requestOptions = {}) {
  let lastErr;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      const response = await fetchWithTimeout(url, requestOptions);
      const text = await response.text();
      if (!response.ok) {
        const shortBody = normalizeSpace(text).slice(0, 220);
        throw new Error(
          `HTTP ${response.status} for ${url}${shortBody ? `: ${shortBody}` : ""}`
        );
      }
      if (!text.trim()) return null;
      return JSON.parse(text);
    } catch (error) {
      lastErr = error;
      if (attempt < retries) {
        await new Promise((resolve) => setTimeout(resolve, STRATEGY_RETRY_DELAY_BASE_MS * (attempt + 1)));
      }
    }
  }
  throw lastErr;
}

function unwrapPayload(raw) {
  if (raw && typeof raw === "object" && raw.payload && typeof raw.payload === "object") {
    return raw.payload;
  }
  return raw;
}

function parsePercent(value) {
  return toNumber(value);
}

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

function formatPctShort(value) {
  if (!Number.isFinite(Number(value))) return null;
  const n = Number(value);
  const sign = n >= 0 ? "+" : "";
  return `${sign}${n.toLocaleString("ru-RU", {
    minimumFractionDigits: Math.abs(n) >= 100 ? 0 : 1,
    maximumFractionDigits: Math.abs(n) >= 100 ? 0 : 2,
  })}%`;
}

function formatPercentOnly(value) {
  if (!Number.isFinite(Number(value))) return null;
  return `${Number(value).toLocaleString("ru-RU", {
    minimumFractionDigits: Math.abs(Number(value)) >= 100 ? 0 : 1,
    maximumFractionDigits: Math.abs(Number(value)) >= 100 ? 0 : 2,
  })}%`;
}

function riskLabelFromCode(code) {
  const c = normalizeLower(code);
  if (c === "conservative") return "Консервативный";
  if (c === "moderate") return "Умеренный";
  if (c === "aggressive") return "Агрессивный";
  return normalizeSpace(code) || "—";
}

function flattenCharacteristics(characteristics) {
  const map = new Map();
  if (!Array.isArray(characteristics)) return map;

  for (const entry of characteristics) {
    if (!entry || typeof entry !== "object") continue;
    if (Array.isArray(entry.items)) {
      for (const item of entry.items) {
        if (!item || typeof item !== "object") continue;
        const id = normalizeSpace(item.id);
        if (!id) continue;
        map.set(id, item.value);
      }
      continue;
    }

    const id = normalizeSpace(entry.id);
    if (!id) continue;
    map.set(id, entry.value);
  }

  return map;
}

function getCharValue(charMap, ids) {
  for (const id of ids || []) {
    if (!id) continue;
    if (charMap.has(id)) {
      const value = charMap.get(id);
      const prepared = normalizeSpace(value);
      if (prepared) return prepared;
    }
  }
  return null;
}

function normalizePortfolioSlice(list) {
  if (!Array.isArray(list)) return [];
  return list
    .map((row) => {
      if (!row || typeof row !== "object") return null;
      const title = normalizeSpace(row.title);
      const percent = parsePercent(row.percent);
      if (!title) return null;
      return {
        title,
        percent: Number.isFinite(percent) ? percent : null,
      };
    })
    .filter(Boolean);
}

function normalizeTopPositions(list) {
  if (!Array.isArray(list)) return [];
  return list
    .map((row) => {
      if (!row || typeof row !== "object") return null;
      const position = row.exchangePosition && typeof row.exchangePosition === "object"
        ? row.exchangePosition
        : {};
      const ticker = normalizeSpace(position.ticker);
      const name = normalizeSpace(position.briefName || position.name);
      const signalsCount = toNumber(row.signalsCount);
      return {
        ticker: ticker || null,
        name: name || null,
        signalsCount: Number.isFinite(signalsCount) ? signalsCount : null,
      };
    })
    .filter((row) => row && (row.ticker || row.name));
}

function normalizeDealSide(value) {
  const code = normalizeLower(value);
  if (!code) return null;
  if (code.includes("buy") || code.includes("purchase") || code.includes("куп")) {
    return "Покупка";
  }
  if (code.includes("sell") || code.includes("прод")) {
    return "Продажа";
  }
  return normalizeSpace(value);
}

function normalizeRecentDeals(detail) {
  const source = detail && typeof detail === "object" ? detail : {};
  const candidates = [
    source.recentDeals,
    source.lastDeals,
    source.latestDeals,
    source.trades,
    source.operations,
    source.latestOperations,
    source.lastOperations,
    source.signalsHistory,
    source.recentSignals,
  ];
  const rawList = candidates.find((row) => Array.isArray(row) && row.length > 0);
  if (!Array.isArray(rawList)) return [];

  const normalized = rawList
    .map((row) => {
      if (!row || typeof row !== "object") return null;
      const instrument =
        row.instrument && typeof row.instrument === "object"
          ? row.instrument
          : row.exchangePosition && typeof row.exchangePosition === "object"
            ? row.exchangePosition
            : row.position && typeof row.position === "object"
              ? row.position
              : {};
      const ticker = normalizeSpace(
        row.ticker || instrument.ticker || instrument.figi || instrument.uid
      );
      const name = normalizeSpace(
        row.name || row.instrumentName || instrument.briefName || instrument.name
      );
      const side = normalizeDealSide(
        row.side || row.operationType || row.type || row.action
      );
      const quantity = toNumber(
        row.quantity || row.qty || row.lots || row.amountLots || row.volume
      );
      const price = parseMoney(row.price || row.priceText || row.priceAmount);
      const amount = parseMoney(row.amount || row.value || row.payment || row.total);
      const date = normalizeSpace(
        row.time ||
          row.date ||
          row.createdAt ||
          row.executedAt ||
          row.operationDate ||
          row.updatedAt
      );
      const timestamp = Number.isFinite(Date.parse(date)) ? Date.parse(date) : null;
      if (!ticker && !name && !side && !Number.isFinite(quantity) && !date) return null;
      return {
        ticker: ticker || null,
        name: name || null,
        side: side || null,
        quantity: Number.isFinite(quantity) ? quantity : null,
        price: Number.isFinite(price) ? price : null,
        amount: Number.isFinite(amount) ? amount : null,
        date: date || null,
        timestamp,
      };
    })
    .filter(Boolean);

  normalized.sort((a, b) => {
    const aTs = Number.isFinite(Number(a.timestamp)) ? Number(a.timestamp) : -1;
    const bTs = Number.isFinite(Number(b.timestamp)) ? Number(b.timestamp) : -1;
    return bTs - aTs;
  });

  return normalized.slice(0, 12);
}

function buildFeesText({ brokerFeeText, managementFeeText, resultFeeText }) {
  const parts = [];
  if (managementFeeText) parts.push(`Упр: ${managementFeeText}`);
  if (resultFeeText) parts.push(`Рез: ${resultFeeText}`);
  if (brokerFeeText) parts.push(`Брок: ${brokerFeeText}`);
  return parts.join(" · ") || null;
}

function normalizeStrategyItem(catalogItem, detailItem, tabIdsSet) {
  const catalog = catalogItem && typeof catalogItem === "object" ? catalogItem : {};
  const detail = detailItem && typeof detailItem === "object" ? detailItem : {};

  const id = normalizeSpace(detail.id || catalog.id);
  if (!id) return null;

  const catalogChar = flattenCharacteristics(catalog.characteristics);
  const detailChar = flattenCharacteristics(detail.characteristics);

  const allTimeReturnText =
    getCharValue(catalogChar, ["relative-yield"]) ||
    getCharValue(detailChar, ["relative-yield"]);
  const allTimeReturnPct = parsePercent(allTimeReturnText);

  const clientsText =
    getCharValue(detailChar, ["slaves-count"]) ||
    getCharValue(catalogChar, ["slaves-count"]);
  const clients = toNumber(clientsText);

  const minAmountText =
    getCharValue(detailChar, ["minimum-base-money-position-quantity"]) ||
    getCharValue(catalogChar, ["minimum-base-money-position-quantity"]);
  const minAmountValue = parseMoney(minAmountText);

  const expectedText =
    getCharValue(detailChar, ["expected-relative-yield"]) ||
    getCharValue(catalogChar, ["expected-relative-yield"]);
  const expectedReturnPct = Number.isFinite(Number(detail.expectedStrategyRelativeYield))
    ? Number(detail.expectedStrategyRelativeYield)
    : parsePercent(expectedText);

  const return12mPct = Number.isFinite(Number(detail.relativeYield12Mth))
    ? Number(detail.relativeYield12Mth)
    : Number.isFinite(Number(catalog.relativeYield)) && normalizeLower(catalog.relativeYieldPeriod) === "year"
      ? Number(catalog.relativeYield)
      : null;

  const managedAmountText =
    getCharValue(detailChar, ["tail-value", "limit-tail-value"]) ||
    getCharValue(catalogChar, ["tail-value", "limit-tail-value"]);
  const managedAmountValue = parseMoney(
    getCharValue(detailChar, ["limit-tail-value"]) ||
      getCharValue(catalogChar, ["limit-tail-value"]) ||
      managedAmountText
  );

  const brokerFeeText = getCharValue(detailChar, ["broker-fee"]);
  const managementFeeText = getCharValue(detailChar, ["management-fee"]);
  const resultFeeText = getCharValue(detailChar, ["result-fee"]);
  const feesText = buildFeesText({
    brokerFeeText,
    managementFeeText,
    resultFeeText,
  });

  const maxDrawdownPct = parsePercent(
    getCharValue(detailChar, ["master-portfolio-max-drawdown"])
  );
  const signalsCount = toNumber(getCharValue(detailChar, ["signals-count"]));
  const signalFrequency = getCharValue(detailChar, ["signal-frequency"]);

  const portfolioRate =
    detail.portfolioRate && typeof detail.portfolioRate === "object"
      ? detail.portfolioRate
      : {};
  const portfolio = {
    types: normalizePortfolioSlice(portfolioRate.types),
    sectors: normalizePortfolioSlice(portfolioRate.sectors),
    companies: normalizePortfolioSlice(portfolioRate.companies),
  };

  const topCompanies = portfolio.companies.slice(0, 10);
  const topCompaniesText = topCompanies
    .slice(0, 3)
    .map((row) => {
      const pct = formatPercentOnly(row.percent);
      return pct ? `${row.title} ${pct}` : row.title;
    })
    .join(", ");

  const topPositions = normalizeTopPositions(detail.topPositions);
  const recentDeals = normalizeRecentDeals(detail);

  const ownerProfile =
    (detail.owner && detail.owner.socialProfile) ||
    (catalog.owner && catalog.owner.socialProfile) ||
    {};

  const tags = Array.isArray(detail.tags)
    ? detail.tags
    : Array.isArray(catalog.tags)
      ? catalog.tags
      : [];

  const shortDescription =
    getCharValue(catalogChar, ["short-description"]) ||
    getCharValue(detailChar, ["short-description"]);
  const description = normalizeSpace(detail.description) || shortDescription || null;

  const score = Number.isFinite(Number(detail.score))
    ? Number(detail.score)
    : Number.isFinite(Number(catalog.score))
      ? Number(catalog.score)
      : null;

  return {
    id,
    title: normalizeSpace(detail.title || catalog.title) || "Без названия",
    status: normalizeLower(detail.status || "active") || "active",
    type: normalizeSpace(detail.type || "") || null,
    baseCurrency: normalizeLower(detail.baseCurrency || catalog.baseCurrency) || null,
    riskProfile: normalizeLower(detail.riskProfile || catalog.riskProfile) || null,
    riskLabel: riskLabelFromCode(detail.riskProfile || catalog.riskProfile),
    score,
    ownerId: normalizeSpace(ownerProfile.id) || null,
    ownerNickname: normalizeSpace(ownerProfile.nickname) || null,
    ownerImage: normalizeSpace(ownerProfile.image) || null,
    clients: Number.isFinite(clients) ? Math.round(clients) : null,
    clientsText: clientsText || null,
    allTimeReturnPct: Number.isFinite(allTimeReturnPct) ? allTimeReturnPct : null,
    allTimeReturnText: allTimeReturnText || null,
    return12mPct: Number.isFinite(return12mPct) ? return12mPct : null,
    managedAmountValue: Number.isFinite(managedAmountValue) ? managedAmountValue : null,
    managedAmountText: managedAmountText || null,
    expectedReturnPct: Number.isFinite(expectedReturnPct) ? expectedReturnPct : null,
    expectedReturnText: expectedText || null,
    minAmountValue: Number.isFinite(minAmountValue) ? minAmountValue : null,
    minAmountText: minAmountText || null,
    feesText: feesText || null,
    brokerFeeText: brokerFeeText || null,
    managementFeeText: managementFeeText || null,
    resultFeeText: resultFeeText || null,
    maxDrawdownPct: Number.isFinite(maxDrawdownPct) ? maxDrawdownPct : null,
    signalsCount: Number.isFinite(signalsCount) ? Math.round(signalsCount) : null,
    signalFrequency: signalFrequency || null,
    description: description || null,
    shortDescription: shortDescription || null,
    ownerDescription: getCharValue(catalogChar, ["owner-description"]) || null,
    topCompaniesText: topCompaniesText || null,
    portfolio,
    topPositions,
    recentDeals,
    tabs: Array.from(tabIdsSet || []).sort(),
    tags: tags
      .map((tag) => {
        if (!tag || typeof tag !== "object") return null;
        const idValue = normalizeSpace(tag.id);
        return idValue || null;
      })
      .filter(Boolean),
    url: `https://www.tbank.ru/invest/strategies/${id}`,
  };
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

function isValidSnapshot(data) {
  return Boolean(
    data &&
      typeof data === "object" &&
      Array.isArray(data.items) &&
      (data.generatedAt == null || typeof data.generatedAt === "string")
  );
}

function snapshotTimestampMs(snapshot) {
  const ts = Date.parse(snapshot && snapshot.generatedAt ? snapshot.generatedAt : "");
  return Number.isFinite(ts) ? ts : Date.now();
}

function isSnapshotStale() {
  if (!state.snapshot || !state.loadedAt) return true;
  return Date.now() - state.loadedAt > STRATEGY_CACHE_TTL_MS;
}

async function ensureDiskSnapshotLoaded() {
  if (state.diskLoaded) return;
  if (state.diskLoadPromise) {
    await state.diskLoadPromise;
    return;
  }

  state.diskLoadPromise = (async () => {
    try {
      const raw = await fs.readFile(STRATEGY_CACHE_FILE, "utf8");
      const parsed = JSON.parse(raw);
      if (isValidSnapshot(parsed)) {
        state.snapshot = parsed;
        state.loadedAt = snapshotTimestampMs(parsed);
      }
    } catch {
      // No disk cache yet.
    } finally {
      state.diskLoaded = true;
      state.diskLoadPromise = null;
    }
  })();

  await state.diskLoadPromise;
}

async function fetchCatalogTabs() {
  const raw = await fetchJson(`${TRACKING_API_BASE}/strategy/catalog/tab`);
  const root = unwrapPayload(raw);
  const items = Array.isArray(root && root.items) ? root.items : [];
  return items
    .map((row) => {
      if (!row || typeof row !== "object") return null;
      const tabId = normalizeSpace(row.tabId);
      if (!tabId) return null;
      return {
        tabId,
        title: normalizeSpace(row.title) || tabId,
        type: normalizeSpace(row.type) || null,
      };
    })
    .filter(Boolean);
}

async function fetchCatalogByTab(tabId = "") {
  const out = [];
  let cursor = "";

  for (let page = 0; page < STRATEGY_MAX_CATALOG_PAGES; page += 1) {
    const params = new URLSearchParams();
    params.set("limit", String(Math.max(1, STRATEGY_CATALOG_LIMIT)));
    if (tabId) params.set("tabId", tabId);
    if (cursor) params.set("cursor", cursor);

    const raw = await fetchJson(`${TRACKING_API_BASE}/strategy/catalog?${params.toString()}`);
    const root = unwrapPayload(raw);
    const items = Array.isArray(root && root.items) ? root.items : [];

    for (const item of items) {
      if (!item || typeof item !== "object") continue;
      const id = normalizeSpace(item.id);
      if (!id) continue;
      out.push(item);
    }

    const nextCursor = normalizeSpace(root && root.nextCursor);
    const hasNext = Boolean(root && root.hasNext);
    if (!hasNext && !nextCursor) break;
    if (!nextCursor) break;
    cursor = nextCursor;
  }

  return out;
}

async function fetchStrategyDetail(strategyId) {
  const id = encodeURIComponent(String(strategyId));
  const raw = await fetchJson(`${TRACKING_API_BASE}/strategy/${id}`);
  return unwrapPayload(raw);
}

async function buildSnapshot() {
  const byId = new Map();
  const tabMembership = new Map();

  const addCatalogItems = (items, tabId = null) => {
    for (const item of items || []) {
      if (!item || typeof item !== "object") continue;
      const id = normalizeSpace(item.id);
      if (!id) continue;
      if (!byId.has(id)) byId.set(id, item);
      if (!tabMembership.has(id)) tabMembership.set(id, new Set());
      if (tabId) tabMembership.get(id).add(tabId);
    }
  };

  const allItems = await fetchCatalogByTab("");
  addCatalogItems(allItems, "all");

  let tabs = [];
  if (STRATEGY_SCAN_ALL_TABS) {
    try {
      tabs = await fetchCatalogTabs();
    } catch (error) {
      console.warn(`[strategies] tabs fetch failed: ${String(error.message || error)}`);
      tabs = [];
    }

    for (const tab of tabs) {
      try {
        const tabItems = await fetchCatalogByTab(tab.tabId);
        addCatalogItems(tabItems, tab.tabId);
      } catch (error) {
        console.warn(
          `[strategies] tab ${tab.tabId} fetch failed: ${String(error.message || error)}`
        );
      }
    }
  }

  const ids = [...byId.keys()];

  const detailFetchResults = await mapLimit(
    ids,
    STRATEGY_DETAIL_CONCURRENCY,
    async (id) => {
      try {
        const detail = await fetchStrategyDetail(id);
        return { id, detail, error: null };
      } catch (error) {
        return {
          id,
          detail: null,
          error: String(error.message || error),
        };
      }
    },
    { continueOnError: true }
  );

  const detailsById = new Map();
  const failedDetails = [];

  for (const row of detailFetchResults) {
    if (row && row.detail && typeof row.detail === "object") {
      detailsById.set(row.id, row.detail);
    } else if (row && row.id) {
      failedDetails.push({ id: row.id, error: row.error || "Unknown error" });
    }
  }

  const normalized = ids
    .map((id) =>
      normalizeStrategyItem(byId.get(id), detailsById.get(id), tabMembership.get(id))
    )
    .filter(Boolean);

  sortStrategiesByAllTimeYield(normalized);

  const nowIso = new Date().toISOString();
  const activeCount = normalized.filter((row) => row.status === "active").length;

  return {
    source: "https://www.tbank.ru/invest/strategies/",
    generatedAt: nowIso,
    total: normalized.length,
    activeCount,
    tabCount: tabs.length,
    scannedAllTabs: STRATEGY_SCAN_ALL_TABS,
    detailLoadedCount: detailsById.size,
    failedDetailsCount: failedDetails.length,
    failedDetails: failedDetails.slice(0, 30),
    items: normalized,
  };
}

async function refreshSnapshot() {
  if (state.refreshPromise) return state.refreshPromise;

  const startedAt = new Date().toISOString();
  const runId = Number(state.refreshRunId || 0) + 1;
  state.refreshRunId = runId;
  let watchdog = null;
  const refreshPromise = (async () => {
    try {
      const snapshot = await buildSnapshot();
      if (state.refreshRunId === runId) {
        state.snapshot = snapshot;
        state.loadedAt = snapshotTimestampMs(snapshot);
        state.lastError = null;
        state.lastRun = {
          ok: true,
          startedAt,
          finishedAt: new Date().toISOString(),
          total: snapshot.total,
          failedDetailsCount: snapshot.failedDetailsCount,
        };
        persistCacheSnapshot(STRATEGY_CACHE_FILE, snapshot);
      }
      return snapshot;
    } catch (error) {
      if (state.refreshRunId === runId) {
        state.lastError = String(error.message || error);
        state.lastRun = {
          ok: false,
          startedAt,
          finishedAt: new Date().toISOString(),
          error: state.lastError,
        };
      }
      if (!state.snapshot) throw error;
      return state.snapshot;
    } finally {
      if (state.refreshWatchdog === watchdog) {
        clearTimerSafe(watchdog);
        state.refreshWatchdog = null;
      }
      if (state.refreshPromise === refreshPromise) {
        state.refreshPromise = null;
      }
    }
  })();
  state.refreshPromise = refreshPromise;

  watchdog = setTimeout(() => {
    if (state.refreshRunId !== runId) return;
    if (state.refreshPromise !== refreshPromise) return;
    state.lastError = `strategies refresh watchdog timeout after ${STRATEGY_REFRESH_TIMEOUT_MS}ms`;
    state.lastRun = {
      ok: false,
      startedAt,
      finishedAt: new Date().toISOString(),
      error: state.lastError,
    };
    state.refreshPromise = null;
    console.warn(
      `[strategies] Refresh watchdog released stuck refresh after ${STRATEGY_REFRESH_TIMEOUT_MS}ms`
    );
  }, STRATEGY_REFRESH_TIMEOUT_MS + 1_000);
  if (typeof watchdog.unref === "function") watchdog.unref();
  if (state.refreshWatchdog) clearTimerSafe(state.refreshWatchdog);
  state.refreshWatchdog = watchdog;

  return refreshPromise;
}

async function getStrategiesPayload({ forceRefresh = false } = {}) {
  await ensureDiskSnapshotLoaded();

  if (forceRefresh) {
    await refreshSnapshot();
  } else if (!state.snapshot) {
    await refreshSnapshot();
  } else if (isSnapshotStale()) {
    refreshSnapshot().catch((error) => {
      console.warn(`[strategies] refresh failed: ${String(error.message || error)}`);
    });
  }

  const snapshot = state.snapshot;
  if (!snapshot || !Array.isArray(snapshot.items)) {
    throw new Error("Не удалось загрузить стратегии автоследования");
  }

  return {
    source: snapshot.source,
    generatedAt: snapshot.generatedAt || null,
    loadedAt: state.loadedAt ? new Date(state.loadedAt).toISOString() : null,
    stale: isSnapshotStale(),
    refreshing: Boolean(state.refreshPromise),
    lastError: state.lastError || null,
    lastRun: state.lastRun || null,
    total: Number(snapshot.total) || snapshot.items.length,
    activeCount:
      Number.isFinite(Number(snapshot.activeCount)) ? Number(snapshot.activeCount) : null,
    tabCount: Number.isFinite(Number(snapshot.tabCount)) ? Number(snapshot.tabCount) : null,
    failedDetailsCount:
      Number.isFinite(Number(snapshot.failedDetailsCount))
        ? Number(snapshot.failedDetailsCount)
        : null,
    items: snapshot.items,
  };
}

function toIsoDateUtc(ts) {
  const d = new Date(ts);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function normalizeSeriesValue(value) {
  const num = toNumber(value);
  return Number.isFinite(num) ? num : null;
}

function buildAnalyticsPoints(interval, values, benchmarkValues) {
  const list = Array.isArray(values) ? values : [];
  if (!list.length) return [];

  const benchmark = Array.isArray(benchmarkValues) ? benchmarkValues : [];

  const fromMsRaw = Date.parse(
    interval && interval.from ? String(interval.from) : ""
  );
  const toMsRaw = Date.parse(interval && interval.to ? String(interval.to) : "");
  const hasFrom = Number.isFinite(fromMsRaw);
  const hasTo = Number.isFinite(toMsRaw);

  const toMs = hasTo ? toMsRaw : Date.now();
  const fromMs = hasFrom
    ? fromMsRaw
    : toMs - Math.max(0, list.length - 1) * 86_400_000;

  const span = Math.max(0, toMs - fromMs);
  const denom = Math.max(1, list.length - 1);

  const out = [];
  for (let i = 0; i < list.length; i += 1) {
    const value = normalizeSeriesValue(list[i]);
    if (value == null) continue;

    const ts =
      list.length <= 1 ? toMs : Math.round(fromMs + (span * i) / denom);
    const benchValue = i < benchmark.length ? normalizeSeriesValue(benchmark[i]) : null;

    out.push({
      timestamp: ts,
      date: toIsoDateUtc(ts),
      value,
      benchmarkValue: benchValue,
    });
  }

  return out;
}

function cleanupAnalyticsCache() {
  if (state.analyticsCache.size <= ANALYTICS_CACHE_MAX_ENTRIES) return;

  const rows = [...state.analyticsCache.entries()].map(([key, entry]) => ({
    key,
    touchedAt:
      entry && Number.isFinite(Number(entry.touchedAt)) ? Number(entry.touchedAt) : 0,
  }));

  rows.sort((a, b) => a.touchedAt - b.touchedAt);
  const removeCount = Math.max(
    1,
    state.analyticsCache.size - ANALYTICS_CACHE_MAX_ENTRIES
  );

  for (let i = 0; i < removeCount && i < rows.length; i += 1) {
    state.analyticsCache.delete(rows[i].key);
  }
}

async function getStrategyAnalytics(strategyId, { forceRefresh = false } = {}) {
  const id = normalizeSpace(strategyId);
  if (!id) throw new Error("Некорректный id стратегии");

  const now = Date.now();
  const cached = state.analyticsCache.get(id);
  if (
    !forceRefresh &&
    cached &&
    cached.payload &&
    Number.isFinite(Number(cached.expiresAt)) &&
    now < Number(cached.expiresAt)
  ) {
    cached.touchedAt = now;
    return cached.payload;
  }

  const raw = await fetchJson(
    `${TRACKING_API_BASE}/strategy/${encodeURIComponent(id)}/analytics/value`
  );
  const data = unwrapPayload(raw);

  if (!data || typeof data !== "object") {
    throw new Error("Пустой ответ графика стратегии");
  }

  const interval =
    data.interval && typeof data.interval === "object" ? data.interval : {};
  const points = buildAnalyticsPoints(interval, data.values, data.benchmarkValues);

  const payload = {
    strategyId: id,
    generatedAt: new Date().toISOString(),
    interval: {
      from: normalizeSpace(interval.from) || null,
      to: normalizeSpace(interval.to) || null,
    },
    relativeYield: normalizeSeriesValue(data.relativeYield),
    benchmarkRelativeYield: normalizeSeriesValue(data.benchmarkRelativeYield),
    points,
  };

  state.analyticsCache.set(id, {
    payload,
    expiresAt: now + STRATEGY_ANALYTICS_CACHE_TTL_MS,
    touchedAt: now,
  });
  cleanupAnalyticsCache();

  return payload;
}

async function getStrategiesStatusSummary() {
  await ensureDiskSnapshotLoaded();

  if (!state.snapshot && !state.refreshPromise) {
    refreshSnapshot().catch((error) => {
      console.warn(`[strategies] status-trigger refresh failed: ${String(error.message || error)}`);
    });
  } else if (state.snapshot && isSnapshotStale() && !state.refreshPromise) {
    refreshSnapshot().catch((error) => {
      console.warn(`[strategies] status-trigger stale refresh failed: ${String(error.message || error)}`);
    });
  }

  const snapshot = state.snapshot;
  return {
    generatedAt: snapshot && snapshot.generatedAt ? snapshot.generatedAt : null,
    loadedAt: state.loadedAt ? new Date(state.loadedAt).toISOString() : null,
    stale: isSnapshotStale(),
    refreshing: Boolean(state.refreshPromise),
    lastError: state.lastError || null,
    lastRun: state.lastRun || null,
    failedDetailsCount:
      snapshot && Number.isFinite(Number(snapshot.failedDetailsCount))
        ? Number(snapshot.failedDetailsCount)
        : 0,
    total:
      snapshot && Number.isFinite(Number(snapshot.total))
        ? Number(snapshot.total)
        : snapshot && Array.isArray(snapshot.items)
          ? snapshot.items.length
          : 0,
  };
}

module.exports = {
  getStrategiesPayload,
  getStrategyAnalytics,
  getStrategiesStatusSummary,
};
