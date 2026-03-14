const express = require("express");
const compression = require("compression");
const path = require("path");
const provider = require("./data-provider");
const strategiesProvider = require("./strategies-provider");

// ── Graceful error handling (prevents silent crashes) ───────────────────────
process.on("unhandledRejection", (reason) => {
  console.error("[FATAL] Unhandled promise rejection:", reason);
});
process.on("uncaughtException", (error) => {
  console.error("[FATAL] Uncaught exception:", error);
  process.exit(1);
});

const app = express();
const PORT = Number(process.env.PORT || 3000);
const HOST = String(process.env.HOST || "0.0.0.0");
const API_CACHE_TTL_MS = Number(process.env.API_CACHE_TTL_MS || 60_000);
const NAV_API_CACHE_TTL_MS = Number(process.env.NAV_API_CACHE_TTL_MS || 120_000);
const STATUS_API_CACHE_TTL_MS = Number(process.env.STATUS_API_CACHE_TTL_MS || 15_000);
const COMPOSITION_API_CACHE_TTL_MS = Number(
  process.env.COMPOSITION_API_CACHE_TTL_MS || 20_000
);
const STRATEGY_API_CACHE_TTL_MS = Number(
  process.env.STRATEGY_API_CACHE_TTL_MS || 30_000
);
const STRATEGY_ANALYTICS_API_CACHE_TTL_MS = Number(
  process.env.STRATEGY_ANALYTICS_API_CACHE_TTL_MS || 90_000
);
const EXPOSE_ERROR_DETAILS =
  process.env.EXPOSE_ERROR_DETAILS != null &&
  !["0", "false", "no"].includes(
    String(process.env.EXPOSE_ERROR_DETAILS).trim().toLowerCase()
  );
const PUBLIC_API_ERROR_MESSAGE = String(
  process.env.PUBLIC_API_ERROR_MESSAGE || "Временная ошибка. Повторите позже."
).trim();
const ACCESS_LINK_TOKEN = String(process.env.ACCESS_LINK_TOKEN || "").trim();
const ACCESS_COOKIE_NAME = String(process.env.ACCESS_COOKIE_NAME || "fi_access").trim();
const ACCESS_COOKIE_MAX_AGE_SEC = Number(
  process.env.ACCESS_COOKIE_MAX_AGE_SEC || 60 * 60 * 24 * 30
);
const REFRESH_COOLDOWN_MS = Number(process.env.REFRESH_COOLDOWN_MS || 30_000);
const API_CACHE_MAX_ENTRIES = Number(process.env.API_CACHE_MAX_ENTRIES || 300);
const REQUEST_TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS || 120_000);
const apiCache = new Map();
const refreshLastTriggeredAt = { compositions: 0, strategies: 0 };
const IS_APP_CONTAINER_DIR = path.resolve(__dirname) === "/app";
const STARTUP_WARMUP_ENABLED =
  process.env.STARTUP_WARMUP_ENABLED != null
    ? !["0", "false", "no"].includes(
        String(process.env.STARTUP_WARMUP_ENABLED).trim().toLowerCase()
      )
    : !IS_APP_CONTAINER_DIR;
const HEALTH_PATHS = new Set([
  "/api/healthz",
  "/api/diag",
  "/healthz",
  "/health",
  "/ready",
  "/live",
]);

app.use(compression({ threshold: 1024 }));

// Global request timeout — prevents hung connections.
app.use((req, res, next) => {
  req.setTimeout(REQUEST_TIMEOUT_MS);
  res.setTimeout(REQUEST_TIMEOUT_MS, () => {
    if (!res.headersSent) {
      res.status(504).json({ error: "Превышено время ожидания запроса" });
    }
  });
  next();
});

function parseCookieHeader(cookieHeader) {
  const out = {};
  const raw = String(cookieHeader || "");
  if (!raw) return out;
  for (const pair of raw.split(";")) {
    const idx = pair.indexOf("=");
    if (idx <= 0) continue;
    const key = pair.slice(0, idx).trim();
    const value = pair.slice(idx + 1).trim();
    if (!key) continue;
    try {
      out[key] = decodeURIComponent(value);
    } catch {
      out[key] = value;
    }
  }
  return out;
}

function hasAccessCookie(req) {
  if (!ACCESS_LINK_TOKEN) return true;
  const cookies = parseCookieHeader(req.headers.cookie);
  return cookies[ACCESS_COOKIE_NAME] === ACCESS_LINK_TOKEN;
}

function setAccessCookie(req, res) {
  const maxAge = Number.isFinite(ACCESS_COOKIE_MAX_AGE_SEC) && ACCESS_COOKIE_MAX_AGE_SEC > 0
    ? Math.floor(ACCESS_COOKIE_MAX_AGE_SEC)
    : 60 * 60 * 24 * 30;
  const parts = [
    `${ACCESS_COOKIE_NAME}=${encodeURIComponent(ACCESS_LINK_TOKEN)}`,
    "Path=/",
    "HttpOnly",
    "SameSite=Lax",
    `Max-Age=${maxAge}`,
  ];
  if (req.secure || req.headers["x-forwarded-proto"] === "https") {
    parts.push("Secure");
  }
  res.setHeader("Set-Cookie", parts.join("; "));
}

function stripAccessQuery(urlString) {
  const text = String(urlString || "/");
  const qIndex = text.indexOf("?");
  if (qIndex < 0) return text || "/";
  const base = text.slice(0, qIndex) || "/";
  const params = new URLSearchParams(text.slice(qIndex + 1));
  params.delete("access");
  const qs = params.toString();
  return qs ? `${base}?${qs}` : base;
}

function isHealthcheckPath(pathname) {
  return HEALTH_PATHS.has(String(pathname || "").trim());
}

app.use((req, res, next) => {
  if (!ACCESS_LINK_TOKEN) {
    next();
    return;
  }

  // Let platform healthcheck pass even when app is private.
  if (isHealthcheckPath(req.path)) {
    next();
    return;
  }

  const accessFromQuery =
    req.query && typeof req.query.access === "string"
      ? req.query.access.trim()
      : "";
  if (accessFromQuery && accessFromQuery === ACCESS_LINK_TOKEN) {
    setAccessCookie(req, res);
    const redirectTo = stripAccessQuery(req.originalUrl || req.url || "/");
    res.redirect(302, redirectTo);
    return;
  }

  if (hasAccessCookie(req)) {
    next();
    return;
  }

  res.status(404).send("Not found");
});

function getApiCacheKey(req) {
  const params = new URLSearchParams();
  const queryEntries = Object.entries(req.query || {}).sort(([a], [b]) =>
    a.localeCompare(b, "en")
  );
  for (const [key, value] of queryEntries) {
    if (Array.isArray(value)) {
      for (const v of value) params.append(key, String(v));
    } else if (value != null) {
      params.set(key, String(value));
    }
  }
  const qs = params.toString();
  return qs ? `${req.path}?${qs}` : req.path;
}

function getCachedJson(key) {
  const entry = apiCache.get(key);
  if (!entry) return null;
  if (Date.now() > entry.expiresAt) {
    apiCache.delete(key);
    return null;
  }
  return entry.payload;
}

function setCachedJson(key, payload, ttlMs) {
  if (!Number.isFinite(ttlMs) || ttlMs <= 0) return;
  if (apiCache.size >= API_CACHE_MAX_ENTRIES) {
    const oldestKey = apiCache.keys().next().value;
    if (oldestKey) apiCache.delete(oldestKey);
  }
  apiCache.set(key, { payload, expiresAt: Date.now() + ttlMs });
}

function invalidateApiCacheByPrefix(prefix) {
  for (const key of [...apiCache.keys()]) {
    if (key === prefix || key.startsWith(`${prefix}?`)) {
      apiCache.delete(key);
    }
  }
}

function sendJson(res, payload, cacheControl) {
  if (cacheControl) res.set("Cache-Control", cacheControl);
  res.json(payload);
}

function toSafeNumber(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function sanitizeErrorFlag(value) {
  if (!value) return null;
  // Return truncated error message instead of just boolean
  return typeof value === "string" ? value.slice(0, 200) : true;
}

function sanitizeProgressMeta(progress) {
  if (!progress || typeof progress !== "object") return null;
  return {
    startedAt: progress.startedAt || null,
    mode: progress.mode || null,
    candidateTotal: Number(progress.candidateTotal) || 0,
    processed: Number(progress.processed) || 0,
    successCount: Number(progress.successCount) || 0,
    noStructureCount: Number(progress.noStructureCount) || 0,
    failedCount: Number(progress.failedCount) || 0,
  };
}

function sanitizeStatusPayload(payload, strategiesSummary) {
  const source = payload && typeof payload === "object" ? payload : {};
  const market = source.market && typeof source.market === "object" ? source.market : {};
  const compositions =
    source.compositions && typeof source.compositions === "object"
      ? source.compositions
      : {};
  const strategies =
    strategiesSummary && typeof strategiesSummary === "object" ? strategiesSummary : {};

  return {
    ok: source.ok !== false,
    source: "private",
    funds: Number(source.funds) || 0,
    generatedAt: source.generatedAt || null,
    generatedAgeDays: toSafeNumber(source.generatedAgeDays),
    staleMarkDays: toSafeNumber(source.staleMarkDays),
    refreshing: {
      market: Boolean(source.refreshing && source.refreshing.market),
      compositions: Boolean(source.refreshing && source.refreshing.compositions),
    },
    market: {
      generatedAt: market.generatedAt || null,
      generatedAgeDays: toSafeNumber(market.generatedAgeDays),
      latestDataDate: market.latestDataDate || null,
      latestDataAgeDays: toSafeNumber(market.latestDataAgeDays),
      refreshing: Boolean(market.refreshing),
      failedFunds: Number(market.failedFunds) || 0,
      failedHistoryBatches: Number(market.failedHistoryBatches) || 0,
      failedListPages: Number(market.failedListPages) || 0,
      nextRetryAt: market.nextRetryAt || null,
      lastError: sanitizeErrorFlag(market.lastError),
    },
    compositions: {
      generatedAt: compositions.generatedAt || null,
      fundUniverse: toSafeNumber(compositions.fundUniverse),
      itemsCount: Number(compositions.itemsCount) || 0,
      refreshedCount: toSafeNumber(compositions.refreshedCount),
      staleMarkDays: toSafeNumber(compositions.staleMarkDays),
      staleCount: Number(compositions.staleCount) || 0,
      maxAgeDays: toSafeNumber(compositions.maxAgeDays),
      latestStructureDate: compositions.latestStructureDate || null,
      refreshing: Boolean(compositions.refreshing),
      progress: sanitizeProgressMeta(compositions.progress),
      failedFunds: Number(compositions.failedFunds) || 0,
      nextRetryAt: compositions.nextRetryAt || null,
      lastError: sanitizeErrorFlag(compositions.lastError),
    },
    strategies: {
      generatedAt: strategies.generatedAt || null,
      loadedAt: strategies.loadedAt || null,
      stale: Boolean(strategies.stale),
      refreshing: Boolean(strategies.refreshing),
      lastError: sanitizeErrorFlag(strategies.lastError),
      failedDetailsCount: Number(strategies.failedDetailsCount) || 0,
      total: Number(strategies.total) || 0,
      lastRun:
        strategies.lastRun && typeof strategies.lastRun === "object"
          ? {
              ok: strategies.lastRun.ok !== false,
              startedAt: strategies.lastRun.startedAt || null,
              finishedAt: strategies.lastRun.finishedAt || null,
            }
          : null,
    },
  };
}

function sanitizeCompositionsPayload(payload) {
  const source = payload && typeof payload === "object" ? payload : {};
  const meta = source.meta && typeof source.meta === "object" ? source.meta : {};
  const items = Array.isArray(source.items) ? source.items : [];
  return {
    items: items.map((row) => {
      const safe = row && typeof row === "object" ? { ...row } : {};
      // Not used by UI; keeps internal parser identifiers private.
      delete safe.source_id;
      return safe;
    }),
    meta: {
      generatedAt: meta.generatedAt || null,
      fundUniverse: toSafeNumber(meta.fundUniverse),
      itemsCount: Number(meta.itemsCount) || 0,
      refreshedCount: toSafeNumber(meta.refreshedCount),
      staleMarkDays: toSafeNumber(meta.staleMarkDays),
      staleCount: Number(meta.staleCount) || 0,
      maxAgeDays: toSafeNumber(meta.maxAgeDays),
      latestStructureDate: meta.latestStructureDate || null,
      refreshing: Boolean(meta.refreshing),
      progress: sanitizeProgressMeta(meta.progress),
      failedFunds: Number(meta.failedFunds) || 0,
      nextRetryAt: meta.nextRetryAt || null,
      lastError: sanitizeErrorFlag(meta.lastError),
    },
  };
}

function sanitizeStrategiesPayload(payload) {
  const source = payload && typeof payload === "object" ? payload : {};
  if (Array.isArray(source)) return source;
  return {
    generatedAt: source.generatedAt || null,
    loadedAt: source.loadedAt || null,
    stale: Boolean(source.stale),
    refreshing: Boolean(source.refreshing),
    lastError: sanitizeErrorFlag(source.lastError),
    total: Number(source.total) || 0,
    activeCount: toSafeNumber(source.activeCount),
    tabCount: toSafeNumber(source.tabCount),
    failedDetailsCount: toSafeNumber(source.failedDetailsCount),
    items: Array.isArray(source.items) ? source.items : [],
  };
}

function isRefreshingStatusPayload(payload) {
  const source = payload && typeof payload === "object" ? payload : {};
  return Boolean(
    (source.refreshing && (source.refreshing.market || source.refreshing.compositions)) ||
      (source.market && source.market.refreshing) ||
      (source.compositions && source.compositions.refreshing) ||
      (source.strategies && source.strategies.refreshing)
  );
}

function sendApiError(res, statusCode, message, error) {
  const payload = { error: PUBLIC_API_ERROR_MESSAGE };
  if (EXPOSE_ERROR_DETAILS && message) {
    payload.publicMessage = String(message);
  }
  if (EXPOSE_ERROR_DETAILS) {
    payload.details = String(error && error.message ? error.message : error || "");
  }
  res.status(statusCode).json(payload);
}

function parseFromTo(query) {
  const normalizeIsoDate = (value) => {
    if (!value) return null;
    const s = String(value).trim();
    return /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : null;
  };
  let from = normalizeIsoDate(query.from);
  let to = normalizeIsoDate(query.to);
  if (from && to && from > to) {
    const tmp = from;
    from = to;
    to = tmp;
  }
  return { from, to };
}

app.get("/api/funds", async (req, res) => {
  try {
    const cacheKey = getApiCacheKey(req);
    const cached = getCachedJson(cacheKey);
    if (cached) {
      sendJson(res, cached, "public, max-age=60");
      return;
    }

    const data = await provider.getFunds();
    setCachedJson(cacheKey, data, API_CACHE_TTL_MS);
    sendJson(res, data, "public, max-age=60");
  } catch (error) {
    sendApiError(res, 500, "Не удалось сформировать список фондов", error);
  }
});

for (const healthPath of HEALTH_PATHS) {
  app.get(healthPath, (req, res) => {
    sendJson(
      res,
      {
        ok: true,
        ts: new Date().toISOString(),
      },
      "no-store"
    );
  });
}

app.get("/api/returns", async (req, res) => {
  try {
    const cacheKey = getApiCacheKey(req);
    const cached = getCachedJson(cacheKey);
    if (cached) {
      sendJson(res, cached, "public, max-age=60");
      return;
    }

    const data = await provider.getReturns();
    setCachedJson(cacheKey, data, API_CACHE_TTL_MS);
    sendJson(res, data, "public, max-age=60");
  } catch (error) {
    sendApiError(res, 500, "Не удалось сформировать доходности фондов", error);
  }
});

app.get("/api/compositions", async (req, res) => {
  try {
    const forceRefresh =
      req.query.refresh != null &&
      String(req.query.refresh).toLowerCase() !== "0" &&
      String(req.query.refresh).toLowerCase() !== "false";

    if (!forceRefresh) {
      const cacheKey = getApiCacheKey(req);
      const cached = getCachedJson(cacheKey);
      if (cached) {
        sendJson(res, cached, "public, max-age=15");
        return;
      }
      const rawData = await provider.getCompositionsPayload({ forceRefresh: false });
      const data = sanitizeCompositionsPayload(rawData);
      setCachedJson(cacheKey, data, COMPOSITION_API_CACHE_TTL_MS);
      sendJson(res, data, "public, max-age=15");
      return;
    }

    const now = Date.now();
    if (now - refreshLastTriggeredAt.compositions < REFRESH_COOLDOWN_MS) {
      res.status(429).json({ error: "Слишком частые запросы на обновление. Повторите позже." });
      return;
    }
    refreshLastTriggeredAt.compositions = now;

    const rawData = await provider.getCompositionsPayload({ forceRefresh: true });
    const data = sanitizeCompositionsPayload(rawData);
    invalidateApiCacheByPrefix("/api/compositions");
    const isRefreshing =
      data &&
      data.meta &&
      typeof data.meta === "object" &&
      Boolean(data.meta.refreshing);
    if (!isRefreshing) {
      setCachedJson("/api/compositions", data, COMPOSITION_API_CACHE_TTL_MS);
    }
    sendJson(res, data, isRefreshing ? "no-store" : "public, max-age=5");
  } catch (error) {
    sendApiError(res, 500, "Не удалось сформировать составы фондов", error);
  }
});

app.get("/api/market", async (req, res) => {
  try {
    const cacheKey = getApiCacheKey(req);
    const cached = getCachedJson(cacheKey);
    if (cached) {
      sendJson(res, cached, "public, max-age=30");
      return;
    }

    const fundType = req.query.fund_type ? String(req.query.fund_type) : "all";
    const data = await provider.getMarketData(fundType);
    setCachedJson(cacheKey, data, API_CACHE_TTL_MS);
    sendJson(res, data, "public, max-age=30");
  } catch (error) {
    sendApiError(res, 500, "Не удалось сформировать данные вкладки Рынок", error);
  }
});

app.get("/api/mc-detail", async (req, res) => {
  try {
    const cacheKey = getApiCacheKey(req);
    const cached = getCachedJson(cacheKey);
    if (cached) {
      sendJson(res, cached, "public, max-age=30");
      return;
    }

    const mc = req.query.mc ? String(req.query.mc) : "";
    const fundType = req.query.fund_type ? String(req.query.fund_type) : "all";
    const data = await provider.getMCDetail(mc, fundType);
    setCachedJson(cacheKey, data, API_CACHE_TTL_MS);
    sendJson(res, data, "public, max-age=30");
  } catch (error) {
    sendApiError(res, 500, "Не удалось сформировать данные вкладки УК", error);
  }
});

app.get("/api/nav/:id", async (req, res) => {
  try {
    const cacheKey = getApiCacheKey(req);
    const cached = getCachedJson(cacheKey);
    if (cached) {
      sendJson(res, cached, "public, max-age=120");
      return;
    }

    const fundId = Number(req.params.id);
    if (!Number.isFinite(fundId)) {
      sendApiError(res, 400, "Некорректный id фонда");
      return;
    }

    const { from, to } = parseFromTo(req.query);
    const data = await provider.getNavByFundId(fundId, from, to);

    if (!data) {
      sendApiError(res, 404, "Фонд не найден");
      return;
    }

    const payload = { data };
    setCachedJson(cacheKey, payload, NAV_API_CACHE_TTL_MS);
    sendJson(res, payload, "public, max-age=120");
  } catch (error) {
    sendApiError(res, 500, "Не удалось загрузить историю фонда", error);
  }
});

// Diagnostic endpoint — test connectivity to data sources
app.get("/api/diag", async (req, res) => {
  const results = {};
  const testFetch = async (name, url) => {
    const start = Date.now();
    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 10_000);
      const r = await fetch(url, {
        signal: controller.signal,
        headers: { "user-agent": "fund-dashboard/1.0" },
      });
      clearTimeout(timer);
      const ms = Date.now() - start;
      results[name] = { ok: r.ok, status: r.status, ms };
    } catch (err) {
      const ms = Date.now() - start;
      results[name] = { ok: false, error: String(err.message || err).slice(0, 200), ms };
    }
  };
  await Promise.all([
    testFetch("investfunds_list", "https://investfunds.ru/funds/?showID=54&limit=50&page=1"),
    testFetch("investfunds_fund", "https://investfunds.ru/funds/4695/"),
    testFetch("tbank_api", "https://www.tbank.ru/api/invest-gw/tracking/api/v1/strategy/catalog/tab"),
  ]);
  results.node_version = process.version;
  results.platform = process.platform;
  results.uptime_min = Math.round(process.uptime() / 60);
  res.json(results);
});

app.get("/api/status", async (req, res) => {
  try {
    const cacheKey = getApiCacheKey(req);
    const cached = getCachedJson(cacheKey);
    if (cached) {
      sendJson(res, cached, "public, max-age=10");
      return;
    }

    const [rawPayload, strategiesSummary] = await Promise.all([
      provider.getStatusSummary(),
      Promise.resolve()
        .then(() => strategiesProvider.getStrategiesStatusSummary())
        .catch(() => null),
    ]);
    const payload = sanitizeStatusPayload(rawPayload, strategiesSummary);
    const isRefreshing = isRefreshingStatusPayload(payload);
    if (!isRefreshing) {
      setCachedJson(cacheKey, payload, STATUS_API_CACHE_TTL_MS);
    }
    sendJson(res, payload, isRefreshing ? "no-store" : "public, max-age=10");
  } catch (error) {
    sendApiError(res, 500, "Не удалось получить статус", error);
  }
});

app.get("/api/strategies", async (req, res) => {
  try {
    const forceRefresh =
      req.query.refresh != null &&
      String(req.query.refresh).toLowerCase() !== "0" &&
      String(req.query.refresh).toLowerCase() !== "false";

    const cacheKey = getApiCacheKey(req);
    if (!forceRefresh) {
      const cached = getCachedJson(cacheKey);
      if (cached) {
        sendJson(res, cached, "public, max-age=20");
        return;
      }
    }

    if (forceRefresh) {
      const now = Date.now();
      if (now - refreshLastTriggeredAt.strategies < REFRESH_COOLDOWN_MS) {
        res.status(429).json({ error: "Слишком частые запросы на обновление. Повторите позже." });
        return;
      }
      refreshLastTriggeredAt.strategies = now;
    }

    const rawPayload = await strategiesProvider.getStrategiesPayload({ forceRefresh });
    const payload = sanitizeStrategiesPayload(rawPayload);
    const isRefreshing = Boolean(payload && payload.refreshing);
    if (forceRefresh) invalidateApiCacheByPrefix("/api/strategies");
    if (!isRefreshing) {
      setCachedJson(
        forceRefresh ? "/api/strategies" : cacheKey,
        payload,
        STRATEGY_API_CACHE_TTL_MS
      );
    }
    sendJson(res, payload, isRefreshing ? "no-store" : forceRefresh ? "public, max-age=5" : "public, max-age=20");
  } catch (error) {
    sendApiError(res, 500, "Не удалось загрузить стратегии автоследования", error);
  }
});

app.get("/api/strategies/:id/analytics", async (req, res) => {
  try {
    const strategyId = String(req.params.id || "").trim();
    if (!strategyId || !/^[a-zA-Z0-9_-]{1,64}$/.test(strategyId)) {
      sendApiError(res, 400, "Некорректный id стратегии");
      return;
    }

    const forceRefresh =
      req.query.refresh != null &&
      String(req.query.refresh).toLowerCase() !== "0" &&
      String(req.query.refresh).toLowerCase() !== "false";

    const cacheKey = getApiCacheKey(req);
    if (!forceRefresh) {
      const cached = getCachedJson(cacheKey);
      if (cached) {
        sendJson(res, cached, "public, max-age=45");
        return;
      }
    }

    const payload = await strategiesProvider.getStrategyAnalytics(strategyId, {
      forceRefresh,
    });
    if (forceRefresh) invalidateApiCacheByPrefix(req.path);
    setCachedJson(
      forceRefresh ? req.path : cacheKey,
      payload,
      STRATEGY_ANALYTICS_API_CACHE_TTL_MS
    );
    sendJson(res, payload, forceRefresh ? "public, max-age=10" : "public, max-age=45");
  } catch (error) {
    sendApiError(res, 500, "Не удалось загрузить график стратегии", error);
  }
});

app.use(
  express.static(path.join(__dirname, "public"), {
    etag: true,
    setHeaders(res, filePath) {
      if (path.basename(filePath) === "index.html") {
        res.setHeader("Cache-Control", "no-cache");
      } else {
        res.setHeader("Cache-Control", "public, max-age=604800, immutable");
      }
    },
  })
);

app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

function startServer(port = PORT, host = HOST) {
  const server = app.listen(port, host, () => {
    const address = server.address();
    const actualPort =
      address && typeof address === "object" && address.port ? address.port : port;
    const actualHost =
      address && typeof address === "object" && address.address
        ? address.address
        : host;
    console.log(
      `Fund dashboard (independent): http://${actualHost}:${actualPort}`
    );
    if (!STARTUP_WARMUP_ENABLED) return;
    // Defer warmup slightly so platform probes can pass quickly on cold start.
    const WARMUP_DELAY_MS = 300;
    const warmupTimer = setTimeout(() => {
      // Preload disk/seed snapshots so first user request is fast.
      provider
        .getDataset()
        .catch((error) =>
          console.warn(`[warmup] dataset preload failed: ${String(error.message || error)}`)
        );
      provider
        .getCompositionsPayload({ forceRefresh: false })
        .catch((error) =>
          console.warn(
            `[warmup] compositions preload failed: ${String(error.message || error)}`
          )
        );
      strategiesProvider
        .getStrategiesPayload({ forceRefresh: false })
        .catch((error) =>
          console.warn(
            `[warmup] strategies preload failed: ${String(error.message || error)}`
          )
        );
    }, WARMUP_DELAY_MS);
    if (typeof warmupTimer.unref === "function") warmupTimer.unref();
  });
  return server;
}

// ── Proactive daily scheduler ────────────────────────────────────────────────
// Checks every 5 minutes; at 10:00 MSK triggers refresh of all data sources.

const DAILY_REFRESH_HOUR_MSK = Number(process.env.DAILY_REFRESH_HOUR_MSK || 10);
const SCHEDULER_INTERVAL_MS = 5 * 60 * 1000;
const schedulerState = { lastRefreshDate: null };

function startDailyScheduler() {
  const tick = async () => {
    try {
      const now = new Date();
      const mskHour = Number(
        new Intl.DateTimeFormat("en-GB", { timeZone: "Europe/Moscow", hour: "2-digit", hour12: false }).format(now)
      );
      const mskDate = new Intl.DateTimeFormat("sv-SE", { timeZone: "Europe/Moscow" }).format(now);

      if (DAILY_REFRESH_HOUR_MSK < 0) return;
      if (!Number.isFinite(mskHour) || mskHour < DAILY_REFRESH_HOUR_MSK) return;
      if (schedulerState.lastRefreshDate === mskDate) return;

      schedulerState.lastRefreshDate = mskDate;
      console.log(`[scheduler] Daily refresh triggered (${mskDate} ${mskHour}:xx MSK)`);

      // Refresh all three data sources in parallel
      provider.getDataset().catch((err) => {
        console.warn(`[scheduler] Market refresh failed: ${String(err.message || err)}`);
      });
      provider.getCompositionsPayload({ forceRefresh: true }).catch((err) => {
        console.warn(`[scheduler] Compositions refresh failed: ${String(err.message || err)}`);
      });
      strategiesProvider.getStrategiesPayload({ forceRefresh: true }).catch((err) => {
        console.warn(`[scheduler] Strategies refresh failed: ${String(err.message || err)}`);
      });
    } catch (err) {
      console.warn(`[scheduler] Tick error: ${String(err.message || err)}`);
    }
  };

  const timer = setInterval(tick, SCHEDULER_INTERVAL_MS);
  if (typeof timer.unref === "function") timer.unref();
  console.log(`[scheduler] Daily auto-refresh: all sources at ${DAILY_REFRESH_HOUR_MSK}:00 MSK`);
}

startDailyScheduler();

if (require.main === module) {
  startServer();
}

module.exports = {
  app,
  startServer,
};
