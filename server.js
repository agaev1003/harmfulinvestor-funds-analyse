const express = require("express");
const compression = require("compression");
const path = require("path");
const provider = require("./data-provider");

const app = express();
const PORT = Number(process.env.PORT || 3000);
const API_CACHE_TTL_MS = Number(process.env.API_CACHE_TTL_MS || 60_000);
const NAV_API_CACHE_TTL_MS = Number(process.env.NAV_API_CACHE_TTL_MS || 120_000);
const STATUS_API_CACHE_TTL_MS = Number(process.env.STATUS_API_CACHE_TTL_MS || 15_000);
const COMPOSITION_API_CACHE_TTL_MS = Number(
  process.env.COMPOSITION_API_CACHE_TTL_MS || 20_000
);
const API_CACHE_MAX_ENTRIES = Number(process.env.API_CACHE_MAX_ENTRIES || 300);
const apiCache = new Map();

app.use(compression({ threshold: 1024 }));

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
    res.status(500).json({
      error: "Не удалось сформировать список фондов",
      details: String(error.message || error),
    });
  }
});

app.get("/api/healthz", (req, res) => {
  sendJson(
    res,
    {
      ok: true,
      ts: new Date().toISOString(),
    },
    "no-store"
  );
});

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
    res.status(500).json({
      error: "Не удалось сформировать доходности фондов",
      details: String(error.message || error),
    });
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
      const data = await provider.getCompositionsPayload({ forceRefresh: false });
      setCachedJson(cacheKey, data, COMPOSITION_API_CACHE_TTL_MS);
      sendJson(res, data, "public, max-age=15");
      return;
    }

    const data = await provider.getCompositionsPayload({ forceRefresh: true });
    invalidateApiCacheByPrefix("/api/compositions");
    setCachedJson("/api/compositions", data, COMPOSITION_API_CACHE_TTL_MS);
    sendJson(res, data, "public, max-age=5");
  } catch (error) {
    res.status(500).json({
      error: "Не удалось сформировать составы фондов",
      details: String(error.message || error),
    });
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
    res.status(500).json({
      error: "Не удалось сформировать данные вкладки Рынок",
      details: String(error.message || error),
    });
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
    res.status(500).json({
      error: "Не удалось сформировать данные вкладки УК",
      details: String(error.message || error),
    });
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
      res.status(400).json({ error: "Некорректный id фонда" });
      return;
    }

    const { from, to } = parseFromTo(req.query);
    const data = await provider.getNavByFundId(fundId, from, to);

    if (!data) {
      res.status(404).json({ error: "Фонд не найден" });
      return;
    }

    const payload = { data };
    setCachedJson(cacheKey, payload, NAV_API_CACHE_TTL_MS);
    sendJson(res, payload, "public, max-age=120");
  } catch (error) {
    res.status(500).json({
      error: "Не удалось загрузить историю фонда",
      details: String(error.message || error),
    });
  }
});

app.get("/api/status", async (req, res) => {
  try {
    const cacheKey = getApiCacheKey(req);
    const cached = getCachedJson(cacheKey);
    if (cached) {
      sendJson(res, cached, "public, max-age=10");
      return;
    }

    const payload = await provider.getStatusSummary();
    setCachedJson(cacheKey, payload, STATUS_API_CACHE_TTL_MS);
    sendJson(
      res,
      payload,
      "public, max-age=10"
    );
  } catch (error) {
    res.status(500).json({
      ok: false,
      error: String(error.message || error),
    });
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

function startServer(port = PORT) {
  const server = app.listen(port, () => {
    console.log(`Fund dashboard (independent): http://localhost:${port}`);
    // Preload disk/seed snapshots on cold start so first user request is fast.
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
  });
  return server;
}

if (require.main === module) {
  startServer();
}

module.exports = {
  app,
  startServer,
};
