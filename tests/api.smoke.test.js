process.env.ACCESS_LINK_TOKEN = "";
process.env.DAILY_MAIN_REFRESH_HOUR_MSK = process.env.DAILY_MAIN_REFRESH_HOUR_MSK || "-1";
process.env.DAILY_COMPOSITION_REFRESH_HOUR_MSK =
  process.env.DAILY_COMPOSITION_REFRESH_HOUR_MSK || "-1";
process.env.DATA_CACHE_TTL_MS =
  process.env.DATA_CACHE_TTL_MS || String(365 * 24 * 60 * 60 * 1000);
process.env.COMPOSITION_CACHE_TTL_MS =
  process.env.COMPOSITION_CACHE_TTL_MS || String(365 * 24 * 60 * 60 * 1000);

const test = require("node:test");
const assert = require("node:assert/strict");

const { startServer } = require("../server");

let server = null;
let baseUrl = "";

async function fetchJson(pathname, timeoutMs = 45_000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(`${baseUrl}${pathname}`, { signal: controller.signal });
    const body = await res.json();
    return { res, body };
  } finally {
    clearTimeout(timer);
  }
}

test.before(async () => {
  server = startServer(0);
  if (!server.listening) {
    await new Promise((resolve) => server.once("listening", resolve));
  }
  const addr = server.address();
  const port = addr && typeof addr === "object" ? addr.port : 0;
  baseUrl = `http://127.0.0.1:${port}`;
});

test.after(async () => {
  if (!server) return;
  await new Promise((resolve, reject) => {
    server.close((err) => (err ? reject(err) : resolve()));
  });
});

test("GET /api/healthz returns ok", async () => {
  const { res, body } = await fetchJson("/api/healthz", 15_000);
  assert.equal(res.status, 200);
  assert.equal(body.ok, true);
  assert.equal(typeof body.ts, "string");
});

test("GET /api/status returns summary payload", async () => {
  const { res, body } = await fetchJson("/api/status");
  assert.equal(res.status, 200);
  assert.equal(body.ok, true);
  assert.equal(typeof body.source, "string");
  assert.equal(typeof body.funds, "number");
  assert.ok(body.generatedAt === null || typeof body.generatedAt === "string");
  assert.ok(body.refreshing && typeof body.refreshing === "object");
  assert.ok(body.compositions && typeof body.compositions === "object");
});

test("GET /api/funds returns non-empty array", async () => {
  const { res, body } = await fetchJson("/api/funds");
  assert.equal(res.status, 200);
  assert.ok(Array.isArray(body));
  assert.ok(body.length > 0);
  const row = body[0];
  assert.ok(row && typeof row === "object");
  assert.ok(Number.isFinite(Number(row.id)));
  assert.equal(typeof row.name, "string");
});

test("GET /api/compositions returns items + meta", async () => {
  const { res, body } = await fetchJson("/api/compositions");
  assert.equal(res.status, 200);
  assert.ok(body && typeof body === "object");
  assert.ok(Array.isArray(body.items));
  assert.ok(body.meta && typeof body.meta === "object");
  assert.ok(
    body.meta.generatedAt === null || typeof body.meta.generatedAt === "string"
  );
  assert.equal(typeof body.meta.refreshing, "boolean");
});
