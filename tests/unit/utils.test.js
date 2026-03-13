const test = require("node:test");
const assert = require("node:assert/strict");

const {
  normalizeSpace,
  normalizeLower,
  csvLikeText,
  toNumber,
  round,
  withTimeout,
  RETRY_DELAY_BASE_MS,
  WARMUP_DELAY_MS,
} = require("../../lib/utils");
const mapLimit = require("../../lib/map-limit");

// ── normalizeSpace ──────────────────────────────────────────────────────────

test("normalizeSpace collapses whitespace and trims", () => {
  assert.equal(normalizeSpace("  hello   world  "), "hello world");
  assert.equal(normalizeSpace("\thello\n\nworld\t"), "hello world");
});

test("normalizeSpace handles nullish values", () => {
  assert.equal(normalizeSpace(null), "");
  assert.equal(normalizeSpace(undefined), "");
  assert.equal(normalizeSpace(""), "");
});

test("csvLikeText is an alias for normalizeSpace", () => {
  assert.equal(csvLikeText, normalizeSpace);
});

// ── normalizeLower ──────────────────────────────────────────────────────────

test("normalizeLower lowercases after normalizing space", () => {
  assert.equal(normalizeLower("  Hello  WORLD  "), "hello world");
});

// ── toNumber ────────────────────────────────────────────────────────────────

test("toNumber parses plain numbers", () => {
  assert.equal(toNumber(42), 42);
  assert.equal(toNumber("123"), 123);
  assert.equal(toNumber("3.14"), 3.14);
});

test("toNumber handles Russian formatting (non-breaking spaces, commas)", () => {
  assert.equal(toNumber("1\u00A0250,50"), 1250.5);
  assert.equal(toNumber("1 000 000"), 1000000);
});

test("toNumber returns null for non-numeric input", () => {
  assert.equal(toNumber(null), null);
  assert.equal(toNumber(undefined), null);
  assert.equal(toNumber(""), null);
  assert.equal(toNumber("-"), null);
  assert.equal(toNumber("abc"), null);
  assert.equal(toNumber(Infinity), null);
  assert.equal(toNumber(NaN), null);
});

test("toNumber handles negative values", () => {
  assert.equal(toNumber("-5,3"), -5.3);
});

// ── round ───────────────────────────────────────────────────────────────────

test("round rounds to given digits", () => {
  assert.equal(round(1.2345, 2), 1.23);
  assert.equal(round(1.2355, 2), 1.24);
  assert.equal(round(100.1, 0), 100);
});

test("round returns null for non-finite input", () => {
  assert.equal(round(NaN), null);
  assert.equal(round(Infinity), null);
});

test("round defaults to 2 digits", () => {
  assert.equal(round(3.456), 3.46);
});

// ── withTimeout ─────────────────────────────────────────────────────────────

test("withTimeout resolves when promise is fast", async () => {
  const result = await withTimeout(Promise.resolve(42), 5000, "test");
  assert.equal(result, 42);
});

test("withTimeout rejects on timeout", async () => {
  const slow = new Promise(() => {});
  await assert.rejects(
    () => withTimeout(slow, 1000, "slow-op"),
    (err) => err.message.includes("slow-op timeout")
  );
});

// ── constants ───────────────────────────────────────────────────────────────

test("constants are defined", () => {
  assert.equal(typeof RETRY_DELAY_BASE_MS, "number");
  assert.ok(RETRY_DELAY_BASE_MS > 0);
  assert.equal(typeof WARMUP_DELAY_MS, "number");
  assert.ok(WARMUP_DELAY_MS > 0);
});

// ── mapLimit ────────────────────────────────────────────────────────────────

test("mapLimit processes all items", async () => {
  const items = [1, 2, 3, 4, 5];
  const results = await mapLimit(items, 2, async (x) => x * 2);
  assert.deepEqual(results, [2, 4, 6, 8, 10]);
});

test("mapLimit respects concurrency limit", async () => {
  let concurrent = 0;
  let maxConcurrent = 0;
  const items = [1, 2, 3, 4, 5, 6];
  await mapLimit(items, 3, async (x) => {
    concurrent++;
    if (concurrent > maxConcurrent) maxConcurrent = concurrent;
    await new Promise((r) => setTimeout(r, 10));
    concurrent--;
    return x;
  });
  assert.ok(maxConcurrent <= 3, `Max concurrent was ${maxConcurrent}, expected <= 3`);
});

test("mapLimit returns empty array for empty input", async () => {
  const results = await mapLimit([], 5, async (x) => x);
  assert.deepEqual(results, []);
});

test("mapLimit throws on first error by default", async () => {
  await assert.rejects(
    () =>
      mapLimit([1, 2, 3], 2, async (x) => {
        if (x === 2) throw new Error("fail");
        return x;
      }),
    (err) => err.message === "fail"
  );
});

test("mapLimit continueOnError stores null for failed items", async () => {
  const results = await mapLimit(
    [1, 2, 3],
    2,
    async (x) => {
      if (x === 2) throw new Error("fail");
      return x * 10;
    },
    { continueOnError: true }
  );
  assert.equal(results[0], 10);
  assert.equal(results[1], null);
  assert.equal(results[2], 30);
});

test("mapLimit preserves result order", async () => {
  const items = [50, 10, 30, 20, 40];
  const results = await mapLimit(items, 3, async (x) => {
    await new Promise((r) => setTimeout(r, x));
    return x;
  });
  assert.deepEqual(results, [50, 10, 30, 20, 40]);
});
