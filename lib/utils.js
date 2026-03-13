const fs = require("fs/promises");
const path = require("path");

const RETRY_DELAY_BASE_MS = 400;
const WARMUP_DELAY_MS = 300;

function normalizeSpace(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function normalizeLower(value) {
  return normalizeSpace(value).toLowerCase();
}

// csvLikeText is functionally identical to normalizeSpace (P13 fix).
const csvLikeText = normalizeSpace;

function toNumber(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
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

function clearTimerSafe(timer) {
  if (!timer) return;
  clearTimeout(timer);
}

async function withTimeout(promise, timeoutMs, label) {
  const ms = Math.max(1_000, Number(timeoutMs) || 1_000);
  let timer = null;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => {
          reject(new Error(`${label} timeout after ${ms}ms`));
        }, ms);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function persistCacheSnapshot(filePath, data) {
  try {
    await fs.mkdir(path.dirname(filePath), { recursive: true });
    await fs.writeFile(filePath, JSON.stringify(data), "utf8");
    return true;
  } catch (error) {
    console.error(
      `[cache] Persist FAILED for ${filePath}: ${String(error.message || error)}`
    );
    return false;
  }
}

module.exports = {
  RETRY_DELAY_BASE_MS,
  WARMUP_DELAY_MS,
  normalizeSpace,
  normalizeLower,
  csvLikeText,
  toNumber,
  round,
  clearTimerSafe,
  withTimeout,
  persistCacheSnapshot,
};
