const { RETRY_DELAY_BASE_MS } = require("./utils");

/**
 * fetch with AbortController-based timeout.
 * `defaultTimeoutMs` is used when options.timeoutMs is not provided.
 */
async function fetchWithTimeout(url, options = {}, defaultTimeoutMs = 25_000) {
  const timeoutMs =
    Number.isFinite(Number(options.timeoutMs)) && Number(options.timeoutMs) > 0
      ? Number(options.timeoutMs)
      : defaultTimeoutMs;
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
        await new Promise((resolve) =>
          setTimeout(resolve, RETRY_DELAY_BASE_MS * (attempt + 1))
        );
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
        await new Promise((resolve) =>
          setTimeout(resolve, RETRY_DELAY_BASE_MS * (attempt + 1))
        );
      }
    }
  }
  throw lastErr;
}

module.exports = {
  fetchWithTimeout,
  fetchText,
  fetchJson,
};
