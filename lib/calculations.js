const { toNumber, round } = require("./utils");
const {
  shiftIsoDate,
  isoDateToUtcDate,
  toIsoFromMs,
  ageDaysFromIsoDate,
  normalizeHoldingName,
  normalizeHoldingKey,
} = require("./parser");

// ── Array search helpers (P11 fix: avoid [...arr].reverse().find()) ─────────

function findLastWhere(arr, predicate) {
  for (let i = arr.length - 1; i >= 0; i--) {
    if (predicate(arr[i])) return arr[i];
  }
  return null;
}

function findLastWithNav(history) {
  return findLastWhere(history, (p) => p.nav != null);
}

function findNavOnOrBefore(history, targetIsoDate) {
  for (let i = history.length - 1; i >= 0; i -= 1) {
    const point = history[i];
    if (point.time <= targetIsoDate && point.nav != null) return point.nav;
  }
  return null;
}

function getLatestAum(history) {
  for (let i = history.length - 1; i >= 0; i -= 1) {
    if (history[i].aum != null) return history[i].aum;
  }
  return 0;
}

function filterHistoryByRange(history, from, to) {
  if (!from && !to) return history;
  return history.filter((p) => {
    if (from && p.time < from) return false;
    if (to && p.time > to) return false;
    return true;
  });
}

// ── Chart series parsing ────────────────────────────────────────────────────

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

// ── Series merge ────────────────────────────────────────────────────────────

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

// ── Return computations ─────────────────────────────────────────────────────

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
  const lastAum = findLastWhere(history, (p) => p.aum != null);
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
  const latestAumPoint = findLastWhere(
    history,
    (p) => p.time <= lastDate && p.aum != null
  );
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

// ── Composition changes ─────────────────────────────────────────────────────

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

// ── Timeseries grouping ─────────────────────────────────────────────────────

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

module.exports = {
  findLastWhere,
  findLastWithNav,
  findNavOnOrBefore,
  getLatestAum,
  filterHistoryByRange,
  parseChartSeries,
  mergeFundSeries,
  mergeValueSeries,
  extractSeriesFromHistory,
  mergeHistoryIncremental,
  computeReturn,
  buildReturnsRow,
  buildHoldingMap,
  buildCompositionChanges,
  buildTimeseriesForGroups,
};
