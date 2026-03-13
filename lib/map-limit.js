/**
 * Unified mapLimit — runs `worker` over `items` with at most `limit` concurrent tasks.
 * Supports `continueOnError` to keep going when individual items fail (stores null).
 *
 * Fixes P3 (two divergent implementations) and P7 (potential race on cursor).
 */
async function mapLimit(items, limit, worker, { continueOnError = false } = {}) {
  if (!items.length) return [];
  const capped = Math.max(1, Math.min(limit, items.length));
  const results = new Array(items.length);
  let cursor = 0;
  let fatalError = null;

  const workers = Array.from({ length: capped }, async () => {
    while (cursor < items.length) {
      if (fatalError) break;
      // Single expression increment avoids any micro-task interleaving issue.
      const i = cursor++;
      if (i >= items.length) break;
      try {
        results[i] = await worker(items[i], i);
      } catch (error) {
        if (continueOnError) {
          results[i] = null;
        } else {
          fatalError = error;
          break;
        }
      }
    }
  });

  await Promise.all(workers);
  if (fatalError) throw fatalError;
  return results;
}

module.exports = mapLimit;
