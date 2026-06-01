/**
 * Sliding-window rate limiter (in-process).
 *
 * Algorithm:
 *   Per userId, keep an array of timestamps for each request that was allowed
 *   within the current window. On every call:
 *     1. Drop timestamps older than (now - WINDOW_MS).
 *     2. If the remaining count >= RATE → reject.
 *     3. Otherwise push `nowMs` and allow.
 *
 * Correctness under concurrency:
 *   Node.js is single-threaded; JS callbacks run to completion without
 *   preemption, so the read-modify-write of the Map is atomic within a
 *   single process — no mutex needed.
 *
 * Multi-instance safety:
 *   In-memory state is NOT shared across Node.js processes/pods.
 *   For multi-instance deployments, replace this module with a Redis-backed
 *   sliding window implemented via a Lua script (see SCALE.md §Rate Limiting).
 */

const RATE = Number(process.env.RATE_LIMIT_PER_MIN || 5);
const WINDOW_MS = 60_000;

// userId -> number[]  (sorted ascending timestamps of allowed requests)
const windows = new Map();

/**
 * Check whether `userId` is within the rate limit and, if so, record the hit.
 *
 * @param {string} userId
 * @param {number} [nowMs]   Override for testing (default: Date.now())
 * @returns {{ ok: boolean, remaining: number, resetMs: number }}
 *   ok        - true if the request is allowed
 *   remaining - how many more requests are allowed in the current window
 *   resetMs   - epoch-ms when the oldest in-window hit will expire
 */
export function checkAndConsume(userId, nowMs = Date.now()) {
  const cutoff = nowMs - WINDOW_MS;

  // Evict timestamps that have slid out of the window
  const hits = (windows.get(userId) || []).filter((t) => t > cutoff);

  if (hits.length >= RATE) {
    // resetMs = when the oldest hit will fall out of the window
    const resetMs = hits[0] + WINDOW_MS;
    return { ok: false, remaining: 0, resetMs };
  }

  hits.push(nowMs);
  windows.set(userId, hits);

  const remaining = RATE - hits.length;
  // If this is the only hit, resetMs is 60s from now; otherwise it's when
  // the oldest hit expires.
  const resetMs = hits[0] + WINDOW_MS;
  return { ok: true, remaining, resetMs };
}

/**
 * Expose internals for testing (reset state between test runs).
 * NOT part of the public API.
 */
export function _resetForTesting() {
  windows.clear();
}
