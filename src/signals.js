/**
 * Signal route handlers.
 *
 * Key production properties:
 *
 * 1. Atomic idempotency
 *    When an Idempotency-Key header is present, the request is routed through
 *    `upsertSignal()` which runs INSERT OR IGNORE + SELECT in one SQLite
 *    transaction. This means N concurrent requests with the same key will all
 *    receive the identical canonical row — there is no check-then-insert race.
 *
 * 2. Retry with exponential backoff + jitter
 *    Every DB call is wrapped in `withRetry`. On a transient error (SQLITE_BUSY,
 *    SQLITE_LOCKED, simulated_db_failure) the call is retried up to MAX_ATTEMPTS
 *    times with exponential delay + full jitter to avoid thundering-herd.
 *    Because idempotency is enforced at the DB level, retrying is safe — the
 *    second (and third) attempt will resolve to the same row.
 *
 * 3. Sliding-window rate limiting (see rateLimit.js)
 */

import { upsertSignal, insertSignal, listSignals } from './db.js';
import { checkAndConsume } from './rateLimit.js';

// ---------------------------------------------------------------------------
// Retry helper
// ---------------------------------------------------------------------------

const MAX_ATTEMPTS = 3;
const BASE_DELAY_MS = 50; // 50 ms → 100 ms → (max 2 attempts beyond first)

/** Error codes that indicate a transient, retriable DB condition. */
function isTransient(err) {
  return (
    err.code === 'SQLITE_BUSY' ||
    err.code === 'SQLITE_LOCKED' ||
    (err.message && err.message.includes('simulated_db_failure'))
  );
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Run `fn` (a synchronous DB call) with exponential backoff + full jitter.
 *
 * Full jitter: delay = random(0, base * 2^(attempt-1))
 * This avoids correlated retries across concurrent callers.
 *
 * @param {() => any} fn   Synchronous function to attempt
 * @returns {Promise<any>}
 */
async function withRetry(fn) {
  let lastErr;
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      return fn();
    } catch (err) {
      lastErr = err;
      if (attempt === MAX_ATTEMPTS || !isTransient(err)) {
        throw err;
      }
      // Full jitter: uniform random in [0, base * 2^(attempt-1)]
      const cap = BASE_DELAY_MS * 2 ** (attempt - 1);
      const jitter = Math.random() * cap;
      await sleep(jitter);
    }
  }
  throw lastErr;
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

/**
 * POST /v1/signals
 *
 * Body:    { userId, type, payload }
 * Headers: X-API-Key (required, checked by server hook)
 *          Idempotency-Key (optional)
 *
 * Responses:
 *   201 — created (new signal)
 *   200 — already exists (idempotent replay)
 *   400 — invalid body
 *   429 — rate limited
 *   503 — DB unavailable after all retries
 */
export async function postSignal(req, reply) {
  const idem = req.headers['idempotency-key'] || null;
  const { userId, type, payload } = req.body || {};

  if (!userId || !type || typeof payload === 'undefined') {
    return reply.code(400).send({ error: 'invalid_body' });
  }

  // Rate limit check (sliding window, in-process)
  const { ok, remaining, resetMs } = checkAndConsume(userId);
  if (!ok) {
    return reply
      .code(429)
      .header('X-RateLimit-Remaining', '0')
      .header('X-RateLimit-Reset', String(resetMs))
      .send({ error: 'rate_limited', remaining, resetMs });
  }

  const nowMs = Date.now();

  try {
    if (idem) {
      // Atomic: INSERT OR IGNORE + SELECT in one transaction — race-free
      const row = await withRetry(() => upsertSignal(userId, type, payload, idem, nowMs));

      // Determine whether this was a fresh insert or a replay.
      // A fresh insert will have createdAt === nowMs (within ms precision).
      // A replay will have a different (earlier) createdAt.
      // Use the row's own createdAt to detect replays.
      const isReplay = row.createdAt !== nowMs;

      return reply
        .code(isReplay ? 200 : 201)
        .header('X-RateLimit-Remaining', String(remaining))
        .header('X-RateLimit-Reset', String(resetMs))
        .header('Idempotency-Key', idem)
        .send(row);
    } else {
      // No idempotency key — plain insert, always creates a new row
      const info = await withRetry(() =>
        insertSignal(userId, type, payload, null, nowMs)
      );
      return reply
        .code(201)
        .header('X-RateLimit-Remaining', String(remaining))
        .header('X-RateLimit-Reset', String(resetMs))
        .send({
          id: Number(info.lastInsertRowid),
          userId,
          type,
          payload: String(payload),
          idempotencyKey: null,
          createdAt: nowMs,
        });
    }
  } catch (err) {
    req.log.error({ err, ctx: 'postSignal' });
    return reply.code(503).send({ error: 'db_unavailable' });
  }
}

/**
 * GET /v1/signals?userId=...&limit=...
 *
 * Returns the most recent `limit` (max 100, default 20) signals for `userId`.
 */
export async function getSignals(req, reply) {
  const { userId, limit = 20 } = req.query || {};
  if (!userId) return reply.code(400).send({ error: 'missing_userId' });

  const lim = Math.min(Number(limit) || 20, 100);

  try {
    const rows = await withRetry(() => listSignals(userId, lim));
    return reply
      .code(200)
      .send({ items: rows, count: rows.length });
  } catch (err) {
    req.log.error({ err, ctx: 'getSignals' });
    return reply.code(503).send({ error: 'db_unavailable' });
  }
}
