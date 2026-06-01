/**
 * SQLite database layer.
 *
 * Concurrency settings:
 *   WAL mode   — allows concurrent readers and one writer; dramatically
 *                reduces SQLITE_BUSY errors under load.
 *   busy_timeout — SQLite will retry internally for up to N ms before
 *                  throwing SQLITE_BUSY, reducing the need for application-
 *                  level retries on contention.
 *
 * Idempotency:
 *   `upsertSignal` uses INSERT OR IGNORE + SELECT inside a single
 *   transaction. This makes dedup atomic — no check-then-insert race.
 */

import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';

const dbPath = process.env.DATABASE_URL || './data/signals.db';
fs.mkdirSync(path.dirname(dbPath), { recursive: true });

const db = new Database(dbPath);

// WAL mode for better concurrent-read throughput and reduced BUSY errors
db.pragma('journal_mode = WAL');
// Give SQLite up to 3 s to wait for a write lock before throwing SQLITE_BUSY
db.pragma('busy_timeout = 3000');

// Schema
db.exec(`
  CREATE TABLE IF NOT EXISTS signals (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id          TEXT    NOT NULL,
    type             TEXT    NOT NULL,
    payload          TEXT    NOT NULL,
    idempotency_key  TEXT    UNIQUE,
    created_at       INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_user_created ON signals(user_id, created_at);
`);

// ---------------------------------------------------------------------------
// Failure simulation (controlled via DB_FAIL_RATE env var, 0–1)
// ---------------------------------------------------------------------------
function maybeFail() {
  const rate = Number(process.env.DB_FAIL_RATE || 0);
  if (rate > 0 && Math.random() < rate) {
    const err = new Error('simulated_db_failure');
    err.code = 'SQLITE_BUSY';
    throw err;
  }
}

// ---------------------------------------------------------------------------
// Prepared statements (compiled once, reused for performance)
// ---------------------------------------------------------------------------
const stmtInsertIgnore = db.prepare(
  `INSERT OR IGNORE INTO signals (user_id, type, payload, idempotency_key, created_at)
   VALUES (?, ?, ?, ?, ?)`
);

const stmtSelectByKey = db.prepare(
  `SELECT id,
          user_id          AS userId,
          type,
          payload,
          idempotency_key  AS idempotencyKey,
          created_at       AS createdAt
   FROM signals
   WHERE idempotency_key = ?`
);

const stmtInsert = db.prepare(
  `INSERT INTO signals (user_id, type, payload, idempotency_key, created_at)
   VALUES (?, ?, ?, ?, ?)`
);

const stmtList = db.prepare(
  `SELECT id,
          user_id          AS userId,
          type,
          payload,
          idempotency_key  AS idempotencyKey,
          created_at       AS createdAt
   FROM signals
   WHERE user_id = ?
   ORDER BY created_at DESC
   LIMIT ?`
);

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Atomically upsert a signal when an idempotency key is provided.
 *
 * Uses INSERT OR IGNORE + SELECT inside a single transaction so that
 * concurrent requests with the same key cannot both insert — one will insert
 * and the other will silently skip, then both read the *same* canonical row.
 *
 * @returns {object} The canonical signal row (newly inserted OR pre-existing)
 */
export function upsertSignal(userId, type, payload, idemKey, nowMs) {
  maybeFail();
  return db.transaction(() => {
    stmtInsertIgnore.run(userId, type, String(payload), idemKey, nowMs);
    return stmtSelectByKey.get(idemKey);
  })();
}

/**
 * Insert a signal without an idempotency key (each call creates a new row).
 *
 * @returns {{ lastInsertRowid: number }} better-sqlite3 run() result
 */
export function insertSignal(userId, type, payload, idemKey, nowMs) {
  maybeFail();
  return stmtInsert.run(userId, type, String(payload), idemKey || null, nowMs);
}

/**
 * Look up a signal by its idempotency key.
 *
 * @returns {object|undefined}
 */
export function getByIdemKey(idemKey) {
  maybeFail();
  return stmtSelectByKey.get(idemKey);
}

/**
 * Return the most recent `limit` signals for a user.
 *
 * @returns {object[]}
 */
export function listSignals(userId, limit) {
  maybeFail();
  return stmtList.all(userId, limit);
}
