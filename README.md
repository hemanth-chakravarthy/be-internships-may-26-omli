# Signals Challenge (Node.js + Fastify)

A minimal, **production-leaning** signal ingestion service built with Fastify and SQLite. Designed for correctness under concurrency, transient DB failures, and idempotent delivery.

---

## Quick Start

```bash
# 1. Install dependencies
npm install

# 2. Configure environment
cp .env.example .env
# Edit .env as needed

# 3. Start the development server
npm run dev
# Server listening at http://localhost:8080

# 4. Run all tests
npm test
```

---

## API Reference

### Authentication
All endpoints (except `GET /healthz`) require an `X-API-Key` header matching the `API_KEY` environment variable.

---

### `POST /v1/signals`

Create a new signal. Supports idempotency to prevent duplicate processing.

**Request Headers:**
| Header | Required | Description |
|--------|----------|-------------|
| `X-API-Key` | ✅ | API key for authentication |
| `Idempotency-Key` | ❌ | Unique key to deduplicate requests |
| `Content-Type` | ✅ | `application/json` |

**Request Body:**
```json
{
  "userId": "string",
  "type": "string",
  "payload": "string"
}
```

**Response Codes:**
| Code | Meaning |
|------|---------|
| `201` | Signal created successfully |
| `200` | Idempotent replay — same resource returned, no duplicate created |
| `400` | Invalid request body |
| `401` | Missing or invalid API key |
| `429` | Rate limit exceeded for this `userId` |
| `503` | DB unavailable after all retry attempts |

**Response Headers:**
| Header | Description |
|--------|-------------|
| `X-RateLimit-Remaining` | Requests remaining in current window |
| `X-RateLimit-Reset` | Epoch-ms when the current window resets |
| `Idempotency-Key` | Echoed back when a key was provided |

**Example:**
```bash
curl -X POST http://localhost:8080/v1/signals \
  -H "X-API-Key: change-me" \
  -H "Idempotency-Key: order-123-signal" \
  -H "Content-Type: application/json" \
  -d '{"userId":"user-abc","type":"purchase","payload":"item-456"}'
```

---

### `GET /v1/signals`

Retrieve recent signals for a user.

**Query Parameters:**
| Parameter | Required | Default | Max | Description |
|-----------|----------|---------|-----|-------------|
| `userId` | ✅ | — | — | User to fetch signals for |
| `limit` | ❌ | `20` | `100` | Number of results (most recent first) |

**Example:**
```bash
curl "http://localhost:8080/v1/signals?userId=user-abc&limit=10" \
  -H "X-API-Key: change-me"
```

---

### `GET /healthz`

Health check endpoint. No authentication required.

```bash
curl http://localhost:8080/healthz
# {"ok":true}
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `API_KEY` | `change-me` | Authentication key for all endpoints |
| `PORT` | `8080` | HTTP server port |
| `DATABASE_URL` | `./data/signals.db` | SQLite database file path |
| `RATE_LIMIT_PER_MIN` | `5` | Max requests per userId per minute |
| `DB_FAIL_RATE` | `0` | Simulated DB failure rate (0–1, for testing) |

---

## Design Decisions

### 1. Atomic Idempotency (Race-Free)

**Problem:** A naive "check then insert" pattern has a TOCTOU race:
```js
// ❌ WRONG — two concurrent requests can both see null and both insert
const existing = getByIdemKey(idemKey);
if (existing) return existing;
insertSignal(...);
```

**Solution:** `INSERT OR IGNORE INTO signals ... + SELECT` in a single SQLite transaction:
```sql
-- Both statements run atomically inside BEGIN / COMMIT
INSERT OR IGNORE INTO signals (...) VALUES (...);
SELECT * FROM signals WHERE idempotency_key = ?;
```

Under any number of concurrent requests with the same `Idempotency-Key`:
- Exactly **one** insert wins.
- The others silently ignore (no error).
- All callers read the **same canonical row** from the subsequent `SELECT`.

The `UNIQUE` constraint on `idempotency_key` is the DB-level guarantee. No application locks needed.

### 2. Sliding-Window Rate Limiter

**Problem:** A naive fixed-window allows a **2× burst at the boundary**:
- 5 requests at t=59s → window resets.
- 5 more requests at t=61s → all allowed.
- Result: 10 requests in 2 seconds.

**Solution:** Per-user array of request timestamps. On each call:
1. Drop timestamps older than 60 seconds.
2. If `count >= RATE` → reject (return `resetMs` = when oldest entry expires).
3. Otherwise → push current timestamp, allow.

This correctly enforces exactly `RATE` requests per any 60-second sliding window.

**Multi-instance note:** This in-memory implementation is correct for a single Node.js process. Node.js is single-threaded, so the Map read-modify-write is atomic (no race). For **horizontal scaling across multiple pods**, replace with a Redis Lua script (see [SCALE.md](SCALE.md#3-rate-limiting-across-instances)).

### 3. Retry with Exponential Backoff + Full Jitter

Every DB call is wrapped in `withRetry()`:

```
Attempt 1 → if SQLITE_BUSY → wait random(0, 50ms)
Attempt 2 → if SQLITE_BUSY → wait random(0, 100ms)
Attempt 3 → if still failing → throw → 503
```

**Full jitter** (`delay = random(0, base * 2^attempt)`) is critical: it prevents all concurrent callers from retrying at the same moment, which would create a thundering herd and amplify DB load.

**Safe to retry:** Because idempotency is enforced at the DB level (`INSERT OR IGNORE`), retrying an idempotent insert is always safe — the second attempt resolves to the same row with no duplicate written.

### 4. SQLite WAL Mode

`PRAGMA journal_mode = WAL` enables:
- Concurrent readers (don't block on writer).
- Writer doesn't block readers.
- Dramatically reduces `SQLITE_BUSY` under concurrent load.

`PRAGMA busy_timeout = 3000` tells SQLite to retry the write lock internally for up to 3 seconds before throwing `SQLITE_BUSY`, reducing the number of errors that reach application-level retry.

---

## Running Tests

```bash
npm test
```

Test files in `tests/`:
| File | What it tests |
|------|--------------|
| `rate-limit.test.js` | Sequential limit, concurrent burst, userId isolation, response headers |
| `idempotency.test.js` | Sequential dedup, 10-way concurrent race, no-key distinct rows, GET verification |
| `retry.test.js` | Retry on transient failures, 100% failure → 503, no duplicates on retry |

Each test spawns an isolated server process with its own temp DB file.

---

## Simulating DB Failures

```bash
# 70% of DB calls will fail — tests retry logic
DB_FAIL_RATE=0.7 npm run dev

# Always fail — service should return 503
DB_FAIL_RATE=1.0 npm run dev
```

---

## Scale Plan

See [SCALE.md](SCALE.md) for a complete 10k RPS design covering:
- Postgres migration + indexes
- Redis sliding-window rate limiting (multi-instance)
- Atomic idempotency across pods
- Connection pooling (PgBouncer)
- Horizontal stateless architecture
- Async queue fanout
- Observability (metrics, logs, alerts)
- Failure mode mitigations
- Infrastructure cost estimate (~$2,575/month for 10k RPS)
