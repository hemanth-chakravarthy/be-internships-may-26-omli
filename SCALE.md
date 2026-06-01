# Scale Plan — Signals Service at 10k RPS

## 1. Data Model & Indexes

### Current Schema (SQLite, single node)
```sql
CREATE TABLE signals (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id          TEXT    NOT NULL,
  type             TEXT    NOT NULL,
  payload          TEXT    NOT NULL,
  idempotency_key  TEXT    UNIQUE,          -- DB-level dedup guarantee
  created_at       INTEGER NOT NULL         -- epoch ms
);
CREATE INDEX idx_user_created ON signals(user_id, created_at);
```

### At 10k RPS → Migrate to PostgreSQL
Swap SQLite for **PostgreSQL 15+** (or Amazon Aurora PostgreSQL). Key changes:

| Concern | SQLite | PostgreSQL |
|---------|--------|------------|
| Write concurrency | Single writer (WAL helps reads) | Multi-writer, MVCC |
| Connection pooling | N/A | PgBouncer (transaction mode) |
| Max connections | 1 writer | ~500 pooled → many more app threads |
| Idempotency constraint | `UNIQUE(idempotency_key)` | Same, enforced by index |

**Additional indexes for 10k RPS:**
```sql
-- Already present: covers GET /v1/signals?userId=... ORDER BY created_at DESC
CREATE INDEX idx_user_created ON signals(user_id, created_at DESC);

-- Partial index for idempotency lookups (only keyed rows)
CREATE INDEX idx_idem_key ON signals(idempotency_key)
  WHERE idempotency_key IS NOT NULL;
```

**Partition by time** once the table exceeds ~100M rows (monthly range partitions on `created_at`). Archive old partitions to cold storage (e.g., S3 via `pg_dump`).

---

## 2. Idempotency Across Instances

### Current (single-process SQLite)
`INSERT OR IGNORE INTO signals ... WHERE idempotency_key = ?` is atomic within one SQLite WAL transaction. Works perfectly for a single process.

### Multi-instance Problem
Multiple Node.js pods each connect to the same Postgres. Two pods receiving the same `Idempotency-Key` simultaneously will both reach the `INSERT`:
- Only one will succeed (Postgres UNIQUE constraint).
- The other gets a `23505 unique_violation` error.

### Solution A — INSERT ... ON CONFLICT DO NOTHING + SELECT (preferred)
```sql
INSERT INTO signals (user_id, type, payload, idempotency_key, created_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (idempotency_key) DO NOTHING;

SELECT * FROM signals WHERE idempotency_key = $4;
```
Both statements run in a `BEGIN`/`COMMIT` transaction with `READ COMMITTED`. The `SELECT` always returns the canonical row regardless of which instance "won" the insert. **No application-level distributed lock needed.**

### Solution B — Redis idempotency cache (optional, for TTL-based expiry)
```lua
-- Lua script (atomic)
local existing = redis.call('GET', KEYS[1])
if existing then return existing end
redis.call('SET', KEYS[1], ARGV[1], 'EX', 86400)  -- 24h TTL
return nil
```
Use this to serve replays from Redis (fast path) and avoid hitting Postgres for repeated keys. The canonical store remains Postgres.

---

## 3. Rate Limiting Across Instances

### Current (in-memory Map, single process)
Works correctly within one Node.js process (single-threaded). Under concurrency tests, the sliding-window timestamp array is race-free.

### Multi-instance — Redis Sliding Window (required for horizontal scale)

Replace `src/rateLimit.js` with a Redis Lua script. Lua scripts are atomic in Redis (single-threaded executor):

```lua
-- KEYS[1] = rate_limit:{userId}
-- ARGV[1] = nowMs (string)
-- ARGV[2] = windowMs
-- ARGV[3] = limit
local key    = KEYS[1]
local now    = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit  = tonumber(ARGV[3])
local cutoff = now - window

redis.call('ZREMRANGEBYSCORE', key, '-inf', cutoff)
local count = redis.call('ZCARD', key)
if count >= limit then
  local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
  return {0, tonumber(oldest[2]) + window}  -- {allowed=false, resetMs}
end
redis.call('ZADD', key, now, now .. ':' .. math.random(1e9))
redis.call('PEXPIRE', key, window)
return {1, 0}  -- {allowed=true}
```

**Redis cluster**: partition rate-limit keys by `{userId}` hash slot for linear horizontal scaling.

---

## 4. Connection Pooling

| Layer | Tool | Setting |
|-------|------|---------|
| Node.js → Postgres | `pg` + `pg-pool` | `max: 20` per pod (avoid connection storms) |
| Postgres | PgBouncer | `pool_mode=transaction`, `max_client_conn=1000` |
| Node.js → Redis | `ioredis` | Cluster-aware client, `maxRetriesPerRequest: 3` |

Avoid `max: 1` (serializes all queries) and `max: 100+` (exhausts Postgres `max_connections`).

---

## 5. Horizontal Scaling — Stateless Workers

```
┌─────────┐        ┌──────────────────────────────────┐
│  Users  │──HTTP──▶  Load Balancer (AWS ALB / nginx) │
└─────────┘        └────────┬──────┬──────┬───────────┘
                            │      │      │
                    ┌───────▼┐ ┌───▼──┐ ┌▼───────┐
                    │ Pod #1 │ │Pod#2 │ │ Pod #3 │   Node.js Fastify
                    └───┬────┘ └──┬───┘ └────┬───┘   (stateless)
                        │         │           │
              ┌─────────▼─────────▼───────────▼──────┐
              │          Redis Cluster                │  rate limit + idem cache
              └──────────────────────────────────────┘
                        │
              ┌─────────▼──────────────────────────────┐
              │     PostgreSQL (Primary + Read Replica) │
              └────────────────────────────────────────┘
```

- **Horizontal**: scale pods behind ALB; each pod is stateless.
- **Read scaling**: direct `GET /v1/signals` queries to a read replica.
- **Write scaling**: all `POST /v1/signals` go to the primary; shard by `userId` hash if a single primary is saturated (>50k write RPS).

---

## 6. Async Fanout with a Queue (Beyond-MVP)

For non-blocking throughput: accept the signal into an **SQS / BullMQ** queue, return `202 Accepted` immediately, and process downstream (analytics, notifications, webhooks) asynchronously.

```
POST /v1/signals
  → validate + rate limit + idempotency check (Redis)
  → enqueue to SQS (fast, ~1ms)
  → return 202 { id, status: "queued" }

Worker pool
  → dequeue + insert into Postgres
  → trigger fanout (analytics, webhooks)
```

This decouples write latency from DB throughput and allows back-pressure without dropping requests.

---

## 7. Observability

### Structured Logging
Every request emits a JSON log line with:
```json
{
  "level": "info",
  "reqId": "abc-123",
  "userId": "u1",
  "idemKey": "key-001",
  "durationMs": 4,
  "status": 201,
  "retryAttempts": 0
}
```

### Metrics (Prometheus / OpenTelemetry)
| Metric | Type | Labels |
|--------|------|--------|
| `signals_requests_total` | Counter | `method`, `status`, `endpoint` |
| `signals_ratelimited_total` | Counter | `userId` |
| `signals_idempotent_replays_total` | Counter | — |
| `signals_db_retries_total` | Counter | `attempt` |
| `signals_db_errors_total` | Counter | `error_code` |
| `signals_request_duration_ms` | Histogram | `endpoint`, `status` |

### Alerting
- P99 latency > 200ms → investigate DB or Redis lag.
- Error rate > 1% → page on-call.
- Rate-limited requests > 10% of traffic → adjust limits or investigate abuse.
- DB retry rate > 5% → Postgres under pressure, scale or tune pooling.

---

## 8. Failure Modes

### DB Down
- In-process retry (3 attempts, exponential backoff + jitter) absorbs transient blips.
- After retries exhausted → `503 db_unavailable` (fail open for reads if stale data is acceptable, fail closed for writes).
- **Circuit breaker** (e.g., `opossum` library): after N consecutive failures, open the circuit for 30s, return 503 immediately without hitting DB. Prevents thundering herd on DB recovery.

### Redis Down (rate limit + idem cache)
- Fail open: allow all requests through, log a warning (`redis_unavailable`).
- **Never let Redis outage cascade into service outage.**
- Idempotency falls back to Postgres-only (slightly slower but correct).

### Partial Outage
- Use health-check endpoints to remove unhealthy pods from the load balancer.
- `GET /healthz` checks DB connectivity (`SELECT 1`) and Redis ping; returns `{ ok: false, db: false, redis: true }` with 503 if any critical dependency is down.

### Retry Storms (Thundering Herd)
- Full jitter on backoff: `delay = random(0, base * 2^attempt)` prevents synchronized retries from all pods hitting DB simultaneously.
- Circuit breaker opens after 5 consecutive failures, preventing pile-on.

---

## 9. 10k RPS Design Sketch & Cost Ballpark

### Traffic Math
- **10,000 RPS** × 60s = 600,000 signals/minute
- Average payload ~200 bytes → ~120 MB/minute ingest
- P99 target: < 50ms end-to-end

### Infrastructure (AWS, rough monthly estimate)

| Component | Size | Monthly Cost |
|-----------|------|-------------|
| **ECS / EKS pods** | 10× `c6i.xlarge` (4 vCPU, 8 GB) | ~$1,500 |
| **ALB** | 10k RPS | ~$50 |
| **Aurora PostgreSQL** | `db.r6g.2xlarge` primary + 1 reader | ~$600 |
| **ElastiCache Redis** | `cache.r6g.large` × 3 (cluster) | ~$300 |
| **SQS** (optional async) | 600k msgs/min | ~$25 |
| **CloudWatch / metrics** | — | ~$100 |
| **Total** | | **~$2,575/month** |

### Throughput per Pod
Each Fastify pod on `c6i.xlarge` can handle ~1,000–2,000 RPS (mostly I/O bound, waiting on DB/Redis). 10 pods gives comfortable headroom for 10k RPS with auto-scaling to 20 pods for peaks.

### Bottleneck Order
1. **Postgres write throughput** (first to saturate at ~5k writes/RPS on a single primary)
2. **Redis** (high throughput, rarely the bottleneck)
3. **Node.js event loop** (non-blocking I/O; rarely the bottleneck for CRUD)
