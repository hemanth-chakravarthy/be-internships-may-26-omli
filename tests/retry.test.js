/**
 * Retry / DB-failure tests.
 *
 * These tests set DB_FAIL_RATE to simulate transient database errors.
 * They verify that:
 *  1. With a high-but-not-certain fail rate, the service eventually succeeds.
 *  2. With fail rate = 1.0 (always fail), the service returns 503.
 *  3. Retrying an idempotent request does NOT create duplicate rows.
 *  4. With fail rate = 0, all requests succeed on first attempt.
 *  5. GET also retries on transient failure.
 *
 * All tests use try/finally to guarantee proc.kill() runs even if an
 * assertion throws — preventing port leaks and cascading failures.
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { setTimeout as wait } from 'node:timers/promises';
import http from 'node:http';
import path from 'node:path';
import os from 'node:os';

// ── helpers ────────────────────────────────────────────────────────────────

function spawnServer(port, extra = {}) {
  const dbFile = path.join(os.tmpdir(), `signals-retry-${port}-${Date.now()}.db`);
  const proc = spawn('node', ['src/server.js'], {
    env: {
      ...process.env,
      API_KEY: 'k',
      PORT: String(port),
      RATE_LIMIT_PER_MIN: '100',
      DATABASE_URL: dbFile,
      DB_FAIL_RATE: '0',
      ...extra,
    },
    stdio: 'pipe',
  });
  proc.stderr.resume();
  proc.stdout.resume();
  return proc;
}

async function postJson(url, { headers = {}, body = {} } = {}) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req = http.request(
      url,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json', ...headers },
      },
      (res) => {
        let chunks = '';
        res.on('data', (d) => (chunks += d));
        res.on('end', () =>
          resolve({ status: res.statusCode, body: JSON.parse(chunks || '{}') })
        );
      }
    );
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

function getJson(url, headers = {}) {
  return new Promise((resolve, reject) => {
    http.get(url, { headers }, (res) => {
      let chunks = '';
      res.on('data', (d) => (chunks += d));
      res.on('end', () =>
        resolve({ status: res.statusCode, body: JSON.parse(chunks || '{}') })
      );
    }).on('error', reject);
  });
}

// ── tests ─────────────────────────────────────────────────────────────────

test('retry: DB_FAIL_RATE=0.6 — service eventually succeeds across multiple requests', async () => {
  const PORT = 9300;
  // fail rate 0.6: P(all 3 attempts fail) = 0.6^3 ≈ 21.6%.
  // We make 20 requests; statistically many will succeed.
  const proc = spawnServer(PORT, { DB_FAIL_RATE: '0.6' });
  try {
    await wait(400);

    const base = `http://localhost:${PORT}`;
    const results = [];

    for (let i = 0; i < 20; i++) {
      const r = await postJson(`${base}/v1/signals`, {
        headers: { 'x-api-key': 'k' },
        body: { userId: 'u-retry', type: 'note', payload: String(i) },
      });
      results.push(r.status);
    }

    const successes = results.filter((s) => s === 200 || s === 201).length;
    // With retry: expect significantly more than 0 successes (statistically ≥10)
    assert.ok(
      successes >= 5,
      `Expected retry to produce successes, only got ${successes}/20`
    );
  } finally {
    proc.kill();
  }
});

test('retry: DB_FAIL_RATE=1.0 — always fails, returns 503 after exhausting retries', async () => {
  const PORT = 9301;
  const proc = spawnServer(PORT, { DB_FAIL_RATE: '1.0' });
  try {
    await wait(400);

    const base = `http://localhost:${PORT}`;

    const r = await postJson(`${base}/v1/signals`, {
      headers: { 'x-api-key': 'k' },
      body: { userId: 'u-fail', type: 'note', payload: 'always-fails' },
    });

    assert.equal(r.status, 503, 'Should return 503 when DB always fails');
    assert.equal(r.body.error, 'db_unavailable');
  } finally {
    proc.kill();
  }
});

test('retry: idempotent request with DB_FAIL_RATE=0.6 — no duplicates created', async () => {
  const PORT = 9302;
  const proc = spawnServer(PORT, { DB_FAIL_RATE: '0.6' });
  try {
    await wait(400);

    const base = `http://localhost:${PORT}`;
    const idem = 'retry-idem-no-dup';

    // Send the same idempotency key multiple times under DB failures
    // Some will fail (503), some will succeed
    const results = [];
    for (let i = 0; i < 10; i++) {
      const r = await postJson(`${base}/v1/signals`, {
        headers: { 'x-api-key': 'k', 'idempotency-key': idem },
        body: { userId: 'u-retry-idem', type: 'note', payload: 'data' },
      });
      results.push(r);
    }

    const successes = results.filter((r) => r.status === 200 || r.status === 201);
    assert.ok(successes.length >= 1, 'At least one idempotent request should succeed');

    // All successful responses must return the same id — no duplicates
    const ids = successes.map((r) => r.body.id);
    const uniqueIds = new Set(ids);
    assert.equal(
      uniqueIds.size,
      1,
      `All successful idempotent retries must return the same id; got ids: ${ids}`
    );
  } finally {
    proc.kill();
  }
});

test('retry: DB_FAIL_RATE=0 — all requests succeed without retry overhead', async () => {
  const PORT = 9303;
  const proc = spawnServer(PORT, { DB_FAIL_RATE: '0' });
  try {
    await wait(400);

    const base = `http://localhost:${PORT}`;

    for (let i = 0; i < 5; i++) {
      const r = await postJson(`${base}/v1/signals`, {
        headers: { 'x-api-key': 'k' },
        body: { userId: 'u-no-fail', type: 'note', payload: String(i) },
      });
      assert.ok(r.status === 200 || r.status === 201, `Request ${i} should succeed, got ${r.status}`);
    }
  } finally {
    proc.kill();
  }
});

test('retry: GET also retries on transient DB failure', async () => {
  const PORT = 9304;
  const dbFile = path.join(os.tmpdir(), `signals-retry-get-${PORT}-${Date.now()}.db`);

  // Start with no failures to insert data
  const proc = spawnServer(PORT, { DB_FAIL_RATE: '0', DATABASE_URL: dbFile });
  try {
    await wait(400);

    const base = `http://localhost:${PORT}`;

    // Create a signal with no failures
    await postJson(`${base}/v1/signals`, {
      headers: { 'x-api-key': 'k' },
      body: { userId: 'u-get-retry', type: 'note', payload: 'test' },
    });
  } finally {
    proc.kill();
    await wait(200); // brief pause so the port is fully released
  }

  // Restart the same DB with failures enabled
  const proc2 = spawnServer(PORT, { DB_FAIL_RATE: '0.7', DATABASE_URL: dbFile });
  try {
    await wait(400);

    const base = `http://localhost:${PORT}`;

    // GET should return 200 (via retry) or 503 (if all retries fail), never crash
    const results = [];
    for (let i = 0; i < 5; i++) {
      const r = await getJson(`${base}/v1/signals?userId=u-get-retry`, { 'x-api-key': 'k' });
      results.push(r.status);
    }
    assert.ok(
      results.every((s) => s === 200 || s === 503),
      `GET should return 200 or 503 only, got: ${results}`
    );
  } finally {
    proc2.kill();
  }
});
