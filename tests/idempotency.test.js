/**
 * Idempotency tests.
 *
 * Tests cover:
 *  1. Basic: same key returns same row (sequential).
 *  2. Concurrency: 10 parallel requests with the same key → all return same id.
 *  3. No key: each request creates a distinct row.
 *  4. Different keys: different rows are created.
 *  5. Key scoping: GET verifies the stored signal matches the created one.
 *
 * Each test spawns an isolated server process in a try/finally block so the
 * process is ALWAYS killed — even if assertions throw — preventing port leaks
 * that would cause cascading failures on subsequent runs.
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
  const dbFile = path.join(os.tmpdir(), `signals-idem-${port}-${Date.now()}.db`);
  const proc = spawn('node', ['src/server.js'], {
    env: {
      ...process.env,
      API_KEY: 'k',
      PORT: String(port),
      RATE_LIMIT_PER_MIN: '100', // high limit so it doesn't interfere
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

// ── tests ─────────────────────────────────────────────────────────────────

test('idempotency: same key returns same row (sequential)', async () => {
  const PORT = 9200;
  const proc = spawnServer(PORT);
  try {
    await wait(400);

    const base = `http://localhost:${PORT}`;
    const idem = 'seq-key-001';

    const a = await postJson(`${base}/v1/signals`, {
      headers: { 'x-api-key': 'k', 'idempotency-key': idem },
      body: { userId: 'u1', type: 'note', payload: 'hello' },
    });
    const b = await postJson(`${base}/v1/signals`, {
      headers: { 'x-api-key': 'k', 'idempotency-key': idem },
      body: { userId: 'u1', type: 'note', payload: 'hello' },
    });

    assert.equal(a.body.id, b.body.id, 'Same id expected for repeated key');
    assert.equal(a.body.idempotencyKey, idem);
    assert.equal(b.body.idempotencyKey, idem);
    // First call should be 201 (created), second 200 (replay)
    assert.equal(a.status, 201, 'First request should be 201');
    assert.equal(b.status, 200, 'Replay request should be 200');
  } finally {
    proc.kill();
  }
});

test('idempotency: 10 concurrent requests with same key → all return same id', async () => {
  const PORT = 9201;
  const proc = spawnServer(PORT);
  try {
    await wait(400);

    const base = `http://localhost:${PORT}`;
    const idem = 'concurrent-key-001';

    // Fire 10 requests simultaneously — the core concurrency test
    const results = await Promise.all(
      Array.from({ length: 10 }, () =>
        postJson(`${base}/v1/signals`, {
          headers: { 'x-api-key': 'k', 'idempotency-key': idem },
          body: { userId: 'u-concurrent', type: 'note', payload: 'race' },
        })
      )
    );

    // All responses must be successful
    const statuses = results.map((r) => r.status);
    assert.ok(
      statuses.every((s) => s === 200 || s === 201),
      `All requests should succeed, got statuses: ${statuses}`
    );

    // All responses must return the same id (exactly one row was created)
    const ids = results.map((r) => r.body.id);
    const uniqueIds = new Set(ids);
    assert.equal(uniqueIds.size, 1, `All concurrent requests should return the same id, got ids: ${ids}`);
  } finally {
    proc.kill();
  }
});

test('idempotency: no key → each request creates a distinct row', async () => {
  const PORT = 9202;
  const proc = spawnServer(PORT);
  try {
    await wait(400);

    const base = `http://localhost:${PORT}`;

    const results = await Promise.all(
      Array.from({ length: 5 }, (_, i) =>
        postJson(`${base}/v1/signals`, {
          headers: { 'x-api-key': 'k' }, // no idempotency-key header
          body: { userId: 'u-nokey', type: 'note', payload: String(i) },
        })
      )
    );

    const ids = results.map((r) => r.body.id);
    const uniqueIds = new Set(ids);

    assert.equal(uniqueIds.size, 5, `Expected 5 distinct ids, got: ${ids}`);
    assert.ok(
      results.every((r) => r.status === 201),
      `All non-idempotent requests should be 201`
    );
  } finally {
    proc.kill();
  }
});

test('idempotency: different keys create different rows', async () => {
  const PORT = 9203;
  const proc = spawnServer(PORT);
  try {
    await wait(400);

    const base = `http://localhost:${PORT}`;

    const r1 = await postJson(`${base}/v1/signals`, {
      headers: { 'x-api-key': 'k', 'idempotency-key': 'key-alpha' },
      body: { userId: 'u-keys', type: 'note', payload: 'a' },
    });
    const r2 = await postJson(`${base}/v1/signals`, {
      headers: { 'x-api-key': 'k', 'idempotency-key': 'key-beta' },
      body: { userId: 'u-keys', type: 'note', payload: 'b' },
    });

    assert.notEqual(r1.body.id, r2.body.id, 'Different keys should produce different ids');
    assert.equal(r1.body.idempotencyKey, 'key-alpha');
    assert.equal(r2.body.idempotencyKey, 'key-beta');
  } finally {
    proc.kill();
  }
});

test('idempotency: GET returns the signal created via idempotent POST', async () => {
  const PORT = 9204;
  const proc = spawnServer(PORT);
  try {
    await wait(400);

    const base = `http://localhost:${PORT}`;
    const idem = 'get-verify-key';

    // Create via idempotent POST
    const created = await postJson(`${base}/v1/signals`, {
      headers: { 'x-api-key': 'k', 'idempotency-key': idem },
      body: { userId: 'u-get', type: 'click', payload: 'btn-1' },
    });
    assert.equal(created.status, 201);

    // Verify via GET
    const listed = await new Promise((resolve, reject) => {
      http.get(
        `${base}/v1/signals?userId=u-get`,
        { headers: { 'x-api-key': 'k' } },
        (res) => {
          let chunks = '';
          res.on('data', (d) => (chunks += d));
          res.on('end', () => resolve(JSON.parse(chunks)));
        }
      ).on('error', reject);
    });

    assert.ok(Array.isArray(listed.items), 'Should return items array');
    assert.equal(listed.items.length, 1, 'Should have exactly 1 item');
    assert.equal(listed.items[0].id, created.body.id);
    assert.equal(listed.items[0].idempotencyKey, idem);
  } finally {
    proc.kill();
  }
});
