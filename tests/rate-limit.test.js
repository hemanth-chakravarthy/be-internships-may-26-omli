/**
 * Rate-limit tests.
 *
 * Each test spawns a fresh server process on a unique port, runs HTTP
 * requests against it, and kills the process in a try/finally block so
 * the port is ALWAYS released — even if an assertion fails.
 *
 * Tests cover:
 *  1. Basic: 5 allowed, 6th is 429.
 *  2. Concurrency: 10 simultaneous requests → ≤ 5 succeed, rest 429.
 *  3. Isolation: different userIds have independent counters.
 *  4. Response headers: X-RateLimit-Remaining decrements correctly.
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
  const dbFile = path.join(os.tmpdir(), `signals-rl-${port}-${Date.now()}.db`);
  const proc = spawn('node', ['src/server.js'], {
    env: {
      ...process.env,
      API_KEY: 'k',
      PORT: String(port),
      RATE_LIMIT_PER_MIN: '5',
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

async function postStatus(url, { headers = {}, body = {} } = {}) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req = http.request(
      url,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json', ...headers },
      },
      (res) => {
        res.resume();
        res.on('end', () => resolve(res.statusCode));
      }
    );
    req.on('error', reject);
    req.write(data);
    req.end();
  });
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
          resolve({ status: res.statusCode, headers: res.headers, body: JSON.parse(chunks || '{}') })
        );
      }
    );
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

// ── tests ─────────────────────────────────────────────────────────────────

test('rate limit: allow 5 per minute, 6th is 429', async () => {
  const PORT = 9100;
  const proc = spawnServer(PORT);
  try {
    await wait(400);

    const base = `http://localhost:${PORT}`;
    const statuses = [];
    for (let i = 0; i < 6; i++) {
      const code = await postStatus(`${base}/v1/signals`, {
        headers: { 'x-api-key': 'k' },
        body: { userId: 'u-basic', type: 'note', payload: String(i) },
      });
      statuses.push(code);
    }

    const counts = statuses.reduce((acc, c) => ((acc[c] = (acc[c] || 0) + 1), acc), {});
    assert.ok(counts[200] >= 5 || counts[201] >= 5, `Expected ≥5 success, got ${JSON.stringify(counts)}`);
    assert.ok(counts[429] >= 1, `Expected ≥1 rate-limited, got ${JSON.stringify(counts)}`);
  } finally {
    proc.kill();
  }
});

test('rate limit: concurrent burst — 10 parallel requests, ≤5 succeed', async () => {
  const PORT = 9101;
  const proc = spawnServer(PORT);
  try {
    await wait(400);

    const base = `http://localhost:${PORT}`;

    // Fire 10 requests simultaneously
    const results = await Promise.all(
      Array.from({ length: 10 }, (_, i) =>
        postStatus(`${base}/v1/signals`, {
          headers: { 'x-api-key': 'k' },
          body: { userId: 'u-burst', type: 'note', payload: String(i) },
        })
      )
    );

    const ok = results.filter((s) => s === 200 || s === 201).length;
    const limited = results.filter((s) => s === 429).length;

    assert.ok(ok <= 5, `Expected ≤5 successes under burst, got ${ok}`);
    assert.ok(ok >= 1, `Expected ≥1 successes, got ${ok}`);
    assert.ok(limited >= 5, `Expected ≥5 rate-limited, got ${limited}`);
  } finally {
    proc.kill();
  }
});

test('rate limit: different userIds have independent counters', async () => {
  const PORT = 9102;
  const proc = spawnServer(PORT);
  try {
    await wait(400);

    const base = `http://localhost:${PORT}`;

    // Exhaust the limit for userA
    for (let i = 0; i < 5; i++) {
      await postStatus(`${base}/v1/signals`, {
        headers: { 'x-api-key': 'k' },
        body: { userId: 'u-a', type: 'note', payload: String(i) },
      });
    }

    // userA should be rate-limited
    const codeA = await postStatus(`${base}/v1/signals`, {
      headers: { 'x-api-key': 'k' },
      body: { userId: 'u-a', type: 'note', payload: 'x' },
    });
    assert.equal(codeA, 429, 'userA should be rate-limited');

    // userB should still be allowed (fresh counter)
    const codeB = await postStatus(`${base}/v1/signals`, {
      headers: { 'x-api-key': 'k' },
      body: { userId: 'u-b', type: 'note', payload: 'x' },
    });
    assert.ok(codeB === 200 || codeB === 201, `userB should be allowed, got ${codeB}`);
  } finally {
    proc.kill();
  }
});

test('rate limit: response includes X-RateLimit-Remaining header', async () => {
  const PORT = 9103;
  const proc = spawnServer(PORT);
  try {
    await wait(400);

    const base = `http://localhost:${PORT}`;

    // First request → remaining should be 4 (out of 5)
    const r1 = await postJson(`${base}/v1/signals`, {
      headers: { 'x-api-key': 'k' },
      body: { userId: 'u-hdr', type: 'note', payload: '1' },
    });
    assert.ok(r1.status === 200 || r1.status === 201, `Expected success, got ${r1.status}`);
    assert.equal(r1.headers['x-ratelimit-remaining'], '4');

    // Second request → remaining should be 3
    const r2 = await postJson(`${base}/v1/signals`, {
      headers: { 'x-api-key': 'k' },
      body: { userId: 'u-hdr', type: 'note', payload: '2' },
    });
    assert.equal(r2.headers['x-ratelimit-remaining'], '3');
  } finally {
    proc.kill();
  }
});
