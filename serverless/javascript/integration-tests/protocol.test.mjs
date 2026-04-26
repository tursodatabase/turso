import test from 'ava';
import { decodeValue, encodeValue, Session } from '../dist/index.js';

// Unit tests for serverless protocol encoding/decoding.
// These test the serverless driver directly — no server needed.

// --- encodeValue ---

test('encodeValue sends integers as type integer, not float', t => {
  const result = encodeValue(42);
  t.is(result.type, 'integer');
  t.is(result.value, '42');
});

test('encodeValue sends zero as type integer', t => {
  const result = encodeValue(0);
  t.is(result.type, 'integer');
  t.is(result.value, '0');
});

test('encodeValue sends floats as type float', t => {
  const result = encodeValue(0.5);
  t.is(result.type, 'float');
  t.is(result.value, 0.5);
});

// --- decodeValue ---

test('decodeValue decodes empty base64 blob as empty Buffer', t => {
  const result = decodeValue({ type: 'blob', base64: '' });
  t.truthy(result, 'empty blob should not decode to null');
  t.is(result.length, 0);
});

test('decodeValue decodes blob without base64 key as empty Buffer', t => {
  const result = decodeValue({ type: 'blob' });
  t.truthy(result, 'blob without base64 key should not decode to null');
  t.true(Buffer.isBuffer(result), 'should be a Buffer');
  t.is(result.length, 0);
});

test('decodeValue decodes unpadded base64 blob', t => {
  // 'aGVsbG8' is 'hello' without trailing '=' padding
  const result = decodeValue({ type: 'blob', base64: 'aGVsbG8' });
  t.is(new TextDecoder().decode(result), 'hello');
});

// --- processCursorEntries: lastInsertRowid ---

async function* cursorEntries(entries) {
  for (const entry of entries) {
    yield entry;
  }
}

test('processCursorEntries preserves lastInsertRowid of 0', async t => {
  const session = new Session({ url: 'http://localhost:0' });
  const entries = cursorEntries([
    { type: 'step_begin', cols: [] },
    { type: 'step_end', affected_row_count: 0, last_insert_rowid: 0 },
  ]);
  const result = await session.processCursorEntries(entries);
  t.is(result.lastInsertRowid, 0);
});

test('processCursorEntries handles numeric lastInsertRowid', async t => {
  const session = new Session({ url: 'http://localhost:0' });
  const entries = cursorEntries([
    { type: 'step_begin', cols: [] },
    { type: 'step_end', affected_row_count: 1, last_insert_rowid: 42 },
  ]);
  const result = await session.processCursorEntries(entries);
  t.is(result.lastInsertRowid, 42);
});

test('processCursorEntries handles string lastInsertRowid', async t => {
  const session = new Session({ url: 'http://localhost:0' });
  const entries = cursorEntries([
    { type: 'step_begin', cols: [] },
    { type: 'step_end', affected_row_count: 1, last_insert_rowid: '42' },
  ]);
  const result = await session.processCursorEntries(entries);
  t.is(result.lastInsertRowid, 42);
});

// --- Connection.prepare() baton continuity (issue #6562) ---

test('prepare() sends describe with the current transaction baton', async t => {
  const { connect } = await import('../dist/index.js');

  const requests = [];
  const originalFetch = globalThis.fetch;

  globalThis.fetch = async (url, opts) => {
    const body = JSON.parse(opts.body);
    requests.push(body);

    // Every pipeline response hands back the same baton so the
    // session stays on one server-side connection.
    const baton = 'txn-baton-abc';

    // Determine what kind of request this is to return the right shape.
    const reqType = body.requests?.[0]?.type;

    if (reqType === 'sequence') {
      // exec('BEGIN') / exec('CREATE TABLE …')
      return new Response(JSON.stringify({
        baton,
        base_url: null,
        results: [{ type: 'ok', response: { type: 'sequence' } }],
      }), { status: 200, headers: { 'Content-Type': 'application/json' } });
    }

    if (reqType === 'describe') {
      return new Response(JSON.stringify({
        baton,
        base_url: null,
        results: [{ type: 'ok', response: {
          type: 'describe',
          result: {
            params: [],
            cols: [{ name: 'id', decltype: 'INTEGER' }],
            is_explain: false,
            is_readonly: false,
          },
        }}],
      }), { status: 200, headers: { 'Content-Type': 'application/json' } });
    }

    // close or anything else
    return new Response(JSON.stringify({
      baton: null,
      base_url: null,
      results: [{ type: 'ok', response: { type: 'close' } }],
    }), { status: 200, headers: { 'Content-Type': 'application/json' } });
  };

  t.teardown(() => { globalThis.fetch = originalFetch; });

  const conn = connect({ url: 'http://fake-host' });
  await conn.exec('BEGIN');
  await conn.exec('CREATE TABLE t (id INTEGER)');
  await conn.prepare('INSERT INTO t VALUES (1)');

  // requests[0] = exec('BEGIN')        → baton: null  (first call)
  // requests[1] = exec('CREATE TABLE') → baton: 'txn-baton-abc'
  // requests[2] = describe             → must also carry 'txn-baton-abc'
  const describeReq = requests[2];
  t.is(describeReq.requests[0].type, 'describe', 'third request should be describe');
  t.is(describeReq.baton, 'txn-baton-abc',
    'describe must carry the transaction baton, not null');
});

// --- normalizeUrl (via Session constructor) ---

test('normalizeUrl rewrites libsql:// to https://', t => {
  const s = new Session({ url: 'libsql://my-db.turso.io' });
  t.is(s['baseUrl'], 'https://my-db.turso.io');
});

test('normalizeUrl rewrites turso:// to https://', t => {
  const s = new Session({ url: 'turso://my-db.turso.io' });
  t.is(s['baseUrl'], 'https://my-db.turso.io');
});

test('normalizeUrl passes through https://', t => {
  const s = new Session({ url: 'https://my-db.turso.io' });
  t.is(s['baseUrl'], 'https://my-db.turso.io');
});

test('normalizeUrl passes through http://', t => {
  const s = new Session({ url: 'http://localhost:8080' });
  t.is(s['baseUrl'], 'http://localhost:8080');
});

test('normalizeUrl turso:// with port', t => {
  const s = new Session({ url: 'turso://my-db.turso.io:443' });
  t.is(s['baseUrl'], 'https://my-db.turso.io:443');
});

test('normalizeUrl libsql:// with port', t => {
  const s = new Session({ url: 'libsql://my-db.turso.io:8080' });
  t.is(s['baseUrl'], 'https://my-db.turso.io:8080');
});

test('normalizeUrl with path', t => {
  const s = new Session({ url: 'turso://my-db.turso.io/v1/db' });
  t.is(s['baseUrl'], 'https://my-db.turso.io/v1/db');
});

test('normalizeUrl with query params', t => {
  const s = new Session({ url: 'libsql://my-db.turso.io?foo=bar' });
  t.is(s['baseUrl'], 'https://my-db.turso.io?foo=bar');
});

test('normalizeUrl passes through ws://', t => {
  const s = new Session({ url: 'ws://localhost:8080' });
  t.is(s['baseUrl'], 'ws://localhost:8080');
});

test('normalizeUrl passes through wss://', t => {
  const s = new Session({ url: 'wss://my-db.turso.io' });
  t.is(s['baseUrl'], 'wss://my-db.turso.io');
});

test('normalizeUrl libsql:// with path and query', t => {
  const s = new Session({ url: 'libsql://my-db.turso.io/db?timeout=30' });
  t.is(s['baseUrl'], 'https://my-db.turso.io/db?timeout=30');
});

// --- Session baton reset on error ---

test('Session resets baton after HTTP error', async t => {
  const session = new Session({ url: 'http://127.0.0.1:1' });

  // Simulate a previous successful request that set a baton
  session['baton'] = 'stale-baton';

  // execute will fail because the server is unreachable
  await t.throwsAsync(async () => {
    await session.execute('SELECT 1');
  }, { any: true });

  // Baton should be null so the next request starts a fresh stream
  t.is(session['baton'], null);
});
