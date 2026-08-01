import test from 'ava';
import { decodeValue, encodeValue, Session, DatabaseError } from '../dist/index.js';

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

test.serial('prepare() sends describe with the current transaction baton', async t => {
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

// --- Session baton reset on error ---

test.serial('Session resets baton after HTTP error', async t => {
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

test.serial('Session.batch encodes named arguments for statement objects', async t => {
  const session = new Session({ url: 'http://fake-host' });
  const requests = [];
  const originalFetch = globalThis.fetch;

  globalThis.fetch = async (url, opts) => {
    requests.push(JSON.parse(opts.body));
    return new Response(
      `${JSON.stringify({ baton: null, base_url: null })}\n${JSON.stringify({ type: 'step_end', affected_row_count: 1 })}\n`,
      { status: 200, headers: { 'Content-Type': 'application/json' } }
    );
  };

  t.teardown(() => { globalThis.fetch = originalFetch; });

  await session.batch([
    { sql: 'INSERT INTO users(name, age) VALUES(:name, :age)', args: { name: 'alice', age: 30 } }
  ]);

  t.deepEqual(requests[0].batch.steps[0].stmt.args, []);
  t.deepEqual(requests[0].batch.steps[0].stmt.named_args, [
    { name: 'name', value: { type: 'text', value: 'alice' } },
    { name: 'age', value: { type: 'integer', value: '30' } },
  ]);
  // The trailing step is the is_autocommit transaction-state probe.
  const probeStep = requests[0].batch.steps.at(-1);
  t.deepEqual(probeStep.condition, { type: 'is_autocommit' });
});

// --- requestHeaders ---

test.serial('Session attaches requestHeaders and lets them override Authorization', async t => {
  const session = new Session({
    url: 'http://fake-host',
    authToken: 'standard-token',
    requestHeaders: {
      'x-custom-header': 'custom-value',
      'Authorization': 'Bearer override-token',
    },
  });
  const capturedHeaders = [];
  const originalFetch = globalThis.fetch;

  globalThis.fetch = async (url, opts) => {
    capturedHeaders.push(opts.headers);
    return new Response(
      `${JSON.stringify({ baton: null, base_url: null })}\n${JSON.stringify({ type: 'step_begin', step: 0, cols: [] })}\n${JSON.stringify({ type: 'step_end', affected_row_count: 0 })}\n`,
      { status: 200, headers: { 'Content-Type': 'application/json' } }
    );
  };

  t.teardown(() => { globalThis.fetch = originalFetch; });

  await session.execute('SELECT 1');

  const headers = capturedHeaders[0];
  t.is(headers['x-custom-header'], 'custom-value');
  t.is(headers['Authorization'], 'Bearer override-token',
    'requestHeaders are applied after standard headers, so they override Authorization');
});

test('Session construction rejects a Host requestHeader instead of silently dropping it', t => {
  // Mixed case to prove the check is case-insensitive.
  for (const hostKey of ['Host', 'host', 'hOsT']) {
    const error = t.throws(
      () => new Session({
        url: 'http://fake-host',
        requestHeaders: { [hostKey]: 'evil-host' },
      }),
      {
        instanceOf: DatabaseError,
        message: "overwriting the 'Host' header is not supported",
      }
    );
    t.is(error.name, 'DatabaseError');
  }
});

test('request building rejects a Host requestHeader added after construction', async t => {
  // Defense in depth: even if a Host header sneaks past construction-time
  // validation (the config object is mutable), the request must not be sent.
  const session = new Session({ url: 'http://fake-host' });
  session['config'].requestHeaders = { Host: 'evil-host' };

  await t.throwsAsync(() => session.execute('SELECT 1'), {
    instanceOf: DatabaseError,
    message: "overwriting the 'Host' header is not supported",
  });
});

// --- per-query requestHeaders ---

test.serial('per-query requestHeaders are attached and override session-level headers', async t => {
  const session = new Session({
    url: 'http://fake-host',
    authToken: 'standard-token',
    requestHeaders: {
      'x-session-header': 'session-value',
      'x-shared-header': 'session-value',
    },
  });
  const capturedHeaders = [];
  const originalFetch = globalThis.fetch;

  globalThis.fetch = async (url, opts) => {
    capturedHeaders.push(opts.headers);
    return new Response(
      `${JSON.stringify({ baton: null, base_url: null })}\n${JSON.stringify({ type: 'step_begin', cols: [] })}\n${JSON.stringify({ type: 'step_end', affected_row_count: 0 })}\n`,
      { status: 200, headers: { 'Content-Type': 'application/json' } }
    );
  };

  t.teardown(() => { globalThis.fetch = originalFetch; });

  await session.execute('SELECT 1', [], false, {
    requestHeaders: {
      'x-shared-header': 'query-value',
      'x-query-header': 'query-value',
      'Authorization': 'Bearer query-token',
    },
  });
  await session.execute('SELECT 1');

  const queryHeaders = capturedHeaders[0];
  t.is(queryHeaders['x-session-header'], 'session-value',
    'session-level headers are still attached');
  t.is(queryHeaders['x-shared-header'], 'query-value',
    'per-query headers override session-level headers');
  t.is(queryHeaders['x-query-header'], 'query-value');
  t.is(queryHeaders['Authorization'], 'Bearer query-token',
    'per-query headers are applied after standard headers, so they override Authorization');

  const followupHeaders = capturedHeaders[1];
  t.is(followupHeaders['x-shared-header'], 'session-value',
    'per-query headers apply to a single call only');
  t.is(followupHeaders['x-query-header'], undefined);
  t.is(followupHeaders['Authorization'], 'Bearer standard-token');
});

test.serial('per-query requestHeaders apply to batch and sequence requests', async t => {
  const session = new Session({ url: 'http://fake-host' });
  const capturedHeaders = [];
  const originalFetch = globalThis.fetch;

  globalThis.fetch = async (url, opts) => {
    capturedHeaders.push(opts.headers);
    if (url.endsWith('/v3/pipeline')) {
      return new Response(JSON.stringify({
        baton: null,
        base_url: null,
        results: [{ type: 'ok', response: { type: 'sequence' } }],
      }), { status: 200, headers: { 'Content-Type': 'application/json' } });
    }
    return new Response(
      `${JSON.stringify({ baton: null, base_url: null })}\n${JSON.stringify({ type: 'step_end', affected_row_count: 0 })}\n`,
      { status: 200, headers: { 'Content-Type': 'application/json' } }
    );
  };

  t.teardown(() => { globalThis.fetch = originalFetch; });

  const queryOptions = { requestHeaders: { 'x-query-header': 'query-value' } };
  await session.batch(['SELECT 1'], undefined, queryOptions);
  await session.sequence('SELECT 1', queryOptions);

  t.is(capturedHeaders.length, 2);
  t.is(capturedHeaders[0]['x-query-header'], 'query-value');
  t.is(capturedHeaders[1]['x-query-header'], 'query-value');
});

test.serial('per-query requestHeaders reject a Host key before the request is sent', async t => {
  const session = new Session({ url: 'http://fake-host' });
  const originalFetch = globalThis.fetch;
  let fetchCalled = false;

  globalThis.fetch = async () => {
    fetchCalled = true;
    return new Response('', { status: 200 });
  };

  t.teardown(() => { globalThis.fetch = originalFetch; });

  await t.throwsAsync(
    () => session.execute('SELECT 1', [], false, { requestHeaders: { Host: 'evil-host' } }),
    {
      instanceOf: DatabaseError,
      message: "overwriting the 'Host' header is not supported",
    }
  );
  t.false(fetchCalled, 'the request must not be sent with a Host override');
});

test.serial('run() treats a trailing requestHeaders-only object as query options, not a bind parameter', async t => {
  const { connect } = await import('../dist/index.js');

  const capturedHeaders = [];
  const capturedArgs = [];
  const originalFetch = globalThis.fetch;

  globalThis.fetch = async (url, opts) => {
    capturedHeaders.push(opts.headers);
    capturedArgs.push(JSON.parse(opts.body).batch.steps[0].stmt.args);
    return new Response(
      `${JSON.stringify({ baton: null, base_url: null })}\n${JSON.stringify({ type: 'step_begin', cols: [] })}\n${JSON.stringify({ type: 'step_end', affected_row_count: 1 })}\n`,
      { status: 200, headers: { 'Content-Type': 'application/json' } }
    );
  };

  t.teardown(() => { globalThis.fetch = originalFetch; });

  const conn = connect({ url: 'http://fake-host' });
  await conn.run('INSERT INTO t VALUES (?)', 1, { requestHeaders: { 'x-query-header': 'query-value' } });

  t.is(capturedHeaders[0]['x-query-header'], 'query-value');
  t.deepEqual(capturedArgs[0], [{ type: 'integer', value: '1' }],
    'the options object must not be bound as a parameter');

  // No bind parameters at all: the single trailing object is still query
  // options, not a named-args object.
  await conn.run('DELETE FROM t', { requestHeaders: { 'x-query-header': 'no-args-value' } });

  t.is(capturedHeaders[1]['x-query-header'], 'no-args-value');
  t.deepEqual(capturedArgs[1], [], 'no parameters must be bound');
});
