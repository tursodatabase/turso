import test from 'ava';
import { decodeValue, encodeValue } from '../dist/protocol.js';
import { Session } from '../dist/session.js';

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
