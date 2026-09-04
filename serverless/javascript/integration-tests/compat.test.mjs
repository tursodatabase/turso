import test from 'ava';
import { createClient, LibsqlError } from '../dist/compat/index.js';

test.serial('createClient validates supported config options', async t => {
  // Valid config should work
  t.notThrows(() => {
    const client = createClient({
      url: process.env.TURSO_DATABASE_URL,
      authToken: process.env.TURSO_AUTH_TOKEN,
    });
    client.close();
  });
});

test.serial('createClient rejects unsupported config options', async t => {
  const error = t.throws(() => {
    createClient({
      url: process.env.TURSO_DATABASE_URL,
      authToken: process.env.TURSO_AUTH_TOKEN,
      encryptionKey: 'some-key', // local encryption - not supported
      syncUrl: 'https://sync.example.com',
    });
  }, { instanceOf: LibsqlError });

  t.is(error.code, 'UNSUPPORTED_CONFIG');
  t.regex(error.message, /encryptionKey.*syncUrl/);
  t.regex(error.message, /Only 'url', 'authToken', and 'remoteEncryptionKey' are supported/);
});

test.serial('createClient accepts remoteEncryptionKey config option', async t => {
  // remoteEncryptionKey should be accepted without throwing
  t.notThrows(() => {
    const client = createClient({
      url: process.env.TURSO_DATABASE_URL,
      authToken: process.env.TURSO_AUTH_TOKEN,
      remoteEncryptionKey: 'dGVzdC1lbmNyeXB0aW9uLWtleQ==', // base64-encoded test key
    });
    client.close();
  });
});

test.serial('createClient requires url config option', async t => {
  const error = t.throws(() => {
    createClient({
      authToken: process.env.TURSO_AUTH_TOKEN,
    });
  }, { instanceOf: LibsqlError });

  t.is(error.code, 'MISSING_URL');
  t.regex(error.message, /Missing required 'url'/);
});

test.serial('createClient works with basic libSQL API', async t => {
  const client = createClient({
    url: process.env.TURSO_DATABASE_URL,
    authToken: process.env.TURSO_AUTH_TOKEN,
  });

  // Test basic functionality
  const result = await client.execute('SELECT 42 as answer');
  t.is(result.rows[0][0], 42);
  t.is(result.columns[0], 'answer');
  
  client.close();
  t.true(client.closed);
});

test.serial('compat execute preserves lastInsertRowid of 0', async t => {
  const client = createClient({
    url: 'http://localhost:0',
  });

  client.session.execute = async () => ({
    columns: [],
    columnTypes: [],
    rows: [],
    rowsAffected: 1,
    lastInsertRowid: 0,
  });

  const result = await client.execute('INSERT INTO users DEFAULT VALUES');
  t.is(result.lastInsertRowid, 0n);
  t.is(typeof result.lastInsertRowid, 'bigint');

  client.close();
});

test.serial('reconnect restores usability after close', async t => {
  const client = createClient({
    url: 'http://localhost:0',
  });

  client.close();
  t.true(client.closed);

  const error = await t.throwsAsync(async () => {
    await client.execute('SELECT 1');
  }, { instanceOf: LibsqlError });
  t.is(error.code, 'CLIENT_CLOSED');

  client.reconnect();
  t.false(client.closed);

  client.session.execute = async () => ({
    columns: ['1'],
    columnTypes: ['INTEGER'],
    rows: [[1]],
    rowsAffected: 0,
    lastInsertRowid: undefined,
  });

  const result = await client.execute('SELECT 1');
  t.is(result.rows[0][0], 1);

  client.close();
});

test.serial('reconnect works while client is active', async t => {
  const client = createClient({
    url: 'http://localhost:0',
  });

  client.session.execute = async () => ({
    columns: ['1'],
    columnTypes: ['INTEGER'],
    rows: [[1]],
    rowsAffected: 0,
    lastInsertRowid: undefined,
  });

  const result1 = await client.execute('SELECT 1');
  t.is(result1.rows[0][0], 1);

  const oldSession = client.session;
  client.reconnect();
  t.false(client.closed);
  t.not(client.session, oldSession);

  client.session.execute = async () => ({
    columns: ['2'],
    columnTypes: ['INTEGER'],
    rows: [[2]],
    rowsAffected: 0,
    lastInsertRowid: undefined,
  });

  const result2 = await client.execute('SELECT 2');
  t.is(result2.rows[0][0], 2);

  client.close();
});
