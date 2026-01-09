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