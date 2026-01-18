import { connect } from '@tursodatabase/react-native';
import {
  expect,
  afterEach,
  beforeEach,
  describe,
  it,
} from '@op-engineering/op-test';

// NOTE: These tests require a valid Turso database URL and auth token
// Set these environment variables or replace with actual values for testing
const TURSO_URL = process.env.TURSO_URL;
const TURSO_AUTH_TOKEN = process.env.TURSO_AUTH_TOKEN;

// Skip sync tests if no credentials provided
const canRunSyncTests = TURSO_URL && TURSO_AUTH_TOKEN;

describe('Sync Database Operations', () => {
  if (!canRunSyncTests) {
    it('SKIPPED - No Turso credentials provided', () => {
      expect(true).toBe(true);
    });
    return;
  }

  let db: any;

  beforeEach(async () => {
    // Create a sync database with a unique path for each test
    const testId = Math.random().toString(36).substring(7);
    db = await connect({
      path: `test-sync-${testId}.db`,
      url: TURSO_URL,
      authToken: TURSO_AUTH_TOKEN,
      bootstrapIfEmpty: true,
    });
  });

  afterEach(async () => {
    if (db) {
      await db.close();
      db = null;
    }
  });

  it('should create sync database and query data', async () => {
    // Sync database was already created in beforeEach via createSyncDatabase()
    // Now we can query it
    const result = await db.get('SELECT 1 as value');
    expect(result).toDeepEqual({ value: 1 });
  });

  it('should perform local operations on sync database', async () => {
    // Create table locally
    await db.exec('CREATE TABLE IF NOT EXISTS test_users (id INTEGER PRIMARY KEY, name TEXT)');

    // Insert data locally
    const insertResult = await db.run('INSERT INTO test_users (name) VALUES (?)', 'Alice');
    expect(insertResult.changes).toBe(1);

    // Query locally
    const row = await db.get('SELECT * FROM test_users WHERE id = ?', insertResult.lastInsertRowid);
    expect(row.name).toBe('Alice');
  });

  it('should push local changes to remote', async () => {
    // Create table and insert data
    await db.exec('CREATE TABLE IF NOT EXISTS test_push (id INTEGER PRIMARY KEY, value TEXT)');
    await db.run('INSERT INTO test_push (value) VALUES (?)', 'test-data');

    // Push changes to remote
    await db.push();

    // If push succeeds, we've successfully synced
    expect(true).toBe(true);
  });

  it('should pull remote changes', async () => {
    // Pull changes from remote
    const hasChanges = await db.pull();

    // hasChanges is boolean indicating if there were changes to apply
    expect(typeof hasChanges).toBe('boolean');
  });

  it('should get sync stats', async () => {
    const stats = await db.stats();

    // Verify stats structure
    expect(typeof stats.cdcOperations).toBe('number');
    expect(typeof stats.mainWalSize).toBe('number');
    expect(typeof stats.revertWalSize).toBe('number');
    expect(typeof stats.networkSentBytes).toBe('number');
    expect(typeof stats.networkReceivedBytes).toBe('number');
  });

  it('should checkpoint the database', async () => {
    // Create some data
    await db.exec('CREATE TABLE IF NOT EXISTS test_checkpoint (id INTEGER PRIMARY KEY)');
    await db.run('INSERT INTO test_checkpoint VALUES (1)');

    // Checkpoint
    await db.checkpoint();

    // If checkpoint succeeds without error, test passes
    expect(true).toBe('boolean');
  });
});

describe('Sync Database - Local vs Sync', () => {
  if (!canRunSyncTests) {
    it('SKIPPED - No Turso credentials provided', () => {
      expect(true).toBe(true);
    });
    return;
  }

  it('should create sync database with URL', async () => {
    const testId = Math.random().toString(36).substring(7);
    const db = await connect({
      path: `test-open-${testId}.db`,
      url: TURSO_URL,
      authToken: TURSO_AUTH_TOKEN,
      bootstrapIfEmpty: true,
    });

    // Should be able to query
    const result = await db.get('SELECT 1 as value');
    expect(result).toDeepEqual({ value: 1 });

    await db.close();
  });

  it('should create local database without URL', async () => {
    const testId = Math.random().toString(36).substring(7);
    const db = await connect({ path: `test-local-${testId}.db` });

    // Should be able to use local operations
    await db.exec('CREATE TABLE test (id INTEGER)');
    await db.run('INSERT INTO test VALUES (1)');
    const result = await db.get('SELECT * FROM test');
    expect(result.id).toBe(1);

    db.close();
  });
});

describe('Sync Database - Partial Sync', () => {
  if (!canRunSyncTests) {
    it('SKIPPED - No Turso credentials provided', () => {
      expect(true).toBe(true);
    });
    return;
  }

  it('should create database with partial sync prefix strategy', async () => {
    const testId = Math.random().toString(36).substring(7);
    const db = await connect({
      path: `test-partial-${testId}.db`,
      url: TURSO_URL,
      authToken: TURSO_AUTH_TOKEN,
      bootstrapIfEmpty: true,
      partialSyncExperimental: {
        bootstrapStrategy: { kind: 'prefix', length: 2 },
        segmentSize: 1024,
        prefetch: true,
      },
    });

    // Should be able to query
    const result = await db.get('SELECT 1 as value');
    expect(result).toDeepEqual({ value: 1 });

    await db.close();
  });

  it('should create database with partial sync query strategy', async () => {
    const testId = Math.random().toString(36).substring(7);
    const db = await connect({
      path: `test-partial-query-${testId}.db`,
      url: TURSO_URL,
      authToken: TURSO_AUTH_TOKEN,
      bootstrapIfEmpty: true,
      partialSyncExperimental: {
        bootstrapStrategy: { kind: 'query', query: 'SELECT * FROM users LIMIT 100' },
        segmentSize: 1024,
        prefetch: false,
      },
    });

    // Should be able to query
    const result = await db.get('SELECT 1 as value');
    expect(result).toDeepEqual({ value: 1 });

    await db.close();
  });
});
