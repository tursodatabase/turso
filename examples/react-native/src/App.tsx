import { useEffect, useState } from 'react';
import { SafeAreaProvider, SafeAreaView } from 'react-native-safe-area-context';
import { StyleSheet, Text, View, ScrollView } from 'react-native';
import { connect, setup, Database } from '@tursodatabase/react-native';

type TestResult = {
  name: string;
  passed: boolean;
  error?: string;
};

// Environment variables injected at build time via babel-plugin-transform-inline-environment-variables
// Set these when running metro: TURSO_DATABASE_URL=... TURSO_AUTH_TOKEN=... npm start
const TURSO_DATABASE_URL = process.env.TURSO_DATABASE_URL as string | undefined;
const TURSO_AUTH_TOKEN = process.env.TURSO_AUTH_TOKEN as string | undefined;
const TURSO_ENCRYPTION_KEY = process.env.TURSO_ENCRYPTION_KEY as string | undefined;
const TURSO_ENCRYPTION_CIPHER = (process.env.TURSO_ENCRYPTION_CIPHER || 'aes256gcm') as
  | 'aes256gcm'
  | 'aes128gcm'
  | 'chacha20poly1305';

// Check if sync tests should run
const SYNC_ENABLED = !!(TURSO_DATABASE_URL && TURSO_AUTH_TOKEN);
// Check if encryption tests should run (requires sync + encryption key)
const ENCRYPTION_ENABLED = !!(SYNC_ENABLED && TURSO_ENCRYPTION_KEY);

export default function App() {
  const [openTime, setOpenTime] = useState(0);
  const [queryTime, setQueryTime] = useState(0);
  const [results, setResults] = useState<TestResult[]>([]);

  useEffect(() => {
    runTests();
  }, []);

  async function runTests() {
    const testResults: TestResult[] = [];

    // Configure logging
    try {
      setup({ logLevel: 'debug' });
      testResults.push({ name: 'Setup logging', passed: true });
    } catch (e) {
      testResults.push({ name: 'Setup logging', passed: false, error: String(e) });
    }

    // Test: Open database
    let db: Database | null = null;
    try {
      const start = performance.now();
      db = await connect({ path: 'test.db' });
      setOpenTime(performance.now() - start);
      testResults.push({ name: 'Open database', passed: true });
    } catch (e) {
      testResults.push({ name: 'Open database', passed: false, error: String(e) });
      setResults(testResults);
      return;
    }

    // cleanup db state
    await db.exec("DROP TABLE IF EXISTS users");

    // Test: Simple SELECT
    try {
      const res = await db.get('SELECT 1 as value');
      if (res?.value === 1) {
        testResults.push({ name: 'SELECT 1', passed: true });
      } else {
        testResults.push({ name: 'SELECT 1', passed: false, error: `Expected {value: 1}, got ${JSON.stringify(res)}` });
      }
    } catch (e) {
      testResults.push({ name: 'SELECT 1', passed: false, error: String(e) });
    }

    // Test: Create table
    try {
      await db.exec('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
      testResults.push({ name: 'CREATE TABLE', passed: true });
    } catch (e) {
      testResults.push({ name: 'CREATE TABLE', passed: false, error: String(e) });
    }

    // Test: Insert data
    try {
      await db.run('INSERT INTO users (name, age) VALUES (?, ?)', 'Alice', 30);
      await db.run('INSERT INTO users (name, age) VALUES (?, ?)', 'Bob', 25);
      testResults.push({ name: 'INSERT data', passed: true });
    } catch (e) {
      testResults.push({ name: 'INSERT data', passed: false, error: String(e) });
    }

    // Test: Query single row
    try {
      const row = await db.get('SELECT * FROM users WHERE name = ?', 'Alice');
      if (row?.name === 'Alice' && row?.age === 30) {
        testResults.push({ name: 'SELECT single row', passed: true });
      } else {
        testResults.push({ name: 'SELECT single row', passed: false, error: `Unexpected result: ${JSON.stringify(row)}` });
      }
    } catch (e) {
      testResults.push({ name: 'SELECT single row', passed: false, error: String(e) });
    }

    // Test: Query all rows
    try {
      const rows = await db.all('SELECT * FROM users ORDER BY name');
      if (rows.length === 2 && rows[0]?.name === 'Alice' && rows[1]?.name === 'Bob') {
        testResults.push({ name: 'SELECT all rows', passed: true });
      } else {
        testResults.push({ name: 'SELECT all rows', passed: false, error: `Unexpected result: ${JSON.stringify(rows)}` });
      }
    } catch (e) {
      testResults.push({ name: 'SELECT all rows', passed: false, error: String(e) });
    }

    // Test: Math operations
    try {
      const res = await db.get('SELECT 2 + 2 as result');
      if (res?.result === 4) {
        testResults.push({ name: 'Math operations', passed: true });
      } else {
        testResults.push({ name: 'Math operations', passed: false, error: `Expected 4, got ${res?.result}` });
      }
    } catch (e) {
      testResults.push({ name: 'Math operations', passed: false, error: String(e) });
    }

    // Test: NULL handling
    try {
      const res = await db.get('SELECT NULL as value');
      if (res?.value === null) {
        testResults.push({ name: 'NULL handling', passed: true });
      } else {
        testResults.push({ name: 'NULL handling', passed: false, error: `Expected null, got ${res?.value}` });
      }
    } catch (e) {
      testResults.push({ name: 'NULL handling', passed: false, error: String(e) });
    }

    // Performance test: 1000 queries
    try {
      await db.exec('CREATE TABLE IF NOT EXISTS perf (id INTEGER PRIMARY KEY, value TEXT)');
      await db.run('INSERT OR REPLACE INTO perf (id, value) VALUES (1, ?)', 'test');

      const start = performance.now();
      for (let i = 0; i < 1000; i++) {
        await db.get('SELECT * FROM perf WHERE id = 1');
      }
      setQueryTime(performance.now() - start);
      testResults.push({ name: 'Performance (1000 queries)', passed: true });
    } catch (e) {
      testResults.push({ name: 'Performance (1000 queries)', passed: false, error: String(e) });
    }

    // Cleanup local database
    try {
      db.close();
      testResults.push({ name: 'Close database', passed: true });
    } catch (e) {
      testResults.push({ name: 'Close database', passed: false, error: String(e) });
    }

    // ========================================
    // SYNC API TESTS (requires env vars)
    // ========================================
    if (SYNC_ENABLED) {
      testResults.push({ name: '--- Sync Tests ---', passed: true });

      let syncDb: Database | null = null;

      // Test: Connect to sync database
      try {
        syncDb = await connect({
          path: 'sync-test.db',
          url: TURSO_DATABASE_URL,
          authToken: TURSO_AUTH_TOKEN,
        });
        testResults.push({ name: 'Sync: Connect', passed: true });
      } catch (e) {
        testResults.push({ name: 'Sync: Connect', passed: false, error: String(e) });
      }

      if (syncDb) {
        // Test: Pull from remote
        try {
          const hasChanges = await syncDb.pull();
          testResults.push({ name: `Sync: Pull (changes: ${hasChanges})`, passed: true });
        } catch (e) {
          testResults.push({ name: 'Sync: Pull', passed: false, error: String(e) });
        }

        // Test: Create table and insert
        try {
          await syncDb.exec('CREATE TABLE IF NOT EXISTS rn_sync_test (id INTEGER PRIMARY KEY, value TEXT, created_at INTEGER DEFAULT (unixepoch()))');
          await syncDb.run('INSERT INTO rn_sync_test (value) VALUES (?)', `test-${Date.now()}`);
          testResults.push({ name: 'Sync: Insert data', passed: true });
        } catch (e) {
          testResults.push({ name: 'Sync: Insert data', passed: false, error: String(e) });
        }

        // Test: Query local data
        try {
          const rows = await syncDb.all('SELECT * FROM rn_sync_test ORDER BY created_at DESC LIMIT 5');
          testResults.push({ name: `Sync: Query (${rows.length} rows)`, passed: true });
        } catch (e) {
          testResults.push({ name: 'Sync: Query', passed: false, error: String(e) });
        }

        // Test: Push to remote
        try {
          await syncDb.push();
          testResults.push({ name: 'Sync: Push', passed: true });
        } catch (e) {
          testResults.push({ name: 'Sync: Push', passed: false, error: String(e) });
        }

        // Test: Get stats
        try {
          const stats = await syncDb.stats();
          testResults.push({ name: `Sync: Stats (ops: ${stats.cdcOperations})`, passed: true });
        } catch (e) {
          testResults.push({ name: 'Sync: Stats', passed: false, error: String(e) });
        }

        // Cleanup sync database
        try {
          syncDb.close();
          testResults.push({ name: 'Sync: Close', passed: true });
        } catch (e) {
          testResults.push({ name: 'Sync: Close', passed: false, error: String(e) });
        }
      }
    } else {
      testResults.push({ name: '--- Sync Tests (skipped: no env vars) ---', passed: true });
    }

    // ========================================
    // ENCRYPTION TESTS (requires sync + encryption key)
    // ========================================
    if (ENCRYPTION_ENABLED) {
      testResults.push({ name: '--- Encryption Tests ---', passed: true });

      let encDb: Database | null = null;

      // Test: Connect with encryption
      try {
        encDb = await connect({
          path: 'encrypted-test.db',
          url: TURSO_DATABASE_URL,
          authToken: TURSO_AUTH_TOKEN,
          remoteEncryption: {
            key: TURSO_ENCRYPTION_KEY!,
            cipher: TURSO_ENCRYPTION_CIPHER,
          },
        });
        testResults.push({ name: 'Encryption: Connect', passed: true });
      } catch (e) {
        testResults.push({ name: 'Encryption: Connect', passed: false, error: String(e) });
      }

      if (encDb) {
        // Test: Pull encrypted data
        try {
          const hasChanges = await encDb.pull();
          testResults.push({ name: `Encryption: Pull (changes: ${hasChanges})`, passed: true });
        } catch (e) {
          testResults.push({ name: 'Encryption: Pull', passed: false, error: String(e) });
        }

        // Test: Create table and insert encrypted
        try {
          await encDb.exec('CREATE TABLE IF NOT EXISTS rn_enc_test (id INTEGER PRIMARY KEY, secret TEXT)');
          await encDb.run('INSERT INTO rn_enc_test (secret) VALUES (?)', `secret-${Date.now()}`);
          testResults.push({ name: 'Encryption: Insert', passed: true });
        } catch (e) {
          testResults.push({ name: 'Encryption: Insert', passed: false, error: String(e) });
        }

        // Test: Query encrypted data
        try {
          const rows = await encDb.all('SELECT * FROM rn_enc_test LIMIT 5');
          testResults.push({ name: `Encryption: Query (${rows.length} rows)`, passed: true });
        } catch (e) {
          testResults.push({ name: 'Encryption: Query', passed: false, error: String(e) });
        }

        // Test: Push encrypted changes
        try {
          await encDb.push();
          testResults.push({ name: 'Encryption: Push', passed: true });
        } catch (e) {
          testResults.push({ name: 'Encryption: Push', passed: false, error: String(e) });
        }

        // Cleanup encrypted database
        try {
          encDb.close();
          testResults.push({ name: 'Encryption: Close', passed: true });
        } catch (e) {
          testResults.push({ name: 'Encryption: Close', passed: false, error: String(e) });
        }
      }
    } else if (SYNC_ENABLED) {
      testResults.push({ name: '--- Encryption Tests (skipped: no key) ---', passed: true });
    } else {
      testResults.push({ name: '--- Encryption Tests (skipped: no env vars) ---', passed: true });
    }

    setResults(testResults);
  }

  const passedCount = results.filter(r => r.passed).length;
  const failedCount = results.filter(r => !r.passed).length;

  return (
    <SafeAreaProvider>
      <SafeAreaView style={styles.container}>
        <Text style={styles.title}>Turso React Native Example</Text>

        <View style={styles.statsContainer}>
          <Text style={styles.statsText}>DB open time: {openTime.toFixed(0)} ms</Text>
          <Text style={styles.statsText}>1000 queries: {queryTime.toFixed(0)} ms</Text>
          <Text style={[styles.statsText, SYNC_ENABLED ? styles.enabled : styles.disabled]}>
            Sync: {SYNC_ENABLED ? 'enabled' : 'disabled'}
          </Text>
          <Text style={[styles.statsText, ENCRYPTION_ENABLED ? styles.enabled : styles.disabled]}>
            Encryption: {ENCRYPTION_ENABLED ? 'enabled' : 'disabled'}
          </Text>
        </View>

        <View style={styles.summaryContainer}>
          <Text style={styles.summaryText}>
            Tests: {passedCount} passed, {failedCount} failed
          </Text>
        </View>

        <ScrollView style={styles.resultsContainer}>
          {results.map((result, index) => (
            <View key={index} style={styles.resultRow}>
              <Text style={[styles.resultIcon, result.passed ? styles.passed : styles.failed]}>
                {result.passed ? '✓' : '✗'}
              </Text>
              <View style={styles.resultTextContainer}>
                <Text style={styles.resultName}>{result.name}</Text>
                {result.error && <Text style={styles.resultError}>{result.error}</Text>}
              </View>
            </View>
          ))}
        </ScrollView>
      </SafeAreaView>
    </SafeAreaProvider>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#1a1a2e',
    padding: 16,
  },
  title: {
    fontSize: 24,
    fontWeight: 'bold',
    color: '#eee',
    textAlign: 'center',
    marginBottom: 16,
  },
  statsContainer: {
    backgroundColor: '#16213e',
    padding: 12,
    borderRadius: 8,
    marginBottom: 12,
  },
  statsText: {
    color: '#4cc9f0',
    fontSize: 16,
    fontFamily: 'monospace',
  },
  enabled: {
    color: '#4ade80',
  },
  disabled: {
    color: '#888',
  },
  summaryContainer: {
    backgroundColor: '#16213e',
    padding: 12,
    borderRadius: 8,
    marginBottom: 12,
  },
  summaryText: {
    color: '#eee',
    fontSize: 16,
    fontWeight: '600',
  },
  resultsContainer: {
    flex: 1,
  },
  resultRow: {
    flexDirection: 'row',
    alignItems: 'flex-start',
    paddingVertical: 8,
    borderBottomWidth: 1,
    borderBottomColor: '#2a2a4a',
  },
  resultIcon: {
    fontSize: 18,
    fontWeight: 'bold',
    marginRight: 12,
    width: 24,
  },
  passed: {
    color: '#4ade80',
  },
  failed: {
    color: '#f87171',
  },
  resultTextContainer: {
    flex: 1,
  },
  resultName: {
    color: '#eee',
    fontSize: 16,
  },
  resultError: {
    color: '#f87171',
    fontSize: 12,
    marginTop: 4,
  },
});
