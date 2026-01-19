import { useEffect, useState } from 'react';
import { SafeAreaProvider, SafeAreaView } from 'react-native-safe-area-context';
import { StyleSheet, Text, View, ScrollView } from 'react-native';
import { connect, setup, Database } from '@tursodatabase/react-native';

type TestResult = {
  name: string;
  passed: boolean;
  error?: string;
};

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

    // Cleanup
    try {
      db.close();
      testResults.push({ name: 'Close database', passed: true });
    } catch (e) {
      testResults.push({ name: 'Close database', passed: false, error: String(e) });
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
