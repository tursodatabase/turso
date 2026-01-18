import React, {useState} from 'react';
import {
  SafeAreaView,
  ScrollView,
  Text,
  View,
  Button,
  StyleSheet,
  StatusBar,
} from 'react-native';
import {openDatabase, version, Database} from 'turso-react-native';

function App(): React.JSX.Element {
  const [logs, setLogs] = useState<string[]>([]);
  const [db, setDb] = useState<Database | null>(null);

  const log = (msg: string) => {
    console.log(msg);
    setLogs(prev => [...prev, `[${new Date().toLocaleTimeString()}] ${msg}`]);
  };

  const clearLogs = () => setLogs([]);

  const runTests = () => {
    clearLogs();
    try {
      // Version check
      log(`Turso version: ${version()}`);

      // Open in-memory database
      log('Opening in-memory database...');
      const database = openDatabase(':memory:');
      setDb(database);
      log(`✓ Database opened (memory: ${database.memory})`);

      // Create table
      log('Creating users table...');
      database.exec(`
        CREATE TABLE users (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL,
          email TEXT UNIQUE,
          age INTEGER,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
      `);
      log('✓ Table created');

      // Insert data using run()
      log('Inserting users...');
      const r1 = database.run(
        'INSERT INTO users (name, email, age) VALUES (?, ?, ?)',
        'Alice',
        'alice@example.com',
        30,
      );
      log(`  → Alice inserted (id: ${r1.lastInsertRowid})`);

      const r2 = database.run(
        'INSERT INTO users (name, email, age) VALUES (?, ?, ?)',
        'Bob',
        'bob@example.com',
        25,
      );
      log(`  → Bob inserted (id: ${r2.lastInsertRowid})`);

      const r3 = database.run(
        'INSERT INTO users (name, email, age) VALUES (?, ?, ?)',
        'Charlie',
        'charlie@example.com',
        35,
      );
      log(`  → Charlie inserted (id: ${r3.lastInsertRowid})`);
      log(`✓ ${r1.changes + r2.changes + r3.changes} users inserted`);

      // Query single row with get()
      log('Querying single user...');
      const alice = database.get('SELECT * FROM users WHERE name = ?', 'Alice');
      log(`✓ Found: ${alice?.name}, age ${alice?.age}`);

      // Query all rows with all()
      log('Querying all users...');
      const allUsers = database.all('SELECT id, name, age FROM users ORDER BY age');
      log(`✓ Found ${allUsers.length} users:`);
      allUsers.forEach(u => log(`  → ${u.name} (${u.age})`));

      // Prepared statement
      log('Testing prepared statement...');
      const stmt = database.prepare('SELECT * FROM users WHERE age > ?');
      const older = stmt.all(26);
      log(`✓ Users older than 26: ${older.map(u => u.name).join(', ')}`);
      stmt.finalize();

      // Transaction
      log('Testing transaction...');
      database.transaction(() => {
        database.run(
          'INSERT INTO users (name, email, age) VALUES (?, ?, ?)',
          'David',
          'david@example.com',
          40,
        );
        database.run(
          'INSERT INTO users (name, email, age) VALUES (?, ?, ?)',
          'Eve',
          'eve@example.com',
          28,
        );
      });
      const count = database.get('SELECT COUNT(*) as count FROM users');
      log(`✓ Transaction complete, total users: ${count?.count}`);

      // Update
      log('Testing update...');
      const updated = database.run(
        'UPDATE users SET age = age + 1 WHERE name = ?',
        'Alice',
      );
      log(`✓ Updated ${updated.changes} row(s)`);

      // Delete
      log('Testing delete...');
      const deleted = database.run('DELETE FROM users WHERE age > ?', 38);
      log(`✓ Deleted ${deleted.changes} row(s)`);

      // Final count
      const final = database.all('SELECT name, age FROM users ORDER BY name');
      log(`Final users: ${final.map(u => `${u.name}(${u.age})`).join(', ')}`);

      log('');
      log('🎉 All tests passed!');
    } catch (error: any) {
      log(`❌ ERROR: ${error.message}`);
      console.error(error);
    }
  };

  const testFileDatabase = () => {
    clearLogs();
    try {
      log('Opening file database...');
      const database = openDatabase('test.db');
      setDb(database);
      log(`✓ Database opened at: ${database.path}`);

      database.exec('CREATE TABLE IF NOT EXISTS counter (id INTEGER PRIMARY KEY, value INTEGER)');
      database.exec('INSERT OR REPLACE INTO counter (id, value) VALUES (1, COALESCE((SELECT value FROM counter WHERE id = 1), 0) + 1)');

      const row = database.get('SELECT value FROM counter WHERE id = 1');
      log(`✓ Counter value: ${row?.value} (persisted across app restarts)`);

      log('');
      log('🎉 File database test passed!');
    } catch (error: any) {
      log(`❌ ERROR: ${error.message}`);
      console.error(error);
    }
  };

  const closeDb = () => {
    if (db) {
      db.close();
      setDb(null);
      log('Database closed');
    }
  };

  return (
    <SafeAreaView style={styles.container}>
      <StatusBar barStyle="light-content" backgroundColor="#1a1a2e" />
      <Text style={styles.title}>🗄️ Turso React Native</Text>
      <Text style={styles.subtitle}>SQLite for React Native</Text>

      <View style={styles.buttons}>
        <Button title="Run Tests" onPress={runTests} color="#4CAF50" />
        <Button title="File DB" onPress={testFileDatabase} color="#2196F3" />
        <Button title="Close" onPress={closeDb} color="#f44336" />
        <Button title="Clear" onPress={clearLogs} color="#9E9E9E" />
      </View>

      <View style={styles.status}>
        <Text style={styles.statusText}>
          DB: {db ? (db.open ? '🟢 Open' : '🔴 Closed') : '⚪ None'}
          {db?.memory ? ' (memory)' : db ? ' (file)' : ''}
        </Text>
      </View>

      <ScrollView style={styles.logs} contentContainerStyle={styles.logsContent}>
        {logs.length === 0 ? (
          <Text style={styles.placeholder}>
            Press "Run Tests" to test the database
          </Text>
        ) : (
          logs.map((msg, i) => (
            <Text
              key={i}
              style={[
                styles.log,
                msg.includes('ERROR') && styles.logError,
                msg.includes('✓') && styles.logSuccess,
                msg.includes('🎉') && styles.logSuccess,
              ]}>
              {msg}
            </Text>
          ))
        )}
      </ScrollView>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#1a1a2e',
  },
  title: {
    fontSize: 28,
    fontWeight: 'bold',
    color: '#eee',
    textAlign: 'center',
    marginTop: 20,
  },
  subtitle: {
    fontSize: 14,
    color: '#888',
    textAlign: 'center',
    marginBottom: 20,
  },
  buttons: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    justifyContent: 'center',
    gap: 10,
    paddingHorizontal: 16,
    marginBottom: 10,
  },
  status: {
    paddingHorizontal: 16,
    paddingVertical: 8,
    backgroundColor: '#16213e',
  },
  statusText: {
    color: '#aaa',
    fontSize: 12,
    textAlign: 'center',
  },
  logs: {
    flex: 1,
    backgroundColor: '#0f0f23',
    margin: 16,
    borderRadius: 8,
  },
  logsContent: {
    padding: 12,
  },
  placeholder: {
    color: '#555',
    fontStyle: 'italic',
    textAlign: 'center',
    marginTop: 40,
  },
  log: {
    color: '#ccc',
    fontFamily: 'monospace',
    fontSize: 12,
    lineHeight: 18,
    marginBottom: 2,
  },
  logError: {
    color: '#ff6b6b',
  },
  logSuccess: {
    color: '#51cf66',
  },
});

export default App;
