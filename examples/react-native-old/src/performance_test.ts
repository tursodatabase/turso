import { connect } from '@tursodatabase/react-native';

export async function performanceTest() {
  const db = await connect({ path: 'perfTest.sqlite' });

  // Create table with 14 columns
  await db.exec(
    `CREATE TABLE IF NOT EXISTS perf_table (
      id INTEGER PRIMARY KEY,
      col1 TEXT, col2 TEXT, col3 TEXT, col4 TEXT, col5 TEXT, col6 TEXT, col7 TEXT,
      col8 TEXT, col9 TEXT, col10 TEXT, col11 TEXT, col12 TEXT, col13 TEXT, col14 TEXT
    )`,
  );

  // Clear table
  await db.exec('DELETE FROM perf_table');

  // Insert a single row for querying
  await db.run(
    `INSERT INTO perf_table (
      col1, col2, col3, col4, col5, col6, col7,
      col8, col9, col10, col11, col12, col13, col14
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    ...Array(14).fill('test'),
  );

  // Performance test: query 1000 times
  let start = performance.now();
  for (let i = 0; i < 1000; i++) {
    await db.get('SELECT * FROM perf_table WHERE id = 1');
  }
  const end = performance.now();

  return end - start;
}
