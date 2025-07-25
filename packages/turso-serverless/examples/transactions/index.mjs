import { connect } from "@tursodatabase/serverless";

const client = connect({
  url: process.env.TURSO_DATABASE_URL,
  authToken: process.env.TURSO_AUTH_TOKEN,
});

await client.batch(
  [
    `DROP TABLE IF EXISTS accounts`,
    `CREATE TABLE accounts (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      balance DECIMAL(10,2) NOT NULL DEFAULT 0.00
    )`,
  ],
  "write",
);

await client.execute("INSERT INTO accounts (name, balance) VALUES (?, ?)", [
  "Alice",
  1000.0,
]);
await client.execute("INSERT INTO accounts (name, balance) VALUES (?, ?)", [
  "Bob",
  500.0,
]);

const tx = await client.transaction();

try {
  console.log("Starting money transfer...");

  await tx.execute("UPDATE accounts SET balance = balance - ? WHERE name = ?", [
    100,
    "Alice",
  ]);

  await tx.execute("UPDATE accounts SET balance = balance + ? WHERE name = ?", [
    100,
    "Bob",
  ]);

  // Commit the transaction
  await tx.commit();
  console.log("Transaction committed successfully!");

  // Check the results
  const result = await client.execute(
    "SELECT name, balance FROM accounts ORDER BY name",
  );
  console.log("Account balances after transfer:");
  for (const row of result.rows) {
    console.log(`  ${row[0]}: $${row[1]}`);
  }
} catch (error) {
  console.error("Transaction failed:", error.message);
  await tx.rollback();
}
