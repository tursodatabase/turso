// Turso Cloud Encryption Example
//
// This example demonstrates how to connect to an encrypted Turso Cloud database
// using the remoteEncryptionKey option.
//
// Environment variables:
//   TURSO_DATABASE_URL       - Your Turso database URL
//   TURSO_AUTH_TOKEN         - Your Turso auth token
//   TURSO_REMOTE_ENCRYPTION_KEY - Base64-encoded encryption key for the database

import { connect } from "@tursodatabase/serverless";

const url = process.env.TURSO_DATABASE_URL;
const authToken = process.env.TURSO_AUTH_TOKEN;
const remoteEncryptionKey = process.env.TURSO_REMOTE_ENCRYPTION_KEY;

if (!url || !authToken) {
    console.error("Error: TURSO_DATABASE_URL and TURSO_AUTH_TOKEN are required");
    process.exit(1);
}

console.log("Connecting to:", url);
console.log("Encryption key provided:", !!remoteEncryptionKey);

const client = connect({
    url,
    authToken,
    remoteEncryptionKey,
});

// Create a table and insert some data
await client.batch(
    [
        "CREATE TABLE IF NOT EXISTS secrets (id INTEGER PRIMARY KEY, data TEXT)",
        "DELETE FROM secrets",
        "INSERT INTO secrets (data) VALUES ('encrypted secret 1')",
        "INSERT INTO secrets (data) VALUES ('encrypted secret 2')",
        "INSERT INTO secrets (data) VALUES ('encrypted secret 3')",
    ],
    "write",
);

console.log("\nInserted 3 secrets into encrypted database");

// Query the data back
const result = await client.execute("SELECT * FROM secrets");
console.log("\nSecrets retrieved:");
for (const row of result.rows) {
    console.log(`  - id=${row.id}, data="${row.data}"`);
}

// Show table info
const tables = await client.execute(
    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
);
console.log("\nTables in database:");
for (const row of tables.rows) {
    console.log(`  - ${row.name}`);
}

await client.close();
console.log("\nDone!");
