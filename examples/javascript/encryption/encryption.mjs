/**
 * Local Database Encryption Example
 *
 * This example demonstrates how to use local database encryption
 * with the Turso JavaScript SDK.
 *
 * Supported ciphers:
 *   - Aes128Gcm
 *   - Aes256Gcm
 *   - Aegis256
 *   - Aegis256x2
 *   - Aegis128l
 *   - Aegis128x2
 *   - Aegis128x4
 */

// Import from local build (run `npm run build` in bindings/javascript/packages/native first)
import { Database } from "../../../bindings/javascript/packages/native/dist/compat.js";
import { unlinkSync, readFileSync } from "node:fs";

const DB_PATH = "encrypted.db";
// 32-byte hex key for aegis256 (256 bits = 32 bytes = 64 hex chars)
const ENCRYPTION_KEY =
  "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";

function main() {
  console.log("=== Turso Local Encryption Example ===\n");

  // Create an encrypted database
  console.log("1. Creating encrypted database...");
  const db = new Database(DB_PATH, {
    encryption: {
      cipher: "aegis256",
      hexkey: ENCRYPTION_KEY,
    },
  });

  // Create a table and insert sensitive data
  console.log("2. Creating table and inserting data...");
  db.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, ssn TEXT)");
  db.exec("INSERT INTO users (name, ssn) VALUES ('Alice', '123-45-6789')");
  db.exec("INSERT INTO users (name, ssn) VALUES ('Bob', '987-65-4321')");

  // Checkpoint to flush data to disk
  db.exec("PRAGMA wal_checkpoint(truncate)");

  // Query the data
  console.log("3. Querying data...");
  const rows = db.prepare("SELECT * FROM users").all();
  console.log("   Users:", rows);

  db.close();

  // Verify the data is encrypted on disk
  console.log("\n4. Verifying encryption...");
  const rawContent = readFileSync(DB_PATH);
  const containsPlaintext =
    rawContent.includes(Buffer.from("Alice")) ||
    rawContent.includes(Buffer.from("123-45-6789"));

  if (containsPlaintext) {
    console.log("   WARNING: Data appears to be unencrypted!");
  } else {
    console.log("   Data is encrypted on disk (plaintext not found)");
  }

  // Reopen with the same key
  console.log("\n5. Reopening database with correct key...");
  const db2 = new Database(DB_PATH, {
    encryption: {
      cipher: "aegis256",
      hexkey: ENCRYPTION_KEY,
    },
  });
  const users = db2.prepare("SELECT name FROM users").all();
  console.log("   Successfully read users:", users.map((u) => u.name));
  db2.close();

  // Demonstrate that wrong key fails
  console.log("\n6. Attempting to open with wrong key (should fail)...");
  try {
    const db3 = new Database(DB_PATH, {
      encryption: {
        cipher: "aegis256",
        hexkey: "aaaaaaa4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327",
      },
    });
    db3.prepare("SELECT * FROM users").all();
    console.log("   ERROR: Should have failed with wrong key!");
  } catch (e) {
    console.log("   Correctly failed:", e.message);
  }

  // Cleanup
  unlinkSync(DB_PATH);
  console.log("\n=== Example completed successfully ===");
}

main();
