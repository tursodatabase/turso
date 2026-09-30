/**
 * Concurrent writes with MVCC
 *
 * BEGIN CONCURRENT lets multiple connections write at the same time without
 * holding an exclusive lock.  Conflicts are detected at commit time: if two
 * transactions touched the same rows, the later one receives a conflict
 * error and must roll back and retry.
 */

import { connect } from "@tursodatabase/database";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const DB_PATH = join(mkdtempSync(join(tmpdir(), "turso-mvcc-")), "hits.db");

function isRetryable(err) {
  const msg = (err?.message ?? "").toLowerCase();
  return msg.includes("conflict") || msg.includes("busy");
}

async function writeWorker(workerId) {
  const val = Math.floor(Math.random() * 100) + 1;
  const db = await connect(DB_PATH);
  try {
    await db.exec("PRAGMA journal_mode = 'mvcc'");
    while (true) {
      await db.exec("BEGIN CONCURRENT");
      try {
        await db.exec(`INSERT INTO hits VALUES (${val})`);
        await db.exec("COMMIT");
        return val;
      } catch (err) {
        try { await db.exec("ROLLBACK"); } catch (_) {}
        if (!isRetryable(err)) throw err;
        // yield to allow other workers to make progress
        await new Promise((r) => setImmediate(r));
      }
    }
  } finally {
    await db.close();
  }
}

// Setup
const setup = await connect(DB_PATH);
await setup.exec("PRAGMA journal_mode = 'mvcc'");
await setup.exec("CREATE TABLE hits (val INTEGER)");
await setup.close();

// Run 16 concurrent writers
const workers = Array.from({ length: 16 }, (_, i) => writeWorker(i));
const results = await Promise.all(workers);
for (const val of results) {
  console.log(`inserted val=${val}`);
}

// Verify
const final = await connect(DB_PATH);
await final.exec("PRAGMA journal_mode = 'mvcc'");
const row = await final.prepare("SELECT COUNT(*) FROM hits").get();
console.log(`total rows: ${Object.values(row)[0]}`);
await final.close();

// Cleanup
rmSync(DB_PATH, { force: true });
rmSync(DB_PATH + "-wal", { force: true });
rmSync(DB_PATH + "-log", { force: true });
