#!/usr/bin/env node
/**
 * Turso Database Sync example with Turso Cloud (with optional remote encryption)
 *
 * Environment variables:
 *   TURSO_REMOTE_URL          - Remote database URL (default: http://localhost:8080)
 *   TURSO_AUTH_TOKEN          - Auth token (optional)
 *   TURSO_REMOTE_ENCRYPTION_KEY - Base64-encoded encryption key (optional)
 */

import { connect } from '@tursodatabase/sync';

async function main() {
    const remoteUrl = process.env.TURSO_REMOTE_URL || 'http://localhost:8080';
    const authToken = process.env.TURSO_AUTH_TOKEN;
    const encryptionKey = process.env.TURSO_REMOTE_ENCRYPTION_KEY;

    console.log(`Remote URL: ${remoteUrl}`);
    console.log(`Auth Token: ${authToken != null}`);
    console.log(`Encryption: ${encryptionKey != null}`);

    const opts = {
        path: ':memory:',
        url: remoteUrl,
        authToken: authToken,
    };

    // use remote encryption if key is provided
    if (encryptionKey) {
        opts.remoteEncryption = {
            key: encryptionKey
        };
    }

    // Connect to the sync database
    const db = await connect(opts);

    // Create table
    await db.exec('CREATE TABLE IF NOT EXISTS t (x TEXT)');

    // Get current row count and insert next numbered row
    const countResult = await db.prepare('SELECT COUNT(*) as cnt FROM t').all();
    const count = countResult[0]?.cnt ?? 0;
    const nextNum = count + 1;

    await db.exec(`INSERT INTO t VALUES ('hello sync #${nextNum}')`);
    await db.push();

    // Query test table contents
    console.log('\nTest table contents:');
    const rows = await db.prepare('SELECT * FROM t').all();
    for (const row of rows) {
        console.log(`  Row: ${row.x}`);
    }

    // Query sqlite_master for all tables
    console.log('\nDatabase tables:');
    const tables = await db.prepare("SELECT name, type FROM sqlite_master WHERE type='table'").all();
    for (const row of tables) {
        console.log(`  - ${row.type}: ${row.name}`);
    }

    // Show database stats
    const stats = await db.stats();
    console.log('\nDatabase stats:');
    console.log(`  Network received: ${stats.networkReceivedBytes} bytes`);
    console.log(`  Network sent: ${stats.networkSentBytes} bytes`);
    console.log(`  Main WAL size: ${stats.mainWalSize} bytes`);

    await db.close();
    console.log('\nDone!');
}

main().catch(err => {
    console.error('Error:', err);
    process.exit(1);
});
