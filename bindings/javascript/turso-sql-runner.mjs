#!/usr/bin/env node
/**
 * SQL runner script for test-runner JavaScript backend.
 * Reads SQL from stdin, executes via @tursodatabase/database, outputs pipe-separated results.
 *
 * Usage: node turso-sql-runner.mjs <database_path> [--readonly]
 *
 * This script expects to be run from the bindings/javascript directory where
 * the @tursodatabase/database package is available.
 *
 * Known limitations:
 * - With safe_integers enabled, DB integers arrive as BigInt and floats as
 *   number. Whole-number floats (e.g. 30.0) get ".0" appended to match SQLite.
 * - Very large integers (exceeding i64) may have precision loss as JavaScript
 *   numbers are IEEE 754 doubles with 53 bits of mantissa precision.
 */

import { pathToFileURL } from 'node:url';
import { splitStatements } from './turso-sql-split.mjs';

async function readStdin() {
    const chunks = [];
    for await (const chunk of process.stdin) {
        chunks.push(chunk);
    }
    return Buffer.concat(chunks).toString('utf-8');
}

function formatValue(value) {
    if (value === null || value === undefined) {
        return '';
    }
    if (typeof value === 'bigint') {
        return value.toString();
    }
    if (typeof value === 'number') {
        // Handle special float values to match SQLite output
        if (value === Infinity) {
            return 'Inf';
        }
        if (value === -Infinity) {
            return '-Inf';
        }
        if (Number.isNaN(value)) {
            return '';  // SQLite returns NULL for NaN
        }
        // With safe_integers enabled, all DB integers arrive as BigInt.
        // Any `number` here came from a REAL column in the database.
        // SQLite uses %.15g format (15 significant digits, trailing zeros removed)
        // toPrecision gives significant digits, parseFloat removes trailing zeros
        let str = parseFloat(value.toPrecision(15)).toString();
        // Ensure float values always include a decimal point (e.g., 30 -> "30.0")
        // to match SQLite output which distinguishes reals from integers.
        if (!str.includes('.') && !str.includes('e') && !str.includes('E')) {
            str += '.0';
        }
        return str;
    }
    if (value instanceof Uint8Array || Buffer.isBuffer(value)) {
        // Output blob as raw bytes (matches SQLite/Rust backend behavior)
        // This will display as text if the bytes are printable ASCII
        return Buffer.from(value).toString('utf-8');
    }
    return String(value);
}

function formatRow(row) {
    // Row is an array in raw mode
    return row.map(formatValue).join('|');
}

async function main() {
    const args = process.argv.slice(2);
    if (args.length < 1) {
        console.error('Usage: turso-sql-runner.mjs <database_path> [--readonly]');
        process.exit(1);
    }

    const dbPath = args[0];
    const readonly = args.includes('--readonly');

    const sql = await readStdin();
    if (!sql.trim()) {
        process.exit(0);
    }

    let db;
    try {
        const { connect } = await import('@tursodatabase/database');
        db = await connect(dbPath, { readonly, experimental: ['triggers', 'attach'] });
        // Enable safe integers to preserve precision for large integers
        db.defaultSafeIntegers(true);
    } catch (err) {
        console.error(`Error: ${err.message}`);
        process.exit(1);
    }

    try {
        // Split into individual statements, filtering out comments and empty lines
        const statements = splitStatements(sql);

        // Accumulate results from ALL queries (matches Rust backend behavior)
        const allResults = [];

        for (const stmt of statements) {
            const trimmed = stmt.trim();
            if (!trimmed) continue;

            const prepared = db.prepare(trimmed);
            prepared.raw(true);
            const rows = await prepared.all();
            allResults.push(...rows);
            prepared.close();
        }

        // Output all accumulated results
        for (const row of allResults) {
            console.log(formatRow(row));
        }

    } catch (err) {
        // Output error in a format the test runner can detect
        console.log(`Error: ${err.message}`);
        process.exit(0); // Exit 0 so the error can be captured as output
    } finally {
        if (db) {
            await db.close();
        }
    }
}

const isMain = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;

if (isMain) {
    main().catch(err => {
        console.error(`Error: ${err.message}`);
        process.exit(1);
    });
}
