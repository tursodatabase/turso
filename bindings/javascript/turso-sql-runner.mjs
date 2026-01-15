#!/usr/bin/env node
/**
 * SQL runner script for turso-test-runner JavaScript backend.
 * Reads SQL from stdin, executes via @tursodatabase/database, outputs pipe-separated results.
 *
 * Usage: node turso-sql-runner.mjs <database_path> [--readonly]
 *
 * This script expects to be run from the bindings/javascript directory where
 * the @tursodatabase/database package is available.
 *
 * Known limitations:
 * - JavaScript's number type doesn't distinguish between 1 and 1.0, so float
 *   formatting may differ from the Rust backend for whole-number floats.
 * - Very large integers (exceeding i64) may have precision loss as JavaScript
 *   numbers are IEEE 754 doubles with 53 bits of mantissa precision.
 */

import { connect } from '@tursodatabase/database';

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
        return value.toString();
    }
    if (value instanceof Uint8Array || Buffer.isBuffer(value)) {
        // Output blob as raw bytes (matches SQLite/Rust backend behavior)
        // This will display as text if the bytes are printable ASCII
        return Buffer.from(value).toString('binary');
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
        db = await connect(dbPath, { readonly });
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
        let allResults = [];

        for (const stmt of statements) {
            const trimmed = stmt.trim();
            if (!trimmed) continue;

            // Check if this is a query that returns rows (includes RETURNING clauses)
            const isQuery = /^\s*(SELECT|PRAGMA|EXPLAIN)/i.test(trimmed) ||
                           /\bRETURNING\b/i.test(trimmed);

            if (isQuery) {
                const prepared = db.prepare(trimmed);
                prepared.raw(true);
                const rows = await prepared.all();
                allResults.push(...rows);
                await prepared.close();
            } else {
                // Non-query statement (INSERT, UPDATE, DELETE, CREATE, etc.)
                await db.exec(trimmed);
            }
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

/**
 * Split SQL text into individual statements.
 *
 * NOTE: This manual splitting is necessary because the JS bindings don't expose
 * an ergonomic way to execute multiple statements while capturing results from
 * the last one. The bindings provide:
 * - db.exec(sql) - executes multiple statements but doesn't return row data
 * - db.prepare(sql) - returns rows but only handles a single statement
 *
 * The native BatchExecutor uses Turso's parser internally (conn.consume_stmt)
 * but doesn't expose a row() method. Adding that would allow us to remove this
 * manual splitting. See: bindings/javascript/src/lib.rs BatchExecutor impl.
 */
function splitStatements(sql) {
    const statements = [];
    let current = '';
    let inString = false;
    let stringChar = '';

    for (let i = 0; i < sql.length; i++) {
        const char = sql[i];
        const nextChar = sql[i + 1];

        if (inString) {
            current += char;
            // Check for escape sequences
            if (char === stringChar) {
                if (nextChar === stringChar) {
                    // Escaped quote
                    current += nextChar;
                    i++;
                } else {
                    inString = false;
                }
            }
        } else if (char === "'" || char === '"') {
            inString = true;
            stringChar = char;
            current += char;
        } else if (char === '-' && nextChar === '-') {
            // Skip single-line comment (don't add to current statement)
            i++; // skip second '-'
            while (i < sql.length && sql[i] !== '\n') {
                i++;
            }
            // i now points to '\n' or end; the for loop will increment past it
        } else if (char === '/' && nextChar === '*') {
            // Skip multi-line comment
            i++; // skip '*'
            while (i < sql.length - 1 && !(sql[i] === '*' && sql[i + 1] === '/')) {
                i++;
            }
            i++; // skip past closing '/'
        } else if (char === ';') {
            if (current.trim()) {
                statements.push(current.trim());
            }
            current = '';
        } else {
            current += char;
        }
    }

    // Add any remaining statement
    if (current.trim()) {
        statements.push(current.trim());
    }

    return statements;
}

main().catch(err => {
    console.error(`Error: ${err.message}`);
    process.exit(1);
});
