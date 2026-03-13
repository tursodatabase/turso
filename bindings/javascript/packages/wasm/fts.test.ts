import { expect, test, afterAll, beforeEach, afterEach, describe } from 'vitest'
import { connect } from './promise-default.js'
import { MainWorker } from './index-default.js'

beforeEach((ctx) => {
    console.log(`[test:start] ${ctx.task.name}`);
})

afterEach((ctx) => {
    console.log(`[test:end] ${ctx.task.name} (${ctx.task.result?.state})`);
})

afterAll(() => {
    console.log('[afterAll] terminating MainWorker');
    MainWorker?.terminate();
    console.log('[afterAll] MainWorker terminated');
})

// FTS tests - these require the wasm-fts feature to be enabled during build
// Skip these tests if FTS is not available

describe('FTS (Full-Text Search)', () => {
    test('fts-basic-create-and-query', async () => {
        const db = await connect(":memory:");

        // Create table and FTS index
        await db.exec("CREATE TABLE documents (id INTEGER PRIMARY KEY, content TEXT)");
        await db.exec("CREATE INDEX docs_fts ON documents USING fts (content)");

        // Insert test documents
        await db.exec("INSERT INTO documents VALUES (1, 'The quick brown fox jumps over the lazy dog')");
        await db.exec("INSERT INTO documents VALUES (2, 'A fast red fox runs through the forest')");
        await db.exec("INSERT INTO documents VALUES (3, 'The lazy cat sleeps all day')");

        // Query using fts_match
        const results = await db.prepare("SELECT id, content FROM documents WHERE fts_match(content, 'fox') ORDER BY id").all();

        expect(results).toEqual([
            { id: 1, content: 'The quick brown fox jumps over the lazy dog' },
            { id: 2, content: 'A fast red fox runs through the forest' }
        ]);

        await db.close();
    })

    test('fts-no-match', async () => {
        const db = await connect(":memory:");

        await db.exec("CREATE TABLE articles (id INTEGER PRIMARY KEY, title TEXT)");
        await db.exec("CREATE INDEX articles_fts ON articles USING fts (title)");

        await db.exec("INSERT INTO articles VALUES (1, 'Introduction to Rust programming')");
        await db.exec("INSERT INTO articles VALUES (2, 'Advanced TypeScript patterns')");

        // Search for a term that doesn't exist
        const results = await db.prepare("SELECT id FROM articles WHERE fts_match(title, 'python')").all();

        expect(results).toEqual([]);

        await db.close();
    })

    test('fts-delete-with-match', async () => {
        const db = await connect(":memory:");

        await db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, x TEXT)");
        await db.exec("CREATE INDEX t_idx ON t USING fts (x)");

        await db.exec("INSERT INTO t VALUES (1, 'hello world')");
        await db.exec("INSERT INTO t VALUES (2, 'goodbye world')");
        await db.exec("INSERT INTO t VALUES (3, 'hello there')");

        // Delete rows matching 'hello'
        await db.exec("DELETE FROM t WHERE fts_match(x, 'hello')");

        const results = await db.prepare("SELECT id, x FROM t ORDER BY id").all();

        expect(results).toEqual([
            { id: 2, x: 'goodbye world' }
        ]);

        await db.close();
    })

    test('fts-update-with-match', async () => {
        const db = await connect(":memory:");

        await db.exec("CREATE TABLE notes (id INTEGER PRIMARY KEY, content TEXT)");
        await db.exec("CREATE INDEX notes_fts ON notes USING fts (content)");

        await db.exec("INSERT INTO notes VALUES (1, 'urgent task one')");
        await db.exec("INSERT INTO notes VALUES (2, 'normal task two')");
        await db.exec("INSERT INTO notes VALUES (3, 'urgent task three')");

        // Update rows matching 'urgent'
        await db.exec("UPDATE notes SET content = 'completed' WHERE fts_match(content, 'urgent')");

        const results = await db.prepare("SELECT id, content FROM notes ORDER BY id").all();

        expect(results).toEqual([
            { id: 1, content: 'completed' },
            { id: 2, content: 'normal task two' },
            { id: 3, content: 'completed' }
        ]);

        await db.close();
    })

    test('fts-insert-then-search', async () => {
        const db = await connect(":memory:");

        await db.exec("CREATE TABLE blog (id INTEGER PRIMARY KEY, body TEXT)");
        await db.exec("CREATE INDEX blog_fts ON blog USING fts (body)");

        // Insert multiple rows
        const insert = db.prepare("INSERT INTO blog (body) VALUES (?)");
        await insert.run("Learning Rust from scratch");
        await insert.run("Database internals explained");
        await insert.run("Building a Rust compiler");
        await insert.run("Web development with JavaScript");

        // Search for 'Rust'
        const rustResults = await db.prepare("SELECT id, body FROM blog WHERE fts_match(body, 'Rust') ORDER BY id").all();
        expect(rustResults.length).toBe(2);
        expect(rustResults[0].body).toContain('Rust');
        expect(rustResults[1].body).toContain('Rust');

        await db.close();
    })

    test('fts-multiple-columns', async () => {
        const db = await connect(":memory:");

        // Create table with multiple text columns
        await db.exec("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, description TEXT)");
        await db.exec("CREATE INDEX products_fts ON products USING fts (name, description)");

        await db.exec("INSERT INTO products VALUES (1, 'Laptop Computer', 'Powerful laptop for work')");
        await db.exec("INSERT INTO products VALUES (2, 'Desktop Computer', 'High performance desktop')");
        await db.exec("INSERT INTO products VALUES (3, 'Wireless Mouse', 'Ergonomic mouse for comfort')");

        // Search across both columns - 'laptop' appears in name for id=1
        const results = await db.prepare("SELECT id, name FROM products WHERE fts_match(name, 'laptop')").all();

        expect(results).toEqual([
            { id: 1, name: 'Laptop Computer' }
        ]);

        await db.close();
    })

    test('fts-with-transaction', async () => {
        const db = await connect(":memory:");

        await db.exec("CREATE TABLE logs (id INTEGER PRIMARY KEY, message TEXT)");
        await db.exec("CREATE INDEX logs_fts ON logs USING fts (message)");

        // Start transaction
        await db.exec("BEGIN");
        await db.exec("INSERT INTO logs VALUES (1, 'error occurred in module A')");
        await db.exec("INSERT INTO logs VALUES (2, 'warning from module B')");
        await db.exec("INSERT INTO logs VALUES (3, 'error in database connection')");
        await db.exec("COMMIT");

        // Search for errors
        const errors = await db.prepare("SELECT id FROM logs WHERE fts_match(message, 'error') ORDER BY id").all();

        expect(errors).toEqual([{ id: 1 }, { id: 3 }]);

        await db.close();
    })

    test('fts-rollback', async () => {
        const db = await connect(":memory:");

        await db.exec("CREATE TABLE data (id INTEGER PRIMARY KEY, text TEXT)");
        await db.exec("CREATE INDEX data_fts ON data USING fts (text)");

        await db.exec("INSERT INTO data VALUES (1, 'initial data')");

        // Start transaction and insert, then rollback
        await db.exec("BEGIN");
        await db.exec("INSERT INTO data VALUES (2, 'temporary data')");
        await db.exec("ROLLBACK");

        // Only initial data should exist
        const all = await db.prepare("SELECT id FROM data").all();
        expect(all).toEqual([{ id: 1 }]);

        // FTS should also not find the rolled back data
        const search = await db.prepare("SELECT id FROM data WHERE fts_match(text, 'temporary')").all();
        expect(search).toEqual([]);

        await db.close();
    })
})
