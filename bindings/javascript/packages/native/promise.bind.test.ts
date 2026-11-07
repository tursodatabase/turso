import { expect, test } from 'vitest'
import { connect } from './promise.js'

test('Statement.bind binds a positional value in the promise API', async () => {
    const db = await connect(':memory:');
    try {
        await db.exec('CREATE TABLE items (key TEXT PRIMARY KEY, value TEXT NOT NULL)');
        await db.exec("INSERT INTO items VALUES ('first', 'first value'), ('second', 'second value')");
        const statement = await db.prepare('SELECT value FROM items WHERE key = ?');

        expect(statement.bind('second')).toBe(statement);
        expect(await statement.get()).toEqual({ value: 'second value' });
        statement.close();
    } finally {
        await db.close();
    }
});
