import { expect, test } from 'vitest'
import { splitStatements } from '../../turso-sql-split.mjs'

test('splitStatements keeps trigger body as one statement', () => {
    const sql = `
        CREATE TRIGGER log_insert AFTER INSERT ON users BEGIN
            INSERT INTO log VALUES('inserted');
            UPDATE stats SET count = count + 1;
        END;
    `;

    expect(splitStatements(sql)).toEqual([
        `CREATE TRIGGER log_insert AFTER INSERT ON users BEGIN
            INSERT INTO log VALUES('inserted');
            UPDATE stats SET count = count + 1;
        END;`
    ]);
})

test('splitStatements handles trigger followed by select', () => {
    const sql = `
        CREATE TRIGGER log_insert AFTER INSERT ON users BEGIN
            INSERT INTO log VALUES('inserted');
        END;
        SELECT 1;
    `;

    expect(splitStatements(sql)).toEqual([
        `CREATE TRIGGER log_insert AFTER INSERT ON users BEGIN
            INSERT INTO log VALUES('inserted');
        END;`,
        'SELECT 1;'
    ]);
})

test('splitStatements ignores END inside trigger string literal', () => {
    const sql = `
        CREATE TRIGGER log_insert AFTER INSERT ON users BEGIN
            INSERT INTO log VALUES('END');
        END;
    `;

    expect(splitStatements(sql)).toEqual([
        `CREATE TRIGGER log_insert AFTER INSERT ON users BEGIN
            INSERT INTO log VALUES('END');
        END;`
    ]);
})

test('splitStatements handles create temp trigger and explain create trigger', () => {
    const tempTrigger = `
        CREATE TEMP TRIGGER log_insert AFTER INSERT ON users BEGIN
            INSERT INTO log VALUES('inserted');
        END;
    `;

    const explainTrigger = `
        EXPLAIN CREATE TRIGGER log_insert AFTER INSERT ON users BEGIN
            INSERT INTO log VALUES('inserted');
        END;
    `;

    expect(splitStatements(tempTrigger)).toHaveLength(1);
    expect(splitStatements(explainTrigger)).toHaveLength(1);
})
