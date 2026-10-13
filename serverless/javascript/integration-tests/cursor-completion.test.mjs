import test from 'ava';
import { connect, Session, DatabaseError } from '../dist/index.js';
import { cursorSqlFixture } from './cursor-sql-fixture.mjs';

for (const rotate of [false, true]) {
  const label = rotate ? 'rotating' : 'stable';
  test.serial(`${label}: valid SELECT, DML and RETURNING retain values and ownership`, async t => {
    const { fixture, connection } = await setup(t, rotate);
    t.deepEqual(await connection.all('SELECT 1 AS a WHERE 0'), []);
    t.is((await connection.run('INSERT INTO values_test VALUES(1, ?)', ['Unicode 🦆'])).changes, 1);
    t.deepEqual(await connection.all('SELECT id,value FROM values_test'), [{ id: 1, value: 'Unicode 🦆' }]);
    t.deepEqual(await connection.all('UPDATE values_test SET value=? WHERE id=1 RETURNING id,value', ['updated']), [{ id: 1, value: 'updated' }]);
    t.deepEqual(await connection.all('DELETE FROM values_test WHERE id=1 RETURNING id'), [{ id: 1 }]);
    t.is((await connection.run('UPDATE values_test SET value=?', ['unused'])).changes, 0);
    t.is((await connection.run('DELETE FROM values_test')).changes, 0);
    t.deepEqual(await connection.all('INSERT INTO values_test VALUES(2,?) RETURNING id,value', ['returned']), [{ id: 2, value: 'returned' }]);
    const statement = await connection.prepare('SELECT 3 AS same,4 AS same,NULL AS absent,x\'0102\' AS bytes,1.5 AS fraction');
    const rows = await statement.raw(true).all();
    t.deepEqual(rows, [[3, 4, null, Buffer.from([1, 2]), 1.5]]);
    connection.defaultSafeIntegers(true);
    t.is((await connection.get('SELECT ? AS n', [9007199254740993n])).n, 9007199254740993n);
    t.false(connection.inTransaction);
    await connection.exec('BEGIN IMMEDIATE');
    await connection.run('INSERT INTO values_test VALUES(3,?)', ['owned']);
    t.true(connection.inTransaction);
    await connection.exec('ROLLBACK');
    t.deepEqual(fixture.read('SELECT id FROM values_test'), [{ id: 2 }]);
  });

  for (const sql of [
    'INSERT INTO constrained VALUES(1,99)',
    'INSERT INTO constrained VALUES(2,1)',
    'INSERT INTO child_test VALUES(1,99)',
    'SELECT missing_column FROM values_test',
  ]) {
    test.serial(`${label}: genuine SQL rejection is preserved: ${sql.split(' ')[0]} ${sql.includes('child_test') ? 'FK' : sql.includes('missing') ? 'prepare' : sql.includes('(2') ? 'UNIQUE' : 'CHECK'}`, async t => {
      const { fixture, connection } = await setup(t, rotate);
      await connection.exec('BEGIN IMMEDIATE');
      let actualError;
      fixture.mutate(({ entries, response }) => {
        actualError = entries.find(entry => entry.type === 'step_error' && entry.step === 0).error;
        deliver(response, entries);
      });
      const error = await t.throwsAsync(() => connection.run(sql), { instanceOf: DatabaseError });
      t.is(error.message, actualError.message);
      t.is(error.code, actualError.code);
      await connection.exec('ROLLBACK');
      t.is(fixture.read('SELECT count(*) AS n FROM constrained')[0].n, 1);
    });
  }

  test.serial(`${label}: real error after begin and a row preserves its SQL error`, async t => {
    const { fixture, connection } = await setup(t, rotate);
    await connection.exec('CREATE TABLE error_values(id INTEGER PRIMARY KEY); INSERT INTO error_values VALUES(1),(2)');
    let actualError;
    fixture.mutate(({ entries, response }) => {
      t.true(entries.some(entry => entry.type === 'step_begin' && entry.step === 0));
      t.true(entries.some(entry => entry.type === 'row'));
      actualError = entries.find(entry => entry.type === 'step_error' && entry.step === 0).error;
      deliver(response, entries);
    });
    const error = await t.throwsAsync(() => connection.all("SELECT CASE WHEN id=2 THEN json('invalid') ELSE id END AS value FROM error_values"));
    t.is(error.message, actualError.message);
  });

  for (const kind of [
    'header_only', 'header_terminator', 'missing_begin', 'missing_end', 'missing_terminator',
    'duplicate_begin', 'duplicate_end', 'duplicate_terminator', 'row_before_begin', 'row_after_end',
    'wrong_step', 'wrong_row_width', 'probe_before_query', 'probe_without_end', 'after_terminator',
    'open_query_eof',
  ]) {
    test.serial(`${label}: incomplete or invalid ${kind} rejects before caller commit`, async t => {
      const { fixture, connection } = await setup(t, rotate);
      const owned = !kind.startsWith('probe_');
      if (owned) await connection.exec('BEGIN IMMEDIATE');
      const sql = kind.startsWith('header_') ? 'INSERT INTO constrained VALUES(1,99)' : 'SELECT 1 AS n';
      fixture.mutate(({ entries, response }) => {
        if (kind.startsWith('header_')) t.true(entries.some(entry => entry.type === 'step_error'));
        deliver(response, changed(entries, kind));
      });
      await t.throwsAsync(() => connection.all(sql), { instanceOf: DatabaseError });
      if (owned) {
        await connection.exec('ROLLBACK');
        const original = fixture.records.find(record => record.sql === 'BEGIN IMMEDIATE');
        const rollback = fixture.records.find(record => record.sql === 'ROLLBACK');
        t.is(rollback.stream, original.stream);
        const writer = connect({ url: fixture.url });
        t.teardown(() => writer.close());
        await writer.exec('BEGIN IMMEDIATE');
        await writer.run('INSERT INTO values_test VALUES(7,?)', ['next writer']);
        await writer.exec('COMMIT');
        t.deepEqual(fixture.read('SELECT id FROM values_test'), [{ id: 7 }]);
      }
    });
  }

  test.serial(`${label}: unknown extensions are ignored without replacing completion`, async t => {
    const { fixture, connection } = await setup(t, rotate);
    fixture.mutate(({ entries, response }) => deliver(response, entries.flatMap(entry => [{ type: 'future_extension', data: true }, entry])));
    t.deepEqual(await connection.all('SELECT 2 AS n'), [{ n: 2 }]);
  });

  test.serial(`${label}: probe errors retain successful query and refresh state`, async t => {
    const { fixture, connection } = await setup(t, rotate);
    fixture.failProbe(true);
    t.deepEqual(await connection.all('SELECT 2 AS n'), [{ n: 2 }]);
    t.false(connection.inTransaction);
    await connection.exec('BEGIN IMMEDIATE');
    t.deepEqual(await connection.all('SELECT 2 AS n'), [{ n: 2 }]);
    t.true(connection.inTransaction);
    await connection.exec('ROLLBACK');
  });

  test.serial(`${label}: probe error after its begin uses fallback without changing the user result`, async t => {
    const { fixture, connection } = await setup(t, rotate);
    fixture.failProbe('after_begin');
    t.deepEqual(await connection.all('SELECT 2 AS n'), [{ n: 2 }]);
    t.false(connection.inTransaction);
  });

  test.serial(`${label}: fatal cursor error retains the genuine error details`, async t => {
    const { fixture, connection } = await setup(t, rotate);
    let actualError;
    fixture.mutate(({ entries, response }) => {
      actualError = entries.find(entry => entry.type === 'step_error').error;
      deliver(response, [{ type: 'error', error: actualError }]);
    });
    const error = await t.throwsAsync(() => connection.run('SELECT missing_column FROM values_test'));
    t.is(error.message, actualError.message);
    t.is(error.code, actualError.code);
  });

  test.serial(`${label}: unknown future SQL error codes remain valid errors`, async t => {
    const { fixture, connection } = await setup(t, rotate);
    fixture.mutate(({ entries, response }) => {
      const error = entries.find(entry => entry.type === 'step_error');
      error.error.code = 'FUTURE_SQL_ERROR_CODE';
      deliver(response, entries);
    });
    const error = await t.throwsAsync(() => connection.run('SELECT missing_column FROM values_test'));
    t.is(error.code, 'FUTURE_SQL_ERROR_CODE');
  });

  for (const type of ['error', 'step_error']) {
    for (const [variant, payload] of [
      ['absent', undefined], ['empty', {}], ['missing_code', { message: 'test error' }],
      ['invalid_message', { message: 7, code: 'UNKNOWN_VALID_CODE' }], ['invalid_code', { message: 'test error', code: null }],
    ]) {
      test.serial(`${label}: raw ${type} with ${variant} error payload rejects`, async t => {
        const { fixture } = await setup(t, rotate);
        const session = new Session({ url: fixture.url });
        t.teardown(() => session.close());
        fixture.mutate(({ entries, response }) => {
          t.true(entries.some(entry => entry.type === 'step_error'));
          deliver(response, [{ type, ...(type === 'step_error' ? { step: 0 } : {}), error: payload },
            ...(type === 'step_error' ? [entries.at(-1)] : [])]);
        });
        const { entries } = await session.executeRaw('SELECT missing_column FROM values_test');
        const error = await t.throwsAsync(async () => { for await (const entry of entries) void entry; });
        t.is(error.code, 'PROTOCOL_ERROR');
      });
    }
  }

  test.serial(`${label}: malformed fallback cannot mask the genuine SQL error`, async t => {
    const { fixture, connection } = await setup(t, rotate);
    await connection.exec('BEGIN IMMEDIATE');
    fixture.failRefresh('malformed');
    let actualError;
    fixture.mutate(({ entries, response }) => {
      actualError = entries.find(entry => entry.type === 'step_error').error;
      deliver(response, entries);
    });
    const error = await t.throwsAsync(() => connection.run('INSERT INTO constrained VALUES(1,99)'));
    t.is(error.message, actualError.message);
    t.is(error.code, actualError.code);
  });

  test.serial(`${label}: malformed refresh cannot mask completion failure`, async t => {
    const { fixture, connection } = await setup(t, rotate);
    await connection.exec('BEGIN IMMEDIATE');
    fixture.failRefresh('malformed');
    fixture.mutate(({ response }) => response.end());
    const error = await t.throwsAsync(() => connection.run('INSERT INTO values_test VALUES(9,?)', ['uncertain']));
    t.is(error.code, 'PROTOCOL_ERROR');
    t.deepEqual(fixture.read('SELECT id FROM values_test'), []);
  });

  test.serial(`${label}: interrupted raw consumer refreshes without claiming completion`, async t => {
    const { fixture } = await setup(t, rotate);
    const session = new Session({ url: fixture.url });
    t.teardown(() => session.close());
    const { entries } = await session.executeRaw('SELECT 1 AS n');
    for await (const entry of entries) {
      if (entry.type === 'row') break;
    }
    t.false(session.inTransaction);
    t.deepEqual((await session.execute('SELECT 2 AS n')).rows.map(row => Array.from(row)), [[2]]);
  });

  test.serial(`${label}: genuine SQL error is not masked by failed fallback`, async t => {
    const { fixture, connection } = await setup(t, rotate);
    await connection.exec('BEGIN IMMEDIATE');
    fixture.failRefresh(true);
    let actualError;
    fixture.mutate(({ entries, response }) => {
      actualError = entries.find(entry => entry.type === 'step_error').error;
      deliver(response, entries);
    });
    const error = await t.throwsAsync(() => connection.run('INSERT INTO constrained VALUES(1,99)'));
    t.is(error.message, actualError.message);
    t.is(error.code, actualError.code);
  });

  test.serial(`${label}: timeout after real header rejects and retains original rollback`, async t => {
    const { fixture, connection } = await setup(t, rotate);
    await connection.exec('BEGIN IMMEDIATE');
    const delivered = deferred();
    const release = deferred();
    fixture.mutate(async ({ entries, response }) => {
      t.true(entries.some(entry => entry.type === 'step_end'));
      delivered.resolve();
      await release.promise;
      deliver(response, entries);
    });
    const pending = connection.run('INSERT INTO values_test VALUES(9,?)', ['partial'], { queryTimeout: 150 });
    await delivered.promise;
    const error = await t.throwsAsync(() => pending);
    t.is(error.code, 'TIMEOUT');
    await connection.exec('ROLLBACK');
    release.resolve();
    t.is(fixture.records.find(record => record.sql === 'ROLLBACK').stream, fixture.records.find(record => record.sql === 'BEGIN IMMEDIATE').stream);
    t.deepEqual(fixture.read('SELECT id FROM values_test'), []);
  });

  test.serial(`${label}: missing framing with failed refresh remains failure, not rollback proof`, async t => {
    const { fixture, connection } = await setup(t, rotate);
    await connection.exec('BEGIN IMMEDIATE');
    fixture.failRefresh(true);
    fixture.mutate(({ response }) => response.end());
    await t.throwsAsync(() => connection.run('INSERT INTO values_test VALUES(9,?)', ['uncertain']));
    t.deepEqual(fixture.read('SELECT id FROM values_test'), []);
    t.false(fixture.records.some(record => record.sql === 'ROLLBACK'));
  });

  test.serial(`${label}: raw entry consumers reject missing completion`, async t => {
    const { fixture } = await setup(t, rotate);
    const session = new Session({ url: fixture.url });
    t.teardown(() => session.close());
    fixture.mutate(({ response }) => response.end());
    const { entries } = await session.executeRaw('SELECT 1');
    await t.throwsAsync(async () => { for await (const entry of entries) void entry; });
  });

  test.serial(`${label}: genuine COMMIT with lost response acknowledgement still rejects`, async t => {
    const { fixture, connection } = await setup(t, rotate);
    await connection.exec('BEGIN IMMEDIATE');
    await connection.run('INSERT INTO values_test VALUES(9,?)', ['committed']);
    const executed = deferred();
    const release = deferred();
    fixture.mutatePipeline(async ({ commands, body, response }) => {
      t.true(commands.some(command => command.sql === 'COMMIT'));
      executed.resolve();
      await release.promise;
      response.writeHead(200).end(JSON.stringify(body));
    });
    const pending = connection.exec('COMMIT', { queryTimeout: 150 });
    await executed.promise;
    await t.throwsAsync(() => pending);
    release.resolve();
    t.deepEqual(fixture.read('SELECT id FROM values_test'), [{ id: 9 }]);
    t.is(fixture.records.filter(record => record.sql === 'COMMIT').length, 1);
  });
}

async function setup(t, rotate) {
  const fixture = await cursorSqlFixture({ rotate });
  const connection = connect({ url: fixture.url, defaultQueryTimeout: 1000 });
  t.teardown(async () => { await connection.close(); await fixture.close(); });
  await connection.exec('CREATE TABLE values_test(id INTEGER PRIMARY KEY,value TEXT); CREATE TABLE constrained(id INTEGER CHECK(id != 1),value INTEGER UNIQUE); INSERT INTO constrained VALUES(3,1); CREATE TABLE parent_test(id INTEGER PRIMARY KEY); CREATE TABLE child_test(id INTEGER,parent_id INTEGER REFERENCES parent_test(id))');
  return { fixture, connection };
}

function deliver(response, entries) {
  response.end(entries.map(entry => JSON.stringify(entry)).join('\n') + '\n');
}

function changed(entries, kind) {
  const begin = entries.find(entry => entry.type === 'step_begin' && entry.step === 0);
  const end = entries.find(entry => entry.type === 'step_end');
  const row = entries.find(entry => entry.type === 'row');
  const terminal = entries.at(-1);
  if (kind === 'header_only') return [];
  if (kind === 'header_terminator') return [terminal];
  if (kind === 'missing_begin') return entries.filter(entry => entry !== begin);
  if (kind === 'missing_end') return entries.filter(entry => entry !== end);
  if (kind === 'missing_terminator') return entries.slice(0, -1);
  if (kind === 'duplicate_begin') return entries.flatMap(entry => entry === begin ? [entry, entry] : [entry]);
  if (kind === 'duplicate_end') return entries.flatMap(entry => entry === end ? [entry, entry] : [entry]);
  if (kind === 'duplicate_terminator') return [...entries, terminal];
  if (kind === 'row_before_begin') return [row, ...entries.filter(entry => entry !== row)];
  if (kind === 'row_after_end') return entries.filter(entry => entry !== row).flatMap(entry => entry === end ? [entry, row] : [entry]);
  if (kind === 'wrong_step') return entries.map(entry => entry === begin ? { ...entry, step: 2 } : entry);
  if (kind === 'wrong_row_width') return entries.map(entry => entry === row ? { ...entry, row: [] } : entry);
  if (kind === 'open_query_eof') return [begin, row];
  if (kind === 'after_terminator') return [...entries, begin];
  if (kind === 'probe_without_end') {
    const probe = entries.findIndex(entry => entry.type === 'step_begin' && entry.step === 1);
    return entries.filter((entry, index) => !(index > probe && entry.type === 'step_end'));
  }
  if (kind === 'probe_before_query') {
    const probe = entries.findIndex(entry => entry.type === 'step_begin' && entry.step === 1);
    return [...entries.slice(probe, -1), ...entries.slice(0, probe), terminal];
  }
  throw new Error('Unknown test mutation');
}

function deferred() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}
