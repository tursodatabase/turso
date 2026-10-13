import { DatabaseSync } from 'node:sqlite';
import { createServer } from 'node:http';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

export async function cursorSqlFixture({ rotate = false } = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'turso-cursor-'));
  const path = join(directory, 'test.sqlite');
  const sessions = new Map();
  const streamIds = new Map();
  const records = [];
  let next = 0;
  let mutation;
  let pipelineMutation;
  let probeFailure = false;
  let refreshFailure = false;
  const server = createServer(async (request, response) => {
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const payload = JSON.parse(Buffer.concat(chunks).toString());
    let token = payload.baton;
    let database = sessions.get(token);
    if (token && !database) {
      response.writeHead(404).end();
      return;
    }
    if (!database) {
      database = new DatabaseSync(path, { enableDoubleQuotedStringLiterals: true });
      database.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=0');
      token = `stream-${++next}`;
      sessions.set(token, database);
      streamIds.set(token, token);
    }
    const id = streamIds.get(token);
    let responseToken = token;
    const advance = () => {
      if (rotate) {
        responseToken = `stream-${++next}`;
        sessions.delete(token);
        sessions.set(responseToken, database);
        streamIds.set(responseToken, id);
      }
    };
    if (request.url === '/v3/cursor') {
      advance();
      response.writeHead(200, { 'Content-Type': 'application/json' });
      response.write(JSON.stringify({ baton: responseToken, base_url: null }) + '\n');
      const entries = [];
      const success = [];
      for (const [step, item] of payload.batch.steps.entries()) {
        if (!allowed(database, item.condition, success)) continue;
        try {
          if (step === 1 && probeFailure) {
            execute(database, id, {
              sql: probeFailure === 'after_begin' ? "SELECT json('invalid')" : 'SELECT missing_probe_column', args: [],
            }, records, {
              begin: cols => entries.push({ type: 'step_begin', step, cols }),
              row: row => entries.push({ type: 'row', row }),
            });
          }
          const result = execute(database, id, item.stmt, records, {
            begin: cols => entries.push({ type: 'step_begin', step, cols }),
            row: row => entries.push({ type: 'row', row }),
          });
          entries.push({ type: 'step_end', affected_row_count: result.affected_row_count, last_insert_rowid: result.last_insert_rowid });
          success[step] = true;
        } catch (error) {
          entries.push({ type: 'step_error', step, error: sqlError(error) });
          success[step] = false;
        }
      }
      entries.push({ type: 'replication_index', replication_index: null });
      const change = mutation;
      mutation = undefined;
      if (change) await change({ entries, response, database });
      else response.end(entries.map(entry => JSON.stringify(entry)).join('\n') + '\n');
      return;
    }
    const results = [];
    let closed = false;
    for (const command of payload.requests) {
      try {
        let result;
        if (command.type === 'sequence') {
          records.push({ stream: id, sql: command.sql });
          database.exec(command.sql);
          result = { type: 'sequence' };
        } else if (command.type === 'get_autocommit') {
          if (refreshFailure) {
            advance();
            if (refreshFailure === 'malformed') response.writeHead(200, { 'Content-Type': 'application/json' }).end('null');
            else response.writeHead(503).end();
            return;
          }
          result = { type: 'get_autocommit', is_autocommit: !database.isTransaction };
        } else if (command.type === 'describe') {
          const statement = database.prepare(command.sql);
          result = { type: 'describe', result: { cols: columns(statement), params: [], is_explain: false, is_readonly: true } };
        } else if (command.type === 'close') {
          database.close();
          sessions.delete(token);
          closed = true;
          result = { type: 'close' };
        } else if (command.type === 'execute') {
          result = { type: 'execute', result: execute(database, id, command.stmt, records) };
        } else if (command.type === 'batch') {
          const step_results = [];
          const step_errors = [];
          const success = [];
          for (const [step, item] of command.batch.steps.entries()) {
            step_results[step] = null;
            step_errors[step] = null;
            if (!allowed(database, item.condition, success)) continue;
            try {
              step_results[step] = execute(database, id, item.stmt, records);
              success[step] = true;
            } catch (error) {
              step_errors[step] = sqlError(error);
              success[step] = false;
            }
          }
          result = { type: 'batch', result: { step_results, step_errors } };
        } else throw new Error('Unsupported fixture command');
        results.push({ type: 'ok', response: result });
      } catch (error) {
        results.push({ type: 'error', error: sqlError(error) });
      }
    }
    if (!closed) advance();
    const body = { baton: closed ? null : responseToken, base_url: null, results };
    const change = pipelineMutation;
    pipelineMutation = undefined;
    if (change) await change({ body, response, commands: payload.requests });
    else response.writeHead(200, { 'Content-Type': 'application/json' }).end(JSON.stringify(body));
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  return {
    url: `http://127.0.0.1:${server.address().port}`,
    records,
    mutate(change) { mutation = change; },
    mutatePipeline(change) { pipelineMutation = change; },
    failProbe(value) { probeFailure = value; },
    failRefresh(value) { refreshFailure = value; },
    read(sql) {
      const database = new DatabaseSync(path);
      try { return database.prepare(sql).all().map(row => ({ ...row })); } finally { database.close(); }
    },
    async close() {
      server.closeAllConnections();
      await new Promise(resolve => server.close(resolve));
      for (const database of sessions.values()) database.close();
      sessions.clear();
      await rm(directory, { recursive: true, force: true });
    },
  };
}

function allowed(database, condition, success) {
  if (!condition) return true;
  if (condition.type === 'is_autocommit') return !database.isTransaction;
  if (condition.type === 'ok') return success[condition.step] === true;
  if (condition.type === 'error') return success[condition.step] === false;
  if (condition.type === 'not') return !allowed(database, condition.cond, success);
  if (condition.type === 'and') return condition.conds.every(c => allowed(database, c, success));
  if (condition.type === 'or') return condition.conds.some(c => allowed(database, c, success));
  throw new Error('Unsupported fixture condition');
}

function execute(database, stream, input, records, output) {
  records.push({ stream, sql: input.sql });
  const statement = database.prepare(input.sql);
  const cols = columns(statement);
  statement.setReadBigInts(true);
  statement.setReturnArrays(true);
  const args = input.named_args?.length
    ? [Object.fromEntries(input.named_args.map(({ name, value }) => [name, decoded(value)]))]
    : (input.args ?? []).map(decoded);
  const before = database.prepare('SELECT total_changes() AS n').get().n;
  output?.begin(cols);
  const rows = [];
  if (cols.length) {
    for (const row of statement.iterate(...args)) {
      rows.push(row);
      if (input.want_rows !== false) output?.row(row.map(encoded));
    }
  }
  const info = cols.length ? {
    changes: database.prepare('SELECT total_changes() AS n').get().n - before,
    lastInsertRowid: database.prepare('SELECT last_insert_rowid() AS n').get().n,
  } : statement.run(...args);
  return {
    cols,
    rows: input.want_rows === false ? [] : rows.map(row => row.map(encoded)),
    affected_row_count: Number(info.changes),
    last_insert_rowid: String(info.lastInsertRowid),
  };
}

function columns(statement) {
  return statement.columns().map(column => ({ name: column.name, decltype: column.type }));
}

function encoded(value) {
  if (value === null) return { type: 'null' };
  if (typeof value === 'bigint') return { type: 'integer', value: String(value) };
  if (typeof value === 'number') return Number.isInteger(value) ? { type: 'integer', value: String(value) } : { type: 'float', value };
  if (typeof value === 'string') return { type: 'text', value };
  if (value instanceof Uint8Array) return { type: 'blob', base64: Buffer.from(value).toString('base64') };
  throw new Error('Unsupported fixture value');
}

function decoded(value) {
  if (value.type === 'null') return null;
  if (value.type === 'integer') return BigInt(value.value);
  if (value.type === 'blob') return Buffer.from(value.base64, 'base64');
  return value.value;
}

function sqlError(error) {
  return { code: error.code ?? 'SQLITE_ERROR', message: error.message };
}
