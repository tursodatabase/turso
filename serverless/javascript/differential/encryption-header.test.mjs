// Property test for the remote encryption key header of the serverless
// driver, from the shared spec's `tests.encryption_header` entry
// (serverless/conformance/differential/spec/ops.json; described in
// serverless/conformance/differential/operations.md and PROTOCOL.md
// section 3.1): a driver configured with key K attaches
// `x-turso-encryption-key: K` to EVERY HTTP request — pipeline and cursor
// endpoints alike — and a driver configured without a key never sends the
// header.
//
// Runs against a local stub HTTP server that records request headers and
// speaks just enough of the protocol for a statement to complete. It needs
// no Turso Cloud database and runs unconditionally — no environment
// configuration, never skips.

import { createServer } from 'node:http';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import test from 'ava';
import * as fc from 'fast-check';

import { connect, ENCRYPTION_KEY_HEADER } from '@tursodatabase/serverless';

// ---------------------------------------------------------------------------
// Load spec from ops.json
// ---------------------------------------------------------------------------

const __dirname = dirname(fileURLToPath(import.meta.url));
const _SPEC = JSON.parse(
  readFileSync(
    join(__dirname, '..', '..', 'conformance', 'differential', 'spec', 'ops.json'),
    'utf8'
  )
);

const ENC = _SPEC.tests.encryption_header;

// ---------------------------------------------------------------------------
// Stub Hrana server — records every request's headers and answers with the
// minimal protocol shapes from PROTOCOL.md sections 5–7.
// ---------------------------------------------------------------------------

// One pipeline result per request, in request order (PROTOCOL.md section 5.2).
function pipelineResult(request) {
  switch (request.type) {
    case 'execute':
      return {
        type: 'ok',
        response: {
          type: 'execute',
          result: {
            cols: [{ name: 'x', decltype: '' }],
            rows: [[{ type: 'integer', value: '1' }]],
            affected_row_count: 0,
            last_insert_rowid: null,
          },
        },
      };
    case 'describe':
      return {
        type: 'ok',
        response: {
          type: 'describe',
          result: { params: [], cols: [{ name: 'x', decltype: '' }], is_explain: false, is_readonly: true },
        },
      };
    case 'get_autocommit':
      return { type: 'ok', response: { type: 'get_autocommit', is_autocommit: true } };
    default:
      // sequence, close, batch — an empty ok response suffices.
      return { type: 'ok', response: { type: request.type } };
  }
}

// Newline-separated cursor stream (PROTOCOL.md section 7.2): first the
// cursor response, then step_begin/row/step_end per step. The stub is
// always in autocommit, so every step — including the driver's trailing
// is_autocommit probe — executes.
function cursorBody(request) {
  const lines = [{ baton: 'stub-baton', base_url: null }];
  request.batch.steps.forEach((step, i) => {
    const wantRows = step.stmt.want_rows;
    lines.push({ type: 'step_begin', step: i, cols: wantRows ? [{ name: 'x', decltype: '' }] : [] });
    if (wantRows) {
      lines.push({ type: 'row', row: [{ type: 'integer', value: '1' }] });
    }
    lines.push({ type: 'step_end', affected_row_count: 0, last_insert_rowid: null });
  });
  lines.push({ type: 'replication_index', replication_index: '0' });
  return lines.map((l) => JSON.stringify(l)).join('\n') + '\n';
}

// Starts the stub on 127.0.0.1 and resolves to { port, requests, close }.
// `requests` accumulates { path, headers } for every request received.
function startStubServer() {
  const requests = [];
  const server = createServer((req, res) => {
    let body = '';
    req.on('data', (chunk) => { body += chunk; });
    req.on('end', () => {
      // Node lowercases incoming header names in req.headers.
      requests.push({ path: req.url, headers: { ...req.headers } });
      const request = JSON.parse(body);
      if (req.url === '/v3/pipeline') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          baton: null,
          base_url: null,
          results: request.requests.map(pipelineResult),
        }));
      } else if (req.url === '/v3/cursor') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(cursorBody(request));
      } else {
        res.writeHead(404);
        res.end();
      }
    });
  });
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      resolve({
        port: server.address().port,
        requests,
        close: () => new Promise((r) => server.close(r)),
      });
    });
  });
}

// ---------------------------------------------------------------------------
// Drive the stub through both endpoints: exec() goes to /v3/pipeline
// (sequence + get_autocommit), all() goes to /v3/cursor, and close() sends
// a final /v3/pipeline close for the baton the cursor issued.
// ---------------------------------------------------------------------------

async function runStatements(config) {
  const conn = connect(config);
  try {
    await conn.exec('SELECT 1');
    await conn.all('SELECT 1');
  } finally {
    await conn.close();
  }
}

function assertEndpointCoverage(requests) {
  const paths = new Set(requests.map((r) => r.path));
  if (requests.length < 3 || !paths.has('/v3/pipeline') || !paths.has('/v3/cursor')) {
    throw new Error(
      `expected pipeline and cursor requests, got: ${JSON.stringify(requests.map((r) => r.path))}`
    );
  }
}

// ---------------------------------------------------------------------------
// The properties
// ---------------------------------------------------------------------------

// Keys drawn from the spec's base64 alphabet, optionally with trailing
// '=' padding.
const arbKey = fc.tuple(
  fc.string({
    minLength: ENC.key_min_len,
    maxLength: ENC.key_max_len,
    unit: fc.constantFrom(...ENC.key_alphabet),
  }),
  fc.constantFrom('', '=', '==')
).map(([body, pad]) => body + pad);

test.serial('encryption header: configured key sent on every request', async (t) => {
  // The driver's exported constant must match the spec's header name.
  t.is(ENCRYPTION_KEY_HEADER, ENC.header);

  const server = await startStubServer();
  try {
    await fc.assert(
      fc.asyncProperty(arbKey, async (key) => {
        server.requests.length = 0;
        await runStatements({
          url: `http://127.0.0.1:${server.port}`,
          authToken: 'x',
          remoteEncryptionKey: key,
        });
        assertEndpointCoverage(server.requests);
        for (const req of server.requests) {
          if (req.headers[ENC.header] !== key) {
            throw new Error(
              `request to ${req.path} carried header ${JSON.stringify(req.headers[ENC.header])}, ` +
              `expected ${JSON.stringify(key)}`
            );
          }
        }
      }),
      { numRuns: ENC.num_examples, verbose: 1 }
    );
    t.pass();
  } catch (e) {
    t.fail(e.message);
  } finally {
    await server.close();
  }
});

test.serial('encryption header: absent on every request without a key', async (t) => {
  const server = await startStubServer();
  try {
    await runStatements({
      url: `http://127.0.0.1:${server.port}`,
      authToken: 'x',
    });
    assertEndpointCoverage(server.requests);
    for (const req of server.requests) {
      t.is(
        req.headers[ENC.header],
        undefined,
        `request to ${req.path} must not carry ${ENC.header}`
      );
    }
    t.pass();
  } finally {
    await server.close();
  }
});
