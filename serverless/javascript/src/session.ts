import {
  executeCursor,
  executePipeline,
  decodeValue,
  type BatchStep,
  type CursorRequest,
  type CursorResponse,
  type CursorEntry,
  type PipelineRequest,
  type PipelineResponse,
  type SequenceRequest,
  type CloseRequest,
  type DescribeRequest,
  type DescribeResult,
  type GetAutocommitRequest,
  type QueryOptions,
  type HttpContext,
} from './protocol.js';
import { DatabaseError } from './error.js';
import { encodeSqlArgs } from './args.js';

/**
 * Locking mode for atomic `batch()` execution. Accepts the same values
 * as the variants of `Connection.transaction(...)`.
 */
export type BatchMode = 'write' | 'read' | 'deferred' | 'immediate' | 'exclusive' | 'concurrent' | string;

function normalizeBatchMode(mode: BatchMode): string {
  switch (String(mode).toLowerCase()) {
    case 'write':
      return 'IMMEDIATE';
    case 'read':
    case 'deferred':
      return 'DEFERRED';
    case 'immediate':
      return 'IMMEDIATE';
    case 'exclusive':
      return 'EXCLUSIVE';
    case 'concurrent':
      return 'CONCURRENT';
    default:
      return String(mode).toUpperCase();
  }
}

/**
 * Configuration options for a session.
 */
export interface SessionConfig {
  /** Database URL */
  url: string;
  /** Authentication token (optional for local development with turso dev) */
  authToken?: string;
  /**
   * Encryption key for the remote database (base64 encoded)
   * to enable access to encrypted Turso Cloud databases.
   */
  remoteEncryptionKey?: string;
  /** Default maximum query execution time in milliseconds before interruption. */
  defaultQueryTimeout?: number;
  /**
   * Extra HTTP headers attached to every request sent to the server.
   * Applied after the standard headers, so they can override e.g.
   * `Authorization`. Passing the `Host` key (case-insensitive) throws —
   * fetch forbids setting it.
   */
  requestHeaders?: Record<string, string>;
}

function normalizeUrl(url: string): string {
  return url.replace(/^libsql:\/\//, 'https://');
}

function isValidIdentifier(str: string): boolean {
  return /^[a-zA-Z_$][a-zA-Z0-9_$]*$/.test(str);
}

/**
 * A database session that manages the connection state and baton.
 * 
 * Each session maintains its own connection state and can execute SQL statements
 * independently without interfering with other sessions.
 */
export class Session {
  private config: SessionConfig;
  private baton: string | null = null;
  private baseUrl: string;
  // Cached autocommit status from the server's last `get_autocommit` answer.
  // A fresh connection is in autocommit (not in a transaction).
  private autocommit: boolean = true;

  constructor(config: SessionConfig) {
    for (const name of Object.keys(config.requestHeaders ?? {})) {
      // `Host` is a forbidden fetch header and would be silently dropped —
      // reject it up front so the caller learns the override never takes effect.
      if (name.toLowerCase() === 'host') {
        throw new DatabaseError("overwriting the 'Host' header is not supported");
      }
    }
    this.config = config;
    this.baseUrl = normalizeUrl(config.url);
  }

  private httpContext(queryOptions?: QueryOptions): HttpContext {
    // Per-query headers are merged over the session-level ones, so a query
    // can override a header the session sets (and both override the
    // standard headers).
    let requestHeaders = this.config.requestHeaders;
    if (queryOptions?.requestHeaders) {
      requestHeaders = { ...requestHeaders, ...queryOptions.requestHeaders };
    }
    return {
      url: this.baseUrl,
      authToken: this.config.authToken,
      remoteEncryptionKey: this.config.remoteEncryptionKey,
      requestHeaders,
    };
  }

  /**
   * Whether the connection is currently inside a transaction.
   *
   * Derived from the server's authoritative `get_autocommit` answer (the same
   * value as `sqlite3_get_autocommit()`), which we refresh on every pipeline
   * request. This is the only reliable signal: a non-null baton does NOT imply
   * a transaction — the server also keeps the stream open for stored SQL or
   * pragma side effects.
   */
  get inTransaction(): boolean {
    return !this.autocommit;
  }

  /**
   * Refresh the cached autocommit status from a pipeline response, reading the
   * answer to the `get_autocommit` request we append to every pipeline call.
   */
  private updateAutocommit(response: PipelineResponse): void {
    if (!response.results) {
      return;
    }
    for (const result of response.results) {
      if (
        result.type === 'ok' &&
        result.response?.type === 'get_autocommit' &&
        typeof result.response.is_autocommit === 'boolean'
      ) {
        this.autocommit = result.response.is_autocommit;
        return;
      }
    }
  }

  private createAbortSignal(queryOptions?: QueryOptions): AbortSignal | undefined {
    const timeout = queryOptions?.queryTimeout ?? this.config.defaultQueryTimeout;
    if (timeout != null && timeout > 0) {
      return AbortSignal.timeout(timeout);
    }
    return undefined;
  }

  /**
   * Describe a SQL statement to get its column metadata.
   * 
   * @param sql - The SQL statement to describe
   * @returns Promise resolving to the statement description
   */
  async describe(sql: string, queryOptions?: QueryOptions): Promise<DescribeResult> {
    const request: PipelineRequest = {
      baton: this.baton,
      requests: [
        { type: "describe", sql: sql } as DescribeRequest,
        { type: "get_autocommit" } as GetAutocommitRequest,
      ]
    };

    let response;
    try {
      response = await executePipeline(this.httpContext(queryOptions), request, this.createAbortSignal(queryOptions));
    } catch (e) {
      this.baton = null;
      this.autocommit = true;
      throw e;
    }

    this.baton = response.baton;
    if (response.base_url) {
      this.baseUrl = response.base_url;
    }
    this.updateAutocommit(response);

    // Check for errors in the response
    if (response.results && response.results[0]) {
      const result = response.results[0];
      if (result.type === "error") {
        throw new DatabaseError(result.error?.message || 'Describe execution failed', result.error?.code);
      }

      if (result.response?.type === "describe" && result.response.result) {
        return result.response.result as DescribeResult;
      }
    }

    throw new DatabaseError('Unexpected describe response');
  }

  /**
   * Execute a SQL statement and return all results.
   *
   * @param sql - The SQL statement to execute
   * @param args - Optional array of parameter values or object with named parameters
   * @param safeIntegers - Whether to return integers as BigInt
   * @returns Promise resolving to the complete result set
   */
  async execute(sql: string, args: any[] | Record<string, any> = [], safeIntegers: boolean = false, queryOptions?: QueryOptions): Promise<any> {
    const { response, entries } = await this.executeRaw(sql, args, queryOptions);
    const result = await this.processCursorEntries(entries, safeIntegers);
    return result;
  }

  /**
   * A trailing batch step gated on `is_autocommit`, appended to every cursor
   * request. The cursor endpoint cannot carry a `get_autocommit` probe, so
   * whether this step executed tells us the connection's transaction state
   * without an extra round trip.
   */
  private static autocommitProbeStep(): BatchStep {
    return {
      stmt: { sql: 'SELECT 1', args: [], named_args: [], want_rows: false },
      condition: { type: 'is_autocommit' },
    };
  }

  /**
   * Filter the probe step's entries out of a cursor stream and update the
   * cached transaction state from whether the probe executed. The probe is
   * always the last step, so everything after its step_begin belongs to it.
   *
   * If the stream ends abnormally (fatal error entry, a probe error, or the
   * consumer stops iterating early) the probe answer is unreliable, so the
   * state is refreshed with a fallback pipeline request instead.
   */
  private async *trackAutocommit(entries: AsyncGenerator<CursorEntry>, probeIdx: number, queryOptions?: QueryOptions): AsyncGenerator<CursorEntry> {
    let sawProbe = false;
    let unreliable = false;
    let completed = false;
    try {
      for await (const entry of entries) {
        if (entry.type === 'step_begin' && entry.step === probeIdx) {
          sawProbe = true;
          continue;
        }
        if (sawProbe && (entry.type === 'row' || entry.type === 'step_end')) {
          continue;
        }
        if (entry.type === 'error' || (entry.type === 'step_error' && entry.step === probeIdx)) {
          unreliable = true;
          if (entry.type === 'step_error') {
            continue;
          }
        }
        yield entry;
      }
      completed = true;
    } finally {
      if (completed && !unreliable) {
        this.autocommit = sawProbe;
      } else {
        await this.refreshAutocommit(queryOptions);
      }
    }
  }

  /**
   * Execute a SQL statement and return the raw response and entries.
   *
   * @param sql - The SQL statement to execute
   * @param args - Optional array of parameter values or object with named parameters
   * @returns Promise resolving to the raw response and cursor entries
   */
  async executeRaw(sql: string, args: any[] | Record<string, any> = [], queryOptions?: QueryOptions): Promise<{ response: CursorResponse; entries: AsyncGenerator<CursorEntry> }> {
    const encodedArgs = encodeSqlArgs(args);

    const request: CursorRequest = {
      baton: this.baton,
      batch: {
        steps: [{
          stmt: {
            sql,
            args: encodedArgs.args,
            named_args: encodedArgs.namedArgs,
            want_rows: true
          }
        }, Session.autocommitProbeStep()]
      }
    };

    let result;
    try {
      result = await executeCursor(this.httpContext(queryOptions), request, this.createAbortSignal(queryOptions));
    } catch (e) {
      this.baton = null;
      this.autocommit = true;
      throw e;
    }

    const { response, entries } = result;
    this.baton = response.baton;
    if (response.base_url) {
      this.baseUrl = response.base_url;
    }

    return { response, entries: this.trackAutocommit(entries, 1, queryOptions) };
  }

  /**
   * Refresh the cached transaction state with a standalone `get_autocommit`
   * pipeline request. Errors are not rethrown — this runs from generator
   * cleanup where an exception would mask the original failure; a dead stream
   * means the server rolled back, so the state resets to autocommit instead.
   */
  private async refreshAutocommit(queryOptions?: QueryOptions): Promise<void> {
    const request: PipelineRequest = {
      baton: this.baton,
      requests: [{ type: 'get_autocommit' } as GetAutocommitRequest],
    };

    let response;
    try {
      response = await executePipeline(this.httpContext(), request, this.createAbortSignal(queryOptions));
    } catch {
      this.baton = null;
      this.autocommit = true;
      return;
    }

    this.baton = response.baton;
    if (response.base_url) {
      this.baseUrl = response.base_url;
    }
    this.updateAutocommit(response);
  }

  /**
   * Process cursor entries into a structured result.
   *
   * @param entries - Async generator of cursor entries
   * @returns Promise resolving to the processed result
   */
  async processCursorEntries(entries: AsyncGenerator<CursorEntry>, safeIntegers: boolean = false): Promise<any> {
    let columns: string[] = [];
    let columnTypes: string[] = [];
    let rows: any[] = [];
    let rowsAffected = 0;
    let lastInsertRowid: number | undefined;

    for await (const entry of entries) {
      switch (entry.type) {
        case 'step_begin':
          if (entry.cols) {
            columns = entry.cols.map(col => col.name);
            columnTypes = entry.cols.map(col => col.decltype || '');
          }
          break;
        case 'row':
          if (entry.row) {
            const decodedRow = entry.row.map(value => decodeValue(value, safeIntegers));
            const rowObject = this.createRowObject(decodedRow, columns);
            rows.push(rowObject);
          }
          break;
        case 'step_end':
          if (entry.affected_row_count !== undefined) {
            rowsAffected = entry.affected_row_count;
          }
          if (entry.last_insert_rowid !== undefined && entry.last_insert_rowid !== null) {
            lastInsertRowid = typeof entry.last_insert_rowid === 'number'
              ? entry.last_insert_rowid
              : parseInt(entry.last_insert_rowid, 10);
          }
          break;
        case 'step_error':
        case 'error':
          throw new DatabaseError(entry.error?.message || 'SQL execution failed', entry.error?.code);
      }
    }

    return {
      columns,
      columnTypes,
      rows,
      rowsAffected,
      lastInsertRowid
    };
  }

  /**
   * Create a row object with both array and named property access.
   * 
   * @param values - Array of column values
   * @param columns - Array of column names
   * @returns Row object with dual access patterns
   */
  createRowObject(values: any[], columns: string[]): any {
    const row = [...values];
    
    // Add column name properties to the array as non-enumerable
    // Only add valid identifier names to avoid conflicts
    columns.forEach((column, index) => {
      if (column && isValidIdentifier(column)) {
        Object.defineProperty(row, column, {
          value: values[index],
          enumerable: false,
          writable: false,
          configurable: true
        });
      }
    });
    
    return row;
  }

  /**
   * Execute multiple SQL statements in a batch.
   *
   * When `mode` is set, the batch is sent as a single Hrana request that
   * also carries `BEGIN <mode>` / `COMMIT` / `ROLLBACK` steps using the
   * server-side condition chain, giving atomic execution in one round-trip.
   * When `mode` is omitted, the user statements are sent as-is and run
   * under autocommit (or whatever transaction is already active on this
   * stream).
   *
   * @param statements - Array of SQL statements to execute.
   * @param mode - Optional locking mode; when set, the batch executes
   *   atomically. Accepts the same values as `Database.transaction(...)`
   *   variants: `"deferred"`, `"immediate"`, `"exclusive"`, `"concurrent"`.
   * @param safeIntegers - When true, integer column values are decoded as
   *   BigInt rather than Number.
   * @returns Promise resolving to an array of per-statement results — one
   *   per input statement, in order — each carrying that statement's
   *   `columns`, `columnTypes`, `rows`, and `rowsAffected`.
   */
  async batch(
    statements: Array<string | { sql: string; args?: any[] | Record<string, any> }>,
    mode?: BatchMode,
    queryOptions?: QueryOptions,
    safeIntegers: boolean = false,
    raw: boolean = false,
  ): Promise<any> {
    const userSteps: BatchStep[] = statements.map(statement => {
      if (typeof statement === 'string') {
        return {
          stmt: { sql: statement, args: [], named_args: [], want_rows: true },
        };
      }
      const encodedArgs = encodeSqlArgs(statement.args ?? []);
      return {
        stmt: {
          sql: statement.sql,
          args: encodedArgs.args,
          named_args: encodedArgs.namedArgs,
          want_rows: true,
        },
      };
    });

    let steps: BatchStep[];
    let firstUserStepIdx = 0;
    let lastUserStepIdx = userSteps.length - 1;
    let beginIdx = -1;
    let commitIdx = -1;
    let rollbackIdx = -1;
    if (mode === undefined) {
      steps = userSteps;
    } else {
      // Atomic batch: BEGIN <mode>, then each user step gated on its
      // predecessor succeeding, then COMMIT gated on the last user step
      // succeeding, then ROLLBACK gated on BEGIN having succeeded *and*
      // COMMIT not having succeeded. The extra ok(BEGIN) guard prevents
      // ROLLBACK from aborting a transaction the caller opened on this
      // stream out of band (e.g. via session.execute("BEGIN")).
      beginIdx = 0;
      firstUserStepIdx = 1;
      lastUserStepIdx = userSteps.length; // 1..userSteps.length inclusive
      commitIdx = lastUserStepIdx + 1;
      rollbackIdx = commitIdx + 1;
      steps = [
        { stmt: { sql: `BEGIN ${normalizeBatchMode(mode)}`, args: [], named_args: [], want_rows: false } },
        ...userSteps.map((step, i) => ({
          ...step,
          condition: { type: 'ok' as const, step: i === 0 ? beginIdx : firstUserStepIdx + i - 1 },
        })),
        {
          stmt: { sql: 'COMMIT', args: [], named_args: [], want_rows: false },
          condition: { type: 'ok' as const, step: lastUserStepIdx },
        },
        {
          stmt: { sql: 'ROLLBACK', args: [], named_args: [], want_rows: false },
          condition: {
            type: 'and' as const,
            conds: [
              { type: 'ok' as const, step: beginIdx },
              { type: 'not' as const, cond: { type: 'ok' as const, step: commitIdx } },
            ],
          },
        },
      ];
    }

    const probeIdx = steps.length;
    const request: CursorRequest = {
      baton: this.baton,
      batch: { steps: [...steps, Session.autocommitProbeStep()] },
    };

    let batchResult;
    try {
      batchResult = await executeCursor(this.httpContext(queryOptions), request, this.createAbortSignal(queryOptions));
    } catch (e) {
      this.baton = null;
      this.autocommit = true;
      throw e;
    }

    const { response, entries } = batchResult;
    this.baton = response.baton;
    if (response.base_url) {
      this.baseUrl = response.base_url;
    }

    // One result per user statement, in input order.
    const results = userSteps.map(() => ({
      columns: [] as string[],
      columnTypes: [] as string[],
      rows: [] as any[],
      rowsAffected: 0,
    }));
    let deferredError: DatabaseError | null = null;

    // step_end / row entries don't carry a step index on the wire; the Hrana
    // server only puts `step` on step_begin / step_error. Track the current
    // step via step_begin so we know which user statement a row or step_end
    // belongs to. Maps the wire step index to a slot in `results`, or
    // undefined for the synthetic BEGIN/COMMIT/ROLLBACK steps.
    let currentResultIdx: number | undefined;
    // Fallback for responses that omit step_begin (e.g. simplified mocks):
    // in non-atomic mode every step_end advances to the next user statement.
    let nextNonAtomicIdx = 0;
    const stepToResultIdx = (step: number | undefined): number | undefined => {
      if (mode === undefined) {
        // Non-atomic batch: every step is a user step, in order.
        return step ?? nextNonAtomicIdx;
      }
      if (step !== undefined && step >= firstUserStepIdx && step <= lastUserStepIdx) {
        return step - firstUserStepIdx;
      }
      return undefined;
    };

    for await (const entry of this.trackAutocommit(entries, probeIdx, queryOptions)) {
      // Once a step has errored the whole batch will throw, so stop
      // decoding and buffering later steps' results while draining the
      // rest of the stream for the probe. Fatal stream errors still
      // surface below.
      if (deferredError !== null && entry.type !== 'error') {
        continue;
      }
      switch (entry.type) {
        case 'step_begin':
          currentResultIdx = stepToResultIdx(entry.step);
          if (currentResultIdx !== undefined && currentResultIdx < results.length && entry.cols) {
            results[currentResultIdx].columns = entry.cols.map(col => col.name);
            results[currentResultIdx].columnTypes = entry.cols.map(col => col.decltype || '');
          }
          break;
        case 'row':
          if (currentResultIdx !== undefined && currentResultIdx < results.length && entry.row) {
            const decodedRow = entry.row.map(value => decodeValue(value, safeIntegers));
            const row = raw
              ? decodedRow
              : this.createRowObject(decodedRow, results[currentResultIdx].columns);
            results[currentResultIdx].rows.push(row);
          }
          break;
        case 'step_end': {
          let idx = currentResultIdx;
          if (idx === undefined && mode === undefined) {
            idx = nextNonAtomicIdx;
          }
          if (idx !== undefined && idx < results.length) {
            if (entry.affected_row_count !== undefined) {
              results[idx].rowsAffected = results[idx].columns.length > 0
                ? 0
                : entry.affected_row_count;
            }
          }
          if (mode === undefined && idx !== undefined) {
            nextNonAtomicIdx = idx + 1;
          }
          currentResultIdx = undefined;
          break;
        }
        case 'step_error':
          // Capture the first error from BEGIN, any user step, or COMMIT
          // and keep draining so the trailing probe (and, in atomic mode,
          // ROLLBACK) is still observed. Errors on the synthetic ROLLBACK
          // step are suppressed — by the time it runs the transaction has
          // already been undone and surfacing a ROLLBACK error would mask
          // the real cause we already captured.
          if (deferredError === null && entry.step !== rollbackIdx) {
            deferredError = new DatabaseError(entry.error?.message || 'Batch execution failed', entry.error?.code);
          }
          currentResultIdx = undefined;
          break;
        case 'error':
          throw new DatabaseError(entry.error?.message || 'Batch execution failed', entry.error?.code);
      }
    }

    if (deferredError !== null) {
      throw deferredError;
    }

    return results;
  }

  /**
   * Execute a sequence of SQL statements separated by semicolons.
   * 
   * @param sql - SQL string containing multiple statements separated by semicolons
   * @returns Promise resolving when all statements are executed
   */
  async sequence(sql: string, queryOptions?: QueryOptions): Promise<void> {
    const request: PipelineRequest = {
      baton: this.baton,
      requests: [
        { type: "sequence", sql: sql } as SequenceRequest,
        { type: "get_autocommit" } as GetAutocommitRequest,
      ]
    };

    let seqResponse;
    try {
      seqResponse = await executePipeline(this.httpContext(queryOptions), request, this.createAbortSignal(queryOptions));
    } catch (e) {
      this.baton = null;
      this.autocommit = true;
      throw e;
    }

    this.baton = seqResponse.baton;
    if (seqResponse.base_url) {
      this.baseUrl = seqResponse.base_url;
    }
    this.updateAutocommit(seqResponse);

    // Check for errors in the response
    if (seqResponse.results && seqResponse.results[0]) {
      const result = seqResponse.results[0];
      if (result.type === "error") {
        throw new DatabaseError(result.error?.message || 'Sequence execution failed', result.error?.code);
      }
    }
  }

  /**
   * Close the session.
   *
   * This sends a close request to the server to properly clean up the stream
   * before resetting the local state.
   */
  async close(): Promise<void> {
    // Only send close request if we have an active baton
    if (this.baton) {
      try {
        const request: PipelineRequest = {
          baton: this.baton,
          requests: [{
            type: "close"
          } as CloseRequest]
        };

        await executePipeline(this.httpContext(), request);
      } catch {
        // Ignore errors during close — the connection might already be closed
        // or the baton may be stale after a timeout.
      }
    }

    // Reset local state
    this.baton = null;
    this.baseUrl = '';
    this.autocommit = true;
  }
}
