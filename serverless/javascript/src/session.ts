import {
  executeCursor,
  executePipeline,
  decodeValue,
  type BatchResultData,
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
  type ExecuteResult,
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

// Rewrite libsql:// and turso:// URLs to https:// and strip any trailing
// slashes, since endpoint paths are appended with a leading slash.
function normalizeUrl(url: string): string {
  return url.replace(/^(libsql|turso):\/\//, 'https://').replace(/\/+$/, '');
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
      this.baseUrl = normalizeUrl(response.base_url);
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
      this.baseUrl = normalizeUrl(response.base_url);
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
      this.baseUrl = normalizeUrl(response.base_url);
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

  createObjectRow(values: any[], columns: string[]): any {
    const row: any = {};
    columns.forEach((column, index) => {
      row[column] = values[index];
    });
    return row;
  }

  /**
   * Execute multiple SQL statements in a batch.
   *
   * The batch is sent as a single `batch` request on the pipeline
   * endpoint (PROTOCOL.md section 6.2), so the whole batch completes in
   * one round-trip. Each statement is gated on its predecessor
   * succeeding, so execution stops at the first failure. When `mode` is
   * set, the request also carries `BEGIN <mode>` / `COMMIT` / `ROLLBACK`
   * steps using the server-side condition chain, giving atomic
   * execution. When `mode` is omitted, the statements run under
   * autocommit (or whatever transaction is already active on this
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
   *   `columns`, `columnTypes`, `rows`, `rowsAffected`, `lastInsertRowid`,
   *   and the server-side execution statistics `rowsRead`, `rowsWritten`,
   *   and `queryDurationMs`. On failure the thrown `DatabaseError` carries
   *   `batchIndex` (the failing statement) and `batchResults` (the results
   *   of the statements that completed).
   */
  async batch(
    statements: Array<string | { sql: string; args?: any[] | Record<string, any> }>,
    mode?: BatchMode,
    queryOptions?: QueryOptions,
    safeIntegers: boolean = false,
    raw: boolean = false,
  ): Promise<any> {
    const userSteps: BatchStep[] = statements.map((statement, index) => {
      if (typeof statement === 'string') {
        return {
          stmt: { sql: statement, args: [], named_args: [], want_rows: true },
        };
      }
      // A value that cannot be encoded fails client-side before anything
      // is sent; report it with the statement's index like any other
      // statement failure. Nothing has executed, so batchResults is empty.
      let encodedArgs;
      try {
        encodedArgs = encodeSqlArgs(statement.args ?? []);
      } catch (e: any) {
        const error = new DatabaseError(
          `batch statement ${index} failed: ${e?.message ?? e}`,
        );
        error.batchIndex = index;
        error.batchResults = [];
        throw error;
      }
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
    let beginIdx = -1;
    let commitIdx = -1;
    if (mode === undefined) {
      // Each statement is gated on its predecessor succeeding, so
      // execution stops at the first failure (matching the Rust and
      // Python drivers and the `sequence` request).
      steps = userSteps.map((step, i) =>
        i === 0 ? step : { ...step, condition: { type: 'ok' as const, step: i - 1 } },
      );
    } else {
      // Atomic batch: BEGIN <mode>, then each user step gated on its
      // predecessor succeeding, then COMMIT gated on the last user step
      // succeeding, then ROLLBACK gated on BEGIN having succeeded *and*
      // COMMIT not having succeeded. The extra ok(BEGIN) guard prevents
      // ROLLBACK from aborting a transaction the caller opened on this
      // stream out of band (e.g. via session.execute("BEGIN")).
      beginIdx = 0;
      firstUserStepIdx = 1;
      const lastUserStepIdx = userSteps.length; // 1..userSteps.length inclusive
      commitIdx = lastUserStepIdx + 1;
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

    const request: PipelineRequest = {
      baton: this.baton,
      requests: [
        { type: 'batch', batch: { steps } },
        { type: 'get_autocommit' },
      ],
    };

    let response: PipelineResponse;
    try {
      response = await executePipeline(this.httpContext(queryOptions), request, this.createAbortSignal(queryOptions));
    } catch (e) {
      this.baton = null;
      this.autocommit = true;
      throw e;
    }

    this.baton = response.baton;
    if (response.base_url) {
      this.baseUrl = normalizeUrl(response.base_url);
    }
    this.updateAutocommit(response);

    const first = response.results?.[0];
    if (!first) {
      throw new DatabaseError('missing batch result in pipeline response');
    }
    if (first.type === 'error') {
      throw new DatabaseError(first.error?.message || 'Batch execution failed', first.error?.code);
    }
    if (first.response?.type !== 'batch') {
      throw new DatabaseError(`expected batch result in pipeline response, got ${first.response?.type}`);
    }
    const batchResult = first.response.result as BatchResultData | undefined;
    const stepResults = batchResult?.step_results;
    const stepErrors = batchResult?.step_errors;
    if (
      !Array.isArray(stepResults) ||
      !Array.isArray(stepErrors) ||
      stepResults.length !== steps.length ||
      stepErrors.length !== steps.length
    ) {
      throw new DatabaseError('batch response does not have one result and one error per step');
    }

    // One result per user statement, in input order; null for statements
    // that did not complete.
    const results: Array<any | null> = statements.map((_, i) => {
      const stepResult = stepResults[firstUserStepIdx + i];
      return stepResult ? this.decodeBatchStepResult(stepResult, safeIntegers, raw) : null;
    });

    // Surface the failing step: BEGIN first, then the user statements
    // (with their index), then COMMIT. Errors on the synthetic ROLLBACK
    // step are suppressed — by the time it runs the transaction has
    // already been undone and surfacing a ROLLBACK error would mask the
    // real cause.
    const throwStepError = (error: { message?: string; code?: string } | null, batchIndex?: number): never => {
      const e = new DatabaseError(error?.message || 'Batch execution failed', error?.code);
      if (batchIndex !== undefined) {
        e.batchIndex = batchIndex;
      }
      e.batchResults = results;
      throw e;
    };
    if (beginIdx >= 0 && stepErrors[beginIdx]) {
      throwStepError(stepErrors[beginIdx]);
    }
    for (let i = 0; i < userSteps.length; i++) {
      const stepError = stepErrors[firstUserStepIdx + i];
      if (stepError) {
        throwStepError(stepError, i);
      }
    }
    if (commitIdx >= 0 && stepErrors[commitIdx]) {
      throwStepError(stepErrors[commitIdx]);
    }

    if (results.some(result => result === null)) {
      throw new DatabaseError('batch response is missing statement results');
    }
    return results;
  }

  /** Decode one statement result of a batch response (section 8.4) into
   * the per-statement result shape returned by `batch()`. */
  private decodeBatchStepResult(stepResult: ExecuteResult, safeIntegers: boolean, raw: boolean): any {
    const columns = (stepResult.cols ?? []).map(col => col.name ?? '');
    const columnTypes = (stepResult.cols ?? []).map(col => col.decltype || '');
    const rows = (stepResult.rows ?? []).map(row => {
      const decoded = row.map(value => decodeValue(value, safeIntegers));
      return raw ? decoded : this.createObjectRow(decoded, columns);
    });
    let lastInsertRowid: number | undefined;
    if (stepResult.last_insert_rowid !== undefined && stepResult.last_insert_rowid !== null) {
      lastInsertRowid = typeof stepResult.last_insert_rowid === 'number'
        ? stepResult.last_insert_rowid
        : parseInt(stepResult.last_insert_rowid, 10);
    }
    const resultSet: any = {
      columns,
      columnTypes,
      rows,
      rowsAffected: columns.length > 0 ? 0 : (stepResult.affected_row_count ?? 0),
      rowsRead: stepResult.rows_read,
      rowsWritten: stepResult.rows_written,
      queryDurationMs: stepResult.query_duration_ms,
    };
    // Only statements that inserted carry the key, so callers can use
    // `"lastInsertRowid" in resultSet` to detect an insert.
    if (lastInsertRowid !== undefined) {
      resultSet.lastInsertRowid = lastInsertRowid;
    }
    return resultSet;
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
      this.baseUrl = normalizeUrl(seqResponse.base_url);
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
