import {
  executeCursor,
  executePipeline,
  decodeValue,
  type BatchStep,
  type CursorRequest,
  type CursorResponse,
  type CursorEntry,
  type PipelineRequest,
  type SequenceRequest,
  type CloseRequest,
  type DescribeRequest,
  type DescribeResult,
  type QueryOptions,
} from './protocol.js';
import { DatabaseError } from './error.js';
import { encodeSqlArgs } from './args.js';

/**
 * Locking mode for atomic `batch()` execution. Accepts the same values
 * as the variants of `Connection.transaction(...)`.
 */
export type BatchMode = 'deferred' | 'immediate' | 'exclusive' | 'concurrent';

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

  constructor(config: SessionConfig) {
    this.config = config;
    this.baseUrl = normalizeUrl(config.url);
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
      requests: [{
        type: "describe",
        sql: sql
      } as DescribeRequest]
    };

    let response;
    try {
      response = await executePipeline(this.baseUrl, this.config.authToken, request, this.config.remoteEncryptionKey, this.createAbortSignal(queryOptions));
    } catch (e) {
      this.baton = null;
      throw e;
    }

    this.baton = response.baton;
    if (response.base_url) {
      this.baseUrl = response.base_url;
    }

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
        }]
      }
    };

    let result;
    try {
      result = await executeCursor(this.baseUrl, this.config.authToken, request, this.config.remoteEncryptionKey, this.createAbortSignal(queryOptions));
    } catch (e) {
      this.baton = null;
      throw e;
    }

    const { response, entries } = result;
    this.baton = response.baton;
    if (response.base_url) {
      this.baseUrl = response.base_url;
    }

    return { response, entries };
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
   * @returns Promise resolving to batch execution results.
   */
  async batch(
    statements: Array<string | { sql: string; args?: any[] | Record<string, any> }>,
    mode?: BatchMode,
    queryOptions?: QueryOptions,
  ): Promise<any> {
    const userSteps: BatchStep[] = statements.map(statement => {
      if (typeof statement === 'string') {
        return {
          stmt: { sql: statement, args: [], named_args: [], want_rows: false },
        };
      }
      const encodedArgs = encodeSqlArgs(statement.args ?? []);
      return {
        stmt: {
          sql: statement.sql,
          args: encodedArgs.args,
          named_args: encodedArgs.namedArgs,
          want_rows: false,
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
        { stmt: { sql: `BEGIN ${mode.toUpperCase()}`, args: [], named_args: [], want_rows: false } },
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

    const request: CursorRequest = {
      baton: this.baton,
      batch: { steps },
    };

    let batchResult;
    try {
      batchResult = await executeCursor(this.baseUrl, this.config.authToken, request, this.config.remoteEncryptionKey, this.createAbortSignal(queryOptions));
    } catch (e) {
      this.baton = null;
      throw e;
    }

    const { response, entries } = batchResult;
    this.baton = response.baton;
    if (response.base_url) {
      this.baseUrl = response.base_url;
    }

    let totalRowsAffected = 0;
    let lastInsertRowid: number | undefined;
    let deferredError: DatabaseError | null = null;

    // step_end entries don't carry a step index on the wire; the Hrana
    // server only puts `step` on step_begin / step_error. Track the
    // current step ourselves by watching step_begin so we know which
    // step_end belongs to a user statement when running in atomic mode.
    let currentStep: number | undefined;
    const isUserStep = (step: number | undefined): boolean => {
      if (mode === undefined) {
        // Non-atomic batch: every step is a user step.
        return true;
      }
      return step !== undefined && step >= firstUserStepIdx && step <= lastUserStepIdx;
    };

    for await (const entry of entries) {
      switch (entry.type) {
        case 'step_begin':
          currentStep = entry.step;
          break;
        case 'step_end':
          if (isUserStep(currentStep)) {
            if (entry.affected_row_count !== undefined) {
              totalRowsAffected += entry.affected_row_count;
            }
            if (entry.last_insert_rowid !== undefined && entry.last_insert_rowid !== null) {
              lastInsertRowid = typeof entry.last_insert_rowid === 'number'
                ? entry.last_insert_rowid
                : parseInt(entry.last_insert_rowid, 10);
            }
          }
          currentStep = undefined;
          break;
        case 'step_error':
          if (mode === undefined) {
            throw new DatabaseError(entry.error?.message || 'Batch execution failed', entry.error?.code);
          }
          // Atomic batch: capture the first error from BEGIN, any user
          // step, or COMMIT and keep draining so ROLLBACK has a chance
          // to clean up. Errors on the synthetic ROLLBACK step are
          // suppressed — by the time it runs the transaction has
          // already been undone and surfacing a ROLLBACK error would
          // mask the real cause we already captured.
          if (deferredError === null && entry.step !== rollbackIdx) {
            deferredError = new DatabaseError(entry.error?.message || 'Batch execution failed', entry.error?.code);
          }
          currentStep = undefined;
          break;
        case 'error':
          throw new DatabaseError(entry.error?.message || 'Batch execution failed', entry.error?.code);
      }
    }

    if (deferredError !== null) {
      throw deferredError;
    }

    return {
      rowsAffected: totalRowsAffected,
      lastInsertRowid,
    };
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
      requests: [{
        type: "sequence",
        sql: sql
      } as SequenceRequest]
    };

    let seqResponse;
    try {
      seqResponse = await executePipeline(this.baseUrl, this.config.authToken, request, this.config.remoteEncryptionKey, this.createAbortSignal(queryOptions));
    } catch (e) {
      this.baton = null;
      throw e;
    }

    this.baton = seqResponse.baton;
    if (seqResponse.base_url) {
      this.baseUrl = seqResponse.base_url;
    }

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

        await executePipeline(this.baseUrl, this.config.authToken, request, this.config.remoteEncryptionKey);
      } catch {
        // Ignore errors during close — the connection might already be closed
        // or the baton may be stale after a timeout.
      }
    }

    // Reset local state
    this.baton = null;
    this.baseUrl = '';
  }
}
