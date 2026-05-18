import {
  executeCursor,
  executePipeline,
  encodeValue,
  decodeValue,
  type CursorRequest,
  type CursorResponse,
  type CursorEntry,
  type PipelineRequest,
  type PipelineResponse,
  type ExecuteRequest,
  type ExecuteResult,
  type BatchRequest,
  type BatchResult,
  type SequenceRequest,
  type CloseRequest,
  type DescribeRequest,
  type DescribeResult,
  type QueryOptions,
  type NamedArg,
  type Value
} from './protocol.js';
import { DatabaseError } from './error.js';

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

  /**
   * Whether the server is currently keeping a stream open for this session.
   *
   * Per the Hrana V2/V3 spec, the server returns a baton in every pipeline
   * response: a non-null baton means the server kept the stream alive (it's
   * inside a transaction, has stored SQL, or has pragma side-effects); a null
   * baton means the server closed the stream because the connection state is
   * back to a clean autocommit slate.
   */
  get isStreamOpen(): boolean {
    return this.baton !== null;
  }

  private createAbortSignal(queryOptions?: QueryOptions): AbortSignal | undefined {
    const timeout = queryOptions?.queryTimeout ?? this.config.defaultQueryTimeout;
    if (timeout != null && timeout > 0) {
      return AbortSignal.timeout(timeout);
    }
    return undefined;
  }

  /**
   * Send a pipeline request, propagating baton and base_url from the response.
   * The server returns baton: null when the stream can be closed; we just trust
   * that and store whatever comes back.
   */
  private async sendPipeline(
    requests: PipelineRequest['requests'],
    queryOptions?: QueryOptions,
  ): Promise<PipelineResponse> {
    const request: PipelineRequest = {
      baton: this.baton,
      requests,
    };

    let response;
    try {
      response = await executePipeline(
        this.baseUrl,
        this.config.authToken,
        request,
        this.config.remoteEncryptionKey,
        this.createAbortSignal(queryOptions),
      );
    } catch (e) {
      this.baton = null;
      throw e;
    }

    this.baton = response.baton;
    if (response.base_url) {
      this.baseUrl = response.base_url;
    }
    return response;
  }

  /**
   * Describe a SQL statement to get its column metadata.
   */
  async describe(sql: string, queryOptions?: QueryOptions): Promise<DescribeResult> {
    const response = await this.sendPipeline(
      [{ type: 'describe', sql } as DescribeRequest],
      queryOptions,
    );

    if (response.results && response.results[0]) {
      const result = response.results[0];
      if (result.type === 'error') {
        throw new DatabaseError(result.error?.message || 'Describe execution failed', result.error?.code);
      }
      if (result.response?.type === 'describe' && result.response.result) {
        return result.response.result as DescribeResult;
      }
    }

    throw new DatabaseError('Unexpected describe response');
  }

  /**
   * Execute a SQL statement and return all results.
   *
   * Uses the /v3/pipeline endpoint so the server runs its auto-close check
   * and closes the stream itself (returning baton: null) when the connection
   * is back in autocommit. The /v3/cursor endpoint does NOT run that check
   * and would leak a stream on the server for every one-shot SELECT.
   */
  async execute(sql: string, args: any[] | Record<string, any> = [], safeIntegers: boolean = false, queryOptions?: QueryOptions): Promise<any> {
    const { positionalArgs, namedArgs } = this.encodeArgs(args);

    const executeReq: ExecuteRequest = {
      type: 'execute',
      stmt: {
        sql,
        args: positionalArgs,
        named_args: namedArgs,
        want_rows: true,
      },
    };

    const response = await this.sendPipeline([executeReq], queryOptions);

    if (response.results && response.results[0]) {
      const result = response.results[0];
      if (result.type === 'error') {
        throw new DatabaseError(result.error?.message || 'SQL execution failed', result.error?.code);
      }
      if (result.response?.type === 'execute' && result.response.result) {
        return this.processExecuteResult(result.response.result as ExecuteResult, safeIntegers);
      }
    }

    throw new DatabaseError('Unexpected execute response');
  }

  /**
   * Execute a SQL statement and stream results via the /v3/cursor endpoint.
   *
   * The cursor endpoint always returns a baton — it doesn't run the
   * auto-close check — so the leftover baton will be cleaned up by the next
   * pipeline call when the server re-evaluates stream state.
   */
  async executeRaw(sql: string, args: any[] | Record<string, any> = [], queryOptions?: QueryOptions): Promise<{ response: CursorResponse; entries: AsyncGenerator<CursorEntry> }> {
    const { positionalArgs, namedArgs } = this.encodeArgs(args);

    const request: CursorRequest = {
      baton: this.baton,
      batch: {
        steps: [{
          stmt: {
            sql,
            args: positionalArgs,
            named_args: namedArgs,
            want_rows: true,
          },
        }],
      },
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
   * Convert an ExecuteResult from the pipeline endpoint into the same row/
   * column shape that processCursorEntries returns from the cursor endpoint.
   */
  private processExecuteResult(execResult: ExecuteResult, safeIntegers: boolean): any {
    const columns = execResult.cols.map(col => col.name);
    const columnTypes = execResult.cols.map(col => col.decltype || '');
    const rows = execResult.rows.map(row => {
      const decodedRow = row.map(value => decodeValue(value, safeIntegers));
      return this.createRowObject(decodedRow, columns);
    });

    let lastInsertRowid: number | undefined;
    if (execResult.last_insert_rowid !== undefined && execResult.last_insert_rowid !== null) {
      lastInsertRowid = typeof execResult.last_insert_rowid === 'number'
        ? execResult.last_insert_rowid
        : parseInt(execResult.last_insert_rowid as string, 10);
    }

    return {
      columns,
      columnTypes,
      rows,
      rowsAffected: execResult.affected_row_count,
      lastInsertRowid,
    };
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
   * Execute multiple SQL statements in a batch. Uses the /v3/pipeline endpoint
   * (see `execute` for why) so the server's auto-close logic applies.
   */
  async batch(statements: string[], queryOptions?: QueryOptions): Promise<any> {
    const batchReq: BatchRequest = {
      type: 'batch',
      batch: {
        steps: statements.map(sql => ({
          stmt: {
            sql,
            args: [],
            named_args: [],
            want_rows: false,
          },
        })),
      },
    };

    const response = await this.sendPipeline([batchReq], queryOptions);

    if (response.results && response.results[0]) {
      const result = response.results[0];
      if (result.type === 'error') {
        throw new DatabaseError(result.error?.message || 'Batch execution failed', result.error?.code);
      }
      if (result.response?.type === 'batch' && result.response.result) {
        return this.processBatchResult(result.response.result as BatchResult);
      }
    }

    throw new DatabaseError('Unexpected batch response');
  }

  private processBatchResult(batchResult: BatchResult): any {
    let totalRowsAffected = 0;
    let lastInsertRowid: number | undefined;

    for (let i = 0; i < batchResult.step_results.length; i++) {
      const stepError = batchResult.step_errors[i];
      if (stepError) {
        throw new DatabaseError(stepError.message || 'Batch execution failed', stepError.code);
      }
      const stepResult = batchResult.step_results[i];
      if (stepResult) {
        totalRowsAffected += stepResult.affected_row_count;
        if (stepResult.last_insert_rowid !== undefined && stepResult.last_insert_rowid !== null) {
          lastInsertRowid = typeof stepResult.last_insert_rowid === 'number'
            ? stepResult.last_insert_rowid
            : parseInt(stepResult.last_insert_rowid as string, 10);
        }
      }
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
   */
  async sequence(sql: string, queryOptions?: QueryOptions): Promise<void> {
    const response = await this.sendPipeline(
      [{ type: 'sequence', sql } as SequenceRequest],
      queryOptions,
    );

    if (response.results && response.results[0]) {
      const result = response.results[0];
      if (result.type === 'error') {
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

  /**
   * Encode arguments (positional array or named-parameter object) into the
   * positional/named Value arrays the Hrana protocol expects.
   */
  private encodeArgs(args: any[] | Record<string, any>): { positionalArgs: Value[]; namedArgs: NamedArg[] } {
    if (Array.isArray(args)) {
      return { positionalArgs: args.map(encodeValue), namedArgs: [] };
    }

    const keys = Object.keys(args);
    const isNumericKeys = keys.length > 0 && keys.every(key => /^\d+$/.test(key));

    if (isNumericKeys) {
      const sortedKeys = keys.sort((a, b) => parseInt(a) - parseInt(b));
      const maxIndex = parseInt(sortedKeys[sortedKeys.length - 1]);
      const positionalArgs: Value[] = new Array(maxIndex);
      for (const key of sortedKeys) {
        const index = parseInt(key) - 1;
        positionalArgs[index] = encodeValue(args[key]);
      }
      for (let i = 0; i < positionalArgs.length; i++) {
        if (positionalArgs[i] === undefined) {
          positionalArgs[i] = { type: 'null' };
        }
      }
      return { positionalArgs, namedArgs: [] };
    }

    const namedArgs: NamedArg[] = Object.entries(args).map(([name, value]) => ({
      name,
      value: encodeValue(value),
    }));
    return { positionalArgs: [], namedArgs };
  }
}
