import {
  executeCursor,
  executePipeline,
  encodeValue,
  decodeValue,
  type CursorRequest,
  type CursorResponse,
  type CursorEntry,
  type PipelineRequest,
  type ExecuteRequest,
  type BatchRequest,
  type BatchResult,
  type SequenceRequest,
  type CloseRequest,
  type DescribeRequest,
  type DescribeResult,
  type ExecuteResult,
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
  private _keepAlive: boolean = false;

  constructor(config: SessionConfig) {
    this.config = config;
    this.baseUrl = normalizeUrl(config.url);
  }

  /**
   * Set keepAlive mode. When true, the stream baton is preserved across
   * requests (for transactions). When false, each pipeline includes a
   * close request and resets the baton.
   */
  setKeepAlive(keepAlive: boolean): void {
    this._keepAlive = keepAlive;
  }

  private createAbortSignal(queryOptions?: QueryOptions): AbortSignal | undefined {
    const timeout = queryOptions?.queryTimeout ?? this.config.defaultQueryTimeout;
    if (timeout != null && timeout > 0) {
      return AbortSignal.timeout(timeout);
    }
    return undefined;
  }

  /**
   * Encode arguments (array or named-parameter object) into positional/named
   * Value arrays for the Hrana protocol.
   */
  private encodeArgs(args: any[] | Record<string, any>): { positionalArgs: Value[]; namedArgs: NamedArg[] } {
    let positionalArgs: Value[] = [];
    let namedArgs: NamedArg[] = [];

    if (Array.isArray(args)) {
      positionalArgs = args.map(encodeValue);
    } else {
      const keys = Object.keys(args);
      const isNumericKeys = keys.length > 0 && keys.every(key => /^\d+$/.test(key));

      if (isNumericKeys) {
        const sortedKeys = keys.sort((a, b) => parseInt(a) - parseInt(b));
        const maxIndex = parseInt(sortedKeys[sortedKeys.length - 1]);
        positionalArgs = new Array(maxIndex);
        for (const key of sortedKeys) {
          const index = parseInt(key) - 1;
          positionalArgs[index] = encodeValue(args[key]);
        }
        for (let i = 0; i < positionalArgs.length; i++) {
          if (positionalArgs[i] === undefined) {
            positionalArgs[i] = { type: 'null' };
          }
        }
      } else {
        namedArgs = Object.entries(args).map(([name, value]) => ({
          name,
          value: encodeValue(value)
        }));
      }
    }

    return { positionalArgs, namedArgs };
  }

  /**
   * Send a pipeline request, updating baton/baseUrl from the response.
   * When not in keepAlive mode, appends a close request and resets the
   * baton afterward.
   */
  private async sendPipeline(
    requests: PipelineRequest['requests'],
    queryOptions?: QueryOptions
  ): Promise<import('./protocol.js').PipelineResponse> {
    const pipelineRequests = [...requests];

    if (!this._keepAlive) {
      pipelineRequests.push({ type: 'close' } as CloseRequest);
    }

    const request: PipelineRequest = {
      baton: this._keepAlive ? this.baton : null,
      requests: pipelineRequests,
    };

    let response;
    try {
      response = await executePipeline(
        this.baseUrl,
        this.config.authToken,
        request,
        this.config.remoteEncryptionKey,
        this.createAbortSignal(queryOptions)
      );
    } catch (e) {
      this.baton = null;
      throw e;
    }

    if (this._keepAlive) {
      this.baton = response.baton;
    } else {
      this.baton = null;
    }

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
      queryOptions
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
   * Uses pipeline (not cursor) so the stream is properly closed.
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
        const execResult = result.response.result as ExecuteResult;
        return this.processExecuteResult(execResult, safeIntegers);
      }
    }

    throw new DatabaseError('Unexpected execute response');
  }

  /**
   * Execute a SQL statement and return the raw response and entries.
   * Uses cursor for streaming support.
   */
  async executeRaw(sql: string, args: any[] | Record<string, any> = [], queryOptions?: QueryOptions): Promise<{ response: CursorResponse; entries: AsyncGenerator<CursorEntry> }> {
    const { positionalArgs, namedArgs } = this.encodeArgs(args);

    const request: CursorRequest = {
      baton: this._keepAlive ? this.baton : null,
      batch: {
        steps: [{
          stmt: {
            sql,
            args: positionalArgs,
            named_args: namedArgs,
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

    if (this._keepAlive) {
      this.baton = response.baton;
    } else {
      // Wrap the generator to null the baton after consumption
      const self = this;
      const originalEntries = entries;
      const wrappedEntries = (async function* () {
        try {
          yield* originalEntries;
        } finally {
          self.baton = null;
        }
      })();
      if (response.base_url) {
        this.baseUrl = response.base_url;
      }
      return { response, entries: wrappedEntries };
    }

    if (response.base_url) {
      this.baseUrl = response.base_url;
    }

    return { response, entries };
  }

  /**
   * Convert an ExecuteResult from the pipeline into the same shape
   * that processCursorEntries returns.
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
   */
  createRowObject(values: any[], columns: string[]): any {
    const row = [...values];

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
   * Uses pipeline (not cursor) so the stream is properly closed.
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
        const batchResult = result.response.result as BatchResult;
        return this.processBatchResult(batchResult);
      }
    }

    throw new DatabaseError('Unexpected batch response');
  }

  /**
   * Convert a BatchResult from the pipeline into the summary shape.
   */
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
   */
  async sequence(sql: string, queryOptions?: QueryOptions): Promise<void> {
    const response = await this.sendPipeline(
      [{ type: 'sequence', sql } as SequenceRequest],
      queryOptions
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
   */
  async close(): Promise<void> {
    if (this.baton) {
      try {
        const request: PipelineRequest = {
          baton: this.baton,
          requests: [{ type: 'close' } as CloseRequest],
        };
        await executePipeline(this.baseUrl, this.config.authToken, request, this.config.remoteEncryptionKey);
      } catch {
        // Ignore errors during close
      }
    }

    this.baton = null;
    this.baseUrl = '';
  }
}
