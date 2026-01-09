import { Session, type SessionConfig } from './session.js';
import { DatabaseError } from './error.js';

/**
 * Configuration options for creating a libSQL-compatible client.
 *
 * @remarks
 * This interface matches the libSQL client configuration. The `url`, `authToken`, and
 * `remoteEncryptionKey` options are supported in the serverless compatibility layer.
 * Other options will throw validation errors.
 */
export interface Config {
  /** Database URL (required) */
  url: string;
  /** Authentication token for the database */
  authToken?: string;
  /** @deprecated Local database encryption key - not supported in serverless mode */
  encryptionKey?: string;
  /**
   * Encryption key for the remote database (base64 encoded)
   * to enable access to encrypted Turso Cloud databases.
   */
  remoteEncryptionKey?: string;
  /** @deprecated Sync server URL - not supported in serverless mode */
  syncUrl?: string;
  /** @deprecated Sync frequency in seconds - not supported in serverless mode */
  syncInterval?: number;
  /** @deprecated Consistency mode - not supported in serverless mode */
  readYourWrites?: boolean;
  /** @deprecated Offline mode support - not supported in serverless mode */
  offline?: boolean;
  /** @deprecated TLS settings - not supported in serverless mode */
  tls?: boolean;
  /** @deprecated Integer handling mode - not supported in serverless mode */
  intMode?: "number" | "bigint" | "string";
  /** @deprecated Custom fetch implementation - not supported in serverless mode */
  fetch?: Function;
  /** @deprecated Concurrent request limit - not supported in serverless mode */
  concurrency?: number;
}

/** Input value types accepted by libSQL statements */
export type InValue = null | string | number | bigint | ArrayBuffer | boolean | Uint8Array | Date;

/** Input arguments - either positional array or named object */
export type InArgs = Array<InValue> | Record<string, InValue>;

/** Input statement - either SQL string or object with sql and args */
export type InStatement = { sql: string; args?: InArgs } | string;

/** Transaction execution modes */
export type TransactionMode = "write" | "read" | "deferred";

/**
 * A result row that can be accessed both as an array and as an object.
 * Supports both numeric indexing (row[0]) and column name access (row.column_name).
 */
export interface Row {
  length: number;
  [index: number]: InValue;
  [name: string]: InValue;
}

/**
 * Result set returned from SQL statement execution.
 */
export interface ResultSet {
  /** Column names in the result set */
  columns: Array<string>;
  /** Column type information */
  columnTypes: Array<string>;
  /** Result rows */
  rows: Array<Row>;
  /** Number of rows affected by the statement */
  rowsAffected: number;
  /** ID of the last inserted row (for INSERT statements) */
  lastInsertRowid: bigint | undefined;
  /** Convert result set to JSON */
  toJSON(): any;
}

/**
 * libSQL-compatible error class with error codes.
 */
export class LibsqlError extends Error {
  /** Machine-readable error code */
  code: string;
  /** Raw numeric error code (if available) */
  rawCode?: number;

  constructor(message: string, code: string, rawCode?: number) {
    super(message);
    this.name = 'LibsqlError';
    this.code = code;
    this.rawCode = rawCode;
  }
}

/**
 * Interactive transaction interface (not implemented in serverless mode).
 * 
 * @remarks
 * Transactions are not supported in the serverless compatibility layer.
 * Calling transaction() will throw a LibsqlError.
 */
export interface Transaction {
  execute(stmt: InStatement): Promise<ResultSet>;
  batch(stmts: Array<InStatement>): Promise<Array<ResultSet>>;
  executeMultiple(sql: string): Promise<void>;
  commit(): Promise<void>;
  rollback(): Promise<void>;
  close(): void;
  closed: boolean;
}

/**
 * libSQL-compatible client interface.
 * 
 * This interface matches the standard libSQL client API for drop-in compatibility.
 * Some methods are not implemented in the serverless compatibility layer.
 */
export interface Client {
  execute(stmt: InStatement): Promise<ResultSet>;
  execute(sql: string, args?: InArgs): Promise<ResultSet>;
  batch(stmts: Array<InStatement>, mode?: TransactionMode): Promise<Array<ResultSet>>;
  migrate(stmts: Array<InStatement>): Promise<Array<ResultSet>>;
  transaction(mode?: TransactionMode): Promise<Transaction>;
  executeMultiple(sql: string): Promise<void>;
  sync(): Promise<any>;
  close(): void;
  closed: boolean;
  protocol: string;
}

class LibSQLClient implements Client {
  private session: Session;
  private _closed = false;
  private _defaultSafeIntegers = false;

  constructor(config: Config) {
    this.validateConfig(config);

    const sessionConfig: SessionConfig = {
      url: config.url,
      authToken: config.authToken || '',
      remoteEncryptionKey: config.remoteEncryptionKey
    };
    this.session = new Session(sessionConfig);
  }

  private validateConfig(config: Config): void {
    // Check for unsupported config options
    const unsupportedOptions: Array<{ key: keyof Config; value: any }> = [];

    if (config.encryptionKey !== undefined) {
      unsupportedOptions.push({ key: 'encryptionKey', value: config.encryptionKey });
    }
    if (config.syncUrl !== undefined) {
      unsupportedOptions.push({ key: 'syncUrl', value: config.syncUrl });
    }
    if (config.syncInterval !== undefined) {
      unsupportedOptions.push({ key: 'syncInterval', value: config.syncInterval });
    }
    if (config.readYourWrites !== undefined) {
      unsupportedOptions.push({ key: 'readYourWrites', value: config.readYourWrites });
    }
    if (config.offline !== undefined) {
      unsupportedOptions.push({ key: 'offline', value: config.offline });
    }
    if (config.tls !== undefined) {
      unsupportedOptions.push({ key: 'tls', value: config.tls });
    }
    if (config.intMode !== undefined) {
      unsupportedOptions.push({ key: 'intMode', value: config.intMode });
    }
    if (config.fetch !== undefined) {
      unsupportedOptions.push({ key: 'fetch', value: config.fetch });
    }
    if (config.concurrency !== undefined) {
      unsupportedOptions.push({ key: 'concurrency', value: config.concurrency });
    }

    if (unsupportedOptions.length > 0) {
      const optionsList = unsupportedOptions.map(opt => `'${opt.key}'`).join(', ');
      throw new LibsqlError(
        `Unsupported configuration options: ${optionsList}. Only 'url', 'authToken', and 'remoteEncryptionKey' are supported in the serverless compatibility layer.`,
        "UNSUPPORTED_CONFIG"
      );
    }

    // Validate required options
    if (!config.url) {
      throw new LibsqlError("Missing required 'url' configuration option", "MISSING_URL");
    }
  }

  get closed(): boolean {
    return this._closed;
  }

  get protocol(): string {
    return "http";
  }

  private normalizeStatement(stmt: InStatement): { sql: string; args: any[] } {
    if (typeof stmt === 'string') {
      return { sql: stmt, args: [] };
    }
    
    const args = stmt.args || [];
    if (Array.isArray(args)) {
      return { sql: stmt.sql, args };
    }
    
    // Convert named args to positional args (simplified)
    return { sql: stmt.sql, args: Object.values(args) };
  }

  private convertResult(result: any): ResultSet {
    const resultSet: ResultSet = {
      columns: result.columns || [],
      columnTypes: result.columnTypes || [],
      rows: result.rows || [],
      rowsAffected: result.rowsAffected || 0,
      lastInsertRowid: result.lastInsertRowid ? BigInt(result.lastInsertRowid) : undefined,
      toJSON() {
        return {
          columns: this.columns,
          columnTypes: this.columnTypes,
          rows: this.rows,
          rowsAffected: this.rowsAffected,
          lastInsertRowid: this.lastInsertRowid?.toString()
        };
      }
    };

    return resultSet;
  }

  async execute(stmt: InStatement): Promise<ResultSet>;
  async execute(sql: string, args?: InArgs): Promise<ResultSet>;
  async execute(stmtOrSql: InStatement | string, args?: InArgs): Promise<ResultSet> {
    try {
      if (this._closed) {
        throw new LibsqlError("Client is closed", "CLIENT_CLOSED");
      }

      let normalizedStmt: { sql: string; args: any[] };
      
      if (typeof stmtOrSql === 'string') {
        const normalizedArgs = args ? (Array.isArray(args) ? args : Object.values(args)) : [];
        normalizedStmt = { sql: stmtOrSql, args: normalizedArgs };
      } else {
        normalizedStmt = this.normalizeStatement(stmtOrSql);
      }

      const result = await this.session.execute(normalizedStmt.sql, normalizedStmt.args, this._defaultSafeIntegers);
      return this.convertResult(result);
    } catch (error: any) {
      throw new LibsqlError(error.message, "EXECUTE_ERROR");
    }
  }

  async batch(stmts: Array<InStatement>, mode?: TransactionMode): Promise<Array<ResultSet>> {
    try {
      if (this._closed) {
        throw new LibsqlError("Client is closed", "CLIENT_CLOSED");
      }

      const sqlStatements = stmts.map(stmt => {
        const normalized = this.normalizeStatement(stmt);
        return normalized.sql; // For now, ignore args in batch
      });

      const result = await this.session.batch(sqlStatements);
      
      // Return array of result sets (simplified - actual implementation would be more complex)
      return [this.convertResult(result)];
    } catch (error: any) {
      throw new LibsqlError(error.message, "BATCH_ERROR");
    }
  }

  async migrate(stmts: Array<InStatement>): Promise<Array<ResultSet>> {
    // For now, just call batch - in a real implementation this would disable foreign keys
    return this.batch(stmts, "write");
  }

  async transaction(mode?: TransactionMode): Promise<Transaction> {
    throw new LibsqlError("Transactions not implemented", "NOT_IMPLEMENTED");
  }

  async executeMultiple(sql: string): Promise<void> {
    try {
      if (this._closed) {
        throw new LibsqlError("Client is closed", "CLIENT_CLOSED");
      }
      
      await this.session.sequence(sql);
    } catch (error: any) {
      throw new LibsqlError(error.message, "EXECUTE_MULTIPLE_ERROR");
    }
  }

  async sync(): Promise<any> {
    throw new LibsqlError("Sync not supported for remote databases", "NOT_SUPPORTED");
  }

  close(): void {
    this._closed = true;
    // Note: The libSQL client interface expects synchronous close,
    // but our underlying session needs async close. We'll fire and forget.
    this.session.close().catch(error => {
      console.error('Error closing session:', error);
    });
  }
}

/**
 * Create a libSQL-compatible client for Turso database access.
 * 
 * This function provides compatibility with the standard libSQL client API
 * while using the Turso serverless driver under the hood.
 * 
 * @param config - Configuration object (only url and authToken are supported)
 * @returns A Client instance compatible with libSQL API
 * @throws LibsqlError if unsupported configuration options are provided
 * 
 * @example
 * ```typescript
 * import { createClient } from "@tursodatabase/serverless/compat";
 * 
 * const client = createClient({
 *   url: process.env.TURSO_DATABASE_URL,
 *   authToken: process.env.TURSO_AUTH_TOKEN
 * });
 * 
 * const result = await client.execute("SELECT * FROM users");
 * console.log(result.rows);
 * ```
 */
export function createClient(config: Config): Client {
  return new LibSQLClient(config);
}