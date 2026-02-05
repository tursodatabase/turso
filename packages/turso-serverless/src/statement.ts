import { 
  decodeValue, 
  type CursorEntry,
  type Column
} from './protocol.js';
import { Session, type SessionConfig } from './session.js';
import { DatabaseError } from './error.js';

/**
 * A prepared SQL statement that can be executed in multiple ways.
 * 
 * Each statement has its own session to avoid conflicts during concurrent execution.
 * Provides three execution modes:
 * - `get(args?)`: Returns the first row or null
 * - `all(args?)`: Returns all rows as an array
 * - `iterate(args?)`: Returns an async iterator for streaming results
 */
export class Statement {
  private session: Session;
  private sql: string;
  private presentationMode: 'expanded' | 'raw' | 'pluck' = 'expanded';
  private safeIntegerMode: boolean = false;
  private columnMetadata: Column[];

  constructor(sessionConfig: SessionConfig, sql: string, columns?: Column[]) {
    this.session = new Session(sessionConfig);
    this.sql = sql;
    this.columnMetadata = columns || [];
  }


  /**
   * Enable raw mode to return arrays instead of objects.
   * 
   * @param raw Enable or disable raw mode. If you don't pass the parameter, raw mode is enabled.
   * @returns This statement instance for chaining
   * 
   * @example
   * ```typescript
   * const stmt = client.prepare("SELECT * FROM users WHERE id = ?");
   * const row = await stmt.raw().get([1]);
   * console.log(row); // [1, "Alice", "alice@example.org"]
   * ```
   */
  raw(raw?: boolean): Statement {
    this.presentationMode = raw === false ? 'expanded' : 'raw';
    return this;
  }

  /**
   * Enable pluck mode to return only the first column value from each row.
   * 
   * @param pluck Enable or disable pluck mode. If you don't pass the parameter, pluck mode is enabled.
   * @returns This statement instance for chaining
   * 
   * @example
   * ```typescript
   * const stmt = client.prepare("SELECT id FROM users");
   * const ids = await stmt.pluck().all();
   * console.log(ids); // [1, 2, 3, ...]
   * ```
   */
  pluck(pluck?: boolean): Statement {
    this.presentationMode = pluck === false ? 'expanded' : 'pluck';
    return this;
  }

  /**
   * Sets safe integers mode for this statement.
   * 
   * @param toggle Whether to use safe integers. If you don't pass the parameter, safe integers mode is enabled.
   * @returns This statement instance for chaining
   */
  safeIntegers(toggle?: boolean): Statement {
    this.safeIntegerMode = toggle === false ? false : true;
    return this;
  }

  /**
   * Get column information for this statement.
   * 
   * @returns Array of column metadata objects matching the native bindings format
   * 
   * @example
   * ```typescript
   * const stmt = await client.prepare("SELECT id, name, email FROM users");
   * const columns = stmt.columns();
   * console.log(columns); // [{ name: 'id', type: 'INTEGER', column: null, database: null, table: null }, ...]
   * ```
   */
  columns(): any[] {
    return this.columnMetadata.map(col => ({
      name: col.name,
      type: col.decltype
    }));
  }

  /**
   * Executes the prepared statement.
   * 
   * @param args - Optional array of parameter values or object with named parameters
   * @returns Promise resolving to the result of the statement
   * 
   * @example
   * ```typescript
   * const stmt = client.prepare("INSERT INTO users (name, email) VALUES (?, ?)");
   * const result = await stmt.run(['John Doe', 'john.doe@example.com']);
   * console.log(`Inserted user with ID ${result.lastInsertRowid}`);
   * ```
   */
  async run(args?: any): Promise<any> {
    const normalizedArgs = this.normalizeArgs(args);
    const result = await this.session.execute(this.sql, normalizedArgs, this.safeIntegerMode);
    return { changes: result.rowsAffected, lastInsertRowid: result.lastInsertRowid };
  }

  /**
   * Execute the statement and return the first row.
   * 
   * @param args - Optional array of parameter values or object with named parameters
   * @returns Promise resolving to the first row or undefined if no results
   * 
   * @example
   * ```typescript
   * const stmt = client.prepare("SELECT * FROM users WHERE id = ?");
   * const user = await stmt.get([123]);
   * if (user) {
   *   console.log(user.name);
   * }
   * ```
   */
  async get(args?: any): Promise<any> {
    const normalizedArgs = this.normalizeArgs(args);
    const result = await this.session.execute(this.sql, normalizedArgs, this.safeIntegerMode);
    const row = result.rows[0];
    if (!row) {
      return undefined;
    }
    
    if (this.presentationMode === 'pluck') {
      // In pluck mode, return only the first column value
      return row[0];
    }
    
    if (this.presentationMode === 'raw') {
      // In raw mode, return the row as a plain array (it already is one)
      // The row object is already an array with column properties added
      return [...row];
    }
    
    // In expanded mode, convert to plain object with named properties  
    const obj: any = {};
    result.columns.forEach((col: string, i: number) => {
      obj[col] = row[i];
    });
    return obj;
  }

  /**
   * Execute the statement and return all rows.
   * 
   * @param args - Optional array of parameter values or object with named parameters
   * @returns Promise resolving to an array of all result rows
   * 
   * @example
   * ```typescript
   * const stmt = client.prepare("SELECT * FROM users WHERE active = ?");
   * const activeUsers = await stmt.all([true]);
   * console.log(`Found ${activeUsers.length} active users`);
   * ```
   */
  async all(args?: any): Promise<any[]> {
    const normalizedArgs = this.normalizeArgs(args);
    const result = await this.session.execute(this.sql, normalizedArgs, this.safeIntegerMode);
    
    if (this.presentationMode === 'pluck') {
      // In pluck mode, return only the first column value from each row
      return result.rows.map((row: any) => row[0]);
    }
    
    if (this.presentationMode === 'raw') {
      return result.rows.map((row: any) => [...row]);
    }
    
    // In expanded mode, convert rows to plain objects with named properties
    return result.rows.map((row: any) => {
      const obj: any = {};
      result.columns.forEach((col: string, i: number) => {
        obj[col] = row[i];
      });
      return obj;
    });
  }

  /**
   * Execute the statement and return an async iterator for streaming results.
   * 
   * This method provides memory-efficient processing of large result sets
   * by streaming rows one at a time instead of loading everything into memory.
   * 
   * @param args - Optional array of parameter values or object with named parameters
   * @returns AsyncGenerator that yields individual rows
   * 
   * @example
   * ```typescript
   * const stmt = client.prepare("SELECT * FROM large_table WHERE category = ?");
   * for await (const row of stmt.iterate(['electronics'])) {
   *   // Process each row individually
   *   console.log(row.id, row.name);
   * }
   * ```
   */
  async *iterate(args?: any): AsyncGenerator<any> {
    const normalizedArgs = this.normalizeArgs(args);
    const { response, entries } = await this.session.executeRaw(this.sql, normalizedArgs);
    
    let columns: string[] = [];
    
    for await (const entry of entries) {
      switch (entry.type) {
        case 'step_begin':
          if (entry.cols) {
            columns = entry.cols.map(col => col.name);
          }
          break;
        case 'row':
          if (entry.row) {
            const decodedRow = entry.row.map(value => decodeValue(value, this.safeIntegerMode));
            if (this.presentationMode === 'pluck') {
              // In pluck mode, yield only the first column value
              yield decodedRow[0];
            } else if (this.presentationMode === 'raw') {
              // In raw mode, yield arrays of values
              yield decodedRow;
            } else {
              // In expanded mode, yield plain objects with named properties (consistent with all())
              const obj: any = {};
              columns.forEach((col: string, i: number) => {
                obj[col] = decodedRow[i];
              });
              yield obj;
            }
          }
          break;
        case 'step_error':
        case 'error':
          throw new DatabaseError(entry.error?.message || 'SQL execution failed');
      }
    }
  }

  /**
   * Normalize arguments to handle both single values and arrays.
   * Matches the behavior of the native bindings.
   */
  private normalizeArgs(args: any): any[] | Record<string, any> {
    // No arguments provided
    if (args === undefined) {
      return [];
    }
    
    // If it's an array, return as-is
    if (Array.isArray(args)) {
      return args;
    }
    
    // Check if it's a plain object (for named parameters)
    if (args !== null && typeof args === 'object' && args.constructor === Object) {
      return args;
    }
    
    // Single value - wrap in array
    return [args];
  }
}
