/* auto-generated by NAPI-RS */
/* eslint-disable */
/** A database connection. */
export declare class Database {
  /**
   * Creates a new database instance.
   *
   * # Arguments
   * * `path` - The path to the database file.
   */
  constructor(path: string)
  /** Returns whether the database is in memory-only mode. */
  get memory(): boolean
  /**
   * Executes a batch of SQL statements.
   *
   * # Arguments
   *
   * * `sql` - The SQL statements to execute.
   *
   * # Returns
   */
  batch(sql: string): void
  /**
   * Prepares a statement for execution.
   *
   * # Arguments
   *
   * * `sql` - The SQL statement to prepare.
   *
   * # Returns
   *
   * A `Statement` instance.
   */
  prepare(sql: string): Statement
  /**
   * Returns the rowid of the last row inserted.
   *
   * # Returns
   *
   * The rowid of the last row inserted.
   */
  lastInsertRowid(): number
  /**
   * Returns the number of changes made by the last statement.
   *
   * # Returns
   *
   * The number of changes made by the last statement.
   */
  changes(): number
  /**
   * Returns the total number of changes made by all statements.
   *
   * # Returns
   *
   * The total number of changes made by all statements.
   */
  totalChanges(): number
  /**
   * Closes the database connection.
   *
   * # Returns
   *
   * `Ok(())` if the database is closed successfully.
   */
  close(): void
  /** Runs the I/O loop synchronously. */
  ioLoopSync(): void
  /** Runs the I/O loop asynchronously, returning a Promise. */
  ioLoopAsync(): Promise<void>
}

/** A prepared statement. */
export declare class Statement {
  reset(): void
  /** Returns the number of parameters in the statement. */
  parameterCount(): number
  /**
   * Returns the name of a parameter at a specific 1-based index.
   *
   * # Arguments
   *
   * * `index` - The 1-based parameter index.
   */
  parameterName(index: number): string | null
  /**
   * Binds a parameter at a specific 1-based index with explicit type.
   *
   * # Arguments
   *
   * * `index` - The 1-based parameter index.
   * * `value_type` - The type constant (0=null, 1=int, 2=float, 3=text, 4=blob).
   * * `value` - The value to bind.
   */
  bindAt(index: number, value: unknown): void
  /**
   * Step the statement and return result code:
   * 1 = Row available, 2 = Done, 3 = I/O needed
   */
  step(): number
  /** Get the current row data according to the presentation mode */
  row(): unknown
  /** Sets the presentation mode to raw. */
  raw(raw?: boolean | undefined | null): void
  /** Sets the presentation mode to pluck. */
  pluck(pluck?: boolean | undefined | null): void
  /** Finalizes the statement. */
  finalize(): void
}
