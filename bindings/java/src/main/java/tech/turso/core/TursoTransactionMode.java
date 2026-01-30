package tech.turso.core;

import java.sql.Connection;

/**
 * Defines the transaction modes supported by Turso (SQLite).
 *
 * <p>Transactions can be DEFERRED, IMMEDIATE, or EXCLUSIVE. The default behavior is DEFERRED.
 */
public enum TursoTransactionMode {

  /**
   * The transaction does not actually start until the database is first accessed.
   *
   * <p>Internally, the {@code BEGIN DEFERRED} statement merely sets a flag on the database
   * connection that turns off the automatic commit that would normally occur when the last
   * statement finishes.
   *
   * <p>If the first statement after {@code BEGIN DEFERRED} is a {@code SELECT}, then a read
   * transaction is started. Subsequent write statements will upgrade the transaction to a write
   * transaction if possible, or return {@code SQLITE_BUSY}. This maximizes concurrency for
   * read-heavy workloads.
   */
  DEFERRED("BEGIN DEFERRED"),

  /**
   * Causes the database connection to start a new write immediately, without waiting for a write
   * statement.
   *
   * <p>The {@code BEGIN IMMEDIATE} might fail with {@code SQLITE_BUSY} if another write transaction
   * is already active on another database connection.
   *
   * <p>This prevents other connections from writing to the database, effectively serializing write
   * transactions and ensuring a stable snapshot for the duration of this transaction.
   */
  IMMEDIATE("BEGIN IMMEDIATE"),

  /**
   * Causes the database connection to start a new write immediately and prevents other database
   * connections from reading out of the database while the transaction is underway.
   */
  EXCLUSIVE("BEGIN EXCLUSIVE");

  private final String sql;

  TursoTransactionMode(String sql) {
    this.sql = sql;
  }

  public String getSql() {
    return sql;
  }

  /**
   * Determine the transaction mode based on the JDBC transaction isolation level.
   *
   * <p>
   *
   * <ul>
   *   <li>{@link Connection#TRANSACTION_READ_UNCOMMITTED} -> {@link #DEFERRED}
   *   <li>{@link Connection#TRANSACTION_READ_COMMITTED} -> {@link #DEFERRED}
   *   <li>{@link Connection#TRANSACTION_REPEATABLE_READ} -> {@link #IMMEDIATE}
   *   <li>{@link Connection#TRANSACTION_SERIALIZABLE} -> {@link #IMMEDIATE}
   * </ul>
   *
   * @param level the JDBC transaction isolation level
   * @return the corresponding TursoTransactionMode
   */
  public static TursoTransactionMode fromIsolationLevel(int level) {
    if (level == Connection.TRANSACTION_READ_UNCOMMITTED
        || level == Connection.TRANSACTION_READ_COMMITTED) {
      return DEFERRED;
    }
    return IMMEDIATE;
  }
}
