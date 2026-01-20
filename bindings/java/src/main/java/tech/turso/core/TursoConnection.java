package tech.turso.core;

import static tech.turso.utils.ByteArrayUtils.stringToUtf8ByteArray;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import tech.turso.annotations.NativeInvocation;
import tech.turso.utils.Logger;
import tech.turso.utils.LoggerFactory;
import tech.turso.utils.TursoExceptionUtils;

public final class TursoConnection {

  private static final Logger logger = LoggerFactory.getLogger(TursoConnection.class);

  private final String url;
  private final long connectionPtr;
  private final TursoDB database;
  private boolean closed;

  // Transaction state fields
  private boolean autoCommit = true;
  private int transactionIsolation = Connection.TRANSACTION_SERIALIZABLE;
  private boolean inTransaction = false;
  private final Object transactionLock = new Object();

  public TursoConnection(String url, String filePath) throws SQLException {
    this(url, filePath, new Properties());
  }

  /**
   * Creates a connection to turso database
   *
   * @param url e.g. "jdbc:turso:fileName"
   * @param filePath path to file
   */
  public TursoConnection(String url, String filePath, Properties properties) throws SQLException {
    this.url = url;
    this.database = open(url, filePath, properties);
    this.connectionPtr = this.database.connect();
  }

  /**
   * Creates a connection using an existing TursoDB instance.
   * This is useful for encrypted databases created with TursoDB.createWithEncryption().
   *
   * @param url e.g. "jdbc:turso:fileName"
   * @param database an existing TursoDB instance
   */
  public TursoConnection(String url, TursoDB database) throws SQLException {
    this.url = url;
    this.database = database;
    this.connectionPtr = this.database.connect();
  }

  private static TursoDB open(String url, String filePath, Properties properties)
      throws SQLException {
    return TursoDB.create(url, filePath);
  }

  public void checkOpen() throws SQLException {
    if (isClosed()) throw new SQLException("database connection closed");
  }

  public String getUrl() {
    return url;
  }

  public void close() throws SQLException {
    if (isClosed()) {
      return;
    }

    // Roll back any pending transaction before closing
    synchronized (transactionLock) {
      if (inTransaction) {
        try {
          executeInternal("ROLLBACK");
        } catch (SQLException e) {
          // Log but don't throw - we're closing anyway
          logger.warn("Failed to rollback transaction during close", e);
        } finally {
          inTransaction = false;
        }
      }
    }

    this._close(this.connectionPtr);
    this.closed = true;
  }

  private native void _close(long connectionPtr);

  private native boolean _getAutoCommit(long connectionPtr);

  public boolean isClosed() throws SQLException {
    return closed;
  }

  public TursoDB getDatabase() {
    return database;
  }

  /**
   * Compiles an SQL statement.
   *
   * @param sql An SQL statement.
   * @return Pointer to statement.
   * @throws SQLException if a database access error occurs.
   */
  public TursoStatement prepare(String sql) throws SQLException {
    return prepare(sql, true);
  }

  /**
   * Compiles an SQL statement with optional transaction start check.
   *
   * @param sql An SQL statement.
   * @param checkTransaction Whether to check and start transaction if needed.
   * @return Pointer to statement.
   * @throws SQLException if a database access error occurs.
   */
  private TursoStatement prepare(String sql, boolean checkTransaction) throws SQLException {
    logger.trace("DriverManager [{}] [SQLite EXEC] {}", Thread.currentThread().getName(), sql);

    // Ensure transaction is started if needed (lazy transaction start)
    if (checkTransaction) {
      ensureTransactionStarted(sql);
    }

    byte[] sqlBytes = stringToUtf8ByteArray(sql);
    if (sqlBytes == null) {
      throw new SQLException("Failed to convert " + sql + " into bytes");
    }
    return new TursoStatement(sql, prepareUtf8(connectionPtr, sqlBytes));
  }

  private native long prepareUtf8(long connectionPtr, byte[] sqlUtf8) throws SQLException;

  // TODO: check whether this is still valid for turso
  /**
   * Checks whether the type, concurrency, and holdability settings for a {@link ResultSet} are
   * supported by the SQLite interface. Supported settings are:
   *
   * <ul>
   *   <li>type: {@link ResultSet#TYPE_FORWARD_ONLY}
   *   <li>concurrency: {@link ResultSet#CONCUR_READ_ONLY})
   *   <li>holdability: {@link ResultSet#CLOSE_CURSORS_AT_COMMIT}
   * </ul>
   *
   * @param resultSetType the type setting.
   * @param resultSetConcurrency the concurrency setting.
   * @param resultSetHoldability the holdability setting.
   */
  public void checkCursor(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
      throw new SQLException("SQLite only supports TYPE_FORWARD_ONLY cursors");
    }
    if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
      throw new SQLException("SQLite only supports CONCUR_READ_ONLY cursors");
    }
    if (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
      throw new SQLException("SQLite only supports closing cursors at commit");
    }
  }

  /**
   * Sets the auto-commit mode for this connection.
   *
   * <p>When auto-commit is enabled (the default), each SQL statement is committed automatically
   * upon completion. When auto-commit is disabled, statements are grouped into transactions that
   * must be explicitly committed or rolled back.
   *
   * <p>If this method is called to enable auto-commit while a transaction is active, the current
   * transaction is committed first.
   *
   * @param autoCommit true to enable auto-commit mode; false to disable it
   * @throws SQLException if a database access error occurs or the connection is closed
   */
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    synchronized (transactionLock) {
      checkOpen();

      if (this.autoCommit == autoCommit) {
        return; // No-op if already in desired mode
      }

      // If enabling autocommit and there's a pending transaction, commit it
      if (autoCommit && inTransaction) {
        commit();
      }

      this.autoCommit = autoCommit;
    }
  }

  /**
   * Gets the current auto-commit mode for this connection.
   *
   * @return true if auto-commit mode is enabled; false otherwise
   * @throws SQLException if a database access error occurs or the connection is closed
   */
  public boolean getAutoCommit() throws SQLException {
    checkOpen();
    return autoCommit;
  }

  /**
   * Commits the current transaction.
   *
   * @throws SQLException if in auto-commit mode, closed, or database error occurs.
   */
  public void commit() throws SQLException {
    synchronized (transactionLock) {
      checkOpen();
      if (autoCommit) {
        throw new SQLException("Cannot commit in autocommit mode.");
      }

      if (inTransaction) {
        executeInternal("COMMIT");
        inTransaction = false;
      }
    }
  }

  /**
   * Rolls back the current transaction.
   *
   * @throws SQLException if in auto-commit mode, closed, or database error occurs.
   */
  public void rollback() throws SQLException {
    synchronized (transactionLock) {
      checkOpen();
      if (autoCommit) {
        throw new SQLException("Cannot rollback in autocommit mode.");
      }

      if (inTransaction) {
        executeInternal("ROLLBACK");
        inTransaction = false;
      }
    }
  }

  /**
   * Lazy transaction starter. Starts a transaction if one isn't active, auto-commit is disabled,
   * and the statement isn't a control command.
   */
  private void ensureTransactionStarted(String sql) throws SQLException {
    if (autoCommit || inTransaction) return;

    // Avoid recursive start for control statements
    String trimmed = sql.trim().toUpperCase();
    if (trimmed.startsWith("BEGIN")
        || trimmed.startsWith("COMMIT")
        || trimmed.startsWith("ROLLBACK")) {
      return;
    }

    String beginMode = TursoTransactionMode.fromIsolationLevel(transactionIsolation).getSql();
    executeInternal(beginMode);
    inTransaction = true;
  }

  /**
   * Internal helper to execute transaction control statements without triggering recursive
   * transaction checks.
   */
  private void executeInternal(String sql) throws SQLException {
    try (TursoStatement stmt = prepare(sql, false)) {
      stmt.execute();
    }
  }

  /**
   * Sets the transaction isolation level.
   *
   * @param level one of the following {@code Connection} constants: {@code
   *     Connection.TRANSACTION_READ_UNCOMMITTED}, {@code Connection.TRANSACTION_READ_COMMITTED},
   *     {@code Connection.TRANSACTION_REPEATABLE_READ}, or {@code
   *     Connection.TRANSACTION_SERIALIZABLE}.
   * @throws SQLException if a database access error occurs, this method is called on a closed
   *     connection or the given parameter is not one of the {@code Connection} constants
   */
  public void setTransactionIsolation(int level) throws SQLException {
    synchronized (transactionLock) {
      checkOpen();

      if (inTransaction) {
        throw new SQLException("Cannot change isolation level while transaction is active.");
      }

      if (level != Connection.TRANSACTION_READ_UNCOMMITTED
          && level != Connection.TRANSACTION_READ_COMMITTED
          && level != Connection.TRANSACTION_REPEATABLE_READ
          && level != Connection.TRANSACTION_SERIALIZABLE) {
        throw new SQLException("Invalid transaction isolation level: " + level);
      }

      this.transactionIsolation = level;
    }
  }

  /**
   * Retrieves the current transaction isolation level.
   *
   * @return the current transaction isolation level
   * @throws SQLException if a database access error occurs or the connection is closed
   */
  public int getTransactionIsolation() throws SQLException {
    checkOpen();
    return transactionIsolation;
  }

  /**
   * Throws formatted SQLException with error code and message.
   *
   * @param errorCode Error code.
   * @param errorMessageBytes Error message.
   */
  @NativeInvocation(invokedFrom = "turso_connection.rs")
  private void throwTursoException(int errorCode, byte[] errorMessageBytes) throws SQLException {
    TursoExceptionUtils.throwTursoException(errorCode, errorMessageBytes);
  }
}
