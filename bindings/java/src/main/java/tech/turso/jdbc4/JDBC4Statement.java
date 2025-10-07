package tech.turso.jdbc4;

import static java.util.Objects.requireNonNull;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import tech.turso.annotations.Nullable;
import tech.turso.annotations.SkipNullableCheck;
import tech.turso.core.TursoResultSet;
import tech.turso.core.TursoStatement;

/**
 * JDBC 4 Statement implementation for Turso databases.
 */
public class JDBC4Statement implements Statement {

  private static final Pattern BATCH_COMPATIBLE_PATTERN =
      Pattern.compile(
          "^\\s*"
              + // Leading whitespace
              "(?:/\\*.*?\\*/\\s*)*"
              + // Optional C-style comments
              "(?:--[^\\n]*\\n\\s*)*"
              + // Optional SQL line comments
              "(?:"
              + // Start of keywords group
              "INSERT|UPDATE|DELETE"
              + ")\\b",
          Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private final JDBC4Connection connection;

  /** The underlying Turso statement. */
  @Nullable protected TursoStatement statement = null;

  /** The number of rows affected by the last update operation. */
  protected long updateCount;

  // Because JDBC4Statement has different life cycle in compared to tursoStatement, let's use this
  // field to manage JDBC4Statement lifecycle
  private boolean closed;
  private boolean closeOnCompletion;

  private final int resultSetType;
  private final int resultSetConcurrency;
  private final int resultSetHoldability;

  private int queryTimeoutSeconds;

  private ReentrantLock connectionLock = new ReentrantLock();

  /**
   * List of SQL statements to be executed as a batch. Used for batch processing as per JDBC
   * specification.
   */
  private List<String> batchCommands = new ArrayList<>();

  public JDBC4Statement(JDBC4Connection connection) {
    this(
        connection,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.CLOSE_CURSORS_AT_COMMIT);
  }

  public JDBC4Statement(
      JDBC4Connection connection,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability) {
    this.connection = connection;
    this.resultSetType = resultSetType;
    this.resultSetConcurrency = resultSetConcurrency;
    this.resultSetHoldability = resultSetHoldability;
  }

  // TODO: should executeQuery run execute right after preparing the statement?
  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    ensureOpen();
    statement =
        this.withConnectionTimeout(
            () -> {
              try {
                // TODO: if sql is a readOnly query, do we still need the locks?
                connectionLock.lock();
                return connection.prepare(sql);
              } finally {
                connectionLock.unlock();
              }
            });

    requireNonNull(statement, "statement should not be null after running execute method");
    return new JDBC4ResultSet(statement.getResultSet());
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    final long previousTotalChanges = statement == null ? 0L : statement.totalChanges();

    execute(sql);
    requireNonNull(statement, "statement should not be null after running execute method");
    final TursoResultSet resultSet = statement.getResultSet();
    resultSet.consumeAll();

    return (int) (statement.totalChanges() - previousTotalChanges);
  }

  @Override
  public void close() throws SQLException {
    if (closed) {
      return;
    }

    if (this.statement != null) {
      this.statement.close();
    }

    closed = true;
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    // TODO
    return 0;
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    // TODO
  }

  @Override
  public int getMaxRows() throws SQLException {
    // TODO
    return 0;
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    // TODO
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    // TODO
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    // TODO
    return 0;
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    if (seconds < 0) {
      throw new SQLException("Query timeout must be greater than 0");
    }
    this.queryTimeoutSeconds = seconds;
  }

  @Override
  public void cancel() throws SQLException {
    // TODO
  }

  @Override
  @SkipNullableCheck
  public SQLWarning getWarnings() throws SQLException {
    // TODO
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
    // TODO
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    // TODO
  }

  /**
   * The <code>execute</code> method executes an SQL statement and indicates the form of the first
   * result. You must then use the methods <code>getResultSet</code> or <code>getUpdateCount</code>
   * to retrieve the result, and <code>getMoreResults</code> to move to any subsequent result(s).
   */
  @Override
  public boolean execute(String sql) throws SQLException {
    ensureOpen();
    return this.withConnectionTimeout(
        () -> {
          try {
            // TODO: if sql is a readOnly query, do we still need the locks?
            connectionLock.lock();
            statement = connection.prepare(sql);
            final long previousChanges = statement.totalChanges();
            final boolean result = statement.execute();
            updateGeneratedKeys();
            updateCount = statement.totalChanges() - previousChanges;

            return result;
          } finally {
            connectionLock.unlock();
          }
        });
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    requireNonNull(statement, "statement is null");
    ensureOpen();
    return new JDBC4ResultSet(statement.getResultSet());
  }

  @Override
  public int getUpdateCount() throws SQLException {
    return (int) updateCount;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    // TODO
  }

  @Override
  public int getFetchDirection() throws SQLException {
    // TODO
    return 0;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    // TODO
  }

  @Override
  public int getFetchSize() throws SQLException {
    // TODO
    return 0;
  }

  @Override
  public int getResultSetConcurrency() {
    return resultSetConcurrency;
  }

  @Override
  public int getResultSetType() {
    return resultSetType;
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    ensureOpen();
    if (sql == null) {
      throw new SQLException("SQL command cannot be null");
    }
    batchCommands.add(sql);
  }

  @Override
  public void clearBatch() throws SQLException {
    ensureOpen();
    batchCommands.clear();
  }

  // TODO: let's make this batch operation atomic
  @Override
  public int[] executeBatch() throws SQLException {
    ensureOpen();

    int[] updateCounts = new int[batchCommands.size()];
    List<String> failedCommands = new ArrayList<>();

    // Execute each command in the batch
    for (int i = 0; i < batchCommands.size(); i++) {
      String sql = batchCommands.get(i);
      try {
        if (!isBatchCompatibleStatement(sql)) {
          failedCommands.add(sql);
          updateCounts[i] = EXECUTE_FAILED;
          BatchUpdateException bue =
              new BatchUpdateException(
                  "Batch entry "
                      + i
                      + " ("
                      + sql
                      + ") was aborted. "
                      + "Batch commands cannot return result sets.",
                  "HY000", // General error SQL state
                  0,
                  updateCounts);
          // Clear the batch after failure
          clearBatch();
          throw bue;
        }

        execute(sql);
        // For DML statements, get the update count
        updateCounts[i] = getUpdateCount();
      } catch (SQLException e) {
        failedCommands.add(sql);
        updateCounts[i] = EXECUTE_FAILED;

        // Create a BatchUpdateException with the partial results
        BatchUpdateException bue =
            new BatchUpdateException(
                "Batch entry " + i + " (" + sql + ") failed: " + e.getMessage(),
                e.getSQLState(),
                e.getErrorCode(),
                updateCounts,
                e.getCause());
        // Clear the batch after failure
        clearBatch();
        throw bue;
      }
    }

    // Clear the batch after successful execution
    clearBatch();
    return updateCounts;
  }

  boolean isBatchCompatibleStatement(String sql) {
    if (sql == null || sql.trim().isEmpty()) {
      return false;
    }

    return BATCH_COMPATIBLE_PATTERN.matcher(sql).find();
  }

  @Override
  public Connection getConnection() {
    return connection;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    return getMoreResults(Statement.CLOSE_CURRENT_RESULT);
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    requireNonNull(statement, "statement should not be null");

    if (current != Statement.CLOSE_CURRENT_RESULT) {
      throw new SQLException("Invalid argument");
    }

    statement.getResultSet().close();
    updateCount = -1;

    return false;
  }

  @Override
  @SkipNullableCheck
  public ResultSet getGeneratedKeys() throws SQLException {
    // TODO
    return null;
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    // TODO: enhance
    return executeUpdate(sql);
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    // TODO: enhance
    return executeUpdate(sql);
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    // TODO: enhance
    return executeUpdate(sql);
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    // TODO: enhance
    return execute(sql);
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    // TODO: enhance
    return execute(sql);
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    // TODO
    return false;
  }

  @Override
  public int getResultSetHoldability() {
    return resultSetHoldability;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return this.closed;
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    // TODO
  }

  @Override
  public boolean isPoolable() throws SQLException {
    // TODO
    return false;
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    if (closed) {
      throw new SQLException("statement is closed");
    }
    closeOnCompletion = true;
  }

  /**
   * Indicates whether the statement should be closed automatically when all its dependent result
   * sets are closed.
   */
  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    if (closed) {
      throw new SQLException("statement is closed");
    }
    return closeOnCompletion;
  }

  @Override
  @SkipNullableCheck
  public <T> T unwrap(Class<T> iface) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    // TODO
    return false;
  }

  protected void updateGeneratedKeys() throws SQLException {
    // TODO
  }

  private <T> T withConnectionTimeout(SQLCallable<T> callable) throws SQLException {
    final int originalBusyTimeoutMillis = connection.getBusyTimeout();
    if (queryTimeoutSeconds > 0) {
      // TODO: set busy timeout
      connection.setBusyTimeout(1000 * queryTimeoutSeconds);
    }

    try {
      return callable.call();
    } finally {
      if (queryTimeoutSeconds > 0) {
        connection.setBusyTimeout(originalBusyTimeoutMillis);
      }
    }
  }

  /**
   * Functional interface for SQL callable operations.
   *
   * @param <T> the return type
   */
  @FunctionalInterface
  protected interface SQLCallable<T> {
    /**
     * Executes the SQL operation.
     *
     * @return the result of the operation
     * @throws SQLException if a database access error occurs
     */
    T call() throws SQLException;
  }

  private void ensureOpen() throws SQLException {
    if (closed) {
      throw new SQLException("Statement is closed");
    }
  }
}
