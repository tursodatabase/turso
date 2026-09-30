package tech.turso.jdbc4;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import tech.turso.annotations.SkipNullableCheck;
import tech.turso.core.TursoConnection;
import tech.turso.core.TursoStatement;

/** JDBC 4 Connection implementation for Turso databases. */
public final class JDBC4Connection implements Connection {

  private final TursoConnection connection;

  private Map<String, Class<?>> typeMap = new HashMap<>();

  /**
   * Creates a new JDBC4 connection.
   *
   * @param url the database URL
   * @param filePath the database file path
   * @throws SQLException if a database access error occurs
   */
  public JDBC4Connection(String url, String filePath) throws SQLException {
    this.connection = new TursoConnection(url, filePath);
  }

  /**
   * Creates a new JDBC4 connection with properties.
   *
   * @param url the database URL
   * @param filePath the database file path
   * @param properties connection properties
   * @throws SQLException if a database access error occurs
   */
  public JDBC4Connection(String url, String filePath, Properties properties) throws SQLException {
    this.connection = new TursoConnection(url, filePath, properties);
  }

  /**
   * Prepares a SQL statement for execution.
   *
   * @param sql the SQL statement to prepare
   * @return the prepared statement
   * @throws SQLException if a database access error occurs
   */
  public TursoStatement prepare(String sql) throws SQLException {
    final TursoStatement statement = connection.prepare(sql);
    statement.initializeColumnMetadata();
    return statement;
  }

  @Override
  public Statement createStatement() throws SQLException {
    return createStatement(
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return createStatement(resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
  }

  @Override
  public Statement createStatement(
      int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    connection.checkOpen();
    connection.checkCursor(resultSetType, resultSetConcurrency, resultSetHoldability);

    return new JDBC4Statement(this);
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    return sql;
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    connection.setAutoCommit(autoCommit);
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return connection.getAutoCommit();
  }

  @Override
  public void commit() throws SQLException {
    connection.commit();
  }

  @Override
  public void rollback() throws SQLException {
    connection.rollback();
  }

  @Override
  public void close() throws SQLException {
    connection.close();
  }

  @Override
  public boolean isClosed() throws SQLException {
    return connection.isClosed();
  }

  @Override
  @SkipNullableCheck
  public DatabaseMetaData getMetaData() throws SQLException {
    return new JDBC4DatabaseMetaData(this);
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    // TODO
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    // TODO
    return false;
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {}

  @Override
  public String getCatalog() throws SQLException {
    return "";
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    connection.setTransactionIsolation(level);
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return connection.getTransactionIsolation();
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
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    return this.typeMap;
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    synchronized (this) {
      this.typeMap = map;
    }
  }

  @Override
  public int getHoldability() throws SQLException {
    connection.checkOpen();
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    connection.checkOpen();
    if (holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
      throw new SQLException("turso only supports CLOSE_CURSORS_AT_COMMIT");
    }
  }

  @Override
  @SkipNullableCheck
  public Savepoint setSavepoint() throws SQLException {
    throw new SQLFeatureNotSupportedException("Savepoints are not supported by Turso");
  }

  @Override
  @SkipNullableCheck
  public Savepoint setSavepoint(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("Savepoints are not supported by Turso");
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    throw new SQLFeatureNotSupportedException("Savepoints are not supported by Turso");
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw new SQLFeatureNotSupportedException("Savepoints are not supported by Turso");
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    return prepareCall(
        sql,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.CLOSE_CURSORS_AT_COMMIT);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return prepareCall(sql, resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
  }

  @Override
  public CallableStatement prepareCall(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    throw new SQLException("turso does not support stored procedures");
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return prepareStatement(
        sql, resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
  }

  @Override
  public PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    connection.checkOpen();
    connection.checkCursor(resultSetType, resultSetConcurrency, resultSetHoldability);
    return new JDBC4PreparedStatement(this, sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    return prepareStatement(sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    // TODO: maybe we can enhance this functionality by using columnIndexes
    return prepareStatement(sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    // TODO: maybe we can enhance this functionality by using columnNames
    return prepareStatement(sql);
  }

  @Override
  public Clob createClob() throws SQLException {
    throw new SQLFeatureNotSupportedException("createClob not supported");
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw new SQLFeatureNotSupportedException("createBlob not supported");
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw new SQLFeatureNotSupportedException("createNClob not supported");
  }

  @Override
  @SkipNullableCheck
  public SQLXML createSQLXML() throws SQLException {
    throw new SQLFeatureNotSupportedException("createSQLXML not supported");
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    if (isClosed()) {
      return false;
    }

    try (Statement statement = createStatement()) {
      return statement.execute("select 1;");
    }
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    // TODO
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    // TODO
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    // TODO
    return "";
  }

  @Override
  @SkipNullableCheck
  public Properties getClientInfo() throws SQLException {
    // TODO
    return null;
  }

  @Override
  @SkipNullableCheck
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    // TODO
    return null;
  }

  @Override
  @SkipNullableCheck
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    // TODO
  }

  @Override
  @SkipNullableCheck
  public String getSchema() throws SQLException {
    // TODO
    return "";
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    if (isClosed()) {
      return;
    }

    close();
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    // TODO
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    // TODO
    return 0;
  }

  @Override
  @SkipNullableCheck
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    // TODO
    return false;
  }

  /**
   * Sets the busy timeout for the connection.
   *
   * @param busyTimeout the timeout in milliseconds
   */
  public void setBusyTimeout(int busyTimeout) {
    // TODO: add support for busy timeout
  }

  /** @return busy timeout in milliseconds. */
  public int getBusyTimeout() {
    // TODO: add support for busyTimeout
    return 0;
  }

  /**
   * Gets the database URL.
   *
   * @return the database URL
   */
  public String getUrl() {
    return this.connection.getUrl();
  }

  /**
   * Checks if the connection is open.
   *
   * @throws SQLException if the connection is closed
   */
  public void checkOpen() throws SQLException {
    connection.checkOpen();
  }
}
