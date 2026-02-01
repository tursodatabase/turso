package tech.turso.jdbc4;

import static java.util.Objects.requireNonNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import tech.turso.annotations.Nullable;
import tech.turso.annotations.SkipNullableCheck;
import tech.turso.core.TursoResultSet;

/** JDBC 4 PreparedStatement implementation for Turso databases. */
public final class JDBC4PreparedStatement extends JDBC4Statement implements PreparedStatement {

  private final String sql;
  private final JDBC4ResultSet resultSet;

  private final int paramCount;
  private Object[] currentBatchParams;
  private final ArrayList<Object[]> batchQueryParams = new ArrayList<>();

  /**
   * Creates a new JDBC4PreparedStatement.
   *
   * @param connection the database connection
   * @param sql the SQL statement to prepare
   * @throws SQLException if a database access error occurs
   */
  public JDBC4PreparedStatement(JDBC4Connection connection, String sql) throws SQLException {
    super(connection);
    this.sql = sql;
    this.statement = connection.prepare(sql);
    this.resultSet = new JDBC4ResultSet(this.statement.getResultSet(), this);
    this.paramCount = statement.parameterCount();
    this.currentBatchParams = new Object[paramCount];
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    // TODO: check bindings etc
    bindParams(currentBatchParams);
    return this.resultSet;
  }

  @Override
  public int executeUpdate() throws SQLException {
    requireNonNull(this.statement);
    bindParams(currentBatchParams);
    final TursoResultSet resultSet = statement.getResultSet();
    resultSet.consumeAll();
    return Math.toIntExact(statement.changes());
  }

  /**
   * This helper method saves a parameter locally without binding it to the underlying native
   * statement. We have to do this so we are able to switch between different sets of parameters
   * when batching queries.
   */
  private void setParam(int parameterIndex, @Nullable Object object) {
    requireNonNull(this.statement);
    currentBatchParams[parameterIndex - 1] = object;
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    requireNonNull(this.statement);
    setParam(parameterIndex, null);
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    requireNonNull(this.statement);
    setParam(parameterIndex, x ? 1 : 0);
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    requireNonNull(this.statement);
    setParam(parameterIndex, x);
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    requireNonNull(this.statement);
    setParam(parameterIndex, x);
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    requireNonNull(this.statement);
    setParam(parameterIndex, x);
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    requireNonNull(this.statement);
    setParam(parameterIndex, x);
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    requireNonNull(this.statement);
    setParam(parameterIndex, x);
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    requireNonNull(this.statement);
    setParam(parameterIndex, x);
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    requireNonNull(this.statement);
    setParam(parameterIndex, x.toString());
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    requireNonNull(this.statement);
    setParam(parameterIndex, x);
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    requireNonNull(this.statement);
    setParam(parameterIndex, x);
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    requireNonNull(this.statement);
    if (x == null) {
      setParam(parameterIndex, null);
    } else {
      long time = x.getTime();
      setParam(parameterIndex, ByteBuffer.allocate(Long.BYTES).putLong(time).array());
    }
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    requireNonNull(this.statement);
    if (x == null) {
      setParam(parameterIndex, null);
    } else {
      long time = x.getTime();
      setParam(parameterIndex, ByteBuffer.allocate(Long.BYTES).putLong(time).array());
    }
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    requireNonNull(this.statement);
    if (x == null) {
      setParam(parameterIndex, null);
    } else {
      long time = x.getTime();
      setParam(parameterIndex, ByteBuffer.allocate(Long.BYTES).putLong(time).array());
    }
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    requireNonNull(this.statement);
    if (x == null) {
      setParam(parameterIndex, null);
      return;
    }
    if (length < 0) {
      throw new SQLException("setAsciiStream length must be non-negative");
    }
    if (length == 0) {
      setParam(parameterIndex, "");
      return;
    }
    try {
      byte[] buffer = new byte[length];
      int offset = 0;
      int read;
      while (offset < length && (read = x.read(buffer, offset, length - offset)) > 0) {
        offset += read;
      }
      String ascii = new String(buffer, 0, offset, StandardCharsets.US_ASCII);
      setParam(parameterIndex, ascii);
    } catch (IOException e) {
      throw new SQLException("Error reading ASCII stream", e);
    }
  }

  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    requireNonNull(this.statement);
    if (x == null) {
      setParam(parameterIndex, null);
      return;
    }
    if (length < 0) {
      throw new SQLException("setUnicodeStream length must be non-negative");
    }
    if (length == 0) {
      setParam(parameterIndex, "");
      return;
    }
    try {
      byte[] buffer = new byte[length];
      int offset = 0;
      int read;
      while (offset < length && (read = x.read(buffer, offset, length - offset)) > 0) {
        offset += read;
      }
      String text = new String(buffer, 0, offset, StandardCharsets.UTF_8);
      setParam(parameterIndex, text);
    } catch (IOException e) {
      throw new SQLException("Error reading Unicode stream", e);
    }
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    requireNonNull(this.statement);
    if (x == null) {
      setParam(parameterIndex, null);
      return;
    }
    if (length < 0) {
      throw new SQLException("setBinaryStream length must be non-negative");
    }
    if (length == 0) {
      setParam(parameterIndex, new byte[0]);
      return;
    }
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      byte[] buffer = new byte[8192];
      int bytesRead;
      int totalRead = 0;
      while (totalRead < length
          && (bytesRead = x.read(buffer, 0, Math.min(buffer.length, length - totalRead))) > 0) {
        baos.write(buffer, 0, bytesRead);
        totalRead += bytesRead;
      }
      byte[] data = baos.toByteArray();
      setParam(parameterIndex, data);
    } catch (IOException e) {
      throw new SQLException("Error reading binary stream", e);
    }
  }

  @Override
  public void clearParameters() {
    this.currentBatchParams = new Object[paramCount];
  }

  @Override
  public void clearBatch() throws SQLException {
    this.batchQueryParams.clear();
    this.currentBatchParams = new Object[paramCount];
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    // TODO
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    requireNonNull(this.statement);
    if (x == null) {
      setParam(parameterIndex, null);
      return;
    }
    if (x instanceof String) {
      setString(parameterIndex, (String) x);
    } else if (x instanceof Integer) {
      setInt(parameterIndex, (Integer) x);
    } else if (x instanceof Long) {
      setLong(parameterIndex, (Long) x);
    } else if (x instanceof Boolean) {
      setBoolean(parameterIndex, (Boolean) x);
    } else if (x instanceof Double) {
      setDouble(parameterIndex, (Double) x);
    } else if (x instanceof Float) {
      setFloat(parameterIndex, (Float) x);
    } else if (x instanceof Byte) {
      setByte(parameterIndex, (Byte) x);
    } else if (x instanceof Short) {
      setShort(parameterIndex, (Short) x);
    } else if (x instanceof byte[]) {
      setBytes(parameterIndex, (byte[]) x);
    } else if (x instanceof Timestamp) {
      setTimestamp(parameterIndex, (Timestamp) x);
    } else if (x instanceof Date) {
      setDate(parameterIndex, (Date) x);
    } else if (x instanceof Time) {
      setTime(parameterIndex, (Time) x);
    } else if (x instanceof BigDecimal) {
      setBigDecimal(parameterIndex, (BigDecimal) x);
    } else if (x instanceof Blob
        || x instanceof Clob
        || x instanceof InputStream
        || x instanceof Reader) {
      throw new SQLException(
          "setObject does not yet support LOB or Stream types because the corresponding set methods are unimplemented. Type found: "
              + x.getClass().getName());
    } else {
      throw new SQLException("Unsupported object type in setObject: " + x.getClass().getName());
    }
  }

  @Override
  public boolean execute() throws SQLException {
    return execute(currentBatchParams);
  }

  /** This helper method runs the statement using the provided parameter values. */
  private boolean execute(Object[] params) throws SQLException {
    // TODO: check whether this is sufficient
    requireNonNull(statement);
    bindParams(params);
    boolean result = statement.execute();
    updateCount = statement.changes();
    return result;
  }

  @Override
  public int[] executeBatch() throws SQLException {
    return Arrays.stream(executeLargeBatch()).mapToInt(l -> (int) l).toArray();
  }

  @Override
  public long[] executeLargeBatch() throws SQLException {
    requireNonNull(this.statement);
    if (batchQueryParams.isEmpty()) {
      return new long[0];
    }
    long[] updateCounts = new long[batchQueryParams.size()];
    if (!isBatchCompatibleStatement(sql)) {
      updateCounts[0] = EXECUTE_FAILED;
      BatchUpdateException bue =
          new BatchUpdateException(
              "Batch commands cannot return result sets.",
              "HY000", // General error SQL state
              0,
              Arrays.stream(updateCounts).mapToInt(l -> (int) l).toArray());
      // Clear the batch after failure
      clearBatch();
      throw bue;
    }
    for (int i = 0; i < batchQueryParams.size(); i++) {
      try {
        statement.reset();
        execute(batchQueryParams.get(i));
        updateCounts[i] = getUpdateCount();
      } catch (SQLException e) {
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
    clearBatch();
    return updateCounts;
  }

  /** Takes the given set of parameters and binds it to the underlying statement. */
  private void bindParams(Object[] params) throws SQLException {
    requireNonNull(statement);
    for (int paramIndex = 1; paramIndex <= params.length; paramIndex++) {
      statement.bindObject(paramIndex, params[paramIndex - 1]);
    }
  }

  @Override
  public void addBatch() {
    batchQueryParams.add(currentBatchParams);
    currentBatchParams = new Object[paramCount];
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    throw new SQLException("addBatch(String) cannot be called on a PreparedStatement");
  }

  @Override
  public void setCharacterStream(int parameterIndex, @Nullable Reader reader, int length)
      throws SQLException {
    requireNonNull(this.statement);
    if (reader == null) {
      setParam(parameterIndex, null);
      return;
    }
    if (length < 0) {
      throw new SQLException("setCharacterStream length must be non-negative");
    }
    if (length == 0) {
      setParam(parameterIndex, "");
      return;
    }
    try {
      char[] buffer = new char[length];
      int offset = 0;
      int read;
      while (offset < length && (read = reader.read(buffer, offset, length - offset)) > 0) {
        offset += read;
      }
      String value = new String(buffer, 0, offset);
      setParam(parameterIndex, value);
    } catch (IOException e) {
      throw new SQLException("Error reading character stream", e);
    }
  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    // TODO
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    // TODO
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    // TODO
  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {
    // TODO
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return this.resultSet;
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    setDate(parameterIndex, x);
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    setTime(parameterIndex, x);
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    // TODO: Apply calendar timezone conversion
    setTimestamp(parameterIndex, x);
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    // TODO
  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    // TODO
  }

  @Override
  @SkipNullableCheck
  public ParameterMetaData getParameterMetaData() throws SQLException {
    // TODO
    return null;
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    // TODO
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    // TODO
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length)
      throws SQLException {
    // TODO
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    // TODO
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    // TODO
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length)
      throws SQLException {
    // TODO
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    // TODO
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    // TODO
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
      throws SQLException {
    // TODO
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    requireLengthIsPositiveInt(length);
    setAsciiStream(parameterIndex, x, (int) length);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    requireLengthIsPositiveInt(length);
    setBinaryStream(parameterIndex, x, (int) length);
  }

  @Override
  public void setCharacterStream(int parameterIndex, @Nullable Reader reader, long length)
      throws SQLException {
    requireLengthIsPositiveInt(length);
    setCharacterStream(parameterIndex, reader, (int) length);
  }

  private void requireLengthIsPositiveInt(long length) throws SQLFeatureNotSupportedException {
    if (length > Integer.MAX_VALUE || length < 0) {
      throw new SQLFeatureNotSupportedException(
          "Data must have a length between 0 and Integer.MAX_VALUE");
    }
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    requireNonNull(this.statement);
    if (x == null) {
      setParam(parameterIndex, null);
      return;
    }
    byte[] data = readBytes(x);
    String ascii = new String(data, StandardCharsets.US_ASCII);
    setParam(parameterIndex, ascii);
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    requireNonNull(this.statement);
    if (x == null) {
      setParam(parameterIndex, null);
      return;
    }
    byte[] data = readBytes(x);
    setParam(parameterIndex, data);
  }

  /**
   * Reads all bytes from the given input stream.
   *
   * @param x the input stream to read
   * @return a byte array containing the data
   * @throws SQLException if an I/O error occurs while reading
   */
  private byte[] readBytes(InputStream x) throws SQLException {
    try {
      int firstByte = x.read();
      if (firstByte == -1) {
        return new byte[0];
      }
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      baos.write(firstByte);
      byte[] buffer = new byte[8192];
      int bytesRead;
      while ((bytesRead = x.read(buffer)) > 0) {
        baos.write(buffer, 0, bytesRead);
      }
      return baos.toByteArray();
    } catch (IOException e) {
      throw new SQLException("Error reading InputStream", e);
    }
  }

  @Override
  public void setCharacterStream(int parameterIndex, @Nullable Reader reader) throws SQLException {
    requireNonNull(this.statement);
    if (reader == null) {
      setParam(parameterIndex, null);
      return;
    }
    try {
      StringBuilder sb = new StringBuilder();
      char[] buffer = new char[8192];
      int read;
      while ((read = reader.read(buffer)) != -1) {
        sb.append(buffer, 0, read);
      }
      setParam(parameterIndex, sb.toString());
    } catch (IOException e) {
      throw new SQLException("Error reading character stream", e);
    }
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    // TODO
  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    // TODO
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    // TODO
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    // TODO
  }
}
