package tech.turso.jdbc4;

import static java.util.Objects.requireNonNull;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import tech.turso.annotations.SkipNullableCheck;
import tech.turso.core.TursoResultSet;

public final class JDBC4PreparedStatement extends JDBC4Statement implements PreparedStatement {

  private final String sql;
  private final JDBC4ResultSet resultSet;

  public JDBC4PreparedStatement(JDBC4Connection connection, String sql) throws SQLException {
    super(connection);
    this.sql = sql;
    this.statement = connection.prepare(sql);
    this.resultSet = new JDBC4ResultSet(this.statement.getResultSet());
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    // TODO: check bindings etc
    return this.resultSet;
  }

  @Override
  public int executeUpdate() throws SQLException {
    requireNonNull(this.statement);
    final TursoResultSet resultSet = statement.getResultSet();
    resultSet.consumeAll();
    return Math.toIntExact(statement.changes());
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    requireNonNull(this.statement);
    this.statement.bindNull(parameterIndex);
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    requireNonNull(this.statement);
    this.statement.bindInt(parameterIndex, x ? 1 : 0);
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    requireNonNull(this.statement);
    this.statement.bindInt(parameterIndex, x);
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    requireNonNull(this.statement);
    this.statement.bindInt(parameterIndex, x);
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    requireNonNull(this.statement);
    this.statement.bindInt(parameterIndex, x);
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    requireNonNull(this.statement);
    this.statement.bindLong(parameterIndex, x);
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    requireNonNull(this.statement);
    this.statement.bindDouble(parameterIndex, x);
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    requireNonNull(this.statement);
    this.statement.bindDouble(parameterIndex, x);
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    requireNonNull(this.statement);
    this.statement.bindText(parameterIndex, x.toString());
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    requireNonNull(this.statement);
    this.statement.bindText(parameterIndex, x);
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    requireNonNull(this.statement);
    this.statement.bindBlob(parameterIndex, x);
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    requireNonNull(this.statement);
    if (x == null) {
      this.statement.bindNull(parameterIndex);
    } else {
      long time = x.getTime();
      this.statement.bindBlob(
          parameterIndex, ByteBuffer.allocate(Long.BYTES).putLong(time).array());
    }
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    requireNonNull(this.statement);
    if (x == null) {
      this.statement.bindNull(parameterIndex);
    } else {
      long time = x.getTime();
      this.statement.bindBlob(
          parameterIndex, ByteBuffer.allocate(Long.BYTES).putLong(time).array());
    }
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    requireNonNull(this.statement);
    if (x == null) {
      this.statement.bindNull(parameterIndex);
    } else {
      long time = x.getTime();
      this.statement.bindBlob(
          parameterIndex, ByteBuffer.allocate(Long.BYTES).putLong(time).array());
    }
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    // TODO
  }

  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    // TODO
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    // TODO
  }

  @Override
  public void clearParameters() throws SQLException {
    // TODO
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    // TODO
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    // TODO
  }

  @Override
  public boolean execute() throws SQLException {
    // TODO: check whether this is sufficient
    requireNonNull(this.statement);
    return statement.execute();
  }

  @Override
  public void addBatch() throws SQLException {
    // TODO
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    throw new SQLException("addBatch(String) cannot be called on a PreparedStatement");
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length)
      throws SQLException {}

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
    // TODO
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    // TODO
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length)
      throws SQLException {
    // TODO
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    // TODO
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    // TODO
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    // TODO
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
