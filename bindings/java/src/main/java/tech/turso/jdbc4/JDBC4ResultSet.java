package tech.turso.jdbc4;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import tech.turso.annotations.Nullable;
import tech.turso.annotations.SkipNullableCheck;
import tech.turso.core.TursoResultSet;

public final class JDBC4ResultSet implements ResultSet, ResultSetMetaData {

  private final TursoResultSet resultSet;

  public JDBC4ResultSet(TursoResultSet resultSet) {
    this.resultSet = resultSet;
  }

  @Override
  public boolean next() throws SQLException {
    return resultSet.next();
  }

  @Override
  public void close() throws SQLException {
    resultSet.close();
  }

  @Override
  public boolean wasNull() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @Nullable
  public String getString(int columnIndex) throws SQLException {
    final Object result = resultSet.get(columnIndex);
    if (result == null) {
      return null;
    }
    return wrapTypeConversion(() -> (String) result);
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    final Object result = resultSet.get(columnIndex);
    if (result == null) {
      return false;
    }
    return wrapTypeConversion(() -> (Long) result != 0);
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    final Object result = resultSet.get(columnIndex);
    if (result == null) {
      return 0;
    }
    return wrapTypeConversion(() -> ((Long) result).byteValue());
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    final Object result = resultSet.get(columnIndex);
    if (result == null) {
      return 0;
    }
    return wrapTypeConversion(() -> ((Long) result).shortValue());
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    final Object result = resultSet.get(columnIndex);
    if (result == null) {
      return 0;
    }
    return wrapTypeConversion(() -> ((Long) result).intValue());
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    final Object result = resultSet.get(columnIndex);
    if (result == null) {
      return 0;
    }
    return wrapTypeConversion(() -> (long) result);
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    final Object result = resultSet.get(columnIndex);
    if (result == null) {
      return 0;
    }
    return wrapTypeConversion(() -> ((Double) result).floatValue());
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    final Object result = resultSet.get(columnIndex);
    if (result == null) {
      return 0;
    }
    return wrapTypeConversion(() -> (double) result);
  }

  // TODO: customize rounding mode?
  @Override
  @Nullable
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    final Object result = resultSet.get(columnIndex);
    if (result == null) {
      return null;
    }
    final double doubleResult = wrapTypeConversion(() -> (double) result);
    final BigDecimal bigDecimalResult = BigDecimal.valueOf(doubleResult);
    return bigDecimalResult.setScale(scale, RoundingMode.HALF_UP);
  }

  @Override
  @Nullable
  public byte[] getBytes(int columnIndex) throws SQLException {
    final Object result = resultSet.get(columnIndex);
    if (result == null) {
      return null;
    }
    return wrapTypeConversion(() -> (byte[]) result);
  }

  @Override
  @Nullable
  public Date getDate(int columnIndex) throws SQLException {
    final Object result = resultSet.get(columnIndex);
    if (result == null) {
      return null;
    }
    return wrapTypeConversion(
        () -> {
          if (result instanceof byte[]) {
            byte[] bytes = (byte[]) result;
            if (bytes.length == Long.BYTES) {
              long time = ByteBuffer.wrap(bytes).getLong();
              return new Date(time);
            }
          }
          throw new SQLException("Cannot convert value to Date: " + result.getClass());
        });
  }

  @Override
  @SkipNullableCheck
  public Time getTime(int columnIndex) throws SQLException {
    final Object result = resultSet.get(columnIndex);
    if (result == null) {
      return null;
    }
    return wrapTypeConversion(
        () -> {
          if (result instanceof byte[]) {
            byte[] bytes = (byte[]) result;
            if (bytes.length == Long.BYTES) {
              long time = ByteBuffer.wrap(bytes).getLong();
              return new Time(time);
            }
          }
          throw new SQLException("Cannot convert value to Date: " + result.getClass());
        });
  }

  @Override
  @SkipNullableCheck
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    final Object result = resultSet.get(columnIndex);
    if (result == null) {
      return null;
    }
    return wrapTypeConversion(
        () -> {
          if (result instanceof byte[]) {
            byte[] bytes = (byte[]) result;
            if (bytes.length == Long.BYTES) {
              long time = ByteBuffer.wrap(bytes).getLong();
              return new Timestamp(time);
            }
          }
          throw new SQLException("Cannot convert value to Timestamp: " + result.getClass());
        });
  }

  @Override
  @SkipNullableCheck
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    final Object result = this.resultSet.get(columnLabel);
    if (result == null) {
      return "";
    }

    return wrapTypeConversion(() -> (String) result);
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @Nullable
  public Date getDate(String columnLabel) throws SQLException {
    final Object result = resultSet.get(columnLabel);
    if (result == null) {
      return null;
    }
    return wrapTypeConversion(
        () -> {
          if (result instanceof byte[]) {
            byte[] bytes = (byte[]) result;
            if (bytes.length == Long.BYTES) {
              long time = ByteBuffer.wrap(bytes).getLong();
              return new Date(time);
            }
          }
          // Try to parse as string if it's stored as TEXT
          if (result instanceof String) {
            return Date.valueOf((String) result);
          }
          throw new SQLException("Cannot convert value to Date: " + result.getClass());
        });
  }

  @Override
  @SkipNullableCheck
  public Time getTime(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return getTimestamp(findColumn(columnLabel));
  }

  @Override
  @SkipNullableCheck
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public SQLWarning getWarnings() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void clearWarnings() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public String getCursorName() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return this;
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    return resultSet.get(columnIndex);
  }

  @Override
  @SkipNullableCheck
  public Object getObject(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @Nullable
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    final Object result = resultSet.get(columnIndex);
    if (result == null) {
      return null;
    }
    final double doubleResult = wrapTypeConversion(() -> (double) result);
    return BigDecimal.valueOf(doubleResult);
  }

  @Override
  @SkipNullableCheck
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean isFirst() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean isLast() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void beforeFirst() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void afterLast() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean first() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean last() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public int getRow() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean previous() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public int getFetchDirection() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public int getFetchSize() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public int getType() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public int getConcurrency() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean rowInserted() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length)
      throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void insertRow() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateRow() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void deleteRow() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void refreshRow() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public Statement getStatement() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public Ref getRef(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public Blob getBlob(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public Clob getClob(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public Array getArray(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public Ref getRef(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public Blob getBlob(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public Clob getClob(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public Array getArray(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @Nullable
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    // TODO: Properly handle timezone conversion with Calendar
    return getDate(columnIndex);
  }

  @Override
  @Nullable
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    // TODO: Properly handle timezone conversion with Calendar
    return getDate(columnLabel);
  }

  @Override
  @SkipNullableCheck
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    // TODO: Properly handle timezone conversion with Calendar
    return getTime(columnIndex);
  }

  @Override
  @SkipNullableCheck
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    // TODO: Properly handle timezone conversion with Calendar
    return getTime(columnLabel);
  }

  @Override
  @SkipNullableCheck
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    // TODO: Apply calendar timezone conversion
    return getTimestamp(columnIndex);
  }

  @Override
  @SkipNullableCheck
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    // TODO: Apply calendar timezone conversion
    return getTimestamp(findColumn(columnLabel));
  }

  @Override
  @SkipNullableCheck
  public URL getURL(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public URL getURL(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public RowId getRowId(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public RowId getRowId(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean isClosed() throws SQLException {
    return !resultSet.isOpen();
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public NClob getNClob(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public NClob getNClob(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length)
      throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  @SkipNullableCheck
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public int getColumnCount() throws SQLException {
    return this.resultSet.getColumnNames().length;
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public int isNullable(int column) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    return Integer.MAX_VALUE;
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    // TODO: should consider "AS" keyword
    return getColumnName(column);
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    if (column > 0 && column <= resultSet.getColumnNames().length) {
      return resultSet.getColumnNames()[column - 1];
    }

    throw new SQLException("Index out of bound: " + column);
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public int getScale(int column) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public String getTableName(int column) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    throw new UnsupportedOperationException("not implemented");
  }

  @FunctionalInterface
  public interface ResultSetSupplier<T> {
    T get() throws Exception;
  }

  private <T> T wrapTypeConversion(ResultSetSupplier<T> supplier) throws SQLException {
    try {
      return supplier.get();
    } catch (Exception e) {
      throw new SQLException("Type conversion failed: " + e);
    }
  }
}
