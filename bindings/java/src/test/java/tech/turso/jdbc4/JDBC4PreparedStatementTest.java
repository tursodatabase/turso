package tech.turso.jdbc4;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.turso.TestUtils;

class JDBC4PreparedStatementTest {

  private JDBC4Connection connection;

  @BeforeEach
  void setUp() throws Exception {
    String filePath = TestUtils.createTempFile();
    String url = "jdbc:turso:" + filePath;
    connection = new JDBC4Connection(url, filePath, new Properties());
  }

  @Test
  void testSetBoolean() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col INTEGER)").execute();
    PreparedStatement stmt =
        connection.prepareStatement("INSERT INTO test (col) VALUES (?), (?), (?)");
    stmt.setBoolean(1, true);
    stmt.setBoolean(2, false);
    stmt.setBoolean(3, true);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT * FROM test;");
    ResultSet rs = stmt2.executeQuery();
    assertTrue(rs.next());
    assertTrue(rs.getBoolean(1));
    assertTrue(rs.next());
    assertFalse(rs.getBoolean(1));
    assertTrue(rs.next());
    assertTrue(rs.getBoolean(1));
  }

  @Test
  void testSetByte() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col INTEGER)").execute();
    PreparedStatement stmt =
        connection.prepareStatement("INSERT INTO test (col) VALUES (?), (?), (?)");
    stmt.setByte(1, (byte) 1);
    stmt.setByte(2, (byte) 2);
    stmt.setByte(3, (byte) 3);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT * FROM test;");
    ResultSet rs = stmt2.executeQuery();
    assertTrue(rs.next());
    assertEquals(1, rs.getByte(1));
    assertTrue(rs.next());
    assertEquals(2, rs.getByte(1));
    assertTrue(rs.next());
    assertEquals(3, rs.getByte(1));
  }

  @Test
  void testSetShort() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col INTEGER)").execute();
    PreparedStatement stmt =
        connection.prepareStatement("INSERT INTO test (col) VALUES (?), (?), (?)");
    stmt.setShort(1, (short) 1);
    stmt.setShort(2, (short) 2);
    stmt.setShort(3, (short) 3);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT * FROM test;");
    ResultSet rs = stmt2.executeQuery();
    assertTrue(rs.next());
    assertEquals(1, rs.getShort(1));
    assertTrue(rs.next());
    assertEquals(2, rs.getShort(1));
    assertTrue(rs.next());
    assertEquals(3, rs.getShort(1));
  }

  @Test
  void testSetInt() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col INTEGER)").execute();
    PreparedStatement stmt =
        connection.prepareStatement("INSERT INTO test (col) VALUES (?), (?), (?)");
    stmt.setInt(1, 1);
    stmt.setInt(2, 2);
    stmt.setInt(3, 3);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT * FROM test;");
    ResultSet rs = stmt2.executeQuery();
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertTrue(rs.next());
    assertEquals(3, rs.getInt(1));
  }

  @Test
  void testSetLong() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col INTEGER)").execute();
    PreparedStatement stmt =
        connection.prepareStatement("INSERT INTO test (col) VALUES (?), (?), (?)");
    stmt.setLong(1, 1L);
    stmt.setLong(2, 2L);
    stmt.setLong(3, 3L);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT * FROM test;");
    ResultSet rs = stmt2.executeQuery();
    assertTrue(rs.next());
    assertEquals(1L, rs.getLong(1));
    assertTrue(rs.next());
    assertEquals(2L, rs.getLong(1));
    assertTrue(rs.next());
    assertEquals(3L, rs.getLong(1));
  }

  @Test
  void testSetFloat() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col REAL)").execute();
    PreparedStatement stmt =
        connection.prepareStatement("INSERT INTO test (col) VALUES (?), (?), (?)");
    stmt.setFloat(1, 1.0f);
    stmt.setFloat(2, 2.0f);
    stmt.setFloat(3, 3.0f);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT * FROM test;");
    ResultSet rs = stmt2.executeQuery();
    assertTrue(rs.next());
    assertEquals(1.0f, rs.getFloat(1));
    assertTrue(rs.next());
    assertEquals(2.0f, rs.getFloat(1));
    assertTrue(rs.next());
    assertEquals(3.0f, rs.getFloat(1));
  }

  @Test
  void testSetDouble() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col REAL)").execute();
    PreparedStatement stmt =
        connection.prepareStatement("INSERT INTO test (col) VALUES (?), (?), (?)");
    stmt.setDouble(1, 1.0);
    stmt.setDouble(2, 2.0);
    stmt.setDouble(3, 3.0);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT * FROM test;");
    ResultSet rs = stmt2.executeQuery();
    assertTrue(rs.next());
    assertEquals(1.0, rs.getDouble(1));
    assertTrue(rs.next());
    assertEquals(2.0, rs.getDouble(1));
    assertTrue(rs.next());
    assertEquals(3.0, rs.getDouble(1));
  }

  @Test
  void testSetBigDecimal() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();
    PreparedStatement stmt =
        connection.prepareStatement("INSERT INTO test (col) VALUES (?), (?), (?)");
    stmt.setBigDecimal(1, new BigDecimal("1.0"));
    stmt.setBigDecimal(2, new BigDecimal("2.0"));
    stmt.setBigDecimal(3, new BigDecimal("3.0"));
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT * FROM test;");
    ResultSet rs = stmt2.executeQuery();
    assertTrue(rs.next());
    assertEquals("1.0", rs.getString(1));
    assertTrue(rs.next());
    assertEquals("2.0", rs.getString(1));
    assertTrue(rs.next());
    assertEquals("3.0", rs.getString(1));
  }

  @Test
  void testSetString() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();
    PreparedStatement stmt =
        connection.prepareStatement("INSERT INTO test (col) VALUES (?), (?), (?)");
    stmt.setString(1, "test1");
    stmt.setString(2, "test2");
    stmt.setString(3, "test3");
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT * FROM test;");
    ResultSet rs = stmt2.executeQuery();
    assertTrue(rs.next());
    assertEquals("test1", rs.getString(1));
    assertTrue(rs.next());
    assertEquals("test2", rs.getString(1));
    assertTrue(rs.next());
    assertEquals("test3", rs.getString(1));
  }

  @Test
  void testSetBytes() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col BLOB)").execute();
    PreparedStatement stmt =
        connection.prepareStatement("INSERT INTO test (col) VALUES (?), (?), (?)");
    stmt.setBytes(1, new byte[] {1, 2, 3});
    stmt.setBytes(2, new byte[] {4, 5, 6});
    stmt.setBytes(3, new byte[] {7, 8, 9});
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT * FROM test;");
    ResultSet rs = stmt2.executeQuery();
    assertTrue(rs.next());
    assertArrayEquals(new byte[] {1, 2, 3}, rs.getBytes(1));
    assertTrue(rs.next());
    assertArrayEquals(new byte[] {4, 5, 6}, rs.getBytes(1));
    assertTrue(rs.next());
    assertArrayEquals(new byte[] {7, 8, 9}, rs.getBytes(1));
  }

  @Test
  void testSetDate() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col BLOB)").execute();
    PreparedStatement stmt =
        connection.prepareStatement("INSERT INTO test (col) VALUES (?), (?), (?)");

    Date date1 = new Date(1000000000000L);
    Date date2 = new Date(1500000000000L);
    Date date3 = new Date(2000000000000L);

    stmt.setDate(1, date1);
    stmt.setDate(2, date2);
    stmt.setDate(3, date3);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT * FROM test;");
    JDBC4ResultSet rs = (JDBC4ResultSet) stmt2.executeQuery();

    assertTrue(rs.next());
    assertEquals(date1, rs.getDate(1));
    assertTrue(rs.next());
    assertEquals(date2, rs.getDate(1));
    assertTrue(rs.next());
    assertEquals(date3, rs.getDate(1));
  }

  @Test
  void testSetTime() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col BLOB)").execute();
    PreparedStatement stmt =
        connection.prepareStatement("INSERT INTO test (col) VALUES (?), (?), (?)");

    Time time1 = new Time(1000000000000L);
    Time time2 = new Time(1500000000000L);
    Time time3 = new Time(2000000000000L);

    stmt.setTime(1, time1);
    stmt.setTime(2, time2);
    stmt.setTime(3, time3);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT * FROM test;");
    JDBC4ResultSet rs = (JDBC4ResultSet) stmt2.executeQuery();

    assertTrue(rs.next());
    assertEquals(time1, rs.getTime(1));
    assertTrue(rs.next());
    assertEquals(time2, rs.getTime(1));
    assertTrue(rs.next());
    assertEquals(time3, rs.getTime(1));
  }

  @Test
  void testSetTimestamp() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col BLOB)").execute();
    PreparedStatement stmt =
        connection.prepareStatement("INSERT INTO test (col) VALUES (?), (?), (?)");

    Timestamp timestamp1 = new Timestamp(1000000000000L);
    Timestamp timestamp2 = new Timestamp(1500000000000L);
    Timestamp timestamp3 = new Timestamp(2000000000000L);

    stmt.setTimestamp(1, timestamp1);
    stmt.setTimestamp(2, timestamp2);
    stmt.setTimestamp(3, timestamp3);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT * FROM test;");
    JDBC4ResultSet rs = (JDBC4ResultSet) stmt2.executeQuery();

    assertTrue(rs.next());
    assertEquals(timestamp1, rs.getTimestamp(1));
    assertTrue(rs.next());
    assertEquals(timestamp2, rs.getTimestamp(1));
    assertTrue(rs.next());
    assertEquals(timestamp3, rs.getTimestamp(1));
  }

  @Test
  void testInsertMultipleTypes() throws SQLException {
    connection
        .prepareStatement("CREATE TABLE test (col1 INTEGER, col2 REAL, col3 TEXT, col4 BLOB)")
        .execute();
    PreparedStatement stmt =
        connection.prepareStatement(
            "INSERT INTO test (col1, col2, col3, col4) VALUES (?, ?, ?, ?), (?, ?, ?, ?)");

    stmt.setInt(1, 1);
    stmt.setFloat(2, 1.1f);
    stmt.setString(3, "row1");
    stmt.setBytes(4, new byte[] {1, 2, 3});

    stmt.setInt(5, 2);
    stmt.setFloat(6, 2.2f);
    stmt.setString(7, "row2");
    stmt.setBytes(8, new byte[] {4, 5, 6});

    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT * FROM test;");
    ResultSet rs = stmt2.executeQuery();

    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals(1.1f, rs.getFloat(2));
    assertEquals("row1", rs.getString(3));
    assertArrayEquals(new byte[] {1, 2, 3}, rs.getBytes(4));

    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertEquals(2.2f, rs.getFloat(2));
    assertEquals("row2", rs.getString(3));
    assertArrayEquals(new byte[] {4, 5, 6}, rs.getBytes(4));
  }

  @Test
  void testSetObjectCoversAllSupportedTypes() throws SQLException {
    connection
        .prepareStatement(
            "CREATE TABLE test ("
                + "col1 INTEGER, "
                + "col2 REAL, "
                + "col3 TEXT, "
                + "col4 BLOB, "
                + "col5 INTEGER, "
                + "col6 TEXT, "
                + "col7 TEXT, "
                + "col8 TEXT, "
                + "col9 TEXT"
                + ")")
        .execute();

    PreparedStatement stmt =
        connection.prepareStatement("INSERT INTO test VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");

    stmt.setObject(1, 42);
    stmt.setObject(2, 3.141592d);
    stmt.setObject(3, "string_value");
    stmt.setObject(4, new byte[] {1, 2, 3});
    stmt.setObject(5, 1L);
    stmt.setObject(6, java.sql.Date.valueOf("2025-10-30"));
    stmt.setObject(7, java.sql.Time.valueOf("10:45:00"));
    stmt.setObject(8, java.sql.Timestamp.valueOf("2025-10-30 10:45:00"));
    stmt.setObject(9, new java.math.BigDecimal("12345.6789"));

    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT * FROM test;");
    ResultSet rs = stmt2.executeQuery();

    assertTrue(rs.next());
    assertEquals(42, rs.getInt(1));
    assertEquals(3.141592d, rs.getDouble(2), 0.000001);
    assertEquals("string_value", rs.getString(3));
    assertArrayEquals(new byte[] {1, 2, 3}, rs.getBytes(4));
    assertTrue(rs.getBoolean(5));
    assertEquals(java.sql.Date.valueOf("2025-10-30"), rs.getDate(6));
    assertEquals(java.sql.Time.valueOf("10:45:00"), rs.getTime(7));
    assertEquals(java.sql.Timestamp.valueOf("2025-10-30 10:45:00"), rs.getTimestamp(8));
    String decimalText = rs.getString(9);
    assertEquals(
        new java.math.BigDecimal("12345.6789").stripTrailingZeros(),
        new java.math.BigDecimal(decimalText).stripTrailingZeros());
  }

  @Test
  void testSetAsciiStream_intLength_insert_and_select() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();

    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    String text = "test";
    byte[] bytes = text.getBytes(StandardCharsets.US_ASCII);
    InputStream stream = new ByteArrayInputStream(bytes);

    stmt.setAsciiStream(1, stream, bytes.length);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT col FROM test");
    ResultSet rs = stmt2.executeQuery();

    assertTrue(rs.next());
    assertEquals(text, rs.getString(1));
  }

  @Test
  void testSetAsciiStream_intLength_nullStream() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();

    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    stmt.setAsciiStream(1, null, 0);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT col FROM test");
    ResultSet rs = stmt2.executeQuery();

    assertTrue(rs.next());
    assertNull(rs.getString(1));
  }

  @Test
  void testSetAsciiStream_intLength_emptyStream() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();

    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");
    InputStream empty = new ByteArrayInputStream(new byte[0]);

    stmt.setAsciiStream(1, empty, 10);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT col FROM test");
    ResultSet rs = stmt2.executeQuery();

    assertTrue(rs.next());
    assertEquals("", rs.getString(1));
  }

  @Test
  void testSetAsciiStream_intLength_negativeLength() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();

    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    String text = "test";
    byte[] bytes = text.getBytes(StandardCharsets.US_ASCII);
    InputStream stream = new ByteArrayInputStream(bytes);

    assertThrows(SQLException.class, () -> stmt.setAsciiStream(1, stream, -1));
  }

  @Test
  void testSetBinaryStream_intLength_insert_and_select() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col BLOB)").execute();

    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    byte[] data = {1, 2, 3, 4, 5};
    InputStream stream = new ByteArrayInputStream(data);

    stmt.setBinaryStream(1, stream, data.length);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT col FROM test");
    ResultSet rs = stmt2.executeQuery();

    assertTrue(rs.next());
    assertArrayEquals(data, rs.getBytes(1));
  }

  @Test
  void testSetBinaryStream_intLength_nullStream() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col BLOB)").execute();

    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    stmt.setBinaryStream(1, null, 0);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT col FROM test");
    ResultSet rs = stmt2.executeQuery();

    assertTrue(rs.next());
    assertNull(rs.getBytes(1));
  }

  @Test
  void testSetBinaryStream_intLength_emptyStream() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col BLOB)").execute();

    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    InputStream empty = new ByteArrayInputStream(new byte[0]);
    stmt.setBinaryStream(1, empty, 10);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT col FROM test");
    ResultSet rs = stmt2.executeQuery();

    assertTrue(rs.next());

    byte[] result = rs.getBytes(1);
    assertNotNull(result);
    assertEquals(0, result.length);
    assertArrayEquals(new byte[0], result);
  }

  @Test
  void testSetBinaryStream_intLength_negativeLength() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col BLOB)").execute();

    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    byte[] data = {1, 2, 3};
    InputStream stream = new ByteArrayInputStream(data);

    assertThrows(SQLException.class, () -> stmt.setBinaryStream(1, stream, -1));
  }

  @Test
  void testSetUnicodeStream_intLength_insert_and_select() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();

    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    String text = "안녕하세요😊 Hello🌏";
    byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
    InputStream stream = new ByteArrayInputStream(bytes);

    stmt.setUnicodeStream(1, stream, bytes.length);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT col FROM test");
    ResultSet rs = stmt2.executeQuery();

    assertTrue(rs.next());
    assertEquals(text, rs.getString(1));
  }

  @Test
  void testSetUnicodeStream_intLength_nullStream() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();

    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    stmt.setUnicodeStream(1, null, 0);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT col FROM test");
    ResultSet rs = stmt2.executeQuery();

    assertTrue(rs.next());
    assertNull(rs.getString(1));
  }

  @Test
  void testSetUnicodeStream_intLength_emptyStream() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();

    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");
    InputStream empty = new ByteArrayInputStream(new byte[0]);

    stmt.setUnicodeStream(1, empty, 10);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT col FROM test");
    ResultSet rs = stmt2.executeQuery();

    assertTrue(rs.next());
    assertEquals("", rs.getString(1));
  }

  @Test
  void testSetUnicodeStream_intLength_negativeLength() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();

    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    String text = "테스트";
    byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
    InputStream stream = new ByteArrayInputStream(bytes);

    assertThrows(SQLException.class, () -> stmt.setUnicodeStream(1, stream, -5));
  }

  @Test
  void testSetAsciiStream_longLength_insert_and_select() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();

    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    String text = "test";
    byte[] bytes = text.getBytes(StandardCharsets.US_ASCII);
    InputStream stream = new ByteArrayInputStream(bytes);

    stmt.setAsciiStream(1, stream, (long) bytes.length);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT col FROM test");
    ResultSet rs = stmt2.executeQuery();

    assertTrue(rs.next());
    assertEquals(text, rs.getString(1));
  }

  @Test
  void testSetAsciiStream_longLength_nullStream() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();

    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    stmt.setAsciiStream(1, null, 0L);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT col FROM test");
    ResultSet rs = stmt2.executeQuery();

    assertTrue(rs.next());
    assertNull(rs.getString(1));
  }

  @Test
  void testSetAsciiStream_longLength_emptyStream() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();

    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");
    InputStream empty = new ByteArrayInputStream(new byte[0]);

    stmt.setAsciiStream(1, empty, 0L);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT col FROM test");
    ResultSet rs = stmt2.executeQuery();

    assertTrue(rs.next());
    assertEquals("", rs.getString(1));
  }

  @Test
  void testSetAsciiStream_longLength_negative() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();

    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");
    InputStream stream = new ByteArrayInputStream("test".getBytes(StandardCharsets.US_ASCII));

    assertThrows(SQLFeatureNotSupportedException.class, () -> stmt.setAsciiStream(1, stream, -1L));
  }

  @Test
  void testSetAsciiStream_longLength_overflow() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();

    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");
    InputStream stream = new ByteArrayInputStream("test".getBytes(StandardCharsets.US_ASCII));

    assertThrows(
        SQLFeatureNotSupportedException.class,
        () -> stmt.setAsciiStream(1, stream, (long) Integer.MAX_VALUE + 1));
  }

  @Test
  void testSetBinaryStream_longLength_insert_and_select() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col BLOB)").execute();

    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    byte[] data = {1, 2, 3, 4, 5};
    InputStream stream = new ByteArrayInputStream(data);

    stmt.setBinaryStream(1, stream, (long) data.length);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT col FROM test");
    ResultSet rs = stmt2.executeQuery();

    assertTrue(rs.next());
    assertArrayEquals(data, rs.getBytes(1));
  }

  @Test
  void testSetBinaryStream_longLength_nullStream() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col BLOB)").execute();

    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    stmt.setBinaryStream(1, null, 0L);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT col FROM test");
    ResultSet rs = stmt2.executeQuery();

    assertTrue(rs.next());
    assertNull(rs.getBytes(1));
  }

  @Test
  void testSetBinaryStream_longLength_emptyStream() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col BLOB)").execute();

    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");
    InputStream empty = new ByteArrayInputStream(new byte[0]);

    stmt.setBinaryStream(1, empty, 0L);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT col FROM test");
    ResultSet rs = stmt2.executeQuery();

    assertTrue(rs.next());

    byte[] result = rs.getBytes(1);
    assertNotNull(result);
    assertEquals(0, result.length);
    assertArrayEquals(new byte[0], result);
  }

  @Test
  void testSetBinaryStream_longLength_negative() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col BLOB)").execute();

    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");
    InputStream stream = new ByteArrayInputStream(new byte[] {1, 2, 3});

    assertThrows(SQLFeatureNotSupportedException.class, () -> stmt.setBinaryStream(1, stream, -1L));
  }

  @Test
  void testSetBinaryStream_longLength_overflow() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col BLOB)").execute();

    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");
    InputStream stream = new ByteArrayInputStream(new byte[] {1, 2, 3});

    assertThrows(
        SQLFeatureNotSupportedException.class,
        () -> stmt.setBinaryStream(1, stream, (long) Integer.MAX_VALUE + 1));
  }

  @Test
  void testSetAsciiStream_noLength_insert_and_select() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();
    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    String text = "test";
    byte[] bytes = text.getBytes(StandardCharsets.US_ASCII);
    InputStream stream = new ByteArrayInputStream(bytes);

    stmt.setAsciiStream(1, stream);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT col FROM test");
    ResultSet rs = stmt2.executeQuery();

    assertTrue(rs.next());
    assertEquals(text, rs.getString(1));
  }

  @Test
  void testSetAsciiStream_noLength_nullStream() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();
    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    stmt.setAsciiStream(1, null);
    stmt.execute();

    ResultSet rs = connection.prepareStatement("SELECT col FROM test").executeQuery();
    assertTrue(rs.next());
    assertNull(rs.getString(1));
  }

  @Test
  void testSetAsciiStream_noLength_emptyStream() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();
    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    InputStream empty = new ByteArrayInputStream(new byte[0]);
    stmt.setAsciiStream(1, empty);
    stmt.execute();

    ResultSet rs = connection.prepareStatement("SELECT col FROM test").executeQuery();
    assertTrue(rs.next());
    assertEquals("", rs.getString(1));
  }

  @Test
  void testSetBinaryStream_noLength_insert_and_select() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col BLOB)").execute();
    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    byte[] data = {1, 2, 3, 4, 5};
    InputStream stream = new ByteArrayInputStream(data);

    stmt.setBinaryStream(1, stream);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT col FROM test");
    ResultSet rs = stmt2.executeQuery();

    assertTrue(rs.next());
    assertArrayEquals(data, rs.getBytes(1));
  }

  @Test
  void testSetBinaryStream_noLength_nullStream() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col BLOB)").execute();
    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    stmt.setBinaryStream(1, null);
    stmt.execute();

    ResultSet rs = connection.prepareStatement("SELECT col FROM test").executeQuery();
    assertTrue(rs.next());
    assertNull(rs.getBytes(1));
  }

  @Test
  void testSetBinaryStream_noLength_emptyStream() throws SQLException {
    connection.prepareStatement("CREATE TABLE test (col BLOB)").execute();
    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    InputStream empty = new ByteArrayInputStream(new byte[0]);
    stmt.setBinaryStream(1, empty);
    stmt.execute();

    ResultSet rs = connection.prepareStatement("SELECT col FROM test").executeQuery();
    assertTrue(rs.next());

    byte[] result = rs.getBytes(1);
    assertNotNull(result);
    assertEquals(0, result.length);
  }

  @Test
  void testSetCharacterStream_intLength_insert_and_select() throws Exception {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();
    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    String text = "test";
    Reader reader = new StringReader(text);

    stmt.setCharacterStream(1, reader, 4);
    stmt.execute();

    ResultSet rs = connection.prepareStatement("SELECT col FROM test").executeQuery();

    assertTrue(rs.next());
    assertEquals("test", rs.getString(1));
  }

  @Test
  void testSetCharacterStream_intLength_shorterThanLength() throws Exception {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();
    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    Reader reader = new StringReader("test");
    stmt.setCharacterStream(1, reader, 5);

    stmt.execute();
    ResultSet rs = connection.prepareStatement("SELECT col FROM test").executeQuery();

    assertTrue(rs.next());
    assertEquals("test", rs.getString(1));
  }

  @Test
  void testSetCharacterStream_intLength_zero() throws Exception {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();
    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    Reader reader = new StringReader("test");
    stmt.setCharacterStream(1, reader, 0);

    stmt.execute();
    ResultSet rs = connection.prepareStatement("SELECT col FROM test").executeQuery();

    assertTrue(rs.next());
    assertEquals("", rs.getString(1));
  }

  @Test
  void testSetCharacterStream_intLength_nullReader() throws Exception {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();
    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    stmt.setCharacterStream(1, null, 10);

    stmt.execute();
    ResultSet rs = connection.prepareStatement("SELECT col FROM test").executeQuery();

    assertTrue(rs.next());
    assertNull(rs.getString(1));
  }

  @Test
  void testSetCharacterStream_intLength_negativeLength() throws Exception {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();
    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    Reader reader = new StringReader("text");

    assertThrows(SQLException.class, () -> stmt.setCharacterStream(1, reader, -1));
  }

  @Test
  void testSetCharacterStream_noLength_insert_and_select() throws Exception {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();
    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    String text = "test";
    Reader reader = new StringReader(text);

    stmt.setCharacterStream(1, reader);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT col FROM test");
    ResultSet rs = stmt2.executeQuery();

    assertTrue(rs.next());
    assertEquals(text, rs.getString(1));
  }

  @Test
  void testSetCharacterStream_noLength_nullReader() throws Exception {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();
    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    stmt.setCharacterStream(1, null);
    stmt.execute();

    ResultSet rs = connection.prepareStatement("SELECT col FROM test").executeQuery();

    assertTrue(rs.next());
    assertNull(rs.getString(1));
  }

  @Test
  void testSetCharacterStream_noLength_emptyReader() throws Exception {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();
    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    Reader empty = new StringReader("");
    stmt.setCharacterStream(1, empty);
    stmt.execute();

    ResultSet rs = connection.prepareStatement("SELECT col FROM test").executeQuery();

    assertTrue(rs.next());
    assertEquals("", rs.getString(1));
  }

  @Test
  void testSetCharacterStream_longLength_insert_and_select() throws Exception {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();
    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    String text = "test";
    Reader reader = new StringReader(text);

    stmt.setCharacterStream(1, reader, (long) text.length());
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT col FROM test");
    ResultSet rs = stmt2.executeQuery();

    assertTrue(rs.next());
    assertEquals(text, rs.getString(1));
  }

  @Test
  void testSetCharacterStream_longLength_nullReader() throws Exception {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();
    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    stmt.setCharacterStream(1, null, 0L);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT col FROM test");
    ResultSet rs = stmt2.executeQuery();

    assertTrue(rs.next());
    assertNull(rs.getString(1));
  }

  @Test
  void testSetCharacterStream_longLength_emptyReader() throws Exception {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();
    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    Reader empty = new StringReader("");

    stmt.setCharacterStream(1, empty, 0L);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT col FROM test");
    ResultSet rs = stmt2.executeQuery();

    assertTrue(rs.next());
    assertEquals("", rs.getString(1));
  }

  @Test
  void testSetCharacterStream_longLength_shorterThanLength() throws Exception {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();
    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    Reader reader = new StringReader("test");

    stmt.setCharacterStream(1, reader, 10L);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT col FROM test");
    ResultSet rs = stmt2.executeQuery();

    assertTrue(rs.next());
    assertEquals("test", rs.getString(1));
  }

  @Test
  void testSetCharacterStream_longLength_zero() throws Exception {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();
    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    Reader reader = new StringReader("test");

    stmt.setCharacterStream(1, reader, 0L);
    stmt.execute();

    PreparedStatement stmt2 = connection.prepareStatement("SELECT col FROM test");
    ResultSet rs = stmt2.executeQuery();

    assertTrue(rs.next());
    assertEquals("", rs.getString(1));
  }

  @Test
  void testSetCharacterStream_longLength_negative() throws Exception {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();
    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    Reader reader = new StringReader("test");

    assertThrows(
        SQLFeatureNotSupportedException.class, () -> stmt.setCharacterStream(1, reader, -1L));
  }

  @Test
  void testSetCharacterStream_longLength_overflow() throws Exception {
    connection.prepareStatement("CREATE TABLE test (col TEXT)").execute();
    PreparedStatement stmt = connection.prepareStatement("INSERT INTO test (col) VALUES (?)");

    Reader reader = new StringReader("test");

    assertThrows(
        SQLFeatureNotSupportedException.class,
        () -> stmt.setCharacterStream(1, reader, (long) Integer.MAX_VALUE + 1));
  }

  @Test
  void execute_insert_should_return_number_of_inserted_elements() throws Exception {
    connection.prepareStatement("CREATE TABLE test (col INTEGER)").execute();
    PreparedStatement prepareStatement =
        connection.prepareStatement("INSERT INTO test (col) VALUES (?), (?), (?)");
    prepareStatement.setInt(1, 1);
    prepareStatement.setInt(2, 2);
    prepareStatement.setInt(3, 3);
    assertEquals(prepareStatement.executeUpdate(), 3);
  }

  @Test
  void execute_update_should_return_number_of_updated_elements() throws Exception {
    connection.prepareStatement("CREATE TABLE test (col INTEGER)").execute();
    connection.prepareStatement("INSERT INTO test (col) VALUES (1), (2), (3)").execute();
    PreparedStatement preparedStatement =
        connection.prepareStatement("UPDATE test SET col = ? where col = 1 ");
    preparedStatement.setInt(1, 4);
    assertEquals(preparedStatement.executeUpdate(), 1);
  }

  @Test
  void execute_delete_should_return_number_of_deleted_elements() throws Exception {
    connection.prepareStatement("CREATE TABLE test (col INTEGER)").execute();
    connection.prepareStatement("INSERT INTO test (col) VALUES (1), (2), (3)").execute();
    PreparedStatement preparedStatement =
        connection.prepareStatement("DELETE  FROM test   where col = ? ");
    preparedStatement.setInt(1, 1);
    assertEquals(preparedStatement.executeUpdate(), 1);
  }

  @Test
  void testBatchInsert() throws Exception {
    connection.prepareStatement("CREATE TABLE test (col1 INTEGER, col2 INTEGER)").execute();
    PreparedStatement preparedStatement =
        connection.prepareStatement("INSERT INTO test (col1, col2) VALUES (?, ?)");

    preparedStatement.setInt(1, 1);
    preparedStatement.setInt(2, 2);
    preparedStatement.addBatch();
    preparedStatement.setInt(1, 3);
    preparedStatement.setInt(2, 4);
    preparedStatement.addBatch();

    assertArrayEquals(new int[] {1, 1}, preparedStatement.executeBatch());

    ResultSet rs = connection.prepareStatement("SELECT * FROM test").executeQuery();
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals(2, rs.getInt(2));
    assertTrue(rs.next());
    assertEquals(3, rs.getInt(1));
    assertEquals(4, rs.getInt(2));
    assertFalse(rs.next());
  }

  @Test
  void testBatchUpdate() throws Exception {
    connection.prepareStatement("CREATE TABLE test (col1 INTEGER, col2 INTEGER)").execute();
    connection.prepareStatement("INSERT INTO test (col1, col2) VALUES (1, 1), (2, 2)").execute();

    PreparedStatement preparedStatement =
        connection.prepareStatement("UPDATE test SET col2=? WHERE col1=?");

    preparedStatement.setInt(1, 5);
    preparedStatement.setInt(2, 1);
    preparedStatement.addBatch();
    preparedStatement.setInt(1, 6);
    preparedStatement.setInt(2, 2);
    preparedStatement.addBatch();
    preparedStatement.setInt(1, 7);
    preparedStatement.setInt(2, 3);
    preparedStatement.addBatch();

    assertArrayEquals(new int[] {1, 1, 0}, preparedStatement.executeBatch());

    ResultSet rs = connection.prepareStatement("SELECT * FROM test").executeQuery();
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals(5, rs.getInt(2));
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertEquals(6, rs.getInt(2));
    assertFalse(rs.next());
  }

  @Test
  void testBatchDelete() throws Exception {
    connection.prepareStatement("CREATE TABLE test (col1 INTEGER, col2 INTEGER)").execute();
    connection.prepareStatement("INSERT INTO test (col1, col2) VALUES (1, 1), (2, 2)").execute();

    PreparedStatement preparedStatement =
        connection.prepareStatement("DELETE FROM test WHERE col1=?");

    preparedStatement.setInt(1, 1);
    preparedStatement.addBatch();
    preparedStatement.setInt(1, 4);
    preparedStatement.addBatch();

    assertArrayEquals(new int[] {1, 0}, preparedStatement.executeBatch());

    ResultSet rs = connection.prepareStatement("SELECT * FROM test").executeQuery();
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertEquals(2, rs.getInt(2));
    assertFalse(rs.next());
  }

  @Test
  void testBatch_implicitAddBatch_shouldIgnore() throws Exception {
    connection.prepareStatement("CREATE TABLE test (col1 INTEGER, col2 INTEGER)").execute();
    PreparedStatement preparedStatement =
        connection.prepareStatement("INSERT INTO test (col1, col2) VALUES (?, ?)");

    preparedStatement.setInt(1, 1);
    preparedStatement.setInt(2, 2);
    preparedStatement.addBatch();
    // we set parameters but don't call addBatch afterward
    // we should only get a result for the first insert statement to match sqlite-jdbc behavior
    preparedStatement.setInt(1, 3);
    preparedStatement.setInt(2, 4);

    assertArrayEquals(new int[] {1}, preparedStatement.executeBatch());

    ResultSet rs = connection.prepareStatement("SELECT * FROM test").executeQuery();
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals(2, rs.getInt(2));
    assertFalse(rs.next());
  }

  @Test
  void testBatch_select_shouldFail() throws Exception {
    connection.prepareStatement("CREATE TABLE test (col1 INTEGER, col2 INTEGER)").execute();
    PreparedStatement preparedStatement =
        connection.prepareStatement("SELECT * FROM test WHERE col1=?");
    preparedStatement.setInt(1, 1);
    preparedStatement.addBatch();
    assertThrows(BatchUpdateException.class, preparedStatement::executeBatch);
  }
}
