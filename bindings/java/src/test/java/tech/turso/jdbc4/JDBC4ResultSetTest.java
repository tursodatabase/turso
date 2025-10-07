package tech.turso.jdbc4;

import static org.junit.jupiter.api.Assertions.*;

import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tech.turso.TestUtils;

class JDBC4ResultSetTest {

  private Statement stmt;

  @BeforeEach
  void setUp() throws Exception {
    String filePath = TestUtils.createTempFile();
    String url = "jdbc:turso:" + filePath;
    final JDBC4Connection connection = new JDBC4Connection(url, filePath, new Properties());
    stmt =
        connection.createStatement(
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            ResultSet.CLOSE_CURSORS_AT_COMMIT);
  }

  @Test
  void invoking_next_before_the_last_row_should_return_true() throws Exception {
    stmt.executeUpdate("CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT);");
    stmt.executeUpdate("INSERT INTO users VALUES (1, 'sinwoo');");
    stmt.executeUpdate("INSERT INTO users VALUES (2, 'seonwoo');");

    // first call to next occur internally
    stmt.executeQuery("SELECT * FROM users");
    ResultSet resultSet = stmt.getResultSet();

    assertTrue(resultSet.next());
  }

  @Test
  void invoking_next_after_the_last_row_should_return_false() throws Exception {
    stmt.executeUpdate("CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT);");
    stmt.executeUpdate("INSERT INTO users VALUES (1, 'sinwoo');");
    stmt.executeUpdate("INSERT INTO users VALUES (2, 'seonwoo');");

    // first call to next occur internally
    stmt.executeQuery("SELECT * FROM users");
    ResultSet resultSet = stmt.getResultSet();

    while (resultSet.next()) {
      // run until next() returns false
    }

    // if the previous call to next() returned false, consecutive call to next() should return false
    // as well
    assertFalse(resultSet.next());
  }

  @Test
  void close_resultSet_test() throws Exception {
    stmt.executeQuery("SELECT 1;");
    ResultSet resultSet = stmt.getResultSet();

    assertFalse(resultSet.isClosed());
    resultSet.close();
    assertTrue(resultSet.isClosed());
  }

  @Test
  void calling_methods_on_closed_resultSet_should_throw_exception() throws Exception {
    stmt.executeQuery("SELECT 1;");
    ResultSet resultSet = stmt.getResultSet();
    resultSet.close();
    assertTrue(resultSet.isClosed());

    assertThrows(SQLException.class, resultSet::next);
  }

  @Test
  void test_getString() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_string (string_col TEXT);");
    stmt.executeUpdate("INSERT INTO test_string (string_col) VALUES ('test');");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_string");
    assertTrue(resultSet.next());
    assertEquals("test", resultSet.getString(1));
  }

  @Test
  void test_getString_returns_null_on_null() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_null (string_col TEXT);");
    stmt.executeUpdate("INSERT INTO test_null (string_col) VALUES (NULL);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_null");
    assertTrue(resultSet.next());
    assertNull(resultSet.getString(1));
  }

  @Test
  void test_getBoolean_true() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_boolean (boolean_col INTEGER);");
    stmt.executeUpdate("INSERT INTO test_boolean (boolean_col) VALUES (1);");
    stmt.executeUpdate("INSERT INTO test_boolean (boolean_col) VALUES (2);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_boolean");

    assertTrue(resultSet.next());
    assertTrue(resultSet.getBoolean(1));

    resultSet.next();
    assertTrue(resultSet.getBoolean(1));
  }

  @Test
  void test_getBoolean_false() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_boolean (boolean_col INTEGER);");
    stmt.executeUpdate("INSERT INTO test_boolean (boolean_col) VALUES (0);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_boolean");
    assertTrue(resultSet.next());
    assertFalse(resultSet.getBoolean(1));
  }

  @Test
  void test_getBoolean_returns_false_on_null() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_null (boolean_col INTEGER);");
    stmt.executeUpdate("INSERT INTO test_null (boolean_col) VALUES (NULL);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_null");
    assertTrue(resultSet.next());
    assertFalse(resultSet.getBoolean(1));
  }

  @Test
  void test_getByte() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_byte (byte_col INTEGER);");
    stmt.executeUpdate("INSERT INTO test_byte (byte_col) VALUES (1);");
    stmt.executeUpdate("INSERT INTO test_byte (byte_col) VALUES (128);"); // Exceeds byte size
    stmt.executeUpdate("INSERT INTO test_byte (byte_col) VALUES (-129);"); // Exceeds byte size

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_byte");

    // Test value that fits within byte size
    assertTrue(resultSet.next());
    assertEquals(1, resultSet.getByte(1));

    // Test value that exceeds byte size (positive overflow)
    assertTrue(resultSet.next());
    assertEquals(-128, resultSet.getByte(1)); // 128 overflows to -128

    // Test value that exceeds byte size (negative overflow)
    assertTrue(resultSet.next());
    assertEquals(127, resultSet.getByte(1)); // -129 overflows to 127
  }

  @Test
  void test_getByte_returns_zero_on_null() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_null (byte_col INTEGER);");
    stmt.executeUpdate("INSERT INTO test_null (byte_col) VALUES (NULL);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_null");
    assertTrue(resultSet.next());
    assertEquals(0, resultSet.getByte(1));
  }

  @Test
  void test_getShort() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_short (short_col INTEGER);");
    stmt.executeUpdate("INSERT INTO test_short (short_col) VALUES (123);");
    stmt.executeUpdate("INSERT INTO test_short (short_col) VALUES (32767);"); // Max short value
    stmt.executeUpdate("INSERT INTO test_short (short_col) VALUES (-32768);"); // Min short value

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_short");

    // Test typical short value
    assertTrue(resultSet.next());
    assertEquals(123, resultSet.getShort(1));

    // Test maximum short value
    assertTrue(resultSet.next());
    assertEquals(32767, resultSet.getShort(1));

    // Test minimum short value
    assertTrue(resultSet.next());
    assertEquals(-32768, resultSet.getShort(1));
  }

  @Test
  void test_getShort_returns_zero_on_null() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_null (short_col INTEGER);");
    stmt.executeUpdate("INSERT INTO test_null (short_col) VALUES (NULL);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_null");
    assertTrue(resultSet.next());
    assertEquals(0, resultSet.getShort(1));
  }

  @Test
  void test_getInt() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_int (int_col INT);");
    stmt.executeUpdate("INSERT INTO test_int (int_col) VALUES (12345);");
    stmt.executeUpdate("INSERT INTO test_int (int_col) VALUES (2147483647);"); // Max int value
    stmt.executeUpdate("INSERT INTO test_int (int_col) VALUES (-2147483648);"); // Min int value

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_int");

    // Test typical int value
    assertTrue(resultSet.next());
    assertEquals(12345, resultSet.getInt(1));

    // Test maximum int value
    assertTrue(resultSet.next());
    assertEquals(2147483647, resultSet.getInt(1));

    // Test minimum int value
    assertTrue(resultSet.next());
    assertEquals(-2147483648, resultSet.getInt(1));
  }

  @Test
  void test_getInt_returns_zero_on_null() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_null (int_col INTEGER);");
    stmt.executeUpdate("INSERT INTO test_null (int_col) VALUES (NULL);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_null");
    assertTrue(resultSet.next());
    assertEquals(0, resultSet.getInt(1));
  }

  @Test
  void test_getLong() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_long (long_col BIGINT);");
    stmt.executeUpdate("INSERT INTO test_long (long_col) VALUES (1234567890);");
    stmt.executeUpdate(
        "INSERT INTO test_long (long_col) VALUES (9223372036854775807);"); // Max long value
    stmt.executeUpdate(
        "INSERT INTO test_long (long_col) VALUES (-9223372036854775808);"); // Min long value

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_long");

    // Test typical long value
    assertTrue(resultSet.next());
    assertEquals(1234567890L, resultSet.getLong(1));

    // Test maximum long value
    assertTrue(resultSet.next());
    assertEquals(9223372036854775807L, resultSet.getLong(1));

    // Test minimum long value
    assertTrue(resultSet.next());
    assertEquals(-9223372036854775808L, resultSet.getLong(1));
  }

  @Test
  void test_getLong_returns_zero_no_null() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_null (long_col INTEGER);");
    stmt.executeUpdate("INSERT INTO test_null (long_col) VALUES (NULL);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_null");
    assertTrue(resultSet.next());
    assertEquals(0L, resultSet.getLong(1));
  }

  @Test
  void test_getFloat() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_float (float_col REAL);");
    stmt.executeUpdate("INSERT INTO test_float (float_col) VALUES (1.23);");
    stmt.executeUpdate(
        "INSERT INTO test_float (float_col) VALUES (3.4028235E38);"); // Max float value
    stmt.executeUpdate(
        "INSERT INTO test_float (float_col) VALUES (1.4E-45);"); // Min positive float value
    stmt.executeUpdate(
        "INSERT INTO test_float (float_col) VALUES (-3.4028235E38);"); // Min negative float value

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_float");

    // Test typical float value
    assertTrue(resultSet.next());
    assertEquals(1.23f, resultSet.getFloat(1), 0.0001);

    // Test maximum float value
    assertTrue(resultSet.next());
    assertEquals(3.4028235E38f, resultSet.getFloat(1), 0.0001);

    // Test minimum positive float value
    assertTrue(resultSet.next());
    assertEquals(1.4E-45f, resultSet.getFloat(1), 0.0001);

    // Test minimum negative float value
    assertTrue(resultSet.next());
    assertEquals(-3.4028235E38f, resultSet.getFloat(1), 0.0001);
  }

  @Test
  void test_getFloat_returns_zero_on_null() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_null (float_col REAL);");
    stmt.executeUpdate("INSERT INTO test_null (float_col) VALUES (NULL);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_null");
    assertTrue(resultSet.next());
    assertEquals(0.0f, resultSet.getFloat(1), 0.0001);
  }

  @Test
  void test_getDouble() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_double (double_col REAL);");
    stmt.executeUpdate("INSERT INTO test_double (double_col) VALUES (1.234567);");
    stmt.executeUpdate(
        "INSERT INTO test_double (double_col) VALUES (1.7976931348623157E308);"); // Max double
    // value
    stmt.executeUpdate(
        "INSERT INTO test_double (double_col) VALUES (4.9E-324);"); // Min positive double value
    stmt.executeUpdate(
        "INSERT INTO test_double (double_col) VALUES (-1.7976931348623157E308);"); // Min negative
    // double value

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_double");

    // Test typical double value
    assertTrue(resultSet.next());
    assertEquals(1.234567, resultSet.getDouble(1), 0.0001);

    // Test maximum double value
    assertTrue(resultSet.next());
    assertEquals(1.7976931348623157E308, resultSet.getDouble(1), 0.0001);

    // Test minimum positive double value
    assertTrue(resultSet.next());
    assertEquals(4.9E-324, resultSet.getDouble(1), 0.0001);

    // Test minimum negative double value
    assertTrue(resultSet.next());
    assertEquals(-1.7976931348623157E308, resultSet.getDouble(1), 0.0001);
  }

  @Test
  void test_getDouble_returns_zero_on_null() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_null (double_col REAL);");
    stmt.executeUpdate("INSERT INTO test_null (double_col) VALUES (NULL);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_null");
    assertTrue(resultSet.next());
    assertEquals(0.0, resultSet.getDouble(1), 0.0001);
  }

  @Test
  void test_getBigDecimal() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_bigdecimal (bigdecimal_col REAL);");
    stmt.executeUpdate("INSERT INTO test_bigdecimal (bigdecimal_col) VALUES (12345.67);");
    stmt.executeUpdate(
        "INSERT INTO test_bigdecimal (bigdecimal_col) VALUES (1.7976931348623157E308);"); // Max
    // double
    // value
    stmt.executeUpdate(
        "INSERT INTO test_bigdecimal (bigdecimal_col) VALUES (4.9E-324);"); // Min positive double
    // value
    stmt.executeUpdate(
        "INSERT INTO test_bigdecimal (bigdecimal_col) VALUES (-12345.67);"); // Negative value

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_bigdecimal");

    // Test typical BigDecimal value
    assertTrue(resultSet.next());
    assertEquals(
        new BigDecimal("12345.67").setScale(2, RoundingMode.HALF_UP),
        resultSet.getBigDecimal(1, 2));

    // Test maximum double value
    assertTrue(resultSet.next());
    assertEquals(
        new BigDecimal("1.7976931348623157E308").setScale(10, RoundingMode.HALF_UP),
        resultSet.getBigDecimal(1, 10));

    // Test minimum positive double value
    assertTrue(resultSet.next());
    assertEquals(
        new BigDecimal("4.9E-324").setScale(10, RoundingMode.HALF_UP),
        resultSet.getBigDecimal(1, 10));

    // Test negative BigDecimal value
    assertTrue(resultSet.next());
    assertEquals(
        new BigDecimal("-12345.67").setScale(2, RoundingMode.HALF_UP),
        resultSet.getBigDecimal(1, 2));
  }

  @Test
  void test_getBigDecimal_returns_null_on_null() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_null (bigdecimal_col REAL);");
    stmt.executeUpdate("INSERT INTO test_null (bigdecimal_col) VALUES (NULL);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_null");
    assertTrue(resultSet.next());
    assertNull(resultSet.getBigDecimal(1, 2));
  }

  @ParameterizedTest
  @MethodSource("byteArrayProvider")
  void test_getBytes(byte[] data) throws Exception {
    stmt.executeUpdate("CREATE TABLE test_bytes (bytes_col BLOB);");
    executeDMLAndAssert(data);
  }

  private static Stream<byte[]> byteArrayProvider() {
    return Stream.of(
        "Hello".getBytes(), "world".getBytes(), new byte[0], new byte[] {0x00, (byte) 0xFF});
  }

  private void executeDMLAndAssert(byte[] data) throws SQLException {
    // Convert byte array to hexadecimal string
    StringBuilder hexString = new StringBuilder();
    for (byte b : data) {
      hexString.append(String.format("%02X", b));
    }
    // Execute DML statement
    stmt.executeUpdate("INSERT INTO test_bytes (bytes_col) VALUES (X'" + hexString + "');");

    // Assert the inserted data
    ResultSet resultSet = stmt.executeQuery("SELECT bytes_col FROM test_bytes");
    assertTrue(resultSet.next());
    assertArrayEquals(data, resultSet.getBytes(1));
  }

  @Test
  void test_getBytes_returns_null_on_null() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_null (bytes_col BLOB);");
    stmt.executeUpdate("INSERT INTO test_null (bytes_col) VALUES (NULL);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_null");
    assertTrue(resultSet.next());
    assertNull(resultSet.getBytes(1));
  }

  @Test
  void test_getXXX_methods_on_multiple_columns() throws Exception {
    stmt.executeUpdate(
        "CREATE TABLE test_integration ("
            + "string_col TEXT, "
            + "boolean_col INTEGER, "
            + "byte_col INTEGER, "
            + "short_col INTEGER, "
            + "int_col INTEGER, "
            + "long_col BIGINT, "
            + "float_col REAL, "
            + "double_col REAL, "
            + "bigdecimal_col REAL, "
            + "bytes_col BLOB);");

    stmt.executeUpdate(
        "INSERT INTO test_integration VALUES ("
            + "'test', "
            + "1, "
            + "1, "
            + "123, "
            + "12345, "
            + "1234567890, "
            + "1.23, "
            + "1.234567, "
            + "12345.67, "
            + "X'48656C6C6F');");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_integration");
    assertTrue(resultSet.next());

    // Verify each column
    assertEquals("test", resultSet.getString(1));
    assertTrue(resultSet.getBoolean(2));
    assertEquals(1, resultSet.getByte(3));
    assertEquals(123, resultSet.getShort(4));
    assertEquals(12345, resultSet.getInt(5));
    assertEquals(1234567890L, resultSet.getLong(6));
    assertEquals(1.23f, resultSet.getFloat(7), 0.0001);
    assertEquals(1.234567, resultSet.getDouble(8), 0.0001);
    assertEquals(
        new BigDecimal("12345.67").setScale(2, RoundingMode.HALF_UP),
        resultSet.getBigDecimal(9, 2));
    assertArrayEquals("Hello".getBytes(), resultSet.getBytes(10));
  }

  @Test
  void test_invalidColumnIndex_outOfBounds() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_invalid (col INTEGER);");
    stmt.executeUpdate("INSERT INTO test_invalid (col) VALUES (1);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_invalid");
    assertTrue(resultSet.next());

    // Test out-of-bounds column index
    assertThrows(SQLException.class, () -> resultSet.getInt(2));
  }

  @Test
  void test_invalidColumnIndex_negative() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_invalid (col INTEGER);");
    stmt.executeUpdate("INSERT INTO test_invalid (col) VALUES (1);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_invalid");
    assertTrue(resultSet.next());

    // Test negative column index
    assertThrows(SQLException.class, () -> resultSet.getInt(-1));
  }

  @Test
  void test_findColumn_with_exact_name() throws Exception {
    stmt.executeUpdate("CREATE TABLE users (id INTEGER, username TEXT, age INTEGER);");
    stmt.executeUpdate("INSERT INTO users VALUES (1, 'minseok', 30);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM users");
    assertTrue(resultSet.next());

    assertEquals(1, resultSet.findColumn("id"));
    assertEquals(2, resultSet.findColumn("username"));
    assertEquals(3, resultSet.findColumn("age"));
  }

  @Test
  void test_findColumn_with_duplicate_names() throws Exception {
    // SQLite allows duplicate column names in SELECT
    stmt.executeUpdate("CREATE TABLE test (a INTEGER, b INTEGER);");
    stmt.executeUpdate("INSERT INTO test VALUES (1, 2);");

    ResultSet resultSet = stmt.executeQuery("SELECT a, a FROM test");
    assertTrue(resultSet.next());

    // Should return the FIRST occurrence
    assertEquals(1, resultSet.findColumn("a"));
  }

  @Test
  void test_findColumn_with_nonexistent_column() throws Exception {
    stmt.executeUpdate("CREATE TABLE users (id INTEGER);");
    stmt.executeUpdate("INSERT INTO users VALUES (1);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM users");
    assertTrue(resultSet.next());

    SQLException exception =
        assertThrows(SQLException.class, () -> resultSet.findColumn("nonexistent"));
    assertEquals("column name nonexistent not found", exception.getMessage());
  }

  @Test
  void test_findColumn_with_alias() throws Exception {
    stmt.executeUpdate("CREATE TABLE users (id INTEGER);");
    stmt.executeUpdate("INSERT INTO users VALUES (1);");

    ResultSet resultSet = stmt.executeQuery("SELECT id AS user_id FROM users");
    assertTrue(resultSet.next());

    // Should find by alias, not original column name
    assertEquals(1, resultSet.findColumn("user_id"));
    SQLException exception = assertThrows(SQLException.class, () -> resultSet.findColumn("id"));
    assertEquals("column name id not found", exception.getMessage());
  }

  @Test
  void test_findColumn_with_empty_string() throws Exception {
    stmt.executeUpdate("CREATE TABLE test (col INTEGER);");
    stmt.executeUpdate("INSERT INTO test VALUES (1);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test");
    assertTrue(resultSet.next());

    SQLException exception = assertThrows(SQLException.class, () -> resultSet.findColumn(""));
    assertEquals("column name not found", exception.getMessage());
  }

  @Test
  void test_findColumn_with_special_characters() throws Exception {
    // SQLite allows spaces and special chars in column names if quoted
    stmt.executeUpdate("CREATE TABLE test ([user name] TEXT, [user-id] INTEGER);");
    stmt.executeUpdate("INSERT INTO test VALUES ('minseok', 1);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test");
    assertTrue(resultSet.next());

    assertEquals(1, resultSet.findColumn("user name"));
    assertEquals(2, resultSet.findColumn("user-id"));
  }

  @Test
  void test_getCharacterStream() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_char_stream (text_col TEXT);");
    stmt.executeUpdate("INSERT INTO test_char_stream (text_col) VALUES ('Hello World');");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_char_stream");
    assertTrue(resultSet.next());

    Reader reader = resultSet.getCharacterStream(1);
    char[] buffer = new char[11];
    int charsRead = reader.read(buffer);

    assertEquals(11, charsRead);
    assertEquals("Hello World", new String(buffer));
  }

  @Test
  void test_getCharacterStream_with_columnLabel() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_char_stream (text_col TEXT);");
    stmt.executeUpdate("INSERT INTO test_char_stream (text_col) VALUES ('Test Data');");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_char_stream");
    assertTrue(resultSet.next());

    Reader reader = resultSet.getCharacterStream("text_col");
    char[] buffer = new char[9];
    reader.read(buffer);

    assertEquals("Test Data", new String(buffer));
  }

  @Test
  void test_getCharacterStream_returns_null_on_null() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_null (text_col TEXT);");
    stmt.executeUpdate("INSERT INTO test_null (text_col) VALUES (NULL);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_null");
    assertTrue(resultSet.next());
    assertNull(resultSet.getCharacterStream(1));
  }

  @Test
  void test_getBigDecimal_with_columnLabel() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_bigdecimal (amount REAL);");
    stmt.executeUpdate("INSERT INTO test_bigdecimal (amount) VALUES (12345.67);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_bigdecimal");
    assertTrue(resultSet.next());

    assertEquals(BigDecimal.valueOf(12345.67), resultSet.getBigDecimal("amount"));
  }

  @Test
  void test_isBeforeFirst_and_isAfterLast() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_position (id INTEGER);");
    stmt.executeUpdate("INSERT INTO test_position VALUES (1);");
    stmt.executeUpdate("INSERT INTO test_position VALUES (2);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_position");

    // Before first row
    assertTrue(resultSet.isBeforeFirst());
    assertFalse(resultSet.isAfterLast());

    // First row
    resultSet.next();
    assertFalse(resultSet.isBeforeFirst());
    assertFalse(resultSet.isAfterLast());

    // Second row
    resultSet.next();
    assertFalse(resultSet.isBeforeFirst());
    assertFalse(resultSet.isAfterLast());

    // After last row
    resultSet.next();
    assertFalse(resultSet.isBeforeFirst());
    assertTrue(resultSet.isAfterLast());
  }

  @Test
  void test_isBeforeFirst_with_empty_resultSet() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_empty (id INTEGER);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_empty");

    // Before calling next()
    assertTrue(resultSet.isBeforeFirst());
    assertFalse(resultSet.isAfterLast());

    // After calling next() on empty ResultSet
    assertFalse(resultSet.next());
    assertFalse(resultSet.isBeforeFirst());
    assertTrue(resultSet.isAfterLast());
  }

  @Test
  void test_getRow() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_row (id INTEGER);");
    stmt.executeUpdate("INSERT INTO test_row VALUES (1);");
    stmt.executeUpdate("INSERT INTO test_row VALUES (2);");
    stmt.executeUpdate("INSERT INTO test_row VALUES (3);");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_row");

    // Before first row
    assertEquals(0, resultSet.getRow());

    // First row
    resultSet.next();
    assertEquals(1, resultSet.getRow());

    // Second row
    resultSet.next();
    assertEquals(2, resultSet.getRow());

    // Third row
    resultSet.next();
    assertEquals(3, resultSet.getRow());

    // After last row
    resultSet.next();
    assertEquals(3, resultSet.getRow());
  }

  @Test
  void test_getDate_with_calendar() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_date_cal (date_col BLOB);");

    // 2025-10-07 03:00:00 UTC in milliseconds
    long utcTime = 1728270000000L;
    byte[] timeBytes = ByteBuffer.allocate(Long.BYTES).putLong(utcTime).array();

    StringBuilder hexString = new StringBuilder();
    for (byte b : timeBytes) {
      hexString.append(String.format("%02X", b));
    }
    stmt.executeUpdate("INSERT INTO test_date_cal (date_col) VALUES (X'" + hexString + "');");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_date_cal");
    assertTrue(resultSet.next());

    // Get date with UTC calendar
    Calendar utcCal = Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"));
    Date utcDate = resultSet.getDate(1, utcCal);

    // Get date with Seoul calendar (UTC+9)
    Calendar seoulCal = Calendar.getInstance(java.util.TimeZone.getTimeZone("Asia/Seoul"));
    Date seoulDate = resultSet.getDate(1, seoulCal);

    // Seoul time should be 9 hours ahead
    long timeDiff = seoulDate.getTime() - utcDate.getTime();
    assertEquals(9 * 60 * 60 * 1000, timeDiff);
  }

  @Test
  void test_getDate_with_calendar_columnLabel() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_date_cal (created_at BLOB);");

    // 2025-10-07 03:00:00 UTC in milliseconds
    long utcTime = 1728270000000L;
    byte[] timeBytes = ByteBuffer.allocate(Long.BYTES).putLong(utcTime).array();

    StringBuilder hexString = new StringBuilder();
    for (byte b : timeBytes) {
      hexString.append(String.format("%02X", b));
    }
    stmt.executeUpdate("INSERT INTO test_date_cal (created_at) VALUES (X'" + hexString + "');");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_date_cal");
    assertTrue(resultSet.next());

    Calendar utcCal = Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"));
    Date date = resultSet.getDate("created_at", utcCal);

    assertNotNull(date);
  }

  @Test
  void test_getTime_with_calendar() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_time_cal (time_col BLOB);");

    // 2025-10-07 03:00:00 UTC in milliseconds
    long utcTime = 1728270000000L;
    byte[] timeBytes = ByteBuffer.allocate(Long.BYTES).putLong(utcTime).array();

    StringBuilder hexString = new StringBuilder();
    for (byte b : timeBytes) {
      hexString.append(String.format("%02X", b));
    }
    stmt.executeUpdate("INSERT INTO test_time_cal (time_col) VALUES (X'" + hexString + "');");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_time_cal");
    assertTrue(resultSet.next());

    // Get time with UTC calendar
    Calendar utcCal = Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"));
    Time utcTime2 = resultSet.getTime(1, utcCal);

    // Get time with Seoul calendar (UTC+9)
    Calendar seoulCal = Calendar.getInstance(java.util.TimeZone.getTimeZone("Asia/Seoul"));
    Time seoulTime = resultSet.getTime(1, seoulCal);

    // Seoul time should be 9 hours ahead
    long timeDiff = seoulTime.getTime() - utcTime2.getTime();
    assertEquals(9 * 60 * 60 * 1000, timeDiff);
  }

  @Test
  void test_getTime_with_calendar_columnLabel() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_time_cal (created_at BLOB);");

    // 2025-10-07 03:00:00 UTC in milliseconds
    long utcTime = 1728270000000L;
    byte[] timeBytes = ByteBuffer.allocate(Long.BYTES).putLong(utcTime).array();

    StringBuilder hexString = new StringBuilder();
    for (byte b : timeBytes) {
      hexString.append(String.format("%02X", b));
    }
    stmt.executeUpdate("INSERT INTO test_time_cal (created_at) VALUES (X'" + hexString + "');");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_time_cal");
    assertTrue(resultSet.next());

    Calendar utcCal = Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"));
    Time time = resultSet.getTime("created_at", utcCal);

    assertNotNull(time);
  }

  @Test
  void test_getTimestamp_with_calendar() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_timestamp_cal (timestamp_col BLOB);");

    // 2025-10-07 03:00:00 UTC in milliseconds
    long utcTime = 1728270000000L;
    byte[] timeBytes = ByteBuffer.allocate(Long.BYTES).putLong(utcTime).array();

    StringBuilder hexString = new StringBuilder();
    for (byte b : timeBytes) {
      hexString.append(String.format("%02X", b));
    }
    stmt.executeUpdate("INSERT INTO test_timestamp_cal (timestamp_col) VALUES (X'" + hexString + "');");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_timestamp_cal");
    assertTrue(resultSet.next());

    // Get timestamp with UTC calendar
    Calendar utcCal = Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"));
    Timestamp utcTimestamp = resultSet.getTimestamp(1, utcCal);

    // Get timestamp with Seoul calendar (UTC+9)
    Calendar seoulCal = Calendar.getInstance(java.util.TimeZone.getTimeZone("Asia/Seoul"));
    Timestamp seoulTimestamp = resultSet.getTimestamp(1, seoulCal);

    // Seoul time should be 9 hours ahead
    long timeDiff = seoulTimestamp.getTime() - utcTimestamp.getTime();
    assertEquals(9 * 60 * 60 * 1000, timeDiff);
  }

  @Test
  void test_getTimestamp_with_calendar_columnLabel() throws Exception {
    stmt.executeUpdate("CREATE TABLE test_timestamp_cal (created_at BLOB);");

    // 2025-10-07 03:00:00 UTC in milliseconds
    long utcTime = 1728270000000L;
    byte[] timeBytes = ByteBuffer.allocate(Long.BYTES).putLong(utcTime).array();

    StringBuilder hexString = new StringBuilder();
    for (byte b : timeBytes) {
      hexString.append(String.format("%02X", b));
    }
    stmt.executeUpdate("INSERT INTO test_timestamp_cal (created_at) VALUES (X'" + hexString + "');");

    ResultSet resultSet = stmt.executeQuery("SELECT * FROM test_timestamp_cal");
    assertTrue(resultSet.next());

    Calendar utcCal = Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"));
    Timestamp timestamp = resultSet.getTimestamp("created_at", utcCal);

    assertNotNull(timestamp);
  }
}
