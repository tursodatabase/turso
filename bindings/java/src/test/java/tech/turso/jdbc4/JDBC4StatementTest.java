package tech.turso.jdbc4;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.BatchUpdateException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.turso.TestUtils;

class JDBC4StatementTest {

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
  void execute_ddl_should_return_false() throws Exception {
    assertFalse(stmt.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT);"));
  }

  @Test
  void execute_insert_should_return_false() throws Exception {
    stmt.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT);");
    assertFalse(stmt.execute("INSERT INTO users VALUES (1, 'turso');"));
  }

  @Test
  void execute_update_should_return_false() throws Exception {
    stmt.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT);");
    stmt.execute("INSERT INTO users VALUES (1, 'turso');");
    assertFalse(stmt.execute("UPDATE users SET username = 'seonwoo' WHERE id = 1;"));
  }

  @Test
  void execute_select_should_return_true() throws Exception {
    stmt.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT);");
    stmt.execute("INSERT INTO users VALUES (1, 'turso');");
    assertTrue(stmt.execute("SELECT * FROM users;"));
  }

  @Test
  void execute_select() throws Exception {
    stmt.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT);");
    stmt.execute("INSERT INTO users VALUES (1, 'turso 1')");
    stmt.execute("INSERT INTO users VALUES (2, 'turso 2')");
    stmt.execute("INSERT INTO users VALUES (3, 'turso 3')");

    ResultSet rs = stmt.executeQuery("SELECT * FROM users;");
    rs.next();
    int rowCount = 0;

    do {
      rowCount++;
      int id = rs.getInt(1);
      String username = rs.getString(2);

      assertEquals(id, rowCount);
      assertEquals(username, "turso " + rowCount);
    } while (rs.next());

    assertEquals(rowCount, 3);
    assertFalse(rs.next());
  }

  @Test
  void close_statement_test() throws Exception {
    stmt.close();
    assertTrue(stmt.isClosed());
  }

  @Test
  void double_close_is_no_op() throws SQLException {
    stmt.close();
    assertDoesNotThrow(() -> stmt.close());
  }

  @Test
  void operations_on_closed_statement_should_throw_exception() throws Exception {
    stmt.close();
    assertThrows(SQLException.class, () -> stmt.execute("SELECT 1;"));
  }

  @Test
  void execute_update_should_return_number_of_inserted_elements() throws Exception {
    assertThat(stmt.executeUpdate("CREATE TABLE s1 (c1);")).isEqualTo(0);
    assertThat(stmt.executeUpdate("INSERT INTO s1 VALUES (0);")).isEqualTo(1);
    assertThat(stmt.executeUpdate("INSERT INTO s1 VALUES (1), (2);")).isEqualTo(2);
    assertThat(stmt.executeUpdate("INSERT INTO s1 VALUES (3), (4), (5);")).isEqualTo(3);
  }

  @Test
  void execute_update_should_return_number_of_updated_elements() throws Exception {
    assertThat(stmt.executeUpdate("CREATE TABLE s1 (c1 INT);")).isEqualTo(0);
    assertThat(stmt.executeUpdate("INSERT INTO s1 VALUES (1), (2), (3);")).isEqualTo(3);
    assertThat(stmt.executeUpdate("UPDATE s1 SET c1 = 0;")).isEqualTo(3);
  }

  @Test
  void execute_update_should_return_number_of_deleted_elements() throws Exception {
    assertThat(stmt.executeUpdate("CREATE TABLE s1 (c1);")).isEqualTo(0);
    assertThat(stmt.executeUpdate("INSERT INTO s1 VALUES (1), (2), (3);")).isEqualTo(3);

    assertThat(stmt.executeUpdate("DELETE FROM s1")).isEqualTo(3);
  }

  /** Tests for batch processing functionality */
  @Test
  void testAddBatch_single_statement() throws SQLException {
    stmt.execute("CREATE TABLE batch_test (id INTEGER PRIMARY KEY, value TEXT);");

    stmt.addBatch("INSERT INTO batch_test VALUES (1, 'test1');");

    int[] updateCounts = stmt.executeBatch();

    assertThat(updateCounts).hasSize(1);
    assertThat(updateCounts[0]).isEqualTo(1);

    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM batch_test;");
    assertTrue(rs.next());
    assertThat(rs.getInt(1)).isEqualTo(1);
  }

  @Test
  void testAddBatch_multiple_statements() throws SQLException {
    stmt.execute("CREATE TABLE batch_test (id INTEGER PRIMARY KEY, value TEXT);");

    stmt.addBatch("INSERT INTO batch_test VALUES (1, 'test1');");
    stmt.addBatch("INSERT INTO batch_test VALUES (2, 'test2');");
    stmt.addBatch("INSERT INTO batch_test VALUES (3, 'test3');");

    int[] updateCounts = stmt.executeBatch();

    assertThat(updateCounts).hasSize(3);
    assertThat(updateCounts[0]).isEqualTo(1);
    assertThat(updateCounts[1]).isEqualTo(1);
    assertThat(updateCounts[2]).isEqualTo(1);

    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM batch_test;");
    assertTrue(rs.next());
    assertThat(rs.getInt(1)).isEqualTo(3);
  }

  @Test
  void testAddBatch_with_updates_and_deletes() throws SQLException {
    stmt.execute("CREATE TABLE batch_test (id INTEGER PRIMARY KEY, value TEXT);");

    stmt.execute(
        "INSERT INTO batch_test VALUES (1, 'initial1'), (2, 'initial2'), (3, 'initial3');");

    stmt.addBatch("UPDATE batch_test SET value = 'updated';");
    stmt.addBatch("DELETE FROM batch_test WHERE id = 2;");
    stmt.addBatch("INSERT INTO batch_test VALUES (4, 'new');");

    int[] updateCounts = stmt.executeBatch();

    assertThat(updateCounts).hasSize(3);
    assertThat(updateCounts[0]).isEqualTo(3); // UPDATE affected 3 row
    assertThat(updateCounts[1]).isEqualTo(1); // DELETE affected 1 row
    assertThat(updateCounts[2]).isEqualTo(1); // INSERT affected 1 row

    // Verify final state
    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM batch_test;");
    assertTrue(rs.next());
    assertThat(rs.getInt(1)).isEqualTo(3); // 3 initial - 1 deleted + 1 inserted = 3
  }

  @Test
  void testClearBatch() throws SQLException {
    stmt.execute("CREATE TABLE batch_test (id INTEGER PRIMARY KEY, value TEXT);");

    stmt.addBatch("INSERT INTO batch_test VALUES (1, 'test1');");
    stmt.addBatch("INSERT INTO batch_test VALUES (2, 'test2');");

    stmt.clearBatch();

    int[] updateCounts = stmt.executeBatch();
    assertThat(updateCounts).isEmpty();

    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM batch_test;");
    assertTrue(rs.next());
    assertThat(rs.getInt(1)).isEqualTo(0);
  }

  @Test
  void testBatch_with_SELECT_should_throw_exception() throws SQLException {
    stmt.execute("CREATE TABLE batch_test (id INTEGER PRIMARY KEY, value TEXT);");
    stmt.execute("INSERT INTO batch_test VALUES (1, 'test1');");

    stmt.addBatch("INSERT INTO batch_test VALUES (2, 'test2');");
    stmt.addBatch("SELECT * FROM batch_test;"); // This should cause an exception
    stmt.addBatch("INSERT INTO batch_test VALUES (3, 'test3');");

    BatchUpdateException exception =
        assertThrows(BatchUpdateException.class, () -> stmt.executeBatch());

    assertTrue(exception.getMessage().contains("Batch commands cannot return result sets"));

    int[] updateCounts = exception.getUpdateCounts();
    assertThat(updateCounts).hasSize(3);
    assertThat(updateCounts[0]).isEqualTo(1); // First INSERT succeeded
    assertThat(updateCounts[1]).isEqualTo(Statement.EXECUTE_FAILED); // SELECT failed
  }

  @Test
  void testBatch_with_null_command_should_throw_exception() {
    assertThrows(SQLException.class, () -> stmt.addBatch(null));
  }

  @Test
  void testBatch_operations_on_closed_statement_should_throw_exception() throws SQLException {
    stmt.close();

    assertThrows(SQLException.class, () -> stmt.addBatch("INSERT INTO test VALUES (1);"));
    assertThrows(SQLException.class, () -> stmt.clearBatch());
    assertThrows(SQLException.class, () -> stmt.executeBatch());
  }

  @Test
  void testBatch_with_syntax_error_should_throw_exception() throws SQLException {
    stmt.execute("CREATE TABLE batch_test (id INTEGER PRIMARY KEY, value TEXT);");

    stmt.addBatch("INSERT INTO batch_test VALUES (1, 'test1');");
    stmt.addBatch("INVALID SQL SYNTAX;"); // This should cause an exception
    stmt.addBatch("INSERT INTO batch_test VALUES (3, 'test3');");

    BatchUpdateException exception =
        assertThrows(BatchUpdateException.class, () -> stmt.executeBatch());

    int[] updateCounts = exception.getUpdateCounts();
    assertThat(updateCounts).hasSize(3);
    assertThat(updateCounts[0]).isEqualTo(1); // First INSERT succeeded
    assertThat(updateCounts[1]).isEqualTo(Statement.EXECUTE_FAILED); // Invalid SQL failed
  }

  @Test
  void testBatch_empty_batch_returns_empty_array() throws SQLException {
    int[] updateCounts = stmt.executeBatch();
    assertThat(updateCounts).isEmpty();
  }

  @Test
  void testBatch_clears_after_successful_execution() throws SQLException {
    stmt.execute("CREATE TABLE batch_test (id INTEGER PRIMARY KEY, value TEXT);");

    stmt.addBatch("INSERT INTO batch_test VALUES (1, 'test1');");
    stmt.executeBatch();

    int[] updateCounts = stmt.executeBatch();
    assertThat(updateCounts).isEmpty();
  }

  @Test
  void testBatch_clears_after_failed_execution() throws SQLException {
    stmt.execute("CREATE TABLE batch_test (id INTEGER PRIMARY KEY, value TEXT);");

    stmt.addBatch("SELECT * FROM batch_test;");

    assertThrows(BatchUpdateException.class, () -> stmt.executeBatch());

    int[] updateCounts = stmt.executeBatch();
    assertThat(updateCounts).isEmpty();
  }

  /** Tests for isBatchCompatibleStatement method */
  @Test
  void testIsBatchCompatibleStatement_compatible_statements() {
    JDBC4Statement jdbc4Stmt = (JDBC4Statement) stmt;

    assertTrue(jdbc4Stmt.isBatchCompatibleStatement("INSERT INTO table VALUES (1, 2);"));
    assertTrue(jdbc4Stmt.isBatchCompatibleStatement("insert into table values (1, 2);"));
    assertTrue(
        jdbc4Stmt.isBatchCompatibleStatement("INSERT INTO table (col1, col2) VALUES (1, 2);"));
    assertTrue(jdbc4Stmt.isBatchCompatibleStatement("INSERT OR REPLACE INTO table VALUES (1);"));
    assertTrue(jdbc4Stmt.isBatchCompatibleStatement("INSERT OR IGNORE INTO table VALUES (1);"));

    assertTrue(jdbc4Stmt.isBatchCompatibleStatement("  INSERT INTO table VALUES (1);"));
    assertTrue(jdbc4Stmt.isBatchCompatibleStatement("\t\nINSERT INTO table VALUES (1);"));
    assertTrue(jdbc4Stmt.isBatchCompatibleStatement("   \n\t  INSERT INTO table VALUES (1);"));

    assertTrue(jdbc4Stmt.isBatchCompatibleStatement("/* comment */ INSERT INTO table VALUES (1);"));
    assertTrue(
        jdbc4Stmt.isBatchCompatibleStatement(
            "/* multi\nline\ncomment */ INSERT INTO table VALUES (1);"));
    assertTrue(
        jdbc4Stmt.isBatchCompatibleStatement("-- line comment\nINSERT INTO table VALUES (1);"));
    assertTrue(
        jdbc4Stmt.isBatchCompatibleStatement(
            "-- comment 1\n-- comment 2\nINSERT INTO table VALUES (1);"));

    assertTrue(
        jdbc4Stmt.isBatchCompatibleStatement(
            "  /* comment */ -- another\n  INSERT INTO table VALUES (1);"));

    assertTrue(jdbc4Stmt.isBatchCompatibleStatement("UPDATE table SET col = 1;"));
    assertTrue(jdbc4Stmt.isBatchCompatibleStatement("update table set col = 1;"));
    assertTrue(
        jdbc4Stmt.isBatchCompatibleStatement("UPDATE table SET col1 = 1, col2 = 2 WHERE id = 3;"));
    assertTrue(jdbc4Stmt.isBatchCompatibleStatement("UPDATE OR REPLACE table SET col = 1;"));

    assertTrue(jdbc4Stmt.isBatchCompatibleStatement("  UPDATE table SET col = 1;"));
    assertTrue(jdbc4Stmt.isBatchCompatibleStatement("\t\nUPDATE table SET col = 1;"));

    assertTrue(jdbc4Stmt.isBatchCompatibleStatement("/* comment */ UPDATE table SET col = 1;"));
    assertTrue(jdbc4Stmt.isBatchCompatibleStatement("-- comment\nUPDATE table SET col = 1;"));

    assertTrue(jdbc4Stmt.isBatchCompatibleStatement("DELETE FROM table;"));
    assertTrue(jdbc4Stmt.isBatchCompatibleStatement("delete from table;"));
    assertTrue(jdbc4Stmt.isBatchCompatibleStatement("DELETE FROM table WHERE id = 1;"));

    assertTrue(jdbc4Stmt.isBatchCompatibleStatement("  DELETE FROM table;"));
    assertTrue(jdbc4Stmt.isBatchCompatibleStatement("\t\nDELETE FROM table;"));

    assertTrue(jdbc4Stmt.isBatchCompatibleStatement("/* comment */ DELETE FROM table;"));
    assertTrue(jdbc4Stmt.isBatchCompatibleStatement("-- comment\nDELETE FROM table;"));
  }

  @Test
  void testIsBatchCompatibleStatement_non_compatible_statements() {
    JDBC4Statement jdbc4Stmt = (JDBC4Statement) stmt;

    assertFalse(jdbc4Stmt.isBatchCompatibleStatement("SELECT * FROM table;"));
    assertFalse(jdbc4Stmt.isBatchCompatibleStatement("select * from table;"));
    assertFalse(jdbc4Stmt.isBatchCompatibleStatement("  SELECT * FROM table;"));
    assertFalse(jdbc4Stmt.isBatchCompatibleStatement("/* comment */ SELECT * FROM table;"));
    assertFalse(jdbc4Stmt.isBatchCompatibleStatement("-- comment\nSELECT * FROM table;"));

    assertFalse(jdbc4Stmt.isBatchCompatibleStatement("EXPLAIN SELECT * FROM table;"));
    assertFalse(jdbc4Stmt.isBatchCompatibleStatement("EXPLAIN QUERY PLAN SELECT * FROM table;"));

    assertFalse(jdbc4Stmt.isBatchCompatibleStatement("PRAGMA table_info(table);"));
    assertFalse(jdbc4Stmt.isBatchCompatibleStatement("PRAGMA foreign_keys = ON;"));

    assertFalse(jdbc4Stmt.isBatchCompatibleStatement("ANALYZE;"));
    assertFalse(jdbc4Stmt.isBatchCompatibleStatement("ANALYZE table;"));

    assertFalse(
        jdbc4Stmt.isBatchCompatibleStatement(
            "WITH cte AS (SELECT * FROM table) SELECT * FROM cte;"));

    assertFalse(jdbc4Stmt.isBatchCompatibleStatement("VACUUM;"));

    assertFalse(jdbc4Stmt.isBatchCompatibleStatement("VALUES (1, 2), (3, 4);"));
  }

  @Test
  void testIsBatchCompatibleStatement_edge_cases() {
    JDBC4Statement jdbc4Stmt = (JDBC4Statement) stmt;

    assertFalse(jdbc4Stmt.isBatchCompatibleStatement(null));
    assertFalse(jdbc4Stmt.isBatchCompatibleStatement(""));
    assertFalse(jdbc4Stmt.isBatchCompatibleStatement("   "));
    assertFalse(jdbc4Stmt.isBatchCompatibleStatement("\t\n"));

    assertFalse(jdbc4Stmt.isBatchCompatibleStatement("/* comment only */"));
    assertFalse(jdbc4Stmt.isBatchCompatibleStatement("-- comment only"));
    assertFalse(jdbc4Stmt.isBatchCompatibleStatement("/* comment */ -- another comment"));

    assertFalse(jdbc4Stmt.isBatchCompatibleStatement("SELECT * FROM table WHERE name = 'INSERT';"));
    assertFalse(
        jdbc4Stmt.isBatchCompatibleStatement("SELECT * FROM table WHERE action = 'DELETE';"));

    assertFalse(jdbc4Stmt.isBatchCompatibleStatement("INSER INTO table VALUES (1);"));
    assertFalse(jdbc4Stmt.isBatchCompatibleStatement("UPDAT table SET col = 1;"));
    assertFalse(jdbc4Stmt.isBatchCompatibleStatement("DELET FROM table;"));
  }

  @Test
  void testIsBatchCompatibleStatement_case_insensitive() {
    JDBC4Statement jdbc4Stmt = (JDBC4Statement) stmt;

    assertTrue(jdbc4Stmt.isBatchCompatibleStatement("Insert INTO table VALUES (1);"));
    assertTrue(jdbc4Stmt.isBatchCompatibleStatement("InSeRt INTO table VALUES (1);"));
    assertTrue(jdbc4Stmt.isBatchCompatibleStatement("UPDATE table SET col = 1;"));
    assertTrue(jdbc4Stmt.isBatchCompatibleStatement("UpDaTe table SET col = 1;"));
    assertTrue(jdbc4Stmt.isBatchCompatibleStatement("Delete FROM table;"));
    assertTrue(jdbc4Stmt.isBatchCompatibleStatement("DeLeTe FROM table;"));
  }
}
