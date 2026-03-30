package tech.turso.jdbc4;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.turso.TestUtils;

public class TransactionTest {

  private Connection connection;
  private String url;

  @BeforeEach
  public void setUp() throws Exception {
    String filePath = TestUtils.createTempFile();
    url = "jdbc:turso:" + filePath;
    connection = DriverManager.getConnection(url, new Properties());
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
  }

  @Test
  public void test_default_auto_commit() throws SQLException {
    assertTrue(connection.getAutoCommit());
  }

  @Test
  public void test_commit_rollback_in_auto_commit_mode() {
    SQLException e1 = assertThrows(SQLException.class, () -> connection.commit());
    assertTrue(e1.getMessage().contains("Cannot commit in autocommit mode"));

    SQLException e2 = assertThrows(SQLException.class, () -> connection.rollback());
    assertTrue(e2.getMessage().contains("Cannot rollback in autocommit mode"));
  }

  @Test
  public void test_basic_commit() throws SQLException {
    connection.setAutoCommit(false);
    assertFalse(connection.getAutoCommit());

    try (Statement stmt = connection.createStatement()) {
      stmt.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)");
      stmt.execute("INSERT INTO test (name) VALUES ('Alice')");
      connection.commit();
    }

    // verify data persists in new connection or same connection
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM test")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
    }
  }

  @Test
  public void test_basic_rollback() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)");
    }

    connection.setAutoCommit(false);

    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO test (name) VALUES ('Bob')");
      connection.rollback();
    }

    // verify data is gone
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM test")) {
      assertTrue(rs.next());
      assertEquals(0, rs.getInt(1));
    }
  }

  @Test
  public void test_auto_commit_toggle_commits() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)");
    }

    connection.setAutoCommit(false);
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO test (name) VALUES ('Charlie')");
    }

    // Setting autoCommit to true should commit the pending transaction
    connection.setAutoCommit(true);
    assertTrue(connection.getAutoCommit());

    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM test")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
    }
  }

  @Test
  public void test_isolation_level() throws SQLException {
    int[] levels = {
      Connection.TRANSACTION_SERIALIZABLE,
      Connection.TRANSACTION_READ_COMMITTED,
      Connection.TRANSACTION_READ_UNCOMMITTED,
      Connection.TRANSACTION_REPEATABLE_READ
    };

    for (int level : levels) {
      connection.setTransactionIsolation(level);
      assertEquals(level, connection.getTransactionIsolation());
    }

    // Should throw if changing during transaction
    connection.setAutoCommit(false);
    connection.setTransactionIsolation(
        Connection.TRANSACTION_SERIALIZABLE); // Valid (not started yet)

    try (Statement stmt = connection.createStatement()) {
      stmt.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)"); // starts transaction
    }

    SQLException e =
        assertThrows(
            SQLException.class,
            () -> connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE));
    assertTrue(
        e.getMessage().contains("Cannot change isolation level while transaction is active"));

    connection.rollback();
  }

  @Test
  public void test_no_op_commit_rollback() throws SQLException {
    connection.setAutoCommit(false);

    // No transaction active yet (lazy start)
    // Should be no-op (no exception)
    connection.commit();
    connection.rollback();

    // Start transaction
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)");
    }

    // Now active, commit it
    connection.commit();

    // Back to inactive
    connection.rollback(); // no-op
  }

  @Test
  public void test_savepoints_not_supported() throws SQLException {
    assertThrows(SQLException.class, () -> connection.setSavepoint());
    assertThrows(SQLException.class, () -> connection.setSavepoint("foo"));
    assertThrows(SQLException.class, () -> connection.rollback(null));
    assertThrows(SQLException.class, () -> connection.releaseSavepoint(null));
  }

  @Test
  public void test_close_without_commit_rolls_back() throws SQLException {
    connection.setAutoCommit(false);

    // Create table and insert data
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("CREATE TABLE test_rollback (id INTEGER PRIMARY KEY, name TEXT)");
      stmt.execute("INSERT INTO test_rollback (name) VALUES ('rollback_me')");
    }

    // Close connection without commit
    connection.close();

    // Reopen connection to check data
    try (Connection newConn = DriverManager.getConnection(url, new Properties());
        Statement stmt = newConn.createStatement()) {

      // Let's expect the table might not exist.
      SQLException e =
          assertThrows(SQLException.class, () -> stmt.executeQuery("SELECT * FROM test_rollback"));
      assertTrue(
          e.getMessage().contains("no such table"),
          "Expected 'no such table' but got: " + e.getMessage());
    }
  }

  @Test
  public void test_mixed_commit_rollback_scenario() throws SQLException {
    connection.setAutoCommit(false);

    // Initial schema setup - commit
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)");
    }
    connection.commit();

    // Insert generic data - commit
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO test (name) VALUES ('item_1')");
      stmt.execute("INSERT INTO test (name) VALUES ('item_2')");
    }
    connection.commit();

    // Insert more data - rollback
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("INSERT INTO test (name) VALUES ('item_to_rollback')");
    }
    connection.rollback();

    // Verify only committed data exists
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM test ORDER BY id")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      assertEquals("item_1", rs.getString(2));

      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
      assertEquals("item_2", rs.getString(2));

      assertFalse(rs.next(), "Should only have 2 committed rows");
    }
  }

  @Test
  public void test_double_close_is_safe() throws SQLException {
    connection.close();
    // Second close should be no-op or safe
    assertDoesNotThrow(() -> connection.close());
    assertTrue(connection.isClosed());
  }

  @Test
  public void test_idempotent_set_auto_commit() throws SQLException {
    assertTrue(connection.getAutoCommit());

    // Setting same value multiple times
    connection.setAutoCommit(true);
    assertTrue(connection.getAutoCommit());

    connection.setAutoCommit(false);
    assertFalse(connection.getAutoCommit());

    connection.setAutoCommit(false); // Second call
    assertFalse(connection.getAutoCommit());
  }

  @Test
  public void test_manual_transaction_control_passthrough() throws SQLException {
    // Even if managed by driver, manual BEGIN/COMMIT should pass through
    // Note: Use with caution as it might desync abstract state
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("BEGIN");
      stmt.execute("CREATE TABLE manual_txn (id INT)");
      stmt.execute("COMMIT");
    }

    try (Statement stmt = connection.createStatement();
        ResultSet rs =
            stmt.executeQuery(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='manual_txn'")) {
      assertTrue(rs.next());
    }
  }
}
