package tech.turso.jdbc4;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    String url = "jdbc:sqlite:" + filePath;
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
    assertFalse(stmt.execute("INSERT INTO users VALUES (1, 'limbo');"));
  }

  @Test
  void execute_update_should_return_false() throws Exception {
    stmt.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT);");
    stmt.execute("INSERT INTO users VALUES (1, 'limbo');");
    assertFalse(stmt.execute("UPDATE users SET username = 'seonwoo' WHERE id = 1;"));
  }

  @Test
  void execute_select_should_return_true() throws Exception {
    stmt.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT);");
    stmt.execute("INSERT INTO users VALUES (1, 'limbo');");
    assertTrue(stmt.execute("SELECT * FROM users;"));
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
}
