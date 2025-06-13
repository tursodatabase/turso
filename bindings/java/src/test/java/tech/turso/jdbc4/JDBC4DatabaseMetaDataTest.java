package tech.turso.jdbc4;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import tech.turso.TestUtils;

class JDBC4DatabaseMetaDataTest {

  private JDBC4Connection connection;
  private JDBC4DatabaseMetaData metaData;

  @BeforeEach
  void set_up() throws Exception {
    String filePath = TestUtils.createTempFile();
    String url = "jdbc:sqlite:" + filePath;
    connection = new JDBC4Connection(url, filePath, new Properties());
    metaData = new JDBC4DatabaseMetaData(connection);
  }

  @Test
  void getURL_should_return_non_empty_string() {
    assertFalse(metaData.getURL().isEmpty());
  }

  @Test
  void getDriverName_should_not_return_empty_string() {
    assertFalse(metaData.getDriverName().isEmpty());
  }

  @Test
  void test_getDriverMajorVersion() {
    metaData.getDriverMajorVersion();
  }

  @Test
  void test_getDriverMinorVersion() {
    metaData.getDriverMinorVersion();
  }

  @Test
  void getDriverVersion_should_not_return_empty_string() {
    assertFalse(metaData.getDriverVersion().isEmpty());
  }

  @Test
  void getTables_with_non_empty_catalog_should_return_empty() throws SQLException {
    ResultSet rs = metaData.getTables("nonexistent", null, null, null);
    assertNotNull(rs);
    assertFalse(rs.next());
    rs.close();
  }

  @Test
  void getTables_with_non_empty_schema_should_return_empty() throws SQLException {
    ResultSet rs = metaData.getTables(null, "schema", null, null);
    assertNotNull(rs);
    assertFalse(rs.next());
    rs.close();
  }

  @Test
  void getTables_should_return_correct_table_info() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY)");
    }

    ResultSet rs = metaData.getTables(null, null, null, null);

    assertNotNull(rs);

    assertTrue(rs.next());

    assertEquals("test_table", rs.getString("TABLE_NAME"));
    assertEquals("TABLE", rs.getString("TABLE_TYPE"));

    assertFalse(rs.next());

    rs.close();
  }

  @Test
  void getTables_with_pattern_should_filter_results() throws SQLException {
    // Create test tables
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("CREATE TABLE test1 (id INTEGER PRIMARY KEY)");
      stmt.execute("CREATE TABLE test2 (id INTEGER PRIMARY KEY)");
      stmt.execute("CREATE TABLE other (id INTEGER PRIMARY KEY)");
    }

    ResultSet rs = metaData.getTables(null, null, "test%", null);

    assertNotNull(rs);

    int tableCount = 0;
    while (rs.next()) {
      String tableName = rs.getString("TABLE_NAME");
      assertTrue(tableName.startsWith("test"));
      tableCount++;
    }

    assertEquals(2, tableCount);

    rs.close();
  }

  @Test
  @Disabled("CREATE VIEW not supported yet")
  void getTables_with_type_filter_should_return_only_views() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("CREATE TABLE my_table (id INTEGER PRIMARY KEY)");
      stmt.execute("CREATE VIEW my_view AS SELECT * FROM my_table");
    }

    ResultSet rs = metaData.getTables(null, null, null, new String[] {"VIEW"});

    assertNotNull(rs);

    assertTrue(rs.next());
    assertEquals("my_view", rs.getString("TABLE_NAME"));
    assertEquals("VIEW", rs.getString("TABLE_TYPE"));

    assertFalse(rs.next());

    rs.close();
  }

  @Test
  @Disabled("CREATE VIEW not supported yet")
  void getTables_with_pattern_and_type_filter_should_work_together() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("CREATE TABLE alpha (id INTEGER)");
      stmt.execute("CREATE TABLE beta (id INTEGER)");
      stmt.execute("CREATE VIEW alpha_view AS SELECT * FROM alpha");
    }

    ResultSet rs = metaData.getTables(null, null, "alpha%", new String[] {"VIEW"});

    assertNotNull(rs);

    assertTrue(rs.next());
    assertEquals("alpha_view", rs.getString("TABLE_NAME"));
    assertEquals("VIEW", rs.getString("TABLE_TYPE"));

    assertFalse(rs.next());

    rs.close();
  }
}
