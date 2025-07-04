package tech.turso.core;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.turso.TestUtils;
import tech.turso.jdbc4.JDBC4Connection;

class TursoStatementTest {

  private JDBC4Connection connection;

  @BeforeEach
  void setUp() throws Exception {
    String filePath = TestUtils.createTempFile();
    String url = "jdbc:turso:" + filePath;
    connection = new JDBC4Connection(url, filePath, new Properties());
  }

  @Test
  void closing_statement_closes_related_resources() throws Exception {
    TursoStatement stmt = connection.prepare("SELECT 1;");
    stmt.execute();

    stmt.close();
    assertTrue(stmt.isClosed());
    assertFalse(stmt.getResultSet().isOpen());
  }

  @Test
  void test_initializeColumnMetadata() throws Exception {
    runSql("CREATE TABLE users (name TEXT, age INT, country TEXT);");
    runSql("INSERT INTO users VALUES ('seonwoo', 30, 'KR');");

    final TursoStatement stmt = connection.prepare("SELECT * FROM users");
    stmt.initializeColumnMetadata();
    final TursoResultSet rs = stmt.getResultSet();
    final String[] columnNames = rs.getColumnNames();

    assertEquals("name", columnNames[0]);
    assertEquals("age", columnNames[1]);
    assertEquals("country", columnNames[2]);
  }

  @Test
  void test_bindNull() throws Exception {
    runSql("CREATE TABLE test (col1 TEXT);");
    TursoStatement stmt = connection.prepare("INSERT INTO test (col1) VALUES (?);");
    stmt.bindNull(1);
    stmt.execute();
    stmt.close();

    TursoStatement selectStmt = connection.prepare("SELECT col1 FROM test;");
    selectStmt.execute();
    assertNull(selectStmt.getResultSet().get(1));
    selectStmt.close();
  }

  @Test
  void test_bindLong() throws Exception {
    runSql("CREATE TABLE test (col1 BIGINT);");
    TursoStatement stmt = connection.prepare("INSERT INTO test (col1) VALUES (?);");
    stmt.bindLong(1, 123456789L);
    stmt.execute();
    stmt.close();

    TursoStatement selectStmt = connection.prepare("SELECT col1 FROM test;");
    selectStmt.execute();
    assertEquals(123456789L, selectStmt.getResultSet().get(1));
    selectStmt.close();
  }

  @Test
  void test_bindDouble() throws Exception {
    runSql("CREATE TABLE test (col1 DOUBLE);");
    TursoStatement stmt = connection.prepare("INSERT INTO test (col1) VALUES (?);");
    stmt.bindDouble(1, 3.14);
    stmt.execute();
    stmt.close();

    TursoStatement selectStmt = connection.prepare("SELECT col1 FROM test;");
    selectStmt.execute();
    assertEquals(3.14, selectStmt.getResultSet().get(1));
    selectStmt.close();
  }

  @Test
  void test_bindText() throws Exception {
    runSql("CREATE TABLE test (col1 TEXT);");
    TursoStatement stmt = connection.prepare("INSERT INTO test (col1) VALUES (?);");
    stmt.bindText(1, "hello");
    stmt.execute();
    stmt.close();

    TursoStatement selectStmt = connection.prepare("SELECT col1 FROM test;");
    selectStmt.execute();
    assertEquals("hello", selectStmt.getResultSet().get(1));
    selectStmt.close();
  }

  @Test
  void test_bindBlob() throws Exception {
    runSql("CREATE TABLE test (col1 BLOB);");
    TursoStatement stmt = connection.prepare("INSERT INTO test (col1) VALUES (?);");
    byte[] blob = {1, 2, 3, 4, 5};
    stmt.bindBlob(1, blob);
    stmt.execute();
    stmt.close();

    TursoStatement selectStmt = connection.prepare("SELECT col1 FROM test;");
    selectStmt.execute();
    assertArrayEquals(blob, (byte[]) selectStmt.getResultSet().get(1));
    selectStmt.close();
  }

  private void runSql(String sql) throws Exception {
    TursoStatement stmt = connection.prepare(sql);
    while (stmt.execute()) {
      stmt.execute();
    }
  }
}
