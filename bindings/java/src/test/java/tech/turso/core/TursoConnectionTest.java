package tech.turso.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import tech.turso.TestUtils;

class TursoConnectionTest {

  @Test
  void closing_connection_twice_frees_it_only_once() throws Exception {
    String dbPath = TestUtils.createTempFile();
    TursoConnection conn = new TursoConnection("jdbc:turso:" + dbPath, dbPath);

    conn.close();
    conn.close();

    assertTrue(conn.isClosed());
  }

  @Test
  void prepare_after_close_throws_instead_of_touching_freed_memory() throws Exception {
    String dbPath = TestUtils.createTempFile();
    TursoConnection conn = new TursoConnection("jdbc:turso:" + dbPath, dbPath);
    conn.close();

    assertThrows(SQLException.class, () -> conn.prepare("SELECT 1"));
  }

  @Test
  void statement_keeps_working_after_its_connection_is_closed() throws Exception {
    String dbPath = TestUtils.createTempFile();
    TursoConnection conn = new TursoConnection("jdbc:turso:" + dbPath, dbPath);
    TursoStatement stmt = conn.prepare("SELECT 1");
    conn.close();

    assertThat(stmt.execute()).isTrue();
    assertThat(stmt.getResultSet().get(1)).isEqualTo(1L);
    stmt.close();
  }
}
