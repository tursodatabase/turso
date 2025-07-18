package tech.turso.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.Properties;
import org.junit.jupiter.api.Test;
import tech.turso.TestUtils;

class TursoDBFactoryTest {

  @Test
  void single_database_should_be_created_when_urls_are_same() throws Exception {
    String filePath = TestUtils.createTempFile();
    String url = "jdbc:turso:" + filePath;
    TursoDB db1 = TursoDBFactory.open(url, filePath, new Properties());
    TursoDB db2 = TursoDBFactory.open(url, filePath, new Properties());
    assertEquals(db1, db2);
  }

  @Test
  void multiple_databases_should_be_created_when_urls_differ() throws Exception {
    String filePath1 = TestUtils.createTempFile();
    String filePath2 = TestUtils.createTempFile();
    String url1 = "jdbc:turso:" + filePath1;
    String url2 = "jdbc:turso:" + filePath2;
    TursoDB db1 = TursoDBFactory.open(url1, filePath1, new Properties());
    TursoDB db2 = TursoDBFactory.open(url2, filePath2, new Properties());
    assertNotEquals(db1, db2);
  }
}
