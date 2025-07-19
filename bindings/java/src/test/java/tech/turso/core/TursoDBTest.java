package tech.turso.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;
import tech.turso.TestUtils;
import tech.turso.TursoErrorCode;
import tech.turso.exceptions.TursoException;

public class TursoDBTest {

  @Test
  void db_should_open_and_close_normally() throws Exception {
    String dbPath = TestUtils.createTempFile();
    TursoDB db = TursoDB.create("jdbc:turso" + dbPath, dbPath);

    db.close();

    assertFalse(db.isOpen());
  }

  @Test
  void throwJavaException_should_throw_appropriate_java_exception() throws Exception {
    String dbPath = TestUtils.createTempFile();
    TursoDB db = TursoDB.create("jdbc:turso:" + dbPath, dbPath);

    final int tursoExceptionCode = TursoErrorCode.TURSO_ETC.code;
    try {
      db.throwJavaException(tursoExceptionCode);
    } catch (Exception e) {
      assertThat(e).isInstanceOf(TursoException.class);
      TursoException tursoException = (TursoException) e;
      assertThat(tursoException.getResultCode().code).isEqualTo(tursoExceptionCode);
    }
  }
}
