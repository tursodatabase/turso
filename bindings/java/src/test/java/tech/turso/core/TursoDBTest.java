package tech.turso.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.nio.file.Files;
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

  @Test
  void db_should_open_with_encryption() throws Exception {
    String dbPath = TestUtils.createTempFile();
    String hexkey = "b1bbfda4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";
    String wrongKey = "aaaaaaa4f589dc9daaf004fe21111e00dc00c98237102f5c7002a5669fc76327";

    // Create encrypted database
    TursoDB db =
        TursoDB.createWithEncryption(
            "jdbc:turso:" + dbPath, dbPath, TursoEncryptionCipher.AEGIS_256, hexkey);
    TursoConnection conn = new TursoConnection("jdbc:turso:" + dbPath, db);

    TursoStatement stmt = conn.prepare("CREATE TABLE t(x TEXT)");
    stmt.execute();
    stmt.close();

    stmt = conn.prepare("INSERT INTO t VALUES ('secret')");
    stmt.execute();
    stmt.close();

    stmt = conn.prepare("PRAGMA wal_checkpoint(truncate)");
    stmt.execute();
    stmt.close();

    conn.close();
    db.close();

    // Verify data is encrypted on disk
    byte[] content = Files.readAllBytes(new File(dbPath).toPath());
    assertThat(content.length).isGreaterThan(1024);
    String contentStr = new String(content);
    assertThat(contentStr).doesNotContain("secret");

    // Verify we can re-open with the same key
    TursoDB db2 =
        TursoDB.createWithEncryption(
            "jdbc:turso:" + dbPath, dbPath, TursoEncryptionCipher.AEGIS_256, hexkey);
    TursoConnection conn2 = new TursoConnection("jdbc:turso:" + dbPath, db2);

    TursoStatement stmt2 = conn2.prepare("SELECT * FROM t");
    boolean hasResult = stmt2.execute();
    assertThat(hasResult).isTrue();
    TursoResultSet rs = stmt2.getResultSet();
    assertThat(rs.get(1)).isEqualTo("secret");
    stmt2.close();

    conn2.close();
    db2.close();

    // Verify opening with wrong key fails
    assertThrows(
        Exception.class,
        () -> {
          TursoDB dbWrongKey =
              TursoDB.createWithEncryption(
                  "jdbc:turso:" + dbPath, dbPath, TursoEncryptionCipher.AEGIS_256, wrongKey);
          TursoConnection connWrongKey = new TursoConnection("jdbc:turso:" + dbPath, dbWrongKey);
          TursoStatement stmtWrongKey = connWrongKey.prepare("SELECT * FROM t");
          stmtWrongKey.execute();
        });

    // Verify opening without encryption fails
    assertThrows(
        Exception.class,
        () -> {
          TursoDB dbNoEncryption = TursoDB.create("jdbc:turso:" + dbPath, dbPath);
          TursoConnection connNoEncryption =
              new TursoConnection("jdbc:turso:" + dbPath, dbNoEncryption);
          TursoStatement stmtNoEncryption = connNoEncryption.prepare("SELECT * FROM t");
          stmtNoEncryption.execute();
        });
  }
}
