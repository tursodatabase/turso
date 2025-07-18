package tech.turso.utils;

import static tech.turso.utils.ByteArrayUtils.utf8ByteBufferToString;

import java.sql.SQLException;
import tech.turso.TursoErrorCode;
import tech.turso.annotations.Nullable;
import tech.turso.exceptions.TursoException;

public final class TursoExceptionUtils {

  /**
   * Throws formatted SQLException with error code and message.
   *
   * @param errorCode Error code.
   * @param errorMessageBytes Error message.
   */
  public static void throwTursoException(int errorCode, byte[] errorMessageBytes)
      throws SQLException {
    String errorMessage = utf8ByteBufferToString(errorMessageBytes);
    throw buildTursoException(errorCode, errorMessage);
  }

  /**
   * Throws formatted SQLException with error code and message.
   *
   * @param errorCode Error code.
   * @param errorMessage Error message.
   */
  public static TursoException buildTursoException(int errorCode, @Nullable String errorMessage)
      throws SQLException {
    TursoErrorCode code = TursoErrorCode.getErrorCode(errorCode);
    String msg;
    if (code == TursoErrorCode.UNKNOWN_ERROR) {
      msg = String.format("%s:%s (%s)", code, errorCode, errorMessage);
    } else {
      msg = String.format("%s (%s)", code, errorMessage);
    }

    return new TursoException(msg, code);
  }
}
