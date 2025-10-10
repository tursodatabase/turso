package tech.turso.utils;

import java.nio.charset.StandardCharsets;
import tech.turso.annotations.Nullable;

/** Utility class for converting between byte arrays and strings using UTF-8 encoding. */
public final class ByteArrayUtils {
  /**
   * Converts a UTF-8 encoded byte array to a string.
   *
   * @param buffer the byte array to convert, may be null
   * @return the string representation, or null if the input is null
   */
  @Nullable
  public static String utf8ByteBufferToString(@Nullable byte[] buffer) {
    if (buffer == null) {
      return null;
    }

    return new String(buffer, StandardCharsets.UTF_8);
  }

  /**
   * Converts a string to a UTF-8 encoded byte array.
   *
   * @param str the string to convert, may be null
   * @return the byte array representation, or null if the input is null
   */
  @Nullable
  public static byte[] stringToUtf8ByteArray(@Nullable String str) {
    if (str == null) {
      return null;
    }
    return str.getBytes(StandardCharsets.UTF_8);
  }
}
