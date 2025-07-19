package tech.turso.core;

import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Factory class for managing and creating instances of {@link TursoDB}. This class ensures that
 * multiple instances of {@link TursoDB} with the same URL are not created.
 */
public final class TursoDBFactory {

  private static final ConcurrentHashMap<String, TursoDB> databaseHolder =
      new ConcurrentHashMap<>();

  private TursoDBFactory() {}

  /**
   * If a database with the same URL already exists, it returns the existing instance. Otherwise, it
   * creates a new instance and stores it in the database holder.
   *
   * @param url the URL of the database
   * @param filePath the path to the database file
   * @param properties additional properties for the database connection
   * @return an instance of {@link tursoDB}
   * @throws SQLException if there is an error opening the connection
   * @throws IllegalArgumentException if the fileName is empty
   */
  public static TursoDB open(String url, String filePath, Properties properties)
      throws SQLException {
    if (filePath.isEmpty()) {
      throw new IllegalArgumentException("filePath should not be empty");
    }

    try {
      return databaseHolder.computeIfAbsent(
          url, (Sneaky<String, TursoDB, SQLException>) u -> TursoDB.create(u, filePath));
    } catch (Exception e) {
      throw new SQLException("Error opening connection", e);
    }
  }

  private interface Sneaky<S, T, E extends Exception> extends Function<S, T> {

    T applySneakily(S s) throws E;

    @Override
    default T apply(S s) {
      try {
        return applySneakily(s);
      } catch (Exception e) {
        throw sneakyThrow(e);
      }
    }

    @SuppressWarnings("unchecked")
    static <T extends Exception> T sneakyThrow(Exception e) throws T {
      throw (T) e;
    }
  }
}
