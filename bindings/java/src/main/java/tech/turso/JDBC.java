package tech.turso;

import java.sql.*;
import java.util.Locale;
import java.util.Properties;
import tech.turso.annotations.Nullable;
import tech.turso.annotations.SkipNullableCheck;
import tech.turso.core.TursoPropertiesHolder;
import tech.turso.jdbc4.JDBC4Connection;
import tech.turso.utils.Logger;
import tech.turso.utils.LoggerFactory;

public final class JDBC implements Driver {

  private static final Logger logger = LoggerFactory.getLogger(JDBC.class);

  private static final String VALID_URL_PREFIX = "jdbc:turso:";

  static {
    try {
      DriverManager.registerDriver(new JDBC());
    } catch (Exception e) {
      logger.error("Failed to register driver", e);
    }
  }

  @Nullable
  public static JDBC4Connection createConnection(String url, Properties properties)
      throws SQLException {
    if (!isValidURL(url)) return null;

    url = url.trim();
    return new JDBC4Connection(url, extractAddress(url), properties);
  }

  private static boolean isValidURL(String url) {
    return (url != null && url.toLowerCase(Locale.ROOT).startsWith(VALID_URL_PREFIX));
  }

  private static String extractAddress(String url) {
    return url.substring(VALID_URL_PREFIX.length());
  }

  @Nullable
  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    return createConnection(url, info);
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return isValidURL(url);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    return TursoConfig.getDriverPropertyInfo();
  }

  @Override
  public int getMajorVersion() {
    return Integer.parseInt(TursoPropertiesHolder.getDriverVersion().split("\\.")[0]);
  }

  @Override
  public int getMinorVersion() {
    return Integer.parseInt(TursoPropertiesHolder.getDriverVersion().split("\\.")[1]);
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  @SkipNullableCheck
  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    // TODO
    return null;
  }
}
