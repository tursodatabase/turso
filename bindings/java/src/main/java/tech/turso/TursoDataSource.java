package tech.turso;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import javax.sql.DataSource;
import tech.turso.annotations.Nullable;
import tech.turso.annotations.SkipNullableCheck;

/** Provides {@link DataSource} API for configuring Turso database connection. */
public final class TursoDataSource implements DataSource {

  private final TursoConfig tursoConfig;
  private final String url;

  /**
   * Creates a datasource based on the provided configuration.
   *
   * @param tursoConfig The configuration for the datasource.
   */
  public TursoDataSource(TursoConfig tursoConfig, String url) {
    this.tursoConfig = tursoConfig;
    this.url = url;
  }

  @Override
  @Nullable
  public Connection getConnection() throws SQLException {
    return getConnection(null, null);
  }

  @Override
  @Nullable
  public Connection getConnection(@Nullable String username, @Nullable String password)
      throws SQLException {
    Properties properties = tursoConfig.toProperties();
    if (username != null) properties.put("user", username);
    if (password != null) properties.put("pass", password);
    return JDBC.createConnection(url, properties);
  }

  @Override
  @SkipNullableCheck
  public PrintWriter getLogWriter() throws SQLException {
    // TODO
    return null;
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    // TODO
  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    // TODO
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    // TODO
    return 0;
  }

  @Override
  @SkipNullableCheck
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    // TODO
    return null;
  }

  @Override
  @SkipNullableCheck
  public <T> T unwrap(Class<T> iface) throws SQLException {
    // TODO
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    // TODO
    return false;
  }
}
