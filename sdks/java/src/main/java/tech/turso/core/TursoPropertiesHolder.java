package tech.turso.core;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import tech.turso.jdbc4.JDBC4DatabaseMetaData;
import tech.turso.utils.Logger;
import tech.turso.utils.LoggerFactory;

public class TursoPropertiesHolder {

  private static final Logger logger = LoggerFactory.getLogger(TursoPropertiesHolder.class);

  private static String driverName = "";
  private static String driverVersion = "";

  static {
    try (InputStream tursoJdbcPropStream =
        JDBC4DatabaseMetaData.class
            .getClassLoader()
            .getResourceAsStream("turso-jdbc.properties"); ) {
      if (tursoJdbcPropStream == null) {
        throw new IOException("Cannot load turso-jdbc.properties from jar");
      }

      final Properties properties = new Properties();
      properties.load(tursoJdbcPropStream);
      driverName = properties.getProperty("driverName");
      driverVersion = properties.getProperty("driverVersion");
    } catch (IOException e) {
      logger.error("Failed to load driverName and driverVersion");
    }
  }

  public static String getDriverName() {
    return driverName;
  }

  public static String getDriverVersion() {
    return driverVersion;
  }
}
