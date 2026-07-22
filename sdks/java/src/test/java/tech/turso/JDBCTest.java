package tech.turso;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import tech.turso.jdbc4.JDBC4Connection;

class JDBCTest {

  @Test
  void null_is_returned_when_invalid_url_is_passed() throws Exception {
    JDBC4Connection connection = JDBC.createConnection("jdbc:invalid:xxx", new Properties());
    assertThat(connection).isNull();
  }

  @Test
  void non_null_connection_is_returned_when_valid_url_is_passed() throws Exception {
    String fileUrl = TestUtils.createTempFile();
    JDBC4Connection connection = JDBC.createConnection("jdbc:turso:" + fileUrl, new Properties());
    assertThat(connection).isNotNull();
  }

  @Test
  void connection_can_be_retrieved_from_DriverManager() throws SQLException {
    try (Connection connection = DriverManager.getConnection("jdbc:turso:sample.db")) {
      assertThat(connection).isNotNull();
    }
  }

  @Test
  void retrieve_version() {
    assertDoesNotThrow(() -> DriverManager.getDriver("jdbc:turso:").getMajorVersion());
    assertDoesNotThrow(() -> DriverManager.getDriver("jdbc:turso:").getMinorVersion());
  }

  @Test
  void all_driver_property_info_should_have_a_description() throws Exception {
    Driver driver = DriverManager.getDriver("jdbc:turso:");
    assertThat(driver.getPropertyInfo(null, null))
        .allSatisfy((info) -> assertThat(info.description).isNotNull());
  }

  @Test
  void return_null_when_protocol_can_not_be_handled() throws Exception {
    assertThat(JDBC.createConnection("jdbc:unknownprotocol:", null)).isNull();
  }
}
