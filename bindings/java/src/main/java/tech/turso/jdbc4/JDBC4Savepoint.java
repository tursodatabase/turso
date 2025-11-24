package tech.turso.jdbc4;

import java.sql.SQLException;
import java.sql.Savepoint;

public class JDBC4Savepoint implements Savepoint {

  private final int id;
  private final String name;

  public JDBC4Savepoint(int id, String name) {
    this.id = id;
    this.name = name;
  }

  @Override
  public int getSavepointId() throws SQLException {
    if (name != null) {
      throw new SQLException("Cannot retrieve id for named savepoint");
    }

    return id;
  }

  @Override
  public String getSavepointName() throws SQLException {
    if (name == null) {
      throw new SQLException("Cannot retrieve name for unnamed savepoint");
    }

    return name;
  }
}
