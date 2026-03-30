package tech.turso;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Main {

  public static void main(String[] args) throws SQLException {
    try (Connection conn = DriverManager.getConnection("jdbc:turso:sample.db")) {
      try (Statement stmt =
          conn.createStatement(
              ResultSet.TYPE_FORWARD_ONLY,
              ResultSet.CONCUR_READ_ONLY,
              ResultSet.CLOSE_CURSORS_AT_COMMIT)) {
        stmt.execute("CREATE TABLE users (id INT PRIMARY KEY, username TEXT);");
        stmt.execute("INSERT INTO users VALUES (1, 'turso');");
        stmt.execute("INSERT INTO users VALUES (2, 'turso');");
        stmt.execute("INSERT INTO users VALUES (3, 'who knows');");
        stmt.execute("SELECT * FROM users");
        System.out.println(
            "result: " + stmt.getResultSet().getInt(1) + ", " + stmt.getResultSet().getString(2));
      }
    }
  }
}
