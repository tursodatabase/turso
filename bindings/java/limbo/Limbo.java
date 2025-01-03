package limbo;

import java.lang.Exception;

public class Limbo {

    static {
        System.loadLibrary("_limbo_java");
    }

    public static void main(String[] args) throws Exception {
        Limbo limbo = new Limbo();
        Connection connection = limbo.getConnection("./database.db");

        Cursor cursor = connection.cursor();
        cursor.execute("SELECT * FROM users;");
        cursor.fetchOne();
    }

    public Connection getConnection(String path) throws Exception {
        long connectionId = connect(path);
        if (connectionId == -1) {
            throw new Exception("Failed to initialize connection");
        }
        return new Connection(connectionId);
    }

    private static native long connect(String path);
}
