package limbo;

public class Limbo {

    public static native long connect(String var0);

    static {
        System.loadLibrary("_limbo_java");
    }

    public static void main(String[] args) throws Exception {
        long connectionId = connect("limbo.db");
        System.out.println("connectionId: " + connectionId);
        Connection connection = new Connection(connectionId);
        connection.cursor();
        connection.close();
        connection.commit();
        connection.cursor();
    }
}
