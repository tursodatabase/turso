package limbo;

public class Limbo {

    public static native Connection connect(String var0);

    static {
        System.loadLibrary("_limbo_java");
    }

    public static void main(String[] args) {
        Connection connection = connect("limbo.db");
        System.out.println(connection.getConnectionId());
        System.out.println("helo world");
    }
}
