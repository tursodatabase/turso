package limbo;

public class Limbo {

    public static native long connect(String var0);
    public static native void test(long connectionId);

    static {
        System.loadLibrary("_limbo_java");
    }

    public static void main(String[] args) {
        long connectionId = connect("limbo.db");
        System.out.println("connectionId: " + connectionId);

        test(connectionId);
    }
}
