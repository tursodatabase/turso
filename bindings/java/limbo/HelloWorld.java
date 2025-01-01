package limbo;

class HelloWorld {
    private static native String hello(String input);

    static {
        System.loadLibrary("_java_limbo");
    }

    public static void main(String[] args) {

        String output = HelloWorld.hello("josh");
        System.out.println(output);
    }
}
