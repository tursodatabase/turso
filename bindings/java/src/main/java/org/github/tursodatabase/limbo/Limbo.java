package org.github.tursodatabase.limbo;

import java.lang.Exception;

public class Limbo {
    private static volatile boolean initialized;

    private Limbo() {
        if (!initialized) {
            System.loadLibrary("_limbo_java");
            initialized = true;
        }
    }

    public static Limbo create() {
        return new Limbo();
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
