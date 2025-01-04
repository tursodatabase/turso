package org.github.tursodatabase.limbo;

import java.lang.Exception;

public class Limbo {
    static {
        System.loadLibrary("_limbo_java");
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
