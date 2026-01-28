package com.turso.sync.reactnative;

import java.io.File;
import java.lang.Exception;
import java.util.HashMap;
import java.util.Map;

import androidx.annotation.NonNull;

import com.facebook.react.bridge.ReactApplicationContext;
import com.facebook.react.bridge.ReactContextBaseJavaModule;
import com.facebook.react.bridge.ReactMethod;
import com.facebook.react.module.annotations.ReactModule;
import com.facebook.react.turbomodule.core.CallInvokerHolderImpl;

// The React Native bridge - exposes methods to JavaScript
@ReactModule(name = TursoModule.NAME)
public class TursoModule extends ReactContextBaseJavaModule {
    public static final String NAME = "Turso";

    static {
        System.loadLibrary("turso_sync_sdk_kit");
        System.loadLibrary("turso-sync-react-native");
    }

    public TursoModule(ReactApplicationContext reactContext) {
        super(reactContext);
    }

    @Override
    @NonNull
    public String getName() {
        return NAME;
    }

    @Override
    public Map<String, Object> getConstants() {
        final Map<String, Object> constants = new HashMap<>();
        ReactApplicationContext context = getReactApplicationContext();

        // getDatabasePath(...) returns file path - so we pass dummy value and remove it
        // after to get directory path
        String dbPath = context.getDatabasePath("tursoDatabaseFile").getAbsolutePath().replace("tursoDatabaseFile", "");
        constants.put("ANDROID_DATABASE_PATH", dbPath);

        String filesPath = context.getFilesDir().getAbsolutePath();
        constants.put("ANDROID_FILES_PATH", filesPath);

        File externalFilesDir = context.getExternalFilesDir(null);
        constants.put("ANDROID_EXTERNAL_FILES_PATH",
                externalFilesDir != null ? externalFilesDir.getAbsolutePath() : null);

        // populate Android and IOS constants to simplify JS code (e.g.
        // IOS_DOCUMENT_PATH ?? ANDROID_DATABASE_PATH)
        constants.put("IOS_DOCUMENT_PATH", null);
        constants.put("IOS_LIBRARY_PATH", null);

        return constants;
    }

    @ReactMethod(isBlockingSynchronousMethod = true)
    public boolean install() {
        try {
            ReactApplicationContext context = getReactApplicationContext();
            // Install native module
            TursoBridge.getInstance().install(context);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void invalidate() {
        super.invalidate();
        TursoBridge.getInstance().invalidate();
    }

    private native void installNativeJsi(long jsiRuntimePtr, Object callInvokerHolder, String dbPath);

    private native void clearStateNativeJsi();
}
