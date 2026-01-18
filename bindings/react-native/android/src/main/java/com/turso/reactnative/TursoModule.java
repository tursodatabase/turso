package com.turso.reactnative;

import androidx.annotation.NonNull;

import com.facebook.react.bridge.ReactApplicationContext;
import com.facebook.react.bridge.ReactContextBaseJavaModule;
import com.facebook.react.bridge.ReactMethod;
import com.facebook.react.module.annotations.ReactModule;
import com.facebook.react.turbomodule.core.CallInvokerHolderImpl;

@ReactModule(name = TursoModule.NAME)
public class TursoModule extends ReactContextBaseJavaModule {
    public static final String NAME = "Turso";

    static {
        System.loadLibrary("turso-react-native");
    }

    public TursoModule(ReactApplicationContext reactContext) {
        super(reactContext);
    }

    @Override
    @NonNull
    public String getName() {
        return NAME;
    }

    @ReactMethod(isBlockingSynchronousMethod = true)
    public boolean install() {
        try {
            ReactApplicationContext context = getReactApplicationContext();

            // Get JSI runtime pointer
            long jsiRuntimePtr = context.getJavaScriptContextHolder().get();
            if (jsiRuntimePtr == 0) {
                return false;
            }

            // Get call invoker
            CallInvokerHolderImpl callInvokerHolder =
                (CallInvokerHolderImpl) context.getCatalystInstance().getJSCallInvokerHolder();

            // Get database path (app's files directory)
            String dbPath = context.getFilesDir().getAbsolutePath();

            // Install native module
            nativeInstall(jsiRuntimePtr, callInvokerHolder, dbPath);

            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private native void nativeInstall(long jsiRuntimePtr, Object callInvokerHolder, String dbPath);
}
