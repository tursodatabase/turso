package com.turso.reactnative;

import com.facebook.react.bridge.ReactContext;
import com.facebook.react.turbomodule.core.CallInvokerHolderImpl;

import java.lang.Exception;

// bridge layer, installNativeJsi/clearStateNativeJsi methods are set through cpp-adapter.cpp
public class TursoBridge {
  private static final TursoBridge instance = new TursoBridge();

  public static TursoBridge getInstance() {
    return instance;
  }

  private TursoBridge() {
  }

  private native void installNativeJsi(
      long jsContextNativePointer,
      CallInvokerHolderImpl jsCallInvokerHolder,
      String dbPath);

  private native void clearStateNativeJsi();

  public void install(ReactContext context) throws Exception {
    long jsContextPointer = context.getJavaScriptContextHolder().get();
    if (jsContextPointer == 0) {
      throw new Exception("jsContextPointer == 0");
    }
    CallInvokerHolderImpl jsCallInvokerHolder = (CallInvokerHolderImpl) context.getCatalystInstance()
        .getJSCallInvokerHolder();

    // getDatabasePath(...) returns file path - so we pass dummy value and remove it
    // after to get directory path
    String dbPath = context.getDatabasePath("tursoDatabaseFile").getAbsolutePath().replace("tursoDatabaseFile", "");

    installNativeJsi(jsContextPointer, jsCallInvokerHolder, dbPath);
  }

  public void invalidate() {
    clearStateNativeJsi();
  }
}