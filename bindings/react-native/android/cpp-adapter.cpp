#include "TursoHostObject.h"
#include <ReactCommon/CallInvokerHolder.h>
#include <fbjni/fbjni.h>
#include <jni.h>
#include <jsi/jsi.h>
#include <typeinfo>

namespace jsi = facebook::jsi;
namespace react = facebook::react;
namespace jni = facebook::jni;

// This file is not using raw jni but rather fbjni, do not change how the native
// functions are registered
// https://github.com/facebookincubator/fbjni/blob/main/docs/quickref.md
struct TursoBridge : jni::JavaClass<TursoBridge>
{
    static constexpr auto kJavaDescriptor = "Lcom/turso/reactnative/TursoBridge;";

    static void registerNatives()
    {
        javaClassStatic()->registerNatives(
            {makeNativeMethod("installNativeJsi", TursoBridge::installNativeJsi),
             makeNativeMethod("clearStateNativeJsi", TursoBridge::clearStateNativeJsi)});
    }

private:
    static void installNativeJsi(
        jni::alias_ref<jni::JObject> thiz, jlong jsiRuntimePtr,
        jni::alias_ref<react::CallInvokerHolder::javaobject> jsCallInvokerHolder,
        jni::alias_ref<jni::JString> dbPath)
    {
        auto jsiRuntime = reinterpret_cast<jsi::Runtime *>(jsiRuntimePtr);
        auto jsCallInvoker = jsCallInvokerHolder->cthis()->getCallInvoker();
        std::string dbPathStr = dbPath->toStdString();

        turso::install(*jsiRuntime, jsCallInvoker, dbPathStr.c_str());
    }

    static void clearStateNativeJsi(jni::alias_ref<jni::JObject> thiz)
    {
        turso::invalidate();
    }
};

JNIEXPORT jint JNI_OnLoad(JavaVM *vm, void *)
{
    return jni::initialize(vm, []
                           { TursoBridge::registerNatives(); });
}