#include <jni.h>
#include <jsi/jsi.h>
#include <ReactCommon/CallInvokerHolder.h>
#include <fbjni/fbjni.h>

#include "TursoHostObject.h"

using namespace facebook;

extern "C" JNIEXPORT void JNICALL
Java_com_turso_reactnative_TursoModule_nativeInstall(
    JNIEnv *env,
    jobject thiz,
    jlong jsiRuntimePtr,
    jobject jsCallInvokerHolder,
    jstring dbPath
) {
    auto *runtime = reinterpret_cast<jsi::Runtime *>(jsiRuntimePtr);
    if (runtime == nullptr) {
        return;
    }

    // Get the call invoker
    auto callInvokerHolder = jni::make_local(
        reinterpret_cast<react::CallInvokerHolder::javaobject>(jsCallInvokerHolder)
    );
    auto callInvoker = callInvokerHolder->cthis()->getCallInvoker();

    // Get the database path
    const char *pathCStr = env->GetStringUTFChars(dbPath, nullptr);
    std::string path(pathCStr);
    env->ReleaseStringUTFChars(dbPath, pathCStr);

    // Install the Turso module
    turso::install(*runtime, callInvoker, path.c_str());
}

extern "C" JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *vm, void *reserved) {
    return jni::initialize(vm, [] {
        // Register native methods if needed
    });
}
