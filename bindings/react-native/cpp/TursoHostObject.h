#pragma once

#include <jsi/jsi.h>
#include <ReactCommon/CallInvoker.h>
#include <memory>
#include <string>

namespace turso {

using namespace facebook;

/**
 * Install the Turso module into the JSI runtime.
 * This creates a global __TursoProxy object with the open() function.
 */
void install(
    jsi::Runtime &rt,
    const std::shared_ptr<react::CallInvoker> &invoker,
    const char *basePath
);

void invalidate();

} // namespace turso
