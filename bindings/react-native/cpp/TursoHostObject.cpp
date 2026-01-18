#include "TursoHostObject.h"
#include "DBHostObject.h"

// Include the Turso C API header
extern "C"
{
#include "turso.h"
}

namespace turso
{

    using namespace facebook;

    // Global base path for database files
    static std::string g_basePath;

    void install(
        jsi::Runtime &rt,
        const std::shared_ptr<react::CallInvoker> &invoker,
        const char *basePath)
    {
        g_basePath = basePath ? basePath : "";

        // Create the module object
        jsi::Object module(rt);

        // open(options) -> Database
        auto open = jsi::Function::createFromHostFunction(
            rt,
            jsi::PropNameID::forAscii(rt, "open"),
            1, // min args
            [](jsi::Runtime &rt, const jsi::Value &thisVal, const jsi::Value *args, size_t count) -> jsi::Value
            {
                if (count < 1)
                {
                    throw jsi::JSError(rt, "open() requires an options object or path string");
                }

                std::string path;

                // Accept either a string path or an options object
                if (args[0].isString())
                {
                    path = args[0].asString(rt).utf8(rt);
                }
                else if (args[0].isObject())
                {
                    jsi::Object options = args[0].asObject(rt);

                    if (!options.hasProperty(rt, "name"))
                    {
                        throw jsi::JSError(rt, "open() options must have a 'name' property");
                    }

                    path = options.getProperty(rt, "name").asString(rt).utf8(rt);
                }
                else
                {
                    throw jsi::JSError(rt, "open() requires an options object or path string");
                }

                // Create the database host object
                auto db = std::make_shared<DBHostObject>(path, g_basePath);
                return jsi::Object::createFromHostObject(rt, db);
            });

        // version() -> string
        auto version = jsi::Function::createFromHostFunction(
            rt,
            jsi::PropNameID::forAscii(rt, "version"),
            0,
            [](jsi::Runtime &rt, const jsi::Value &thisVal, const jsi::Value *args, size_t count) -> jsi::Value
            {
                const char *ver = turso_version();
                return jsi::String::createFromUtf8(rt, ver);
            });

        // setup(options) -> void
        auto setup = jsi::Function::createFromHostFunction(
            rt,
            jsi::PropNameID::forAscii(rt, "setup"),
            1,
            [](jsi::Runtime &rt, const jsi::Value &thisVal, const jsi::Value *args, size_t count) -> jsi::Value
            {
                if (count < 1 || !args[0].isObject())
                {
                    throw jsi::JSError(rt, "setup() requires an options object");
                }

                jsi::Object options = args[0].asObject(rt);

                // Store log level in a static variable to ensure lifetime
                static std::string logLevelStr;

                // Get log level if provided
                if (options.hasProperty(rt, "logLevel"))
                {
                    jsi::Value logLevelVal = options.getProperty(rt, "logLevel");
                    if (logLevelVal.isString())
                    {
                        logLevelStr = logLevelVal.asString(rt).utf8(rt);
                    }
                }

                turso_config_t config = {nullptr, logLevelStr.empty() ? nullptr : logLevelStr.c_str()};

                // Call turso_setup
                const char *error = nullptr;
                turso_status_code_t status = turso_setup(&config, &error);

                if (status != TURSO_OK)
                {
                    std::string errorMsg = error ? error : "Unknown error in turso_setup";
                    throw jsi::JSError(rt, errorMsg);
                }

                return jsi::Value::undefined();
            });

        module.setProperty(rt, "open", std::move(open));
        module.setProperty(rt, "version", std::move(version));
        module.setProperty(rt, "setup", std::move(setup));

        // Install as global __TursoProxy
        rt.global().setProperty(rt, "__TursoProxy", std::move(module));
    }

    // TODO: implement invalidation logic
    void invalidate() {}

} // namespace turso
