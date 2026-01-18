#include "TursoHostObject.h"
#include "TursoDatabaseHostObject.h"
#include "TursoSyncDatabaseHostObject.h"
#include <turso.h>
#include <turso_sync.h>

namespace turso
{

    using namespace facebook;

    // Global base path for database files (for backward compatibility)
    static std::string g_basePath;

    void install(
        jsi::Runtime &rt,
        const std::shared_ptr<react::CallInvoker> &invoker,
        const char *basePath)
    {
        g_basePath = basePath ? basePath : "";

        // Create the module object
        jsi::Object module(rt);

        // newDatabase(path, dbConfig) -> TursoDatabaseHostObject
        // Factory for creating local-only databases
        auto newDatabase = jsi::Function::createFromHostFunction(
            rt,
            jsi::PropNameID::forAscii(rt, "newDatabase"),
            1, // min args
            [](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *args, size_t count) -> jsi::Value
            {
                if (count < 1 || !args[0].isString())
                {
                    throw jsi::JSError(rt, "newDatabase() requires path string as first argument");
                }

                std::string path = args[0].asString(rt).utf8(rt);

                // Build database config
                turso_database_config_t db_config = {0};
                db_config.async_io = 0;  // Sync IO for now
                db_config.path = path.c_str();
                db_config.experimental_features = nullptr;
                db_config.vfs = nullptr;
                db_config.encryption_cipher = nullptr;
                db_config.encryption_hexkey = nullptr;

                // Parse optional dbConfig object (second argument)
                if (count >= 2 && args[1].isObject())
                {
                    jsi::Object config = args[1].asObject(rt);

                    // TODO: Parse additional config options if needed
                    // e.g., vfs, experimental_features, etc.
                }

                // Create database instance
                const turso_database_t* database = nullptr;
                const char* error = nullptr;
                turso_status_code_t status = turso_database_new(&db_config, &database, &error);

                if (status != TURSO_OK)
                {
                    std::string errorMsg = error ? error : "Failed to create database";
                    throw jsi::JSError(rt, errorMsg);
                }

                // Wrap in TursoDatabaseHostObject
                auto dbObj = std::make_shared<TursoDatabaseHostObject>(
                    const_cast<turso_database_t*>(database)
                );
                return jsi::Object::createFromHostObject(rt, dbObj);
            });

        // newSyncDatabase(dbConfig, syncConfig) -> TursoSyncDatabaseHostObject
        // Factory for creating sync-enabled embedded replica databases
        auto newSyncDatabase = jsi::Function::createFromHostFunction(
            rt,
            jsi::PropNameID::forAscii(rt, "newSyncDatabase"),
            2, // min args
            [](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *args, size_t count) -> jsi::Value
            {
                if (count < 2 || !args[0].isObject() || !args[1].isObject())
                {
                    throw jsi::JSError(rt, "newSyncDatabase() requires dbConfig and syncConfig objects");
                }

                jsi::Object dbConfigObj = args[0].asObject(rt);
                jsi::Object syncConfigObj = args[1].asObject(rt);

                // Parse dbConfig
                if (!dbConfigObj.hasProperty(rt, "path"))
                {
                    throw jsi::JSError(rt, "dbConfig must have 'path' property");
                }
                std::string path = dbConfigObj.getProperty(rt, "path").asString(rt).utf8(rt);

                turso_database_config_t db_config = {0};
                db_config.async_io = 0;  // Sync IO for now
                db_config.path = path.c_str();
                db_config.experimental_features = nullptr;
                db_config.vfs = nullptr;
                db_config.encryption_cipher = nullptr;
                db_config.encryption_hexkey = nullptr;

                // Parse syncConfig
                turso_sync_database_config_t sync_config = {0};

                // path (already set in db_config, but sync_config also needs it)
                sync_config.path = path.c_str();

                // remoteUrl (optional)
                static std::string remoteUrl;
                if (syncConfigObj.hasProperty(rt, "remoteUrl"))
                {
                    remoteUrl = syncConfigObj.getProperty(rt, "remoteUrl").asString(rt).utf8(rt);
                    sync_config.remote_url = remoteUrl.c_str();
                }
                else
                {
                    sync_config.remote_url = nullptr;
                }

                // clientName (optional)
                static std::string clientName;
                if (syncConfigObj.hasProperty(rt, "clientName"))
                {
                    clientName = syncConfigObj.getProperty(rt, "clientName").asString(rt).utf8(rt);
                    sync_config.client_name = clientName.c_str();
                }
                else
                {
                    sync_config.client_name = nullptr;
                }

                // longPollTimeoutMs
                if (syncConfigObj.hasProperty(rt, "longPollTimeoutMs"))
                {
                    sync_config.long_poll_timeout_ms = static_cast<int32_t>(
                        syncConfigObj.getProperty(rt, "longPollTimeoutMs").asNumber()
                    );
                }
                else
                {
                    sync_config.long_poll_timeout_ms = 0;
                }

                // bootstrapIfEmpty
                if (syncConfigObj.hasProperty(rt, "bootstrapIfEmpty"))
                {
                    sync_config.bootstrap_if_empty = syncConfigObj.getProperty(rt, "bootstrapIfEmpty").getBool();
                }
                else
                {
                    sync_config.bootstrap_if_empty = false;
                }

                // reservedBytes
                if (syncConfigObj.hasProperty(rt, "reservedBytes"))
                {
                    sync_config.reserved_bytes = static_cast<int32_t>(
                        syncConfigObj.getProperty(rt, "reservedBytes").asNumber()
                    );
                }
                else
                {
                    sync_config.reserved_bytes = 0;
                }

                // Partial sync options
                if (syncConfigObj.hasProperty(rt, "partialBootstrapStrategyPrefix"))
                {
                    sync_config.partial_bootstrap_strategy_prefix = static_cast<int32_t>(
                        syncConfigObj.getProperty(rt, "partialBootstrapStrategyPrefix").asNumber()
                    );
                }
                else
                {
                    sync_config.partial_bootstrap_strategy_prefix = 0;
                }

                static std::string partialBootstrapStrategyQuery;
                if (syncConfigObj.hasProperty(rt, "partialBootstrapStrategyQuery"))
                {
                    partialBootstrapStrategyQuery = syncConfigObj.getProperty(rt, "partialBootstrapStrategyQuery").asString(rt).utf8(rt);
                    sync_config.partial_bootstrap_strategy_query = partialBootstrapStrategyQuery.c_str();
                }
                else
                {
                    sync_config.partial_bootstrap_strategy_query = nullptr;
                }

                if (syncConfigObj.hasProperty(rt, "partialBootstrapSegmentSize"))
                {
                    sync_config.partial_bootstrap_segment_size = static_cast<size_t>(
                        syncConfigObj.getProperty(rt, "partialBootstrapSegmentSize").asNumber()
                    );
                }
                else
                {
                    sync_config.partial_bootstrap_segment_size = 0;
                }

                if (syncConfigObj.hasProperty(rt, "partialBootstrapPrefetch"))
                {
                    sync_config.partial_bootstrap_prefetch = syncConfigObj.getProperty(rt, "partialBootstrapPrefetch").getBool();
                }
                else
                {
                    sync_config.partial_bootstrap_prefetch = false;
                }

                // Create sync database instance
                const turso_sync_database_t* database = nullptr;
                const char* error = nullptr;
                turso_status_code_t status = turso_sync_database_new(&db_config, &sync_config, &database, &error);

                if (status != TURSO_OK)
                {
                    std::string errorMsg = error ? error : "Failed to create sync database";
                    throw jsi::JSError(rt, errorMsg);
                }

                // Wrap in TursoSyncDatabaseHostObject
                auto dbObj = std::make_shared<TursoSyncDatabaseHostObject>(
                    const_cast<turso_sync_database_t*>(database)
                );
                return jsi::Object::createFromHostObject(rt, dbObj);
            });

        // version() -> string
        auto version = jsi::Function::createFromHostFunction(
            rt,
            jsi::PropNameID::forAscii(rt, "version"),
            0,
            [](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *, size_t) -> jsi::Value
            {
                const char *ver = turso_version();
                return jsi::String::createFromUtf8(rt, ver);
            });

        // setup(options) -> void
        auto setup = jsi::Function::createFromHostFunction(
            rt,
            jsi::PropNameID::forAscii(rt, "setup"),
            1,
            [](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *args, size_t count) -> jsi::Value
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

        module.setProperty(rt, "newDatabase", std::move(newDatabase));
        module.setProperty(rt, "newSyncDatabase", std::move(newSyncDatabase));
        module.setProperty(rt, "version", std::move(version));
        module.setProperty(rt, "setup", std::move(setup));

        // Install as global __TursoProxy
        rt.global().setProperty(rt, "__TursoProxy", std::move(module));
    }

    void invalidate()
    {
        // Cleanup if needed
    }

} // namespace turso
