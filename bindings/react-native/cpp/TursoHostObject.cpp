#include "TursoHostObject.h"
#include "TursoDatabaseHostObject.h"
#include "TursoSyncDatabaseHostObject.h"

#include <cstdio>   // For FILE, fopen, fread, fwrite, fclose, fseek, ftell, remove, rename
#include <cstdlib>  // For additional standard library functions

extern "C" {
#include <turso.h>
#include <turso_sync.h>
}

namespace turso
{

    using namespace facebook;

    // Global base path for database files
    static std::string g_basePath;

    /**
     * Normalize a database path:
     * - If path is absolute (starts with '/'), use as-is
     * - If path is ':memory:', use as-is
     * - Otherwise, prepend basePath
     */
    static std::string normalizePath(const std::string &path)
    {
        // Special cases: absolute path or in-memory
        if (path.empty() || path[0] == '/' || path == ":memory:")
        {
            return path;
        }

        // Relative path - prepend basePath
        if (g_basePath.empty())
        {
            return path;
        }

        // Combine basePath + path
        if (g_basePath[g_basePath.length() - 1] == '/')
        {
            return g_basePath + path;
        }
        else
        {
            return g_basePath + "/" + path;
        }
    }

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

                // Normalize path (prepend basePath if relative)
                std::string normalizedPath = normalizePath(path);

                // Build database config
                turso_database_config_t db_config = {0};
                db_config.async_io = 1;  // Default to async IO for React Native
                db_config.path = normalizedPath.c_str();
                db_config.experimental_features = nullptr;
                db_config.vfs = nullptr;
                db_config.encryption_cipher = nullptr;
                db_config.encryption_hexkey = nullptr;

                // Parse optional dbConfig object (second argument)
                if (count >= 2 && args[1].isObject())
                {
                    jsi::Object config = args[1].asObject(rt);

                    // Parse async_io if provided
                    if (config.hasProperty(rt, "async_io"))
                    {
                        db_config.async_io = config.getProperty(rt, "async_io").getBool() ? 1 : 0;
                    }
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

                // Normalize path (prepend basePath if relative)
                std::string normalizedPath = normalizePath(path);

                turso_database_config_t db_config = {0};
                db_config.async_io = 1;  // Default to async IO for React Native
                db_config.path = normalizedPath.c_str();

                // Parse async_io if provided in dbConfig
                if (dbConfigObj.hasProperty(rt, "async_io"))
                {
                    db_config.async_io = dbConfigObj.getProperty(rt, "async_io").getBool() ? 1 : 0;
                }
                db_config.experimental_features = nullptr;
                db_config.vfs = nullptr;
                db_config.encryption_cipher = nullptr;
                db_config.encryption_hexkey = nullptr;

                // Parse syncConfig
                turso_sync_database_config_t sync_config = {0};

                // path (already set in db_config, but sync_config also needs it)
                sync_config.path = normalizedPath.c_str();

                // remoteUrl (optional)
                std::string remoteUrl;
                if (syncConfigObj.hasProperty(rt, "remoteUrl"))
                {
                    jsi::Value remoteUrlVal = syncConfigObj.getProperty(rt, "remoteUrl");
                    if (!remoteUrlVal.isNull() && !remoteUrlVal.isUndefined())
                    {
                        remoteUrl = remoteUrlVal.asString(rt).utf8(rt);
                        sync_config.remote_url = remoteUrl.c_str();
                    }
                    else
                    {
                        sync_config.remote_url = nullptr;
                    }
                }
                else
                {
                    sync_config.remote_url = nullptr;
                }

                // clientName (optional)
                std::string clientName;
                if (syncConfigObj.hasProperty(rt, "clientName"))
                {
                    jsi::Value clientNameVal = syncConfigObj.getProperty(rt, "clientName");
                    if (!clientNameVal.isNull() && !clientNameVal.isUndefined())
                    {
                        clientName = clientNameVal.asString(rt).utf8(rt);
                        sync_config.client_name = clientName.c_str();
                    }
                    else
                    {
                        sync_config.client_name = nullptr;
                    }
                }
                else
                {
                    sync_config.client_name = nullptr;
                }

                // longPollTimeoutMs
                if (syncConfigObj.hasProperty(rt, "longPollTimeoutMs"))
                {
                    jsi::Value longPollVal = syncConfigObj.getProperty(rt, "longPollTimeoutMs");
                    if (!longPollVal.isNull() && !longPollVal.isUndefined())
                    {
                        sync_config.long_poll_timeout_ms = static_cast<int32_t>(longPollVal.asNumber());
                    }
                    else
                    {
                        sync_config.long_poll_timeout_ms = 0;
                    }
                }
                else
                {
                    sync_config.long_poll_timeout_ms = 0;
                }

                // bootstrapIfEmpty
                if (syncConfigObj.hasProperty(rt, "bootstrapIfEmpty"))
                {
                    jsi::Value bootstrapVal = syncConfigObj.getProperty(rt, "bootstrapIfEmpty");
                    if (!bootstrapVal.isNull() && !bootstrapVal.isUndefined())
                    {
                        sync_config.bootstrap_if_empty = bootstrapVal.getBool();
                    }
                    else
                    {
                        sync_config.bootstrap_if_empty = false;
                    }
                }
                else
                {
                    sync_config.bootstrap_if_empty = false;
                }

                // reservedBytes
                if (syncConfigObj.hasProperty(rt, "reservedBytes"))
                {
                    jsi::Value reservedVal = syncConfigObj.getProperty(rt, "reservedBytes");
                    if (!reservedVal.isNull() && !reservedVal.isUndefined())
                    {
                        sync_config.reserved_bytes = static_cast<int32_t>(reservedVal.asNumber());
                    }
                    else
                    {
                        sync_config.reserved_bytes = 0;
                    }
                }
                else
                {
                    sync_config.reserved_bytes = 0;
                }

                // Partial sync options
                if (syncConfigObj.hasProperty(rt, "partialBootstrapStrategyPrefix"))
                {
                    jsi::Value prefixVal = syncConfigObj.getProperty(rt, "partialBootstrapStrategyPrefix");
                    if (!prefixVal.isNull() && !prefixVal.isUndefined())
                    {
                        sync_config.partial_bootstrap_strategy_prefix = static_cast<int32_t>(prefixVal.asNumber());
                    }
                    else
                    {
                        sync_config.partial_bootstrap_strategy_prefix = 0;
                    }
                }
                else
                {
                    sync_config.partial_bootstrap_strategy_prefix = 0;
                }

                std::string partialBootstrapStrategyQuery;
                if (syncConfigObj.hasProperty(rt, "partialBootstrapStrategyQuery"))
                {
                    jsi::Value queryVal = syncConfigObj.getProperty(rt, "partialBootstrapStrategyQuery");
                    if (!queryVal.isNull() && !queryVal.isUndefined())
                    {
                        partialBootstrapStrategyQuery = queryVal.asString(rt).utf8(rt);
                        sync_config.partial_bootstrap_strategy_query = partialBootstrapStrategyQuery.c_str();
                    }
                    else
                    {
                        sync_config.partial_bootstrap_strategy_query = nullptr;
                    }
                }
                else
                {
                    sync_config.partial_bootstrap_strategy_query = nullptr;
                }

                if (syncConfigObj.hasProperty(rt, "partialBootstrapSegmentSize"))
                {
                    jsi::Value segmentVal = syncConfigObj.getProperty(rt, "partialBootstrapSegmentSize");
                    if (!segmentVal.isNull() && !segmentVal.isUndefined())
                    {
                        sync_config.partial_bootstrap_segment_size = static_cast<size_t>(segmentVal.asNumber());
                    }
                    else
                    {
                        sync_config.partial_bootstrap_segment_size = 0;
                    }
                }
                else
                {
                    sync_config.partial_bootstrap_segment_size = 0;
                }

                if (syncConfigObj.hasProperty(rt, "partialBootstrapPrefetch"))
                {
                    jsi::Value prefetchVal = syncConfigObj.getProperty(rt, "partialBootstrapPrefetch");
                    if (!prefetchVal.isNull() && !prefetchVal.isUndefined())
                    {
                        sync_config.partial_bootstrap_prefetch = prefetchVal.getBool();
                    }
                    else
                    {
                        sync_config.partial_bootstrap_prefetch = false;
                    }
                }
                else
                {
                    sync_config.partial_bootstrap_prefetch = false;
                }

                // Remote encryption options
                std::string remoteEncryptionKey;
                if (syncConfigObj.hasProperty(rt, "remoteEncryptionKey"))
                {
                    jsi::Value keyVal = syncConfigObj.getProperty(rt, "remoteEncryptionKey");
                    if (!keyVal.isNull() && !keyVal.isUndefined())
                    {
                        remoteEncryptionKey = keyVal.asString(rt).utf8(rt);
                        sync_config.remote_encryption_key = remoteEncryptionKey.c_str();
                    }
                    else
                    {
                        sync_config.remote_encryption_key = nullptr;
                    }
                }
                else
                {
                    sync_config.remote_encryption_key = nullptr;
                }

                std::string remoteEncryptionCipher;
                if (syncConfigObj.hasProperty(rt, "remoteEncryptionCipher"))
                {
                    jsi::Value cipherVal = syncConfigObj.getProperty(rt, "remoteEncryptionCipher");
                    if (!cipherVal.isNull() && !cipherVal.isUndefined())
                    {
                        remoteEncryptionCipher = cipherVal.asString(rt).utf8(rt);
                        sync_config.remote_encryption_cipher = remoteEncryptionCipher.c_str();
                    }
                    else
                    {
                        sync_config.remote_encryption_cipher = nullptr;
                    }
                }
                else
                {
                    sync_config.remote_encryption_cipher = nullptr;
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

                std::string logLevelStr;

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

        // fsReadFile(path) -> ArrayBuffer
        auto fsReadFile = jsi::Function::createFromHostFunction(
            rt,
            jsi::PropNameID::forAscii(rt, "fsReadFile"),
            1,
            [](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *args, size_t count) -> jsi::Value
            {
                if (count < 1 || !args[0].isString())
                {
                    throw jsi::JSError(rt, "fsReadFile() requires path string");
                }

                std::string path = args[0].asString(rt).utf8(rt);

                // Open file for reading
                FILE* file = fopen(path.c_str(), "rb");
                if (!file)
                {
                    // File not found - return null (caller will handle as empty)
                    return jsi::Value::null();
                }

                // Get file size
                fseek(file, 0, SEEK_END);
                long size = ftell(file);
                fseek(file, 0, SEEK_SET);

                if (size <= 0)
                {
                    fclose(file);
                    // Empty file - return empty ArrayBuffer
                    jsi::Function arrayBufferCtor = rt.global().getPropertyAsFunction(rt, "ArrayBuffer");
                    jsi::Object arrayBuffer = arrayBufferCtor.callAsConstructor(rt, 0).asObject(rt);
                    return arrayBuffer;
                }

                // Read file contents
                jsi::Function arrayBufferCtor = rt.global().getPropertyAsFunction(rt, "ArrayBuffer");
                jsi::Object arrayBuffer = arrayBufferCtor.callAsConstructor(rt, static_cast<int>(size)).asObject(rt);
                jsi::ArrayBuffer buf = arrayBuffer.getArrayBuffer(rt);

                size_t bytesRead = fread(buf.data(rt), 1, size, file);
                fclose(file);

                if (bytesRead != static_cast<size_t>(size))
                {
                    throw jsi::JSError(rt, "Failed to read complete file");
                }

                return arrayBuffer;
            });

        // fsWriteFile(path, arrayBuffer) -> void
        auto fsWriteFile = jsi::Function::createFromHostFunction(
            rt,
            jsi::PropNameID::forAscii(rt, "fsWriteFile"),
            2,
            [](jsi::Runtime &rt, const jsi::Value &, const jsi::Value *args, size_t count) -> jsi::Value
            {
                if (count < 2 || !args[0].isString() || !args[1].isObject())
                {
                    throw jsi::JSError(rt, "fsWriteFile() requires path string and ArrayBuffer");
                }

                std::string path = args[0].asString(rt).utf8(rt);
                jsi::ArrayBuffer buffer = args[1].asObject(rt).getArrayBuffer(rt);

                // Write atomically using temporary file + rename
                std::string tempPath = path + ".tmp";

                // Open temp file for writing
                FILE* file = fopen(tempPath.c_str(), "wb");
                if (!file)
                {
                    throw jsi::JSError(rt, "Failed to open file for writing");
                }

                // Write data
                size_t size = buffer.size(rt);
                if (size > 0)
                {
                    size_t written = fwrite(buffer.data(rt), 1, size, file);
                    fclose(file);

                    if (written != size)
                    {
                        remove(tempPath.c_str());
                        throw jsi::JSError(rt, "Failed to write complete file");
                    }
                }
                else
                {
                    fclose(file);
                }

                // Atomic rename (replaces old file)
                if (rename(tempPath.c_str(), path.c_str()) != 0)
                {
                    remove(tempPath.c_str());
                    throw jsi::JSError(rt, "Failed to rename temp file");
                }

                return jsi::Value::undefined();
            });

        module.setProperty(rt, "newDatabase", std::move(newDatabase));
        module.setProperty(rt, "newSyncDatabase", std::move(newSyncDatabase));
        module.setProperty(rt, "version", std::move(version));
        module.setProperty(rt, "setup", std::move(setup));
        module.setProperty(rt, "fsReadFile", std::move(fsReadFile));
        module.setProperty(rt, "fsWriteFile", std::move(fsWriteFile));

        // Install as global __TursoProxy
        rt.global().setProperty(rt, "__TursoProxy", std::move(module));
    }

    void invalidate()
    {
        // Cleanup if needed
    }

} // namespace turso
