cmake_minimum_required(VERSION 3.22)
project(turso-react-native)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find packages
find_package(ReactAndroid REQUIRED CONFIG)
find_package(fbjni REQUIRED CONFIG)

# Source files
set(TURSO_SOURCES
    cpp-adapter.cpp
    ../cpp/TursoHostObject.cpp
    ../cpp/TursoDatabaseHostObject.cpp
    ../cpp/TursoConnectionHostObject.cpp
    ../cpp/TursoStatementHostObject.cpp
    ../cpp/TursoSyncDatabaseHostObject.cpp
    ../cpp/TursoSyncOperationHostObject.cpp
    ../cpp/TursoSyncIoItemHostObject.cpp
    ../cpp/TursoSyncChangesHostObject.cpp
)

# Create shared library
add_library(${PROJECT_NAME} SHARED ${TURSO_SOURCES})

# Include directories
target_include_directories(${PROJECT_NAME} PRIVATE
    ../cpp
    ../libs/android
)

# Pre-built Rust shared library (.so) loaded at runtime
add_library(turso_sync_sdk_kit SHARED IMPORTED)

# IMPORTED_NO_SONAME is important to properly adjust import paths for runtime by linker (so they will not be reused from build time)
set_target_properties(turso_sync_sdk_kit PROPERTIES
    IMPORTED_LOCATION ${CMAKE_SOURCE_DIR}/../libs/android/${ANDROID_ABI}/libturso_sync_sdk_kit.so
    IMPORTED_NO_SONAME TRUE
)

# Link libraries (record runtime dependency on Rust .so)
target_link_libraries(${PROJECT_NAME}
    ReactAndroid::jsi
    ReactAndroid::reactnative
    fbjni::fbjni
    turso_sync_sdk_kit
    android
    log
)
