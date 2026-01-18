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
    ../cpp/DBHostObject.cpp
    ../cpp/StatementHostObject.cpp
)

# Create shared library
add_library(${PROJECT_NAME} SHARED ${TURSO_SOURCES})

# Include directories
target_include_directories(${PROJECT_NAME} PRIVATE
    ../cpp
    ../libs/android
)

# Pre-built Rust static library
set(RUST_LIB_DIR ${CMAKE_SOURCE_DIR}/../libs/android/${ANDROID_ABI})
add_library(turso_sdk_kit STATIC IMPORTED)
set_target_properties(turso_sdk_kit PROPERTIES
    IMPORTED_LOCATION ${RUST_LIB_DIR}/libturso_sdk_kit.a
)

# Get include directories from React targets without linking their .so files
get_target_property(JSI_INCLUDE_DIRS ReactAndroid::jsi INTERFACE_INCLUDE_DIRECTORIES)
get_target_property(REACT_INCLUDE_DIRS ReactAndroid::reactnative INTERFACE_INCLUDE_DIRECTORIES)
get_target_property(FBJNI_INCLUDE_DIRS fbjni::fbjni INTERFACE_INCLUDE_DIRECTORIES)

target_include_directories(${PROJECT_NAME} PRIVATE
    ${JSI_INCLUDE_DIRS}
    ${REACT_INCLUDE_DIRS}
    ${FBJNI_INCLUDE_DIRS}
)

# Link only our Rust library and system libs
# React Native libs are loaded at runtime by the app
target_link_libraries(${PROJECT_NAME}
    turso_sdk_kit
    android
    log
)
