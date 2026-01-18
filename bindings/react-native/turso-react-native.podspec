require "json"

package = JSON.parse(File.read(File.join(__dir__, "package.json")))

Pod::Spec.new do |s|
  s.name         = "turso-react-native"
  s.version      = package["version"]
  s.summary      = package["description"]
  s.homepage     = package["homepage"]
  s.license      = package["license"]
  s.authors      = package["author"]

  s.platforms    = { :ios => "13.0" }
  s.source       = { :git => "https://github.com/tursodatabase/turso.git", :tag => "#{s.version}" }

  s.source_files = [
    "ios/**/*.{h,m,mm}",
    "cpp/**/*.{h,hpp,cpp}"
  ]

  # Vendored Rust static library
  s.vendored_libraries = "libs/ios/libturso_sync_sdk_kit.a"

  # Header search paths
  s.pod_target_xcconfig = {
    "CLANG_CXX_LANGUAGE_STANDARD" => "c++20",
    "HEADER_SEARCH_PATHS" => [
      "$(PODS_TARGET_SRCROOT)/cpp",
      "$(PODS_TARGET_SRCROOT)/libs/ios"
    ].join(" "),
    "OTHER_LDFLAGS" => "-lc++",
    "DEFINES_MODULE" => "YES",
    "GCC_PRECOMPILE_PREFIX_HEADER" => "NO"
  }

  # User header search paths for consumers
  s.user_target_xcconfig = {
    "HEADER_SEARCH_PATHS" => "$(PODS_ROOT)/turso-react-native/cpp $(PODS_ROOT)/turso-react-native/libs/ios"
  }

  # Build script to compile Rust
  s.script_phase = {
    :name => "Build Turso Rust Library",
    :script => 'set -e; cd "${PODS_TARGET_SRCROOT}"; if [ ! -f "libs/ios/libturso_sync_sdk_kit.a" ]; then echo "Building Rust library for iOS..."; ./scripts/build-rust-ios.sh; fi',
    :execution_position => :before_compile,
    :shell_path => "/bin/bash"
  }

  # Dependencies
  s.dependency "React-Core"
  s.dependency "React-callinvoker"
  s.dependency "ReactCommon/turbomodule/core"
end
