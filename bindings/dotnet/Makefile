ifeq ($(OS),Windows_NT)
	OS_TARGET = windows64
else 
    UNAME_S := $(shell uname -s)
	ifeq ($(UNAME_S),Linux)
		OS_TARGET = linux64
	endif
	ifeq ($(UNAME_S),Darwin)
		UNAME_P := $(shell uname -p)
		ifeq ($(UNAME_P),x86_64)
			OS_TARGET = macos64
		else
			OS_TARGET = macosarm64
		endif
	endif
endif

RUST_RELEASE_OPT ?= 
RUST_PROFILE_DIR ?= debug
DOTNET_RELEASE_OPT ?=
IOS_FRAMEWORK_NAME = libturso_sdk_kit
IOS_DYLIB_FILE = libturso_sdk_kit.dylib
IOS_FRAMEWORK_BUILD_DIR = ./rs_compiled/ios-frameworks
IOS_XCFRAMEWORK_DIR = ./rs_compiled/ios-universal/native/$(IOS_FRAMEWORK_NAME).xcframework

all:
	echo "make [build|test|benchmark|build-production]"

build-rust-windows64:
	cargo build -p turso_sdk_kit --target-dir ./rs_compiled --target x86_64-pc-windows-msvc $(RUST_RELEASE_OPT)

build-rust-windowsarm64:
	cargo build -p turso_sdk_kit --target-dir ./rs_compiled --target aarch64-pc-windows-msvc $(RUST_RELEASE_OPT)

build-rust-linux64:
	cargo build -p turso_sdk_kit --target-dir ./rs_compiled --target x86_64-unknown-linux-gnu $(RUST_RELEASE_OPT)

build-rust-linuxarm64:
	cargo build -p turso_sdk_kit --target-dir ./rs_compiled --target aarch64-unknown-linux-gnu $(RUST_RELEASE_OPT)

build-rust-macos64:
	cargo build -p turso_sdk_kit --target-dir ./rs_compiled --target x86_64-apple-darwin $(RUST_RELEASE_OPT)

build-rust-macosarm64:
	cargo build -p turso_sdk_kit --target-dir ./rs_compiled --target aarch64-apple-darwin $(RUST_RELEASE_OPT)

build-rust-androidarm64:
	cargo ndk --target aarch64-linux-android --platform 31 build -p turso_sdk_kit --target-dir ./rs_compiled $(RUST_RELEASE_OPT)

build-rust-androidarm:
	cargo ndk --target armv7-linux-androideabi --platform 31 build -p turso_sdk_kit --target-dir ./rs_compiled $(RUST_RELEASE_OPT)

build-rust-androidx64:
	cargo ndk --target x86_64-linux-android --platform 31 build -p turso_sdk_kit --target-dir ./rs_compiled $(RUST_RELEASE_OPT)

build-rust-androidx86:
	cargo ndk --target i686-linux-android --platform 31 build -p turso_sdk_kit --target-dir ./rs_compiled $(RUST_RELEASE_OPT)

build-rust-android: build-rust-androidarm64 build-rust-androidarm build-rust-androidx64 build-rust-androidx86

build-rust-iosarm64:
	cargo build -p turso_sdk_kit --target-dir ./rs_compiled --target aarch64-apple-ios $(RUST_RELEASE_OPT)

build-rust-iossimulatorarm64:
	cargo build -p turso_sdk_kit --target-dir ./rs_compiled --target aarch64-apple-ios-sim $(RUST_RELEASE_OPT)

build-rust-ios: build-rust-iosarm64 build-rust-iossimulatorarm64

package-rust-iosframework:
	rm -rf "$(IOS_FRAMEWORK_BUILD_DIR)" "$(IOS_XCFRAMEWORK_DIR)"
	mkdir -p "$(IOS_FRAMEWORK_BUILD_DIR)/device/$(IOS_FRAMEWORK_NAME).framework" "$(IOS_FRAMEWORK_BUILD_DIR)/simulator/$(IOS_FRAMEWORK_NAME).framework" ./rs_compiled/ios-universal/native
	cp ./rs_compiled/aarch64-apple-ios/$(RUST_PROFILE_DIR)/$(IOS_DYLIB_FILE) "$(IOS_FRAMEWORK_BUILD_DIR)/device/$(IOS_FRAMEWORK_NAME).framework/$(IOS_FRAMEWORK_NAME)"
	install_name_tool -id @rpath/$(IOS_FRAMEWORK_NAME).framework/$(IOS_FRAMEWORK_NAME) "$(IOS_FRAMEWORK_BUILD_DIR)/device/$(IOS_FRAMEWORK_NAME).framework/$(IOS_FRAMEWORK_NAME)"
	cp ./rs_compiled/aarch64-apple-ios-sim/$(RUST_PROFILE_DIR)/$(IOS_DYLIB_FILE) "$(IOS_FRAMEWORK_BUILD_DIR)/simulator/$(IOS_FRAMEWORK_NAME).framework/$(IOS_FRAMEWORK_NAME)"
	install_name_tool -id @rpath/$(IOS_FRAMEWORK_NAME).framework/$(IOS_FRAMEWORK_NAME) "$(IOS_FRAMEWORK_BUILD_DIR)/simulator/$(IOS_FRAMEWORK_NAME).framework/$(IOS_FRAMEWORK_NAME)"
	printf '%s\n' \
	'<?xml version="1.0" encoding="UTF-8"?>' \
	'<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">' \
	'<plist version="1.0">' \
	'<dict>' \
	'  <key>CFBundleDevelopmentRegion</key>' \
	'  <string>en</string>' \
	'  <key>CFBundleExecutable</key>' \
	'  <string>$(IOS_FRAMEWORK_NAME)</string>' \
	'  <key>CFBundleIdentifier</key>' \
	'  <string>com.turso.turso-sdk-kit</string>' \
	'  <key>CFBundleInfoDictionaryVersion</key>' \
	'  <string>6.0</string>' \
	'  <key>CFBundlePackageType</key>' \
	'  <string>FMWK</string>' \
	'  <key>CFBundleSignature</key>' \
	'  <string>????</string>' \
	'  <key>CFBundleVersion</key>' \
	'  <string>1.0.0</string>' \
	'  <key>CFBundleShortVersionString</key>' \
	'  <string>1.0.0</string>' \
	'  <key>MinimumOSVersion</key>' \
	'  <string>12.2</string>' \
	'</dict>' \
	'</plist>' > "$(IOS_FRAMEWORK_BUILD_DIR)/device/$(IOS_FRAMEWORK_NAME).framework/Info.plist"
	cp "$(IOS_FRAMEWORK_BUILD_DIR)/device/$(IOS_FRAMEWORK_NAME).framework/Info.plist" "$(IOS_FRAMEWORK_BUILD_DIR)/simulator/$(IOS_FRAMEWORK_NAME).framework/Info.plist"
	xcodebuild -create-xcframework -framework "$(IOS_FRAMEWORK_BUILD_DIR)/device/$(IOS_FRAMEWORK_NAME).framework" -framework "$(IOS_FRAMEWORK_BUILD_DIR)/simulator/$(IOS_FRAMEWORK_NAME).framework" -output "$(IOS_XCFRAMEWORK_DIR)"

build-rust: 
	echo "Building for $(OS_TARGET)"
	$(MAKE) build-rust-$(OS_TARGET)

build-dotnet:
	dotnet build $(DOTNET_RELEASE_OPT) ./src/Turso.Data/Turso.Data.csproj
	dotnet build $(DOTNET_RELEASE_OPT) ./src/Turso.Data.Sqlite/Turso.Data.Sqlite.csproj
	dotnet build $(DOTNET_RELEASE_OPT) ./src/Turso.EntityFrameworkCore.Sqlite/Turso.EntityFrameworkCore.Sqlite.csproj
	dotnet build $(DOTNET_RELEASE_OPT) ./src/Turso.Platform.Client/Turso.Platform.Client.csproj
	dotnet build $(DOTNET_RELEASE_OPT) ./src/Turso.Serverless.Client/Turso.Serverless.Client.csproj

# Generate the Turso Platform API client from the pinned OpenAPI spec.
generate-platform-client:
	dotnet build ./src/Turso.Platform.Client/Turso.Platform.Client.csproj

build: build-rust build-dotnet

build-production:
	$(MAKE) build RUST_RELEASE_OPT='--release' DOTNET_RELEASE_OPT='-c Release'

test: build
	dotnet test ./src/Turso.Tests/Turso.Tests.csproj

benchmark: build-production
	dotnet run -c Release --project ./src/Benchmarks/Benchmarks.csproj

pack-nativeaot-static:
	dotnet pack -c Release ./src/Turso.Data.Sqlite.NativeAot/Turso.Data.Sqlite.NativeAot.csproj -p:NativeAotRid=win-x64
	dotnet pack -c Release ./src/Turso.Data.Sqlite.NativeAot/Turso.Data.Sqlite.NativeAot.csproj -p:NativeAotRid=win-arm64
	dotnet pack -c Release ./src/Turso.Data.Sqlite.NativeAot/Turso.Data.Sqlite.NativeAot.csproj -p:NativeAotRid=linux-x64
	dotnet pack -c Release ./src/Turso.Data.Sqlite.NativeAot/Turso.Data.Sqlite.NativeAot.csproj -p:NativeAotRid=linux-arm64
	dotnet pack -c Release ./src/Turso.Data.Sqlite.NativeAot/Turso.Data.Sqlite.NativeAot.csproj -p:NativeAotRid=osx-x64
	dotnet pack -c Release ./src/Turso.Data.Sqlite.NativeAot/Turso.Data.Sqlite.NativeAot.csproj -p:NativeAotRid=osx-arm64

pack:
	dotnet pack -c Release ./src/Turso.Data.Sqlite/Turso.Data.Sqlite.csproj
	dotnet pack -c Release ./src/Turso.EntityFrameworkCore.Sqlite/Turso.EntityFrameworkCore.Sqlite.csproj
	$(MAKE) pack-nativeaot-static
	dotnet pack -c Release ./src/Turso.Platform.Client/Turso.Platform.Client.csproj
	dotnet pack -c Release ./src/Turso.Serverless.Client/Turso.Serverless.Client.csproj
