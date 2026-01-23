OS := $(shell uname)
CFLAGS := -Iinclude
LDFLAGS := -lm
ARCHS_IOS = aarch64-apple-ios aarch64-apple-ios-sim
ARCHS_ANDROID = aarch64-linux-android armv7-linux-androideabi x86_64-linux-android i686-linux-android
IOS_LIB_FILE = libturso_sync_sdk_kit.dylib
IOS_FINAL_LIB_FILE = turso-sync-sdk-kit
LIB_FILE = libturso_sync_sdk_kit.so
HEADERS = turso_sync.h turso.h
SDK_KIT_DIR = ../../sdk-kit
SYNC_SDK_KIT_DIR = ../../sync/sdk-kit


ifeq ($(OS),Darwin)
	CFLAGS += -framework Security -framework CoreServices
endif

.PHONY: all $(ARCHS_IOS) ios $(ARCHS_ANDROID) android

notify:
	osascript -e 'display notification "Build completed" with title "turso"'
	echo "🟢 Build Completed!"

lint:
	cargo clippy --all-features --all-targets -- --no-deps -D warnings

fix:
	cargo clippy --all-targets --all-features --fix --allow-dirty --allow-staged
	cargo fmt --all

nuke:
	rm -rf ./target

ios: clean ios-build package-dylibs notify

clean:
	rm -rf libs/ios

package-dylibs:
	mkdir -p libs/ios
	rm -rf libs/ios/turso*.xcframework
	cp -r templates/turso*.xcframework libs/ios
	cp "$(SDK_KIT_DIR)/turso.h" libs/ios/
	cp "$(SYNC_SDK_KIT_DIR)/turso_sync.h" libs/ios/

# Pack device dylibs
	cp ../../target/aarch64-apple-ios/release/$(IOS_LIB_FILE) libs/ios/$(IOS_FINAL_LIB_FILE)

	cp libs/ios/$(IOS_FINAL_LIB_FILE) libs/ios/turso-sync-sdk-kit.xcframework/ios-arm64/turso-sync-sdk-kit.framework/
	install_name_tool -id @rpath/turso-sync-sdk-kit.framework/$(IOS_FINAL_LIB_FILE) libs/ios/turso-sync-sdk-kit.xcframework/ios-arm64/turso-sync-sdk-kit.framework/$(IOS_FINAL_LIB_FILE)
	codesign -f -s - --identifier com.turso.turso-sync-sdk-kit libs/ios/turso-sync-sdk-kit.xcframework/ios-arm64/turso-sync-sdk-kit.framework/$(IOS_FINAL_LIB_FILE)

	mkdir -p libs/ios/turso-sync-sdk-kit.xcframework/ios-arm64/turso-sync-sdk-kit.framework/Headers
	cp "$(SDK_KIT_DIR)/turso.h" libs/ios/turso-sync-sdk-kit.xcframework/ios-arm64/turso-sync-sdk-kit.framework/Headers/
	cp "$(SYNC_SDK_KIT_DIR)/turso_sync.h" libs/ios/turso-sync-sdk-kit.xcframework/ios-arm64/turso-sync-sdk-kit.framework/Headers/

# Pack simulator dylibs
	cp ../../target/aarch64-apple-ios-sim/release/$(IOS_LIB_FILE) libs/ios/$(IOS_FINAL_LIB_FILE)

	cp libs/ios/$(IOS_FINAL_LIB_FILE) libs/ios/turso-sync-sdk-kit.xcframework/ios-arm64-simulator/turso-sync-sdk-kit.framework/
	install_name_tool -id @rpath/turso-sync-sdk-kit.framework/$(IOS_FINAL_LIB_FILE) libs/ios/turso-sync-sdk-kit.xcframework/ios-arm64-simulator/turso-sync-sdk-kit.framework/$(IOS_FINAL_LIB_FILE)
	codesign -f -s - --identifier com.turso.turso-sync-sdk-kit libs/ios/turso-sync-sdk-kit.xcframework/ios-arm64-simulator/turso-sync-sdk-kit.framework/$(IOS_FINAL_LIB_FILE)

	mkdir -p libs/ios/turso-sync-sdk-kit.xcframework/ios-arm64-simulator/turso-sync-sdk-kit.framework/Headers
	cp "$(SDK_KIT_DIR)/turso.h" libs/ios/turso-sync-sdk-kit.xcframework/ios-arm64-simulator/turso-sync-sdk-kit.framework/Headers/
	cp "$(SYNC_SDK_KIT_DIR)/turso_sync.h" libs/ios/turso-sync-sdk-kit.xcframework/ios-arm64-simulator/turso-sync-sdk-kit.framework/Headers/

ios-build:
	rustup target add aarch64-apple-ios
	cargo build --release --target aarch64-apple-ios --package turso_sync_sdk_kit
	rustup target add aarch64-apple-ios-sim
	cargo build --release --target aarch64-apple-ios-sim --package turso_sync_sdk_kit

android: $(ARCHS_ANDROID)
	rm -rf libs/android
	mkdir -p libs/android/arm64-v8a
	mkdir -p libs/android/armeabi-v7a
	mkdir -p libs/android/x86_64
	mkdir -p libs/android/x86

	cp ../../target/aarch64-linux-android/release/$(LIB_FILE) libs/android/arm64-v8a/$(LIB_FILE)
	cp ../../target/armv7-linux-androideabi/release/$(LIB_FILE) libs/android/armeabi-v7a/$(LIB_FILE)
	cp ../../target/x86_64-linux-android/release/$(LIB_FILE) libs/android/x86_64/$(LIB_FILE)
	cp ../../target/i686-linux-android/release/$(LIB_FILE) libs/android/x86/$(LIB_FILE)
	cp "$(SDK_KIT_DIR)/turso.h" libs/android
	cp "$(SYNC_SDK_KIT_DIR)/turso_sync.h" libs/android

$(ARCHS_ANDROID): %:
	rustup target add $@
	cargo ndk --target $@ --platform 31 build --package turso_sync_sdk_kit --release
