WORKSPACE_ROOT := ../..
CRATE := turso_sdk_kit
LIB := libturso_sdk_kit.a
TARGET_DIR := $(WORKSPACE_ROOT)/target
APPLE_TARGETS := aarch64-apple-ios aarch64-apple-ios-sim aarch64-apple-darwin x86_64-apple-darwin
BUILD_DIR := .build/xcframework

# Must track the platforms declared in Package.swift. Without these, rustc
# stamps the build machine's OS version onto the objects and the archive
# silently stops being loadable on the versions the package advertises.
export MACOSX_DEPLOYMENT_TARGET := 13.0
export IPHONEOS_DEPLOYMENT_TARGET := 16.0

.PHONY: rust-targets xcframework clean test

rust-targets:
	rustup target add $(APPLE_TARGETS)

xcframework: clean
	for t in $(APPLE_TARGETS); do \
		cargo build -p $(CRATE) --release --target $$t || exit 1; \
	done
	mkdir -p $(BUILD_DIR)/headers $(BUILD_DIR)/macos
	cp $(WORKSPACE_ROOT)/sdk-kit/turso.h $(BUILD_DIR)/headers/
	cp Support/module.modulemap $(BUILD_DIR)/headers/
	lipo -create -output $(BUILD_DIR)/macos/$(LIB) \
		$(TARGET_DIR)/aarch64-apple-darwin/release/$(LIB) \
		$(TARGET_DIR)/x86_64-apple-darwin/release/$(LIB)
	xcodebuild -create-xcframework \
		-library $(TARGET_DIR)/aarch64-apple-ios/release/$(LIB) -headers $(BUILD_DIR)/headers \
		-library $(TARGET_DIR)/aarch64-apple-ios-sim/release/$(LIB) -headers $(BUILD_DIR)/headers \
		-library $(BUILD_DIR)/macos/$(LIB) -headers $(BUILD_DIR)/headers \
		-output TursoSdkKit.xcframework

clean:
	rm -rf TursoSdkKit.xcframework $(BUILD_DIR)

test: xcframework
	swift test
