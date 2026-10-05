# Build the libuv + Turso example.
#
# Prerequisites:
#   - libuv headers and library (macOS: `brew install libuv`,
#     Debian/Ubuntu: `apt install libuv1-dev`)
#   - the Turso C library, built with:
#       cargo build -p turso_sdk_kit --release
#
# Run `make` from this directory; `make run` builds and executes the example.

# Location of the Turso repository root. `make` is expected to run from this
# directory, so the repo root is two levels up. Override to build from elsewhere.
TURSO_ROOT ?= $(abspath $(CURDIR)/../..)

CC ?= cc
UNAME := $(shell uname -s)

# Where libuv lives on each platform.
ifeq ($(UNAME),Darwin)
UV_PREFIX ?= $(shell brew --prefix libuv 2>/dev/null || echo /opt/homebrew)
UV_CFLAGS ?= -I$(UV_PREFIX)/include
UV_LDFLAGS ?= -L$(UV_PREFIX)/lib -luv
else
UV_CFLAGS ?=
UV_LDFLAGS ?= -luv
endif

BUILD_DIR := build
TURSO_LIB_DIR := $(TURSO_ROOT)/target/release
TURSO_INCLUDE := $(TURSO_ROOT)/sdk-kit

CFLAGS ?= -O2 -g -Wall -Wextra -std=c11
LDFLAGS := -L$(TURSO_LIB_DIR) -lturso_sdk_kit $(UV_LDFLAGS)
INCLUDES := -I$(TURSO_INCLUDE) $(UV_CFLAGS)

all: $(BUILD_DIR)/libuv-example

$(BUILD_DIR)/libuv-example: main.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(INCLUDES) -o $@ main.c $(LDFLAGS)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

# `make run` uses a fresh database path on purpose: see README.md ("Known
# limitation"). Opening an already-populated database with async_io=1 is the one
# operation the C ABI cannot drive, so a second run against the same file would
# abort with the diagnostic the example prints.
DB ?= $(shell mktemp -u /tmp/libuv-example-XXXXXX.db)
run: all
	DYLD_LIBRARY_PATH=$(TURSO_LIB_DIR) $(BUILD_DIR)/libuv-example $(DB)

clean:
	rm -rf $(BUILD_DIR) libuv-example.db

.PHONY: all run clean
