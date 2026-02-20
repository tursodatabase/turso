# Makefile for the Turso TCL extension.
#
# The shared library wraps libturso_sqlite3 and exposes an in-process sqlite3
# Tcl command compatible with the upstream SQLite Tcl binding, enabling the
# testing/sqlite3 harness to run without a subprocess per statement.
#
# Primary targets (Linux / Docker — no host toolchain required):
#   docker-build   Build libturso_tcl.so inside a Debian container.
#   docker-test    Build and run the probe tests inside a Debian container.
#
# Local targets (requires TCL dev headers on the host):
#   build          Build for the current platform (macOS → .dylib, Linux → .so).
#   clean          Remove built artefacts.
#
# Usage:
#   make docker-build            # produce bindings/tcl/libturso_tcl.so
#   make docker-test             # build + run probe tests
#   make build                   # local build when TCL headers are available
#   make clean

ROOT_DIR  := $(shell cd ../.. && pwd)
TURSO_LIB := $(ROOT_DIR)/target/debug
SRC       := $(ROOT_DIR)/bindings/tcl/turso_tcl.c
INCLUDE   := $(ROOT_DIR)/sqlite3/include

# ---- Docker -----------------------------------------------------------------

DOCKER_IMAGE ?= rust:bookworm

.PHONY: docker-build
docker-build:
	docker run --rm \
	  -v "$(ROOT_DIR):/workspace" \
	  -w /workspace \
	  $(DOCKER_IMAGE) \
	  bash -c '\
	    apt-get update -qq && \
	    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq tcl-dev && \
	    cargo build -p turso_sqlite3 2>&1 && \
	    gcc -shared -fPIC -o bindings/tcl/libturso_tcl.so \
	        bindings/tcl/turso_tcl.c \
	        -I sqlite3/include \
	        -I /usr/include/tcl8.6 \
	        -L target/debug \
	        -lturso_sqlite3 \
	        -Wl,-rpath,'"'"'$$ORIGIN/../../target/debug'"'"' \
	        2>&1 && \
	    echo "Build OK: bindings/tcl/libturso_tcl.so" \
	  '

# Build the shared library and run the probe tests in one container so the
# apt install and cargo build happen only once.
.PHONY: docker-test
docker-test:
	docker run --rm \
	  -v "$(ROOT_DIR):/workspace" \
	  -w /workspace \
	  $(DOCKER_IMAGE) \
	  bash -c '\
	    apt-get update -qq && \
	    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq tcl-dev && \
	    cargo build -p turso_sqlite3 2>&1 && \
	    gcc -shared -fPIC -o bindings/tcl/libturso_tcl.so \
	        bindings/tcl/turso_tcl.c \
	        -I sqlite3/include \
	        -I /usr/include/tcl8.6 \
	        -L target/debug \
	        -lturso_sqlite3 \
	        -Wl,-rpath,'"'"'$$ORIGIN/../../target/debug'"'"' \
	        2>&1 && \
	    echo "Build OK: bindings/tcl/libturso_tcl.so" && \
	    LD_LIBRARY_PATH=target/debug tclsh bindings/tcl/test_probes.tcl \
	  '

# ---- Local build (macOS / Linux with TCL dev headers) -----------------------

UNAME_S := $(shell uname -s 2>/dev/null)

ifeq ($(UNAME_S),Darwin)
  # Prefer Homebrew tcl-tk via pkg-config; fall back to the Xcode SDK
  # Tcl.framework headers present on any machine with Command Line Tools.
  _TCL_SDK_HDR := $(shell xcrun --show-sdk-path 2>/dev/null)/System/Library/Frameworks/Tcl.framework/Versions/8.5/Headers
  TCL_INCLUDE  ?= $(shell pkg-config --cflags tcl 2>/dev/null || echo "-I$(_TCL_SDK_HDR)")
  TCL_LIBS     ?= $(shell pkg-config --libs   tcl 2>/dev/null || echo "-framework Tcl")
  SHARED_FLAG   = -dynamiclib
  TARGET        = $(ROOT_DIR)/bindings/tcl/libturso_tcl.dylib
else
  TCL_INCLUDE  ?= $(shell pkg-config --cflags tcl 2>/dev/null || echo "-I/usr/include/tcl8.6")
  TCL_LIBS     ?= $(shell pkg-config --libs   tcl 2>/dev/null || echo "-ltcl8.6")
  SHARED_FLAG   = -shared -fPIC
  TARGET        = $(ROOT_DIR)/bindings/tcl/libturso_tcl.so
endif

.PHONY: build
build:
	cargo build -p turso_sqlite3
	gcc $(SHARED_FLAG) -o $(TARGET) \
	    $(SRC) \
	    $(TCL_INCLUDE) \
	    -I$(INCLUDE) \
	    -L$(TURSO_LIB) \
	    -lturso_sqlite3 \
	    $(TCL_LIBS) \
	    -Wl,-rpath,'$(TURSO_LIB)'
	@echo "Built: $(TARGET)"

# ---- Clean ------------------------------------------------------------------

.PHONY: clean
clean:
	rm -f $(ROOT_DIR)/bindings/tcl/libturso_tcl.so \
	      $(ROOT_DIR)/bindings/tcl/libturso_tcl.dylib
