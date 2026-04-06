# Use Homebrew LLVM if available (Apple clang lacks the WASM backend).
CLANG := $(shell command -v /opt/homebrew/opt/llvm/bin/clang 2>/dev/null || \
                 command -v /usr/local/opt/llvm/bin/clang 2>/dev/null || \
                 echo clang)

CFLAGS  = --target=wasm32-unknown-unknown -nostdlib -fno-builtin -O2
LDFLAGS = -Wl,--no-entry -Wl,--export-all -Wl,--initial-memory=131072

udfs.wasm: udfs.c ../../c/turso_wasm.h
	$(CLANG) $(CFLAGS) $(LDFLAGS) -o $@ $<

clean:
	rm -f udfs.wasm

.PHONY: clean
