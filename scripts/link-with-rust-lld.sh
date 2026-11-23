#!/bin/sh
# link-with-rust-lld.sh - The linker for x86_64-unknown-linux-gnu (see .cargo/config.toml)
#
# Runs `cc` with the LLD linker that ships with the Rust toolchain. On a
# machine with 4 cores, LLD links a debug build of tursodb in 1.4 seconds, and
# GNU ld in 10 seconds. Rust 1.90 and later use this linker by default on this
# target. When rust-toolchain.toml moves to 1.90 or later, remove this script,
# but keep GNU ld for the crates in the `case` below.
#
# rustc puts the bin directory of its toolchain first in PATH when it runs the
# linker. That directory has rust-lld and gcc-ld/ld.lld. If they are not
# there, the script runs `cc` with its default linker.
#
# In a Nix build environment (NIX_CC is set), the script runs `cc` with its
# default linker, because -fuse-ld=lld skips the Nix linker wrapper, which
# adds the path of the Nix dynamic linker to each binary.
#
# The test binaries of the Node.js bindings (turso_node and turso_sync_js)
# refer to Node-API symbols that only the node process defines. GNU ld with
# --warn-unresolved-symbols (see .cargo/config.toml) resolves these symbols
# to 0. LLD puts them into the dynamic symbol table, and because the binaries
# link with -z now, they stop at start with "undefined symbol". So these two
# crates link with GNU ld. Cargo sets CARGO_PKG_NAME for rustc, and rustc
# gives its environment to the linker.
#
# -z nostart-stop-gc: with Rust 1.88, LLD removes the sections that only the
# __start_ and __stop_ symbols refer to, and the link fails with undefined
# symbols. linkme (a dependency of antithesis_sdk) uses these symbols. GNU ld
# keeps these sections, and this option makes LLD keep them too.

case "$CARGO_PKG_NAME" in
  turso_node | turso_sync_js) exec cc "$@" ;;
esac

if [ -z "$NIX_CC" ] && lld=$(command -v rust-lld) && [ -x "${lld%/*}/gcc-ld/ld.lld" ]; then
  exec cc -B"${lld%/*}/gcc-ld" -fuse-ld=lld -Wl,-z,nostart-stop-gc "$@"
fi
exec cc "$@"
