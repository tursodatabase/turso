#!/bin/sh
# link-with-rust-lld.sh - The linker for x86_64-unknown-linux-gnu (see .cargo/config.toml)
#
# Runs `cc` with the LLD linker that ships with the Rust toolchain. On a
# machine with 4 cores, LLD links a debug build of tursodb in 1.4 seconds, and
# GNU ld in 10 seconds. Rust 1.90 and later use this linker by default on this
# target, so remove this script when rust-toolchain.toml moves to 1.90 or later.
#
# rustc puts the bin directory of its toolchain first in PATH when it runs the
# linker. That directory has rust-lld and gcc-ld/ld.lld. If they are not
# there, the script runs `cc` with its default linker.
#
# In a Nix build environment (NIX_CC is set), the script runs `cc` with its
# default linker, because -fuse-ld=lld skips the Nix linker wrapper, which
# adds the path of the Nix dynamic linker to each binary.
#
# -z nostart-stop-gc: with Rust 1.88, LLD removes the sections that only the
# __start_ and __stop_ symbols refer to, and the link fails with undefined
# symbols. linkme (a dependency of antithesis_sdk) uses these symbols. GNU ld
# keeps these sections, and this option makes LLD keep them too.

if [ -z "$NIX_CC" ] && lld=$(command -v rust-lld) && [ -x "${lld%/*}/gcc-ld/ld.lld" ]; then
  exec cc -B"${lld%/*}/gcc-ld" -fuse-ld=lld -Wl,-z,nostart-stop-gc "$@"
fi
exec cc "$@"
