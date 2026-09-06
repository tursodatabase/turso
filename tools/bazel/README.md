# Bazel Rust metadata

`Cargo.toml` owns Rust package names, editions, and external normal, dev, build,
and proc-macro dependencies. `crate_universe` reads the Cargo workspace and
generates those dependencies. The macros in `rust.bzl` add that Cargo-owned
metadata to small `rules_rust` rules.

BUILD files continue to own Bazel-only choices: first-party dependencies,
features and configuration variants, native link inputs, visibility, data,
platform selects, sharding, and packaging. Native binding rules remain direct
when a wrapper would hide those choices.

After changing Cargo targets or dependencies, run:

```sh
./tools/bazel/validate.sh
```

The command checks Cargo/Bazel target parity, regenerates crate_universe lock
data, and fails if any generated lock file changes. Commit an intentional lock
change and rerun it. The parity checker excludes only the published-header
build script in `extensions/core` and the release-only Shuttle stress test;
their reasons live next to the exclusions in the checker.
