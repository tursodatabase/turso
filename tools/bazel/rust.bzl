"""Small Rust rules that keep Cargo package metadata authoritative."""

load("@crates//:defs.bzl", _aliases = "aliases", _all_crate_deps = "all_crate_deps", _crate_edition = "crate_edition")
load("@rules_rust//cargo:defs.bzl", _cargo_build_script = "cargo_build_script", _cargo_toml_env_vars = "cargo_toml_env_vars")
load("@rules_rust//rust:defs.bzl", _rust_binary = "rust_binary", _rust_library = "rust_library", _rust_proc_macro = "rust_proc_macro", _rust_shared_library = "rust_shared_library", _rust_static_library = "rust_static_library", _rust_test = "rust_test")

def turso_cargo_package_env(name = "cargo_env", **kwargs):
    """Generates Cargo package environment variables from the package manifest."""
    _cargo_toml_env_vars(
        name = name,
        src = kwargs.pop("src", "Cargo.toml"),
        workspace = kwargs.pop("workspace", "//:Cargo.toml"),
        **kwargs
    )

def turso_cargo_build_script(name = "build_script", cargo_target = "custom-build:build-script-build", deps = [], tags = [], **kwargs):
    """Defines a Cargo build script with Cargo build dependencies and metadata."""
    _cargo_build_script(
        name = name,
        srcs = kwargs.pop("srcs", ["build.rs"]),
        aliases = _cargo_aliases(["build"], []),
        crate_root = kwargs.pop("crate_root", "build.rs"),
        deps = deps + _cargo_deps(["build"]),
        edition = _crate_edition(),
        tags = _cargo_tags(cargo_target, tags),
        **kwargs
    )

def turso_rust_library(name, cargo_target = None, cargo_deps = ["normal"], cargo_proc_macro_deps = ["proc_macro"], deps = [], proc_macro_deps = [], srcs = None, tags = [], **kwargs):
    """Defines a Rust library with its external dependencies supplied by Cargo."""
    if srcs == None:
        srcs = native.glob(["src/**/*.rs"])
    _rust_library(
        name = name,
        srcs = srcs,
        aliases = _cargo_aliases(cargo_deps, cargo_proc_macro_deps),
        deps = deps + _cargo_deps(cargo_deps),
        edition = _crate_edition(),
        proc_macro_deps = proc_macro_deps + _cargo_proc_macro_deps(cargo_proc_macro_deps),
        tags = _cargo_tags("lib:" + name if cargo_target == None else cargo_target, tags),
        **kwargs
    )

def turso_rust_proc_macro(name, cargo_target = None, deps = [], srcs = None, tags = [], **kwargs):
    """Defines a Rust proc macro with its external dependencies supplied by Cargo."""
    if srcs == None:
        srcs = native.glob(["src/**/*.rs"])
    _rust_proc_macro(
        name = name,
        srcs = srcs,
        aliases = _cargo_aliases(["normal"], []),
        deps = deps + _cargo_deps(["normal"]),
        edition = _crate_edition(),
        tags = _cargo_tags(cargo_target or "proc-macro:" + name, tags),
        **kwargs
    )

def turso_rust_binary(name, cargo_target = None, cargo_deps = ["normal"], cargo_proc_macro_deps = ["proc_macro"], deps = [], proc_macro_deps = [], srcs = None, tags = [], **kwargs):
    """Defines a Rust binary with its external dependencies supplied by Cargo."""
    if srcs == None:
        srcs = native.glob(["src/**/*.rs"])
    _rust_binary(
        name = name,
        srcs = srcs,
        aliases = _cargo_aliases(cargo_deps, cargo_proc_macro_deps),
        deps = deps + _cargo_deps(cargo_deps),
        edition = _crate_edition(),
        proc_macro_deps = proc_macro_deps + _cargo_proc_macro_deps(cargo_proc_macro_deps),
        tags = _cargo_tags("bin:" + name if cargo_target == None else cargo_target, tags),
        **kwargs
    )

def turso_rust_benchmark(name, cargo_target = None, cargo_deps = ["normal", "normal_dev"], cargo_proc_macro_deps = ["proc_macro_dev"], src = None, srcs = None, tags = [], **kwargs):
    """Defines a Cargo benchmark compiled as a Bazel Rust binary."""
    if src == None:
        src = "benches/" + name + ".rs"
    if srcs == None:
        srcs = [src]
    turso_rust_binary(
        name = name,
        cargo_deps = cargo_deps,
        cargo_proc_macro_deps = cargo_proc_macro_deps,
        cargo_target = "bench:" + name if cargo_target == None else cargo_target,
        srcs = srcs,
        crate_root = src,
        tags = tags,
        **kwargs
    )

def turso_rust_test(name, cargo_target = None, cargo_deps = ["normal_dev"], cargo_proc_macro_deps = ["proc_macro_dev"], deps = [], proc_macro_deps = [], tags = [], **kwargs):
    """Defines a Rust test with its external dev dependencies supplied by Cargo."""
    args = dict(kwargs)
    args.update({
        "name": name,
        "aliases": _cargo_aliases(cargo_deps, cargo_proc_macro_deps),
        "deps": deps + _cargo_deps(cargo_deps),
        "proc_macro_deps": proc_macro_deps + _cargo_proc_macro_deps(cargo_proc_macro_deps),
        "tags": _cargo_tags(cargo_target, tags),
    })
    if "crate" not in kwargs:
        args["edition"] = _crate_edition()
    _rust_test(**args)

def turso_rust_shared_library(name, cargo_target = None, cargo_deps = ["normal"], cargo_proc_macro_deps = ["proc_macro"], deps = [], proc_macro_deps = [], tags = [], **kwargs):
    """Defines a Rust shared library with external dependencies supplied by Cargo."""
    _rust_shared_library(
        name = name,
        aliases = _cargo_aliases(cargo_deps, cargo_proc_macro_deps),
        deps = deps + _cargo_deps(cargo_deps),
        edition = _crate_edition(),
        proc_macro_deps = proc_macro_deps + _cargo_proc_macro_deps(cargo_proc_macro_deps),
        tags = _cargo_tags(cargo_target, tags),
        **kwargs
    )

def turso_rust_static_library(name, cargo_target = None, cargo_deps = ["normal"], cargo_proc_macro_deps = ["proc_macro"], deps = [], proc_macro_deps = [], tags = [], **kwargs):
    """Defines a Rust static library with external dependencies supplied by Cargo."""
    _rust_static_library(
        name = name,
        aliases = _cargo_aliases(cargo_deps, cargo_proc_macro_deps),
        deps = deps + _cargo_deps(cargo_deps),
        edition = _crate_edition(),
        proc_macro_deps = proc_macro_deps + _cargo_proc_macro_deps(cargo_proc_macro_deps),
        tags = _cargo_tags(cargo_target, tags),
        **kwargs
    )

def _cargo_aliases(deps, proc_macro_deps):
    return _aliases(
        normal = "normal" in deps,
        normal_dev = "normal_dev" in deps,
        build = "build" in deps,
        proc_macro = "proc_macro" in proc_macro_deps,
        proc_macro_dev = "proc_macro_dev" in proc_macro_deps,
    )

def _cargo_deps(classes):
    if not classes:
        return []
    return _all_crate_deps(
        normal = "normal" in classes,
        normal_dev = "normal_dev" in classes,
        build = "build" in classes,
    )

def _cargo_proc_macro_deps(classes):
    if not classes:
        return []
    return _all_crate_deps(
        proc_macro = "proc_macro" in classes,
        proc_macro_dev = "proc_macro_dev" in classes,
    )

def _cargo_tags(cargo_target, tags):
    if not cargo_target:
        return tags
    return tags + ["cargo-target=" + cargo_target]
