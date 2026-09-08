# Turso local fork

Source: https://github.com/quickwit-oss/tantivy

Release: `0.26.2` (tag has no `v` prefix).
Commit: `72d1ef9a6468aa68bbc69dcc80cdf60aaf64364d`.
Annotated tag object: `6acbddd43d68c185362044e5ebdbed8566df37e1`.
The crates.io release records the same commit in `.cargo_vcs_info.json`.
Its archive SHA-256 is
`861facfabd71044968f364837f9a083b56464ba5a59079f88706ee5c451ca069`.
Imported from the GitHub source archive of that commit. Original MIT license,
copyright notices, tests, and workspace crates are retained. No remote fork
is required. Turso's `core/Cargo.toml` selects this directory by path; upstream
path dependencies select the included companion crates, including ownedbytes.
`tantivy-fst` remains an unchanged registry dependency.

This import does not change Tantivy behavior. Formatting normalization, if
needed by Turso's stable toolchain, is recorded in a separate commit.

## Standalone tests

`.github/workflows/vendor-tantivy.yml` in the Turso repository tests this
workspace and all eight companion crates with the repository Rust toolchain.
Run `cargo test --manifest-path vendor/tantivy/Cargo.toml --locked --workspace`
from the repository root. CI also tests no-default-feature libraries and the
upstream `mmap,quickwit,failpoints` configuration. No-default-feature examples
and doctests are omitted because they require mmap. Quickwit doctests run
separately with default features enabled, as upstream does, because some
tokenizer examples require stemmer.

The checked-in standalone `Cargo.lock` includes upstream development
dependencies, separately from Turso's production lock. It pins `ordered-float`
to 5.1.0 because 5.5.0 requires Rust 1.90 while Turso uses Rust 1.88. Preserve
toolchain compatibility when updating this lock. Root workspace test commands
do not cover this excluded workspace's own tests.
