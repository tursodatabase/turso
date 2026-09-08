# Turso FST fork

Imported from https://github.com/quickwit-inc/fst at commit
`29e16b3ec1b62fa21bcf4ba65d28088e67a5663f`, the source commit recorded in
the published `tantivy-fst` 0.5.0 package. Its `src` tree was compared byte
for byte with the registry package before import. MIT and Unlicense notices
are retained. Rustfmt-only normalization is recorded separately.

The import makes no format or behavior changes. Both local Tantivy and its
SSTable companion use this path dependency. It provides the source boundary
for range-backed decoding of existing FST dictionaries without rebuilding
indexes or changing the format discriminator.

## Standalone tests

`.github/workflows/vendor-fst.yml` in the Turso repository runs unit tests and
doctests with default features (including regex) and with no default features,
using the repository Rust toolchain. Run
`cargo test --manifest-path vendor/tantivy-fst/Cargo.toml --locked` from the
repository root; add `--no-default-features` for the second configuration.
The standalone `Cargo.lock` includes upstream development dependencies and is
independent of Turso's production lock. Tantivy's standalone CI also runs when
this crate changes, covering its use by Tantivy and SSTable.
