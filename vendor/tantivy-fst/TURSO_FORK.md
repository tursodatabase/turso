# Turso FST fork

Imported from https://github.com/quickwit-inc/fst at commit
`29e16b3ec1b62fa21bcf4ba65d28088e67a5663f`, the source commit recorded in
the published `tantivy-fst` 0.5.0 package. Its `src` tree was compared byte
for byte with the registry package before import. MIT and Unlicense notices
are retained. Rustfmt-only normalization is recorded separately.

The import makes no format or behavior changes. Both local Tantivy and its
SSTable companion use this path dependency.

## Standalone tests

`.github/workflows/vendor-fst.yml` in the Turso repository runs unit tests and
doctests with default features (including regex) and with no default features,
using the repository Rust toolchain. Run
`cargo test --manifest-path vendor/tantivy-fst/Cargo.toml --locked` from the
repository root; add `--no-default-features` for the second configuration.
The standalone `Cargo.lock` includes upstream development dependencies and is
independent of Turso's production lock. Tantivy's standalone CI also runs when
this crate changes, covering its use by Tantivy and SSTable.

The next layer adds `raw::Node::from_range`: validated decoding from a window
ending at a node's original file address. Node windows need at most 4,619
bytes, including the largest 256-transition node. Transition targets retain
global addresses even when they are outside the window. This preserves
format versions 1 and 2 and does not require rebuilding existing indexes.

This decoder alone does not page Tantivy dictionaries or impose a query
memory cap. Those require injected range reads and suspendible traversal
in callers, plus paging the separately encoded term information.
