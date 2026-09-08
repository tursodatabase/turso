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

`asynchronous::Fst` opens with two 16-byte metadata reads, then performs
lookup and automaton streaming through an injected `RangeReader`. No runtime
is used. Node read requests are at most 4,619 bytes. Only one returned node
window is retained; stream ancestors keep addresses and automaton state,
so traversal state grows with key depth. Reader cache/backing allocations
are outside this accounting.

Delayed-reader tests compare all 20,001 keys with the resident reader,
track retained range bytes, repeatedly poll pending operations, and check
errors and cancellation without skipped results. These APIs alone do not
page Tantivy dictionaries or impose a query memory cap: callers still need
to use suspendible traversal and page the separately encoded term information.

`raw::ChunkBuilder` separates CPU node construction from output delivery.
Ordered insertion and finalization can be resumed one encoded node at a time;
the encoder's output buffer is limited to 8 KiB. Tantivy awaits its injected
writer between chunks. There is no synchronous storage callback or runtime.
After output failure/cancellation, the caller must discard the builder rather
than replay a node whose address is already registered. Registry and unfinished
key state remain separate allocations, not part of a total-memory bound.

Byte-for-byte tests compare with the original writer for empty maps, 20,001
keys, a 100,000-byte key, large values and wide nodes. Standalone tests pass:
132 unit + 14 doc tests with default features; 119 unit + 14 doc tests without
default features. Five upstream doctests remain ignored in each configuration.
