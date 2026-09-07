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
`tantivy-fst` is separately vendored at `../tantivy-fst`; its provenance is
recorded there. Its import is isolated from decoder changes in the PR stack.
The imported Rust sources are normalized by Turso's `cargo fmt --all` (which
also visits local path dependencies). Upstream's nightly-only formatting
options produce warnings on Turso's stable toolchain but do not prevent it.

This is a local dependency: publishing Turso core would strip the path and
select upstream 0.26.2, which lacks this API. A fork package/version strategy
is required before publishing; none is configured or authorized here.

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

## Local changes

`common::ReadQueue`, re-exported by `tantivy::directory`, injects logical
snapshot files. Native Rust futures submit exact byte ranges and suspend until
their owner completes the request. Tantivy knows neither Turso nor Tokio.
Synchronous reads on these handles return Unsupported, not a retryable error.
No runtime, hidden blocking call, background task, or busy-polling is used.

`core/index_method/fts/read.rs::SnapshotIo` retains the future and services
its requests through the transaction-bound B-tree cursor. It seeks the required
512 KiB chunk rows and returns the pager's real Completion through IOResult.
Early reentry returns the outstanding Completion without resubmission; errors
are checked before cursor reentry. Dropping the operation cancels its private
future; storage retains ownership of buffers already submitted to it. Requests
are sequential, so no additional CompletionGroup is needed around a pager's
own single/group completion.

Async reader opening covers footers, composite metadata, dictionaries,
fieldnorms, fast fields and store metadata. Searcher opening obtains global
BM25 statistics without reading all postings/positions. Scorer construction
awaits selected term payloads. Term, phrase/slop, Boolean, boost/constant,
all/empty, automaton, phrase-prefix, and range query weights support this path.
Once constructed, scorers decode resident payloads without storage I/O.
Unsupported external Weight implementations fail explicitly by default.

The `Directory` trait exposes object-safe `get_file_handle_async`,
`open_read_async` and `atomic_read_async` methods. Async managed-file opening awaits directory lookup
before footer I/O, including through `Box<dyn Directory>`. Implementations
must opt in; the default returns Unsupported rather than blocking. RamDirectory
and Turso's resident-registry directories perform ready, memory-only lookups.
`ManagedDirectory::wrap_async`, `Index::open_async` and `Index::load_metas_async`
await management/index metadata reads. Turso retains the entire open future
until the Searcher is ready, then installs its index and parser. The synchronous
and asynchronous entry points share metadata parsing and corruption handling.
Metadata still occupies a complete buffer; this is not a metadata memory cap.
Directory mutation, atomic metadata writes, sync and writer interfaces
remain synchronous; this is not yet an entirely asynchronous directory API.

`Searcher::stream` returns a native `SearchStream` whose async `next` retains
one segment scorer at a time and uses global snapshot statistics. Turso's
unordered MATCH paths use it and retain only the current rowid column; crossing
a segment boundary in `query_next` can yield a Completion. LIMIT exhaustion
drops the stream and column without opening another segment. The delayed-I/O
fixture checks skipped segment reads, global BM25 parity and payload release.
The snapshot's metadata and a single scorer's payloads are still not paged.

Turso uses the native path for MATCH, ranked search, rowid lookup/deletion,
and merge input reads. Searchers and logical handles stay snapshot-private;
they are not admitted to the shared searcher cache. Registry/tombstone scans
and transactional publication remain owned by Turso. Newly serialized file
buffers are discarded after conversion to publication rows, not retained for
the transaction's lifetime. The FST merger uses a typed k-way heap union so
its suspended future is Send without unsafe trait assertions.

## Boundaries and remaining synchronous work

This removes the requirement to preload every visible index file for a search,
but is **not a fully paged or bounded-memory Tantivy implementation**:

- FST dictionaries and term-info metadata are resident. Selected fieldnorms,
  encoded columns and each requested term's postings/positions are read as
  contiguous allocations. Large terms and broad queries can still be large.
- The adapter retains at most eight 512 KiB chunk rows per operation. This
  bounds that cache only, not pinned reader bytes, results or total memory.
- Merge reads postings/positions term by term, but opens whole fast-field,
  fieldnorm and store inputs. OPTIMIZE can still merge every visible segment.
- Segment serialization/finalization remains synchronous CPU work into
  BuildDirectory memory. Output files and pending publication rows are not
  streamed with storage backpressure. Actual B-tree publication is resumable,
  but native asynchronous serialization is unfinished.
- Synchronous upstream APIs remain for resident/ordinary directories. This is
  an additive async API, not a conversion of every Tantivy public operation.
  The old Turso resident-open/cache machinery remains dormant and should be
  removed once the resident test fixtures no longer depend on it.

Fully streaming output requires changing serialization/finalization contracts,
not wrapping std::io::Write in an async signature. Block-paged scorers likewise
need a fallible resumable DocSet contract; the current owned-slice decoders
cannot yield while traversing a payload. Neither is claimed here.

## Verification

Executed in the Turso workspace:

```sh
cargo check -p turso_core --features fts
cargo fmt --all -- --check
cargo test -p turso_core --features fts index_method::fts --lib
cargo test -p core_tester --test integration_tests fts_
```

The FTS suites cover 23 unit tests and 109 integration tests. The async-only
unit fixture compares scores and addresses against resident readers, checks
that opening leaves position payloads unread, and exercises merge, tombstones,
repeated Pending polls, injected errors and cancellation. The queued-I/O SQL
fixture spans multiple chunks and checks cold MATCH/phrase/ranked/rowid reads,
read failures, pending-statement reset, delayed OPTIMIZE, merge-read errors and
rollback after merge. Native queue tests check invalid/empty ranges, short
responses and dropped requests. The chunk-cache test checks eviction and the
eight-row capacity. These suites, cargo check and formatting passed.

`cargo clippy -p turso_core --features fts --lib -- --deny=warnings` failed
on an unfulfilled `clippy::new_without_default` expectation in the unchanged
`core/json/cache.rs:107`. The lint was not suppressed and that file was not
modified. The complete workspace suite and memory benchmarks were not run.
