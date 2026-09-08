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
Directory mutation has opt-in `atomic_write_async`, `delete_async` and
`sync_directory_async` methods; their defaults return Unsupported. Index
creation and final merge metadata publication await these methods, preserving the
sync-before-publish ordering. RamDirectory and Turso's private BuildDirectory
implement these as ready, memory-only operations. Native segment writers use
injected output; synchronous compatibility writers remain available.

Managed metadata writes serialize across clones with a runtime-free async
mutex. No lock guard over the managed-path set survives an await. A failed or
cancelled mutation prevents further metadata writes on that managed instance
and its clones: the owner must drain submitted storage before reopening.
Cancellation is not rollback and must not race a new write against an older
one still running in the driver. Mixing synchronous mutation/GC with an
in-flight asynchronous metadata mutation is not supported.

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
and transactional publication remain owned by Turso. Builds stream component
chunks before publishing a registry row. Merge output still captures full files.
The FST merger uses a typed k-way heap union so
its suspended future is Send without unsafe trait assertions.

## Boundaries and remaining synchronous work

### Production async completion checklist

- [x] Snapshot/index/reader opening: transaction-bound queued reads, no whole-index preload.
- [x] Lookup and supported query construction: await dictionary, statistics and payload reads.
- [x] Store fetch: await compressed-block reads; decompression is CPU work.
- [x] Current traversal/collectors: decode already-resident scorer payloads without storage calls.
  Block-paged traversal is separate work; current posting/position allocations remain unbounded.
- [x] Index creation and final merge metadata publication: await injected mutation/sync operations.
- [x] Segment build output opening: replace the synchronous component writer requirement.
- [x] Segment build output bytes: bounded pending buffers, partial-write progress and real backpressure.
- [x] Build serialization/finalization: retain encoder/footer progress across output suspension.
- [x] Build publication: insert output chunks resumably and publish the registry only after close.
- [ ] Merge/OPTIMIZE output: use the same native writer and transaction-safe publication path.
- [ ] Production delayed-output coverage: short writes, errors, cancellation, rollback and snapshots.
- [ ] Final configuration coverage: fork CI matrices and Turso FTS suites; inspect published Actions.

This list describes the Turso production path, not every synchronous upstream
compatibility API. Read payload paging, spill budgets and CPU time-slicing do
not become complete merely because an I/O operation can suspend.

### Native output implementation (selected by production builds, not merges)

`Directory::open_write_async` returns an injected append-only `AsyncWrite`.
Open, short writes, flush and consuming finish are native queued operations;
no synchronous writer adapter or runtime is involved. The queue owns submitted
bytes until acknowledgment. A failed or cancelled writer cannot be retried.
Managed outputs append the existing checksum footer before awaiting finish.

Turso's `OutputDirectory` and `OutputIo` drive that queue through the backing
B-tree and real Completions, alongside reads on the same cursor. Each open file
retains at most one 512 KiB tail. Submitted writes are at most 512 KiB; row
insertion and pager buffers are additional allocations. Flushing a partial
chunk replaces its exact key rather than creating a duplicate. Finalization
releases the tail. No registry row is published by this driver. Scratch rows
are transaction-private and removed before returning a successful build;
unfinished output is rejected. Errors require abandoning the build and rolling
back, not retrying the partially mutated cursor as a new operation.

Native composite/fieldnorm output writes directly to this sink. Native postings
and positions encode one 128-value block at a time into scratch streams. Each
scratch stream buffers at most 64 KiB before spilling through injected output;
small terms need no temporary B-tree files. Copies use at most 64 KiB reads.
This avoids retaining arbitrarily large encoded term bodies or skip arrays.
It does not bound input document arrays, resident fieldnorms, or total memory.
The FST builder emits one node at a time into a bounded 8 KiB CPU buffer; the
dictionary awaits each emitted chunk and spools term information in 256-term
blocks. Native inverted-index serialization connects these streams across
fields and poisons an abandoned field's parent. These native dictionary and
inverted-index writers target the FST format, not the Quickwit SSTable format.
Recorded postings use the same lending document cursor as synchronous output;
the async field dispatch preserves basic/frequency/position and JSON encoding.
Native store output retains one block plus its compressed form, awaits writes,
and spools each checkpoint layer in eight-checkpoint blocks. An individual
stored document can exceed the target block size; this is not a hard byte cap.
Its append and finalization methods reject reuse after cancellation or failure.
Native column output preserves full numerical codecs using replayable resident
input iterators and small encoded chunks. Optional, multivalued, string, bytes,
and IP columns return Unsupported; Turso's FTS schema uses one full i64 rowid
column. Input arenas, norms and documents remain resident, not memory-capped.
`SegmentWriter::for_segment_async`, `add_document_async` and `finalize_async`
connect these serializers. Failed or cancelled appends poison writer reuse.
Turso's build future owns that writer across Completions; only finalized output
with completed scratch cleanup becomes a registry row. Abort drops the future
and the enclosing statement transaction rolls back its private chunk rows.
Merge output still needs conversion.

Executed with this production build path: 109 Turso FTS integration tests and
31 core FTS tests passed. Native segment parity compares all six components for
0, 1 and 8,193 documents, with three early polls and seven-byte writes per
output request. Scratch threshold tests cover resident and spilled copies.

Standalone tests compare recorded postings across all three record modes,
signed integers and nested JSON against synchronous output, with three early
polls and seven-byte acknowledgments at every output boundary. Store tests
compare complete file bytes and retrieved documents across empty and multilayer
indexes, three block sizes, and every enabled compressor. They mix document
encoding with already-encoded merge input and test cancellation/error poisoning.

`cargo test --locked -p turso_core --features fts,io_memory_yield --lib index_method::fts::output`
passes three tests: format parity at compression/chunk boundaries, all posting
record modes, repeated early resumption, partial chunk replacement, scratch
read lifetime/cleanup, and rollback; plus injected Completion errors and
cancellation at four delayed boundaries each, with no published output rows.
Dictionary parity covers 65,537 terms and metadata block boundaries. A complete
inverted-component test compares text and numeric field output, including
block-WAND statistics, against the synchronous reference's bytes.
The native queue's two tests also cover short acknowledgments and poisoned
writers. These are component/driver tests, not production SQL output coverage.

`PagedTermDictionary` provides a separate format-compatible reader over the
local FST fork's injected async traversal and paged term information. It opens
with 64 metadata bytes, then reads at most one 4,619-byte FST window or one
4,604-byte term-info block. It retains no whole dictionary arrays. Its tests
exercise delayed queued reads, Turso Completion delivery, block boundaries,
malformed metadata, and cancellation/errors between finding a term and reading
its information. Key/automaton state and backing reader caches are not included
in those read-size bounds.

`Query::weight_async` and the asynchronous BM25 statistics methods now carry
suspension through nested queries. Turso's ranked and streaming paths await
weight construction; `Searcher::stream` is asynchronous too. Custom queries
and statistics providers must opt in explicitly (the defaults return
Unsupported, never call synchronous storage methods). Statistics providers
are Send + Sync. CPU-only built-in weight construction remains immediately
ready.

MoreLikeThis supports async weight construction from either stored documents
or supplied field values. Stored-document and term-frequency reads await the
injected reader; the generated Boolean query also builds its weights async.
Term selection and filtering share the synchronous algorithm. The decoded
source document is released after extracting term frequencies.

`Searcher::doc_async`, `StoreReader::get_async` and
`StoreReader::get_document_bytes_async` are available without Quickwit and need
no runtime. The StoreReader methods no longer accept an Executor argument;
this is a fork API change for Quickwit callers. Store-block decompression and
document deserialization now run on the caller, not a background executor.
They remain synchronous CPU work, and block/document size and term-frequency
maps have no enforced byte limit. This is not a CPU-latency or memory bound.

Regex-phrase scorer construction awaits fieldnorms, dictionary stream opening,
postings and positions. Synchronous and asynchronous entry points share the
same bucketing/scoring algorithm. Automaton term collection uses fallible async
advancement; automaton states must be Send when used as a Weight because they
survive suspension. FST stream opening remains ready CPU work on resident
bytes, while SSTable stream opening awaits its existing asynchronous prefetch.
Regex-phrase expansion results and scorer payloads are still resident, not bounded.

Range, phrase-prefix and automaton scorers await stream opening and fallible
advancement, and process each term's postings before advancing to the next term.
They no longer build an intermediate Vec<TermInfo> for the entire expansion.
Range/automaton result bitsets and phrase-prefix suffix postings are still
retained. Bounds and expansion limits are unchanged; no FST range-traversal
optimization is included. The delayed-I/O fixture checks inclusive/exclusive
bounds, missing terms, zero/small expansion limits, score/count parity and
postings-read counts against the range limit, with tombstones in the snapshot.

Production query/merge callers have **not yet switched** to the paged reader.
The converted expansion callers still select resident streams. Dictionary
opening and stream builders must select the paged backend before these awaits
can suspend on FST traversal itself. The FST backend's
`get_async` currently does a CPU-only lookup in resident bytes. The remaining
resident dictionaries described below are still on the current SQL path.

`AsyncTermStreamer` supports resident and paged input cursors. `AsyncTermMerger`
retains initialization, the pending input index and partial matches across
errors/cancellation. Its current term information is captured before input
advancement rather than reread synchronously by ordinal. The index merge loop
awaits field opening, stream opening and merger advancement. Production streams
are still resident; the delayed-I/O tests exercise the paged variant directly.
No range-traversal changes are included in this layer.

This removes the requirement to preload every visible index file for a search,
but is **not a fully paged or bounded-memory Tantivy implementation**:

- FST dictionaries and term-info metadata are resident. Selected fieldnorms,
  encoded columns and each requested term's postings/positions are read as
  contiguous allocations. Large terms and broad queries can still be large.
- The adapter retains at most eight 512 KiB chunk rows per operation. This
  bounds that cache only, not pinned reader bytes, results or total memory.
- Merge reads postings/positions term by term, but opens whole fast-field,
  fieldnorm and store inputs. OPTIMIZE can still merge every visible segment.
- Segment builds stream output with storage backpressure. Merge serialization
  still writes into BuildDirectory and retains complete output files before
  converting them to publication rows; native merge output is unfinished.
- Synchronous upstream APIs remain for resident/ordinary directories. This is
  an additive async API, not a conversion of every Tantivy public operation.
  The old Turso resident-open/cache machinery remains dormant and should be
  removed once the resident test fixtures no longer depend on it.

Fully streaming merge output requires integrating native serialization,
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

The FTS suites cover 28 unit tests and 109 integration tests. The async-only
unit fixture compares scores and addresses against resident readers, checks
that opening leaves position payloads unread, and exercises merge, tombstones,
repeated Pending polls, injected errors and cancellation. The queued-I/O SQL
fixture spans multiple chunks and checks cold MATCH/phrase/ranked/rowid reads,
read failures, pending-statement reset, delayed OPTIMIZE, merge-read errors and
rollback after merge. Native queue tests check invalid/empty ranges, short
responses and dropped requests. The chunk-cache test checks eviction and the
eight-row capacity. These suites, cargo check and formatting passed.

Metadata creation is also driven through Turso Completions. The standalone
directory tests inject delayed writes/syncs and an error at each creation
boundary, check repeated Pending polls and concurrent registrations, and
cancel writes while the driver retains their bytes. Synchronous storage
methods panic in that fixture. The directory suite passes 37 tests.

The weight-construction test uses a paged statistics provider whose synchronous
methods panic, delivers its reads through Completions, and checks exact
multi-segment score parity for term, phrase/slop, Boolean, boost, constant and
disjunction-max queries. Repeated Pending polls, errors, cancellation, disabled
scoring and unsupported custom queries are covered. The standalone Tantivy
query suite passes 229 tests (3 ignored).

The MoreLikeThis fixture exercises stored-document reads through Completions,
repeated Pending polls, read errors, cancellation and subsequent successful
reads. Stored and supplied-value queries match resident scores/results; empty
input errors match and disabled scoring is rejected before I/O. The standalone
store suite passes 25 tests.

The regex-phrase fixture uses delayed, Completion-driven reads over 600
documents. It covers common and rare terms, sparse-bucket rollover, exact score
and count parity, slop, boost, missing terms, invalid regexes and expansion
limits. Errors and cancellation are injected after partial posting construction;
discarded requests are cancelled and fresh scorers still match resident results.

The async merger test injects an error and a cancellation at every read boundary
in a mixed paged/resident merge, checking keys, ordinals and term information.
The Turso dictionary fixture also merges 1,025 terms through Completions.

The standalone Tantivy dictionary suite has 22 passing tests, including the
paged reader; the term-offset overflow regression also passes. Command:
`cargo test --manifest-path vendor/tantivy/Cargo.toml --lib termdict --target-dir /tmp/turso-tantivy-target`.
On Rust 1.88 the ignored standalone development lockfile selected
`ordered-float` 5.1.0 (5.5.0 requires Rust 1.90); the Turso lockfile and
production dependencies were not changed for this test setup.

`cargo clippy -p turso_core --features fts --lib -- --deny=warnings` failed
on an unfulfilled `clippy::new_without_default` expectation in the unchanged
`core/json/cache.rs:107`. The lint was not suppressed and that file was not
modified. The complete workspace suite and memory benchmarks were not run.
