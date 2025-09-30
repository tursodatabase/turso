# Changelog

## 0.1.5 -- 2025-09-10

### Added

* add missing module type for browser package (Nikita Sivukhin)
* Implement 2-args json_each (Mikaël Francoeur)
* Add OPFS support to JavaScript bindings (Nikita Sivukhin)
* test/fuzz: add UPDATE/DELETE fuzz test (Jussi Saurio)
* add gen-bigass-database.py (Jussi Saurio)
* Add assertion: we read a page with the correct id (Jussi Saurio)
* support float without fractional part (Lâm Hoàng Phúc)
* expr: use more efficient implementation for binary condition exprs (Jussi Saurio)
* Add json_each table-valued function (1-arg only) (Mikaël Francoeur)
* Add io_uring support to stress (Pekka Enberg)
* Refactor LIMIT/OFFSET handling to support expressions (bit-aloo)
* Encryption: add support for other AEGIS and AES-GCM cipher variants (Frank Denis)
* introduce package.json for separate *-browser package (both database and sync) (Nikita Sivukhin)
* introduce `eq/contains/starts_with/ends_with_ignore_ascii_case` macros (Lâm Hoàng Phúc)
* introduce `match_ignore_ascii_case` macro (Lâm Hoàng Phúc)
* core: Make strict schema support experimental (Pekka Enberg)
* core/printf: support for more basic substitution types (Luiz Gustavo)
* Return sqlite_version() without being initialized (Preston Thorpe)
* Support encryption for raw WAL frames (Gaurav Sarma)
* bindings/java: Implement date, time related methods under JDBC4PreparedStatement (Kim Seon Woo)
* Support cipher and encryption key URI options (William Souza)
* Implement UPSERT (Preston Thorpe)
* CLI: implement `Line` output .mode (Andrey Oskin)
* add sqlite integrity check back (Pedro Muniz)
* core: Initial pass on synchronous pragma (Pekka Enberg)
* Introduce and propagate `IOContext` as required (Avinash Sajjanshetty)
* Add some docs on encryption (Avinash Sajjanshetty)
* sqlite3: Implement sqlite3_malloc() and sqlite3_free() (Pekka Enberg)
* sqlite3: Implement sqlite3_next_stmt() (Pekka Enberg)
* core/translate: Add  support (Pekka Enberg)
* sqlite3: Implement sqlite3_db_filename() (Pekka Enberg)
* flake.nix: add uv dependency to nativeBuildInputs (Ceferino Patino)
* sqlite3: Implement sqlite3_bind_parameter_index() (Pekka Enberg)
* sqlite3: Implement sqlite3_clear_bindings() (Pekka Enberg)
* sqlite3: Implement sqlite3_get_autocommit() (Pekka Enberg)
* Add support for AEGIS encryption algorithm (Avinash Sajjanshetty)
* bindings/java: Implement batch operations for JDBC4Statement (Kim Seon Woo)
* Add syntax highlighting for EXPLAIN and ANALYZE (Alex Miller)
* Add basic support for ANALYZE statement (Alex Miller)
* correctly implement offset() in parser (Lâm Hoàng Phúc)
* Switch to new parser in core (Levy A.)
* github: Remove Intel Mac support (Pekka Enberg)
* add remove_file method to the IO (Nikita Sivukhin)
* Add libc fault injection to Antithesis (Pekka Enberg)
* core/mvcc:  support for MVCC (Pere Diaz Bou)
* SQLite C API improvements: add column type and column decltype (Danawan Bimantoro)
* Initial pass to support per page encryption (Avinash Sajjanshetty)

### Updated
* clean `print_query_result` (Lâm Hoàng Phúc)
* update update-script to properly handle JS workspace (Nikita Sivukhin)
* no need `QueryStatistics` if `self.opts.timer` is not set (Lâm Hoàng Phúc)
* optimizer: convert outer join to inner join if possible (Jussi Saurio)
* Handle case where null flag is set in op_column (Jussi Saurio)
* remove &1 (Lâm Hoàng Phúc)
* reduce cloning `Arc<Page>` (Lâm Hoàng Phúc)
* Evaluate left join seek key condition again after null row (Jussi Saurio)
* use mlugg/setup-zig instead of unmaintained action (Kingsword)
* Prevent setting of encryption keys if already set (Gaurav Sarma)
* Remove RefCell from Cursor (Pedro Muniz)
* Page Cache: optimize and use sieve/Gclock hybird algorithm in place of LRU (Preston Thorpe)
* core: handle edge cases for read_varint (Sonny)
* Persistence for DBSP-based materialized views (Glauber Costa)
* io_uring: prevent out of order operations that could interfere with durability (Preston Thorpe)
* core: Simplify WalFileShared life cycle (Pekka Enberg)
* prevent modification to system tables. (Glauber Costa)
* mark completion as done only after callback will be executed (Nikita Sivukhin)
* core/mvcc: make commit_txn return on I/O (Pere Diaz Bou)
* windows iterator returns no values for shorter slice (Lâm Hoàng Phúc)
* Unify resolution of aggregate functions (Piotr Rżysko)
* replace some matches with `match_ignore_ascii_case` macro (Lâm Hoàng Phúc)
* Make io_uring sound for connections on multiple threads (Preston Thorpe)
* build native package for ARM64 (Nikita Sivukhin)
* refactor parser fmt (Lâm Hoàng Phúc)
* string sometimes used as identifier quoting (Lâm Hoàng Phúc)
* CURRENT_TIMESTAMP can fallback TK_ID (Lâm Hoàng Phúc)
* remove `turso_sqlite3_parser` from `turso_parser` (Lâm Hoàng Phúc)
* Simulate I/O in memory (Pedro)
* Simulate I/O in memory (Pedro Muniz)
* Refactor encryption to manage authentication tag internally (bit-aloo)
* Unify handling of grouped and ungrouped aggregations (Piotr Rżysko)
* Evict page from cache if page is unlocked and unloaded (Pedro Muniz)
* Use u64 for file offsets in I/O and calculate such offsets in u64 (Preston Thorpe)
* Document how to use CDC (Pavan Nambi)
* Upgrade Rust version in simulator build Dockerfile (Preston Thorpe)
* Parse booleans to integer literals in expressions (Preston Thorpe)
* Simulator Profiles (Pedro Muniz)
* Change views to use DBSP circuits (Glauber Costa)
* core/wal: cache file size (Pere Diaz Bou)
* Remove some code duplication in the CLI (Preston Thorpe)
* core/translate: parse_table remove unnecessary clone of table name (Pere Diaz Bou)
* Update COMPAT.md to remove CREATE INDEX default disabled (Preston Thorpe)
* core/translate: remove unneessary agg clones (Pere Diaz Bou)
* core/vdbe: Micro-optimize "zero_or_null" opcode (Pekka Enberg)
* translate: with_capacity insns (Pere Diaz Bou)
* perf: avoid constructing PageType in helper methods (Jussi Saurio)
* refactor/perf: remove BTreePageInner (Jussi Saurio)
* Improve integrity check (Nikita Sivukhin)
* translate/insert: Improve string format performance (Pere Diaz Bou)
* core/schema: get_dependent_materialized_views_unnormalized (Pere Diaz Bou)
* core/util: emit literal, cow instead of replace (Pere Diaz Bou)
* core/translate: sanize_string fast path improvement (Pere Diaz Bou)
* core/io: Switch Unix I/O to use libc::pwrite() (Pekka Enberg)
* Update README.md for Go documentation (Preston Thorpe)
* improve sync engine (Nikita Sivukhin)
* Remove Go bindings (Preston Thorpe)
* core/storage: Micro-optimize Pager::commit_dirty_pages() (Pekka Enberg)
* Rename Go driver to `turso` to not conflict with sqlite3 (Preston Thorpe)
* Refactor: `Cell` instead of `RefCell` to store `CipherMode` in connection (Avinash Sajjanshetty)
* Improve documentation of page pinning (Jussi Saurio)
* Remove double indirection in the Parser (Pedro Muniz")
* Fail CI run if Turso output differs from SQLite in TPC-H queries (Jussi Saurio)
* Decouple SQL generation from Simulator crate (Pedro Muniz)
* Make fill_cell_payload() safe for async IO and cache spilling (Jussi Saurio)
* Remove Windows IO in place of Generic IO (Preston Thorpe)
* Improve encryption API (Avinash Sajjanshetty)
* Remove double indirection in the Parser (Pedro Muniz)
* Update TPC-H running instructions in PERF.md (Alex Miller)
* Truncate the WAL on last connection close (Preston Thorpe)
* DBSP projection (Pekka Enberg)
* Use vectored I/O for appending WAL frames (Preston Thorpe)
* Remove unnecessary argument from Pager::end_tx() (Nikita Sivukhin)
* refactor/btree: rewrite the find_free_cell() function (Jussi Saurio)
* refactor/btree: rewrite the free_cell_range() function (Jussi Saurio)
* Remove Result from signature (Mikaël Francoeur)
* Remove duplicated attribute in (bit-aloo)
* reduce cloning Token in parser (Lâm Hoàng Phúc)
* refactor encryption module and make it configurable (Avinash Sajjanshetty)
* Replace a couple refcells for types that trivially impl Copy (Preston Thorpe)
* wal-api: allow to mix frames insert with SQL execution (Nikita Sivukhin)
* move check code into parser (Lâm Hoàng Phúc)
* Serialize compat tests and use Mutex::lock() instead of Mutex::try_lock() in UnixIO (Jussi Saurio)
* sim: remove "run_once faults" (Jussi Saurio)
* should not return a Completion when there is a page cache hit (Pedro Muniz)
* github: Reduce Python build matrix (Pekka Enberg)
* Page cache truncate (Nikita Sivukhin)
* Wal api checkpoint seq (Nikita Sivukhin)
* Use more structured approach in translate_insert (Jussi Saurio)
* Remove hardcoded flag usage in DBHeader for encryption (Avinash Sajjanshetty)
* properly execute pragmas - they may require some IO (Nikita Sivukhin)
* Wal checkpoint upper bound (Nikita Sivukhin)
* Improve WAL checkpointing performance (Preston Thorpe)
* core/mvcc: store txid in conn and reset transaction state on commit (Pere Diaz Bou)
* core/mvcc: start first rowid at 1 (Pere Diaz Bou)
* refactor/vdbe: move insert-related seeking to VDBE from BTreeCursor (Jussi Saurio)

### Fixed
* Fix clear_page_cache method and rollback (Preston Thorpe)
* Fix read_entire_wal_dumb: incrementally build the frame cache (Preston Thorpe)
* Fix merge script to prompt if tests are still in progress (Preston Thorpe)
* SQL generation fixes (Pekka Enberg)
* Fix affinity handling in MakeRecord (Pekka Enberg)
* Fix infinite loop when IO failure happens on allocating first page (Preston Thorpe)
* Fix crash in Next opcode if cursor stack has no pages (Jussi Saurio)
* cli: Fix dump compatibility in "PRAGMA foreign_keys" (Pekka Enberg)
* Small fixes (Nikita Sivukhin)
* Avoid allocating and then immediately fallbacking errors in affinity (Jussi Saurio)
* Fix float formatting and comparison + Blob concat (Levy A.)
* Fix infinite loop when query starts comment token ("--") (Lâm Hoàng Phúc)
* Fix sqlite3 test cases (Pekka Enberg)
* Fix non-determinism in simulator (Pedro Muniz)
* Fix column count in ImmutableRow (Glauber Costa)
* Fix memory leak in page cache during balancing (Preston Thorpe)
* Fix `sim-schema` command (Pedro Muniz)
* Propagate decryption error from the callback (Avinash Sajjanshetty)
* Fix sorter column deduplication (Piotr Rżysko)
* Fix missing functions after revert (Pedro Muniz)
* ci: fix merge-pr issue to escape command-line backticks (Ceferino Patino)
* Fix several issues with integrity_check (Jussi Saurio)
* core/io: Fix build on Android and iOS (Pekka Enberg)
* WAL txn: fix reads from DB file (Nikita Sivukhin)
* Fix blob type handling in JavaScript (Pekka Enberg)
* Fix: all indexes need to be updated if the rowid changes (Jussi Saurio)
* Fix: in UPDATE, insert rowid into index instead of NULL (Jussi Saurio)
* Fix: normalize table name in DELETE (Jussi Saurio)

## 0.1.4 -- 2025-08-20

### Added
* bindings/rust: Add  method (Pekka Enberg)
* Add helper to convert io::clock::Instant to useable format (Preston Thorpe)
* bindings/javascript: Add TypeScript declarations to package (Pekka Enberg)
* add missing closing tag (Glauber Costa)
* add metrics and implement the .stats command (Glauber Costa)
* Add bench-sqlite script and makefile command (Preston Thorpe)
* Fix simulator docker build by adding new sync directory (Preston Thorpe)
* core/mvcc: schema_did_change support and find last valid version (Pere Diaz Bou)
* Add list databases and open database commands to the MCP server (Glauber Costa)
* Add documentation and rename functions (Mikaël Francoeur)
* Add io_yield macros to reduce boilerplate (Preston Thorpe)
* Add --keep-files flag to allow for inspection of files for successful simulator runs (Preston Thorpe)
* core/printf: support for the %i operand (Luiz Gustavo)
* Add parser to Dockerfiles for cargo chef (Jussi Saurio)
* Add framework for testing extensions in TCL (Piotr Rżysko)
* Fix WAL initialization to last committed frame (Nikita Sivukhin)
* sim: add Property::TableHasExpectedContent (Jussi Saurio)
* Properly implement  CLI command (Preston Thorpe)
* docs: add Claude Code MCP integration guide (Braden Wong)
* Add assertion for expected write amount in writev callback (Preston Thorpe)
* sim: add Property::ReadYourUpdatesBack (Jussi Saurio)
* Add support for unlikely(X) (bit-aloo)
* Implement normal views (Glauber Costa)
* turso-sync: support checkpoint (Nikita Sivukhin)
* bindings/javascript: Add async connect() function (Pekka Enberg)
* Direct schema mutation – add RenameColumn instruction (Levy A.)
* Implement Aggregations for DBSP views (Glauber Costa)
* stop silently ignoring unsupported features in incremental view where clauses (Jussi Saurio)
* turso-sync: support updates and schema changes (Nikita Sivukhin)
* turso-cdc: add updates column for cdc table (Nikita Sivukhin)
* docs: improve README initialization section clarity (Braden Wong)
* Add support for Full checkpoint mode in the WAL (Preston Thorpe)
* Add support for PRAGMA freelist_count (bit-aloo)
* Initial pass on incremental view maintenance with DBSP (Glauber Costa)
* SQLite C API improvements: add bind_text and bind_blob (Danawan Bimantoro)
* perf/clickbench: enable rest of queries since we support DISTINCT and REGEXP_REPLACE (Jussi Saurio)
* Add table name to the delete bytecode (Glauber Costa)
* Initial pass on incremental view maintenance with DBSP (Glauber Costa)
* docs: fix CLI command and add homebrew install instructions for MacOS (Mattia)
* Reimplement LimboRwLock in the WAL (Preston Thorpe)
* BufferPool: add arena backed pool to support fixed opcodes and coalescing (Preston Thorpe)
* translate: return parse errors for unsupported features instead of silently ignoring (Jussi Saurio)
* Add query only pragma (bit-aloo)
* Direct schema mutation – add  instruction (Levy A.)
* Add .clone CLI command to copy database files (Preston Thorpe)
* PageContent: make read_x/write_x methods private and add dedicated methods (Jussi Saurio)
* SQLite C API improvements: add basic bind and column functions (Danawan Bimantoro)
* javascript: Implement Statement.iterate() (Pekka Enberg)
* Fix panic on loading extension on brand new connection (Preston Thorpe)
* implement the MaxPgCount opcode (Glauber Costa)
* Direct schema mutation – add  instruction (Levy A.)
* Add regexp capture (bit-aloo)
* Add load_insn macro for compiler hint in vdbe::execute hot path (Preston Thorpe)
* test/fuzz: add ALTER TABLE column ops to tx isolation fuzz test (Jussi Saurio)
* core/mvcc: implement exists (Pere Diaz Bou)
* tests/fuzz_transactions: add tests for fuzzing transactions with MVCC (Pere Diaz Bou)
* perf/btree: implement fast algorithm for defragment_page (Jussi Saurio)
* bindings/rust: add with_mvcc option, open with path too! (Pere Diaz Bou)
* core/mvcc: implement seeking operations with rowid (Pere Diaz Bou)
* bindings/rust: add with_mvcc option (Pere Diaz Bou)
* Add bitmap to track pages in arenas (Preston Thorpe)
* Direct schema mutation – add  instruction (Levy A.)
* core/mvcc: fix new rowid on restart (Pere Diaz Bou)
* Implement JavaScript bindings with minimal Rust core (Pekka Enberg)
* test/fuzz/transactions: add "PRAGMA wal_checkpoint" to txn isolation fuzz test (Jussi Saurio)
* Support the OFFSET clause for Compound select (meteorgan)
* Introduce some state machines in preparation for IO Completions refactor (Pedro Muniz)
* Add cli Dockerfile (Pere Diaz Bou)
* Implement the Cast opcode (Glauber Costa)
* Support VALUES clauses for compound select (meteorgan)
* fix: add packages to sim/antithesis dockerfiles for cargo-chef (Jussi Saurio)
* Add vector_concat and vector_slice support (bit-aloo)
* bindings/rust: Add Connection::execute_batch() (Rohith Suresh)
* bindings/java: Throw UnsupportedOperationException for unimplemented … (Pekka Enberg)
* turso-sync package: initial commit (Nikita Sivukhin)

### Updated
* JavaScript improvements (Pekka Enberg)
* bindings/javascript: Rename  to (Pekka Enberg)
* Small pager cleanups (Jussi Saurio)
* Do not begin or end transactions in nested statement (Jussi Saurio)
* make the MCP server instructions more visible on the README (Glauber Costa)
* FaultyQuery enabled by default (Pedro Muniz)
* hide our age (Glauber Costa)
* make sure our responses are compliant with MCP (Glauber Costa)
* Move sync code to own directory (Pekka Enberg)
* Refactor: use regular save/restore context mechanism for delete balancing (Jussi Saurio)
* Improve handling of inserts with column names (Wallys Ferreira)
* emit SetCookie when creating a view (Glauber Costa)
* unify halts (Glauber Costa)
* Update stale in memory wal header after restarting log (Preston Thorpe)
* sync-engine: Use SQL over HTTP instead of WAL push (Nikita Sivukhin)
* Convert SQLite parser in Rust by hand (Lâm Hoàng Phúc)
* Ensure we fsync the db file in all paths that we checkpoint (Preston Thorpe)
* Revive async io extension PR (Preston Thorpe)
* sim: reduce frequency of compound selects and complex joins (Jussi Saurio)
* Use BufferPool owned by Database instead of a static global (Jussi Saurio)
* sync-engine: avoid unnecessary WAL push (Nikita Sivukhin)
* use virtual root page for sqlite_schema (Mikaël Francoeur)
* io_uring: Gracefully handle submission queue overflow (Preston Thorpe)
* Disable unused variables in cargo clippy for CI (Pedro Muniz)
* Refactor: atomic ordering (Preston Thorpe)
* Document the I/O model (Pedro Muniz)
* disable checkpoint: adjust semantic (Nikita Sivukhin)
* SDK: enable indices everywhere (Nikita Sivukhin)
* Manual updates (Pekka Enberg)
* Wait for I/O completions (Pedro Muniz)
* IO Cleanups to use  and (Pedro Muniz)
* move our dbsp-based views to materialized views (Glauber Costa)
* More State machines (Pedro Muniz)
* Rename page -> slot for arenas + buffer pool (Preston Thorpe)
* Unify JavaScript package README files (Pekka Enberg)
* simple README with warning (Nikita Sivukhin)
* core/wal: Minor checkpointing cleanups and optimizations (Preston Thorpe)
* perf/btree: optimize op_column (Jussi Saurio)
* Update PERF.md with mobibench instructions (Preston Thorpe)
* Handle single, double and unquoted strings in values clause (Mikaël Francoeur)
* Use rusqlite 0.37 with bundled SQLite everywhere (Jussi Saurio)
* Feat/pragma module list (Lucas Forato)
* remove turso-sync as now we have turso-sync-engine (Nikita Sivukhin)
* Sorter IO Completions (Pedro Muniz)
* Simulator should delete files after a successful run (Pedro Muniz)
* turso-sync: js package (Nikita Sivukhin)
* global allocator should not be set for library, only for executables (Pedro Muniz)
* Evaluate WHERE conditions after LEFT JOIN (Piotr Rżysko)
* Btree cache usable space (Jussi Saurio)
* Rename JavaScript package to (Pekka Enberg)
* perf: a few small insert optimizations (Jussi Saurio)
* javascript: Organize test cases better (Pekka Enberg)
* only allow multiples of 64 for performance in arena bitmap (Preston Thorpe)
* btree: Use correct byte offsets for page 1 in defragmentation (Jussi Saurio)
* bench/insert: use PRAGMA synchronous=full (Jussi Saurio)
* JavaScript improvements (Pekka Enberg)
* turso-sync: rewrite (Nikita Sivukhin)
* bench/insert: use locking_mode EXCLUSIVE and journal_mode=WAL for sqlite (Jussi Saurio)
* IO More State Machine (Pedro Muniz)
* refactor/btree: cleanup write/delete/balancing states (Jussi Saurio)
* cdc: emit entries for schema changes (Nikita Sivukhin)
* Coll seq (Glauber Costa)
* Remove RefCell from Buffer in IO trait methods and PageContents (Preston Thorpe)
* Remove Clone impl for Buffer and PageContent (Preston Thorpe)
* More state machine + Return IO in places where completions are created (Pedro Muniz)
* cleanup: remove unused page uptodate flag (Jussi Saurio)
* Relax I/O configuration attribute to cover all Unixes (Pedro Muniz)
* Update defragment page to defragment in-place (João Severo)
* Integrate virtual tables with optimizer (Piotr Rżysko)
* Reprepare Statements when Schema changes (Pedro Muniz)
* More State Machines in preparation for tracking IO Completions (Pedro Muniz)
* coalesce any adjacent buffers from writev calls into fewer iovecs (Preston Thorpe)
* extend raw WAL API with few more methods (Nikita Sivukhin)
* Use pwrite for single buffer pwritev call in unix IO (Preston Thorpe)
* hide dangerous methods behind conn_raw_api feature (Nikita Sivukhin)
* preserve files in IO memory backend (Nikita Sivukhin)
* Improve SQLite3 TCL test suite (Pekka Enberg)
* Make completions idempotent (Preston Thorpe)
* perf/btree: skip seek in move_to_rightmost() if we are already on rightmost page (Jussi Saurio)
* perf/pager: dont clear page cache on commit (Jussi Saurio)
* Rename liblimbo_sqlite3 to libturso_sqlite3 (Pekka Enberg)
* core: Fold HeaderRef to pager module (Pekka Enberg)
* perf/vdbe: remove eager cloning in op_comparison (Jussi Saurio)
* Update cargo-dist to the latest official version (Hiroaki Yutani)
* bindings/rust: Enhance API by removing verbosity (Diego Reis)
* use state machine for NoConflict opcode (Mikaël Francoeur)
* state_machine: remove State associated type (Pere Diaz Bou)
* Single quotes inside a string literal have to be doubled in (Diego Reis)
* core/mvcc: Move commit_txn() to generic state machinery (Pere Diaz Bou)
* bindings/javascript: Reduce VM/native crossing overhead (Pekka Enberg)
* Enable indexes by default (Jussi Saurio)
* perf/btree: improve performance of rowid() function (Jussi Saurio)
* core/mvcc: Persist changes through pager on commit (Pere Diaz Bou)
* Open a temporary on-disk file for ephemeral tables (Jussi Saurio)
* Force Sqlite to parse schema on connection benchmark (Levy A.)
* more compat police (Glauber Costa)
* Bury limbo-wasm (Diego Reis)
* IN queries (Glauber Costa)
* IN queries (Glauber Costa)
* vdbe: Disallow checkpointing in transaction (Jussi Saurio)
* Serverless JavaScript driver improvements (Pekka Enberg)
* Direct `DatabaseHeader` reads and writes – `with_header` and `with_header_mut` (Levy A.)
* refactor/btree: simplify get_next_record()/get_prev_record() (Jussi Saurio)
* Accumulate/batch vectored writes when backfilling during checkpoint (Preston Thorpe)
* remove non-existent opcode (Glauber Costa)
* skip invalid inputs in cosine distance prop test (bit-aloo)
* core/mvcc: Switch to parking_lot RwLock (Pekka Enberg)
* Rewrite the WAL (Preston Thorpe)
* turso-sync: bidirectional sync for local db (Nikita Sivukhin)
* Change more function signatures to return Completions (Pedro Muniz)
* Clean up conversion between InsnFunctionStepResult and StepResult (Diego Reis)
* core/mvcc: simplify mvcc cursor types (Pere Diaz Bou)
* bindings/javascript: Run tests serially (Diego Reis)
* Javascript testing cleanups (Pekka Enberg)
* Javascript API improvements (Pekka Enberg)
* Change function signatures to return IO Completions (Pedro Muniz)

### Fixed
* Fix page locked panic (Pedro Muniz)
* Remove assertions from Completion::complete() and Completion::error() (Jussi Saurio)
* Completion Error (Pedro Muniz)
* fix/sim: prevent sim from trying to create an existing table or index (Jussi Saurio)
* Fail simulator on parse errors (Jussi Saurio)
* fix pragma table_info for views (Glauber Costa)
* Fix two issues in simulator (Jussi Saurio)
* Fix distinct order by (Jussi Saurio)
* Fix UPDATE: Do not use an index for iteration if that index is going to be updated (Jussi Saurio)
* Fix non-4096 page sizes (Jussi Saurio)
* fix: Handle fresh INSERTs in materialized view incremental maintenance (Glauber Costa)
* Fix: do computations on usable_space as usize, not as u16 (Jussi Saurio)
* Sync engine fixes (Nikita Sivukhin)
* Fix tables renaming to existing index and rename indexed columns on (Levy A.)
* Fix max_frame determination and comments in WAL checkpointing (Preston Thorpe)
* turso-sync: fix schema bug (Nikita Sivukhin)
* Fix: Rename clickbench output file limbo/turso (Henrik Ingo)
* Reprepare fix on write statement (Pedro Muniz)
* Fix view processing in the VDBE (Jussi Saurio)
* Fix performance regression in prepare (Piotr Rżysko)
* Fix JavaScript bindings packaging (Nikita Sivukhin)
* bench/insert: fix expected return value from pragma (Jussi Saurio)
* Fix segfault on schema update for virtual tables (Preston Thorpe)
* core/btree: fix re-entrancy bug in insert_into_page() (Jussi Saurio)
* Fix  performance regression (Jussi Saurio)
* fix/wal: remove start_pages_in_frames_hack to prevent checkpoint data loss (Jussi Saurio)
* fix/core/translate: ALTER TABLE DROP COLUMN: ensure schema cookie is updated even when target table is empty (Jussi Saurio)
* io_uring: setup plumbing for Fixed opcodes (Preston Thorpe)
* JavaScript serverless driver fixes (Pekka Enberg)
* Fix vector deserialization alignment and blob/text empty mismatch (bit-aloo)
* fix/wal: reset ongoing checkpoint state when checkpoint fails (Jussi Saurio)
* Fix parser error for repetition in row values (Diego Reis)
* fix/wal: only rollback WAL if txn was write + fix start state for WalFile (Jussi Saurio)
* fix/bindings/rust: return errors instead of swallowing them and returning None (Jussi Saurio)
* fix/wal: make db_changed check detect cases where max frame happens to be the same (Jussi Saurio)
* fix/wal: reset page cache when another connection checkpointed in between (Jussi Saurio)
* Fix merge script to prevent incorrectly marking merged contributor PRs as closed (Preston Thorpe)
* Fix concat_ws to match sqlite behavior (bit-aloo)
* bindings/rust: return errors instead of vibecoded numbers (Jussi Saurio)

## 0.1.3 -- 2025-07-29

### Added
* bindings/rust: Add WAL API support (Nikita Sivukhin)
* Implement the Returning statement for inserts and updates (Glauber Costa)
* implement the pragma encoding (Glauber Costa)
* support doubly qualified identifiers (Glauber Costa)
* mark detach as supported (Glauber Costa)
* Support ATTACH (read only) (Glauber Costa)
* serverless: Add DatabasError type (Pekka Enberg)
* btree/balance: support case where immediate parent page of unbalanced child page also overflows (Jussi Saurio)
* serverless: Add Statement.run() method (Pekka Enberg)
* make add dirty to change flag and also add page to the dirty list (Nikita Sivukhin)
* types: less noisy Debug implementation for ImmutableRecord (Jussi Saurio)
* Safe `AtomicUsize` wrapper for `db_state`: add `DbState` and `AtomicDbState` (Levy A.)
* Add Github workflow for Turso serverless package (Pekka Enberg)
* Add Rickrolling Turso blog post to contrib (Avinash Sajjanshetty)
* Add `@tursodatabase/serverless` package (Pekka Enberg)
* Implement pragma database_list (Glauber Costa)
* fix/test: fix and unignore incorrectly implemented test (Jussi Saurio)
* implement Debug for Database (Glauber Costa)
* implement pragma application_id (Glauber Costa)
* implement write side of pragma schema_version (Glauber Costa)
* Simplify blocking operations – add `io.block(fn)` for IO trait implementors (Levy A.)
* bindings/rust: Initial support for transactions API (Diego Reis)
* claude sonnet forgot to run clippy when implementing mcp server (Jussi Saurio)
* bindings/js: support iterator, and more kinds of params (Mikaël Francoeur)
* Add a native MCP server (Glauber Costa)
* sim: add order by to some queries (Jussi Saurio)
* Core: Introduce external sorting (Iaroslav Zeigerman)
* forgot to set the state to NewTrunk if we have more leaf pages than free entries (Pedro Muniz)
* Implement IO latency correctly in simulator (Pedro Muniz)
* page cache: temporarily increase default size until WAL spill is implemented (Jussi Saurio)
* compat: add integrity_check (Pere Diaz Bou)
* sim: provide additional context in assertion failures (Jussi Saurio)
* btree: add some assertions related to #2106 (Jussi Saurio)
* bench: add insert benchmark (batch sizes: 1,10,100) (Jussi Saurio)
* Support page_size pragma setting (meteorgan)

### Updated
* bindings/javascript: Generate native npm packages at publish (Pekka Enberg)
* Replace custom wasm bindings with napi-rs (Diego Reis)
* core: Enforce shared database object per database file (Pekka Enberg)
* btree/pager: Improve update performance by reusing freelist pages in allocate_page() (Jussi Saurio)
* VDBE/op_column: use references to cursor payload instead of cloning (Jussi Saurio)
* io/unix: wrap file with Mutex (Pere Diaz Bou)
* core: Clone everything in schema (Pere Diaz Bou)
* core/translate: Handle Expr::Id in `CREATE INDEX` (Kristofer)
* Thread-safe `WindowsFile` (Levy A.)
* io_uring: use Arc pointer for user data of entries (Preston Thorpe)
* Stop checkpointing the entire WAL after every write when wal frame size > threshold (Preston Thorpe)
* compat police (Glauber Costa)
* remove upsert statement from COMPAT.md (Glauber Costa)
* Update limbo -> turso in manual.md (stano)
* bindings/javascript: Switch to napi v3 (Diego Reis)
* Simplify sum() aggregation logic (bit-aloo)
* Ignore WAL frames after bad checksum (Pere Diaz Bou)
* parser: Distinguish quoted identifiers and unify Id into Name enum (bit-aloo)
* btree: clear overflow pages when insert overwrites a cell (= UPDATE) (Jussi Saurio)
* Append WAL frames one by one (Pere Diaz Bou)
* pager: Clear stale page cache if database changed (Jussi Saurio)
* sim/aws: ignore child process exits with code 137 (Jussi Saurio)
* WAL insert API: force schema re-parse if necessary after WAL sync session end (Nikita Sivukhin)
* WAL insert: mark pages as dirty (Nikita Sivukhin)
* emit SetCookie after DropTable (Glauber Costa)
* Bail early for read-only virtual tables (Preston Thorpe)
* measure only the time it takes to open the actual connection (Glauber Costa)
* Explicit rowid insert (Nikita Sivukhin)
* Deserialize keys only once when sorting immutable records (Iaroslav Zeigerman)
* WAL insert API (Nikita Sivukhin)
* Pager: clear overflow cells when freeing page (Jussi Saurio)
* make readonly a property of the database (Glauber Costa)
* use default hasher for the sake of determinism (Nikita Sivukhin)
* Load static extensions once and store on Database instead of once per connection (Preston Thorpe)
* wal: write txn fail in case max_frame change midway (Pere Diaz Bou)
* Improve Antithesis tests (Pekka Enberg)
* core/lib: init_pager lock shared wal until filled (Pere Diaz Bou)
* test/stress&sim: enable indexes by default (Jussi Saurio)
* Usable space unwrap (Pedro Muniz)
* BTreeTable::to_sql: wrap special column names in brackets (Nils Koch)
* Avoid redundant decoding of record headers when reading sorted chunk files (Iaroslav Zeigerman)
* compat: change page_size pragma and RowData opcode to yes (meteorgan)
* Explain the Turso challenge (Glauber Costa)
* gh workflow for dart (test, precompile, publish), only test is activated (Andika Tanuwijaya)
* bindings/rust: Return number of rows changed from Connection::execute() (Rohith Suresh)
* bindings/java: Make TursoDB and TursoDB factory thread-safe (Mikaël Francoeur)
* use `wasm32-wasip1` target instead of `wasm32-wasi` (Nils Koch)
* improve handling of double quotes (Glauber Costa)
* Remove `find_cell`, allow overwrite of index interior cell (Jussi Saurio)
* sim: change --disable-create-index flag to --enable-create-index (default false) (Jussi Saurio)
* Reanimate MVCC (Pekka Enberg)
* Replace verbose IO Completion methods with helpers (Preston Thorpe)
* Use pread and pwrite in run_once (Ihor Andrianov)
* `make_from_btree` should wait for IO to complete (Pedro Muniz)
* chore: update rust to version 1.88.0 (Nils Koch)
* refactor/btree&vdbe: fold index key info (sort order, collations) into a single struct (Jussi Saurio)
* core: Copy-on-write for in-memory schema (Levy A.)
* simulator: Disable `INSERT INTO .. SELECT` for being slow (Pekka Enberg)
* Async IO: registration of file descriptors (Preston Thorpe)
* Clean up AST unparsing, remove `ToSqlString` (Levy A.)
* rename operation_xxx to change_xxx to make naming more consistent (Nikita Sivukhin)
* Separate user-callable cacheflush from internal cacheflush logic (Diego Reis)
* make unixepoch to return i64 (Nikita Sivukhin)
* sim: ignore fsync faults (Jussi Saurio)
* Updates to the simulator (Alperen Keleş)
* small refactor: rename "amount" to "extra_amount" (Nikita Sivukhin)
* refactor: Changes CursorResult to IOResult (Diego Reis)
* btree: unify table&index seek page boundary handling (Jussi Saurio)
* Treat table-valued functions as tables (Piotr Rżysko)

### Fixed
* perf: fix logic error in is_simple_count() (Jussi Saurio)
* Fix writing wal header for async IO (Preston Thorpe)
* bindings/javascript: Fix tracing and `SqliteError` message not populated (Levy A.)
* Fix sum() to follow the SQLite semantics (FamHaggs)
* Fix error handling when binding column references while translating the UPDATE statement (Iaroslav Zeigerman)
* Fix schema reparse logic (Nikita Sivukhin)
* Fix page_count pragma (meteorgan)
* sqlite3: Improve SQLite error handling and fix C-string safety (Ceferino Patino)
* fix: SUM returns correct float for mixed numeric/non-numeric types & return value on empty set (Axel Tobieson Rova)
* silence clippy errors with features disabled (Glauber Costa)
* fix raw read frame WAL API (Nikita Sivukhin)
* Work around CREATE TABLE whitespace issue (Jussi Saurio)
* Fix issues in alter table tests and fix a small error in BTreeTable::to_sql (Jussi Saurio)
* Fix duplicate SET statement compatibility with SQLite (Ihor Andrianov)
* test/clickbench: fix clickbench (Jussi Saurio)
* fix/sim: actually enable indexes by default (Jussi Saurio)
* fix/btree/balance: interior cell insertion can leave page overfull (Jussi Saurio)
* Fix `error: Stable 1.88.0 is not available` in Nix flake (Levy A.)
* Fix column order for multi-row insertion (Nikita Sivukhin)
* doc: fix intra-repo links for contribution guide and license, and expand binding links (bit-aloo)
* fix opcodes missing a database register (Glauber Costa)
* sorter: fix sorter panic on SortedChunkIOState::WaitingForRead (Jussi Saurio)
* Fix parent page stack location after interior node replacement (Jussi Saurio)
* Fix rollback for TxErrors (Diego Reis)
* make most instrumentation levels to be Debug or Trace instead (Pedro Muniz)
* Property `FaultyQuery` should fail if we encounter an error that is not expected (Pedro Muniz)
* translate/create index: fix wrong collations (Jussi Saurio)
* bind/java: Fix Linux x86 build release (Diego Reis)
* fix/btree: fix insert_into_cell() logic (Jussi Saurio)
* cli: fix not being able to redirect traces to a file from inline query (Jussi Saurio)
* bind/javascript: Fix presentation mode disabling logic (Diego Reis)
* btree: fix post-balancing seek bug in delete path (Jussi Saurio)
* btree: fix trying to go upwards when we are already at the end of the entire btree (Jussi Saurio)
* btree: fix interior cell replacement in btrees with depth >=3 (Jussi Saurio)
* extensions/vtab: fix i32 being passed as i64 across FFI boundary (Jussi Saurio)
* Fix CSV import in the shell (Jussi Saurio)
* fix record header size calculations and incorrect assumptions (Jussi Saurio)
* bindings/js: fix more tests (Mikaël Francoeur)

## 0.1.2 -- 2025-07-15

### Added

* Add `fuzz` to CI checks (Levy A.)
* Add async header accessor functionality (Zaid Humayun)
* core/vector: Euclidean distance support for vector search (KarinaMilet)
* Fix: OP_NewRowId to generate semi random rowid when largest rowid is `i64::MAX` (Krishna Vishal)
* vdbe: fix some issues with min() and max() and add ignored fuzz test (Jussi Saurio)
* github: Update to newest Nyrkiö Github action (Henrik Ingo)
* Add Nyrkiö to partners section in README (Henrik Ingo)
* Support except operator for Compound select (meteorgan)
* btree: fix incorrect comparison implementation in key_exists_in_index() (Jussi Saurio)
* bindings/java: Implement required methods to run on JetBrains Datagrip (Kim Seon Woo)
* add interactive transaction to property insert-values-select (Pere Diaz Bou)
* add pere to antithesis (Pere Diaz Bou)
* cli: Add support for `.headers` command (Pekka Enberg)
* stress: add a way to run stress with indexes enabled (Jussi Saurio)
* sim: add feature flags (indexes,mvcc) to CLI args (Jussi Saurio)
* Add multi select test in JDBC4StatementTest (Kim Seon Woo)
* bindings/dart initial implementation (Andika Tanuwijaya)
* bindings/javascript: Implement Database.open (Lucas Forato)
* add `libsql_disable_wal_checkpoint` (Pedro Muniz)
* Add a threshold for large page cache values (Krishna Vishal)
* Rollback schema support (Pere Diaz Bou)
* add a README for the rust bindings (Glauber Costa)
* add a basic readme for the typescript binding (Glauber Costa)
* add a benchmark for connection time versus number of tables (Glauber Costa)
* Add opening new connection from a sqlite compatible URI, read-only connections (Preston Thorpe)
* Simplify `PseudoCursor` implementation (Levy A.)
* bind/js: add tests for expand (Mikaël Francoeur)

### Updated

* Gopher is biologically closer to beavers than hamsters (David Shekunts)
* build: Update cargo-dist to 0.28.6 (Pekka Enberg)
* cli: Fail import command if table does not exists (Pekka Enberg)
* Assert I/O read and write sizes (Pere Diaz Bou)
* do not check rowid alias for null (Nikita Sivukhin)
* CDC functions (Nikita Sivukhin)
* Ignore double quotes around table names (Zaid Humayun)
* Efficient Record Comparison and Incremental Record Parsing (Krishna Vishal)
* `parse_schema_rows` optimizations (Levy A.)
* Simulator - only output color on terminal (Mikaël Francoeur)
* Btree: more balance docs (Jussi Saurio)
* btree: Improve balance non root docs (Jussi Saurio)
* properly set last_checksum after recovering wal (Pere Diaz Bou)
* btree/chore: remove unnecessary parameters to .cell_get() (Jussi Saurio)
* core/btree: Make cell field names consistent (Jussi Saurio)
* Enforce TCL 8.6+ in compatibility tests (Mikaël Francoeur)
* Minor refactoring of btree (meteorgan)
* bindings/python: Start transaction implicitly in execute() (Pekka Enberg)
* sqlite3_ondisk: generalize left-child-pointer reading function to both index/table btrees (Jussi Saurio)
* sim: post summary to slack (Jussi Saurio)
* stress clippy (Jussi Saurio)
* Synchronize WAL checkpointing (Pere Diaz Bou)
* Reachable assertions in Antithesis Python Test for better logging (Pedro Muniz")
* bindings/python: close connection only when reference count is one (Pere Diaz Bou)
* parser: use YYSTACKDEPTH (Lâm Hoàng Phúc)
* CI: remove duplicate fuzz run (Jussi Saurio)
* Use binary search in find_cell() (Ihor Andrianov)
* Use `str_to_f64` on float conversion (Levy A.)
* parser: replace KEYWORDS with matching (Lâm Hoàng Phúc)
* Reachable assertions in Antithesis Python Test for better logging (Pedro Muniz)
* treat ImmutableRecord as Value::Blob (Nikita Sivukhin)
* remove experimental_flag from script + remove -q flag default flag from `TestTursoShell` (Pedro Muniz)
* Change data capture (Nikita Sivukhin)
* Import subset of SQLite TCL tests (Pekka Enberg)
* bindings/java: Merge JavaScript test suites (Mikaël Francoeur)
* Import JavaScript bindings test suite from libSQL (Mikaël Francoeur)
* bindings/java: Rename to Turso (Diego Reis)
* Antithesis schema rollback tests (Pekka Enberg)
* Disable adaptive colors when output_mode is list (meteorgan)
* core/storage: Switch to turso_assert in btree.rs (Pekka Enberg)
* core: Disable `ROLLBACK` statement (Pekka Enberg")
* Rust binding improvements (Pedro Muniz")
* `from_uri` was not passing mvcc and indexes flag to database creation for memory path (Pedro Muniz)
* Turso, not Limbo, in pyproject.toml (Simon Willison)
* Rename Limbo -> Turso in python tests (Preston Thorpe)
* clarify discord situation (Glauber Costa)
* automatically select terminal colors for pretty mode (Glauber Costa)
* remote query_mode from ProgramBuilderOpts and from function arguments (Nikita Sivukhin)
* limbo -> turso (Glauber Costa)
* Rust binding improvements (Pedro Muniz)

### Fixed

* test/fuzz: fix rowid_seek_fuzz not being a proper fuzz test (Jussi Saurio)
* b-tree: fix bug in case when no matching rows was found in seek in the leaf page (Nikita Sivukhin)
* Fix clippy errors for Rust 1.88.0 (Nils Koch)
* sim: return LimboError::Busy when busy, instead of looping forever (Jussi Saurio)
* btree/balance/validation: fix divider cell insert validation (Jussi Saurio)
* btree/balance/validation: fix use-after-free in rightmost ptr validation (Jussi Saurio)
* core/translate: Fix "misuse of aggregate function" error message (Pekka Enberg)
* core/translate: Return error if SELECT needs tables and there are none (Mikaël Francoeur)
* antithesis: Fix transaction management (Pekka Enberg)
* core: Fix resolve_function() error messages (Pekka Enberg)
* VDBE: fix op_insert re-entrancy (Jussi Saurio)
* VDBE: fix op_idx_insert re-entrancy (Jussi Saurio)
* bindings/javascript: Improve error handling compatibility with `better-sqlite3` (Mikaël Francoeur)
* uv run ruff format && uv run ruff check --fix (Jussi Saurio)
* vdbe: fix compilation (Pere Diaz Bou)
* core/translate: Fix aggregate star error handling in prepare_one_sele… (Pekka Enberg)
* Fix infinite loops, rollback problems, and other bugs found by I/O fault injection (Pedro Muniz)
* core/translate: Unify no such table error messages (Pekka Enberg)
* Fix `ScalarFunc::Glob` to handle NULL and other value types (Krishna Vishal)
* fix: buffer pool is not thread safe problem (KaguraMilet)
* Fix index update when INTEGER PRIMARY KEY (rowid alias) (Adrian-Ryan Acala)
* Fix Python test import naming (Pedro Muniz)
* Fix boxed memory leaks (Ihor Andrianov)
* bindings/javascript: Formatting and typos (Mikaël Francoeur)

## 0.1.1 -- 2025-06-30

### Fixed

* JavaScript packaging (Pekka Enberg)

### Updated

* simulator: FsyncNoWait + Faulty Query (Pedro Muniz)

## 0.1.0 -- 2025-06-30

### Added

* bindings/rust: Add feature flag to enable indexes (Pekka Enberg)
* core: Add Antithesis-aware `turso_assert` (Pekka Enberg)
* Fix database header contents on initialization (Pere Diaz Bou)
* Support insersect operator for compound select (meteorgan)
* Simulator: add latency to File IO (Pedro Muniz)
* write page1 on database initialization (Pere Diaz Bou)
* `Rollback` simple support (Pere Diaz Bou)
* core/db&pager: fix locking for initializing empty database (Jussi Saurio)
* sim: add Fault::ReopenDatabase (Jussi Saurio)
* Fix database header initialization (Diego Reis)
* Add Pedro to email recipients for antithesis (Pedro Muniz)
* bindings/rust: Implement Debug for Connection (Charlie)
* Fix: add uv sync to all packages for pytest github action (Pedro Muniz)
* Implement RowData opcode (meteorgan)
* Support indent for Goto opcode when executing explain (meteorgan)

### Updated

* core: Disable `ROLLBACK` statement (Pekka Enberg)
* WAL record db_size frame on commit last frame (Pere Diaz Bou)
* Eliminate core extension dependencies (Pekka Enberg)
* Move completion extension dependency to CLI (Pekka Enberg)
* Rename `limbo` crate to `turso` (Pekka Enberg)
* Rename `limbo_sqlite3_parser` crate to `turso_sqlite3_parser` (Pekka Enberg)
* Rename `limbo_ext` crate to `turso_ext` (Pekka Enberg)
* Rename `limbo_macros` to `turso_macros` (Pekka Enberg)
* stress: Log reopen and reconnect (Pekka Enberg)
* Rename `limbo_core` crate to `turso_core` (Pekka Enberg)
* github: Run simulator on pull requests (Pekka Enberg)
* bindings/rust: Named params (Andika Tanuwijaya)
* Rename Limbo to Turso in the README and other files (Glauber Costa)
* Remove dependency on test extension pkg (Preston Thorpe)
* Cache `reserved_space` and `page_size` values at Pager init to prevent doing redundant IO (Krishna Vishal)
* cli: Rename CLI to Turso (Pekka Enberg)
* bindings/javascript: Rename package to `@tursodatabase/turso` (Pekka Enberg)
* bindings/python: Rename package to `pyturso` (Pekka Enberg)
* Rename Limbo to Turso Database (Pekka Enberg)
* Bring back TPC-H benchmarks (Pekka Enberg)
* Switch to runtime flag for enabling indexes (Pekka Enberg)
* stress: reopen db / reconnect to db every now and then (Jussi Saurio)
* Bring back some merge conflicts code (Pedro Muniz)
* simulator: integrity check per query (Pedro Muniz)
* stress: Improve progress reporting (Pekka Enberg)
* Improve extension compatibility testing (Piotr Rżysko)
* Ephemeral Table in Update (Pedro Muniz)
* Use UV more in python related scripts and actions (Pedro Muniz)
* Copy instrumented image and symbols in Dockerfile.antithesis (eric-dinh-antithesis)
* ` op_transaction` `end_read_tx` in case of `begin_write_tx` is busy (Pere Diaz Bou)
* antithesis-tests: Make test drivers robust when database is locked (Pekka Enberg)

### Fixed

* tests/integration: Fix write path test on Windows (Pekka Enberg)
* Fix deleting previous rowid when rowid is in the Set Clause (Pedro Muniz)
* Fix executing multiple statements (Pere Diaz Bou)
* Fix evaluation of ISNULL/NOTNULL in OR expressions (Piotr Rżysko)
* bindings/javascript: Fix StepResult:IO handling (Pekka Enberg)
* fix: use uv run instead of uvx for Pytest (Pedro Muniz)
* sim: when loading bug, dont panic if there are no runs (Jussi Saurio)
* sim: fix singlequote escaping and unescaping (Jussi Saurio)
* Fix btree balance and seek after overwritten cell overflows (Jussi Saurio)
* chore: fix clippy warnings (Nils Koch)
* Fix CI errors (Piotr Rżysko)
* Fix infinite aggregation loop when sorting is not required (Piotr Rżysko)
* Fix DELETE not emitting constant `WhereTerms` (Pedro Muniz)
* Fix handling of non-aggregate expressions (Piotr Rżysko)
* Fix fuzz issue #1763 by using the `log2` & `log10` functions where applicable (Luca Muscat)

## 0.0.22 -- 2025-06-19

### Added
* Implement pragma wal_checkpoint(<MODE>) (Pedro Muniz)
* Add abbreviated alias for `.quit` and `.exit` (Krishna Vishal)
* Add manual WAL sync before checkpoint in con.close, fix async bug in checkpoint (Preston Thorpe)
* Complete ALTER TABLE implementation (Levy A.)
* Add affinity-based type coercion for seek and comparison operation (Krishna Vishal)
* bindings/javascript: Add pragma() support (Anton Harniakou)
* Add sleep between write tests to avoid database locking issues (Pedro Muniz)
* bindings/java: Implement JDBC4DatabaseMetadata getTables (Kim Seon Woo)
* bindings/javascript: Add source property to Statement (Anton Harniakou)
* Support `sqlite_master` schema table name alias (Anton Harniakou)
* js-bindings/implement .name property (Anton Harniakou)
* bindings/java: Add support for Linux build (Diego Reis)
* bindings/javascript: Add database property to Statement (Anton Harniakou)
* simulator: add CREATE INDEX to interactions (Jussi Saurio)
* Add support for pragma table-valued functions (Piotr Rżysko)
* bindings/javascript: Add proper exec() method and raw() mode (Diego Reis)
* Add simulator-docker-runner for running limbo-sim in a loop on AWS (Jussi Saurio)
* simulator: add option to disable BugBase (Jussi Saurio)
* simulator: switch to tracing, run io.run_once and add update queries (Pere Diaz Bou)
* Fix: aggregate regs must be initialized as NULL at the start (Jussi Saurio)
* add stress test with 1 thread 10k iterations to ci (Pere Diaz Bou)

### Updated

* antithesis: Build Python package from sources (Pekka Enberg)
* overwrite sqlite3 in install_sqlite (Pere Diaz Bou)
* stress: Run integrity check for every iteration (Pekka Enberg)
* core: Clean up `integrity_check()` (Pekka Enberg)
* Simple integrity check on btree (Pere Diaz Bou)
* Make SQLite bindings thread-safe (Pekka Enberg)
* Switch Connection to use Arc instead of Rc (Pekka Enberg)
* Drop unused code in op_column (meteorgan)
* NOT NULL constraint (Anton Harniakou)
* Simulator Ast Generation + Simulator Unary Operator + Refactor to use `limbo_core::Value` in Simulator for massive code reuse (Pedro Muniz)
* Refactor compound select (meteorgan)
* Disable index usage in DELETE because it does not work safely (Jussi Saurio)
* Simulator: Better Shrinking (Pedro Muniz)
* Simulator integrity_check (Pedro Muniz)
* Remove leftover info trace (Jussi Saurio)
* Namespace functions that operate on `Value` (Pedro Muniz)
* Remove plan.to_sql_string() from optimize_plan() as it panics on TODOs (Jussi Saurio)
* Minor: use use_eq_ignore_ascii_case in some places (Anton Harniakou)
* Remove the FromValue trait (Anton Harniakou)
* bindings/javascript: Refactor presentation mode and enhance test suite (Diego Reis)
* Beginnings of AUTOVACUUM (Zaid Humayun)
* Reverse Parse Limbo `ast` and Plans (Pedro Muniz)
* simulator: log the interaction about to be executed with INFO (Jussi Saurio)
* stress: Use temporary file unless one explicitly specified (Jussi Saurio)
* Write database header via normal pager route (meteorgan)
* simulator: options to disable certain query types (Pedro Muniz)
* Make cursor seek reentrant (Pedro Muniz)
* Pass input string to `translate` function (Pedro Muniz)
* Small tracing enhancement (Pedro Muniz)
* Adjust write cursors for delete to avoid opening more than once. (Pedro Muniz)
* Convert u64 rowid to i64 (Pere Diaz Bou)
* Use tempfile in constraint test (Jussi Saurio)
* Remove frame id from key (Pere Diaz Bou)
* clear page cache on transaction failure (Pere Diaz Bou)

### Fixed

* Fix incorrect lossy conversion of `Value::Blob` to a utf-8 `String` (Luca Muscat)
* Fix update queries to set `n_changes` (Kim Seon Woo)
* bindings/rust: Fix Rows::next() I/O dispatcher handling (Pekka Enberg)
* cli: fix panic of queries with less than 7 chars (Nils Koch)
* Return parse error instead of corrupt error for `no such table` (Pedro Muniz)
* simulator: disable all ansi encodings for debug print log file (Pedro Muniz)
* Fix large inserts to unique indexes hanging (Jussi Saurio)
* betters instrumentation for btree related operations + cleaner debug for `RefValue` (Pedro Muniz)
* sim/aws: fix vibecoding errors in logic (Jussi Saurio)
* Fix incorrect handling of OR clauses in HAVING (Jussi Saurio)
* fix: Incorrect placeholder label in where clause translation (Pedro Muniz)
* Fix rowid to_sql_string (Pedro Muniz)
* Fix incorrect usage of indexes with non-contiguous columns (Jussi Saurio)
* BTree traversal refactor and bugfixes (Pere Diaz Bou)
* `LimboRwLock` write and read lock fixes (Pere Diaz Bou)
* fix: make keyword_token safe by validating UTF-8 input (ankit)
* Fix UPDATE straight up not working on non-unique indexes (Jussi Saurio)
* Fix:  ensure `PRAGMA cache_size` changes persist only for current session (meteorgan)
* sim/aws: fix sim timeout handling (Jussi Saurio)
* Fix WAL frame checksum mismatch (Diego Reis)
* Set maximum open simulator-created issues (Jussi Saurio)
* Fix cursors not being opened for indexes in DELETE (Jussi Saurio)
* Fix: allow DeferredSeek on more than one cursor per program (Jussi Saurio)
* Fix stress test to ignore unique constraint violation (krishna sindhur)
* Fix ProgramBuilder::cursor_ref not having unique keys (Jussi Saurio)
* Fix `serialize()` unreachable panic (Krishna Vishal)
* Btree: fix cursor record state not being updated in insert_into_page() (Jussi Saurio)

## 0.0.21 - 2025-05-28

### Added

* Add Schema reference to Resolver - needed for adhoc subquery planning (Jussi Saurio)
* Use the SetCookie opcode to implement user_version pragma (meteorgan)
* Add libsql_wal_get_frame() API (Pekka Enberg)
* Fix bug: op_vopen should replace cursor slot, not add new one (Jussi Saurio)
* bind/js: Add support for bind() method and reduce boilerplate (Diego Reis)
* Add PThorpe92 to codeowners file for extensions + go bindings (Preston Thorpe)
* Refactor: add stable internal_id property to TableReference (Jussi Saurio)
* refactor: introduce walk_expr() and walk_expr_mut() to reduce repetitive pattern matching (Jussi Saurio)
* Add some comments for values statement (meteorgan)
* fix bindings/wasm wal file creation by implementing `generate_random_number` (오웬)
* core/pragma: Add support for update user_version (Diego Reis)
* Support values statement and values in select (meteorgan)
* Initial Support for Nested Translation (Pedro Muniz)
* bindings/rust: Add pragma methods (Diego Reis)
* Add collation column to Index struct (Jussi Saurio)
* Add support for DISTINCT aggregate functions (Jussi Saurio)
* bindings/javascript: Add Statement.iterate() method (Diego Reis)
* (btree): Implement support for handling offset-based payload access with overflow support (Krishna Vishal)
* Add labeler workflow and reorganize macros (Preston Thorpe)
* Update Nyrkiö change detection to newest version (Henrik Ingo)
* perf/ci: add basic tpc-h benchmark (Jussi Saurio)
* Add `libsql_wal_frame_count()` API (Pekka Enberg)
* Restructure optimizer to support join reordering (Jussi Saurio)
* Add `rustfmt` to rust-toolchain.toml (Pekka Enberg)

### Updated

* Make WhereTerm::consumed a Cell<bool> (Jussi Saurio)
* Use lifetimes in walk_expr() to guarantee that child expr has same lifetime as parent expr (Jussi Saurio)
* Small VDBE insn tweaks (Jussi Saurio)
* Reset idx delete state after successful finish (Pere Diaz Bou)
* feature: `INSERT INTO <table> SELECT` (Pedro Muniz)
* Small cleanups to pager/wal/vdbe - mostly naming (Jussi Saurio)
* bindings/javascript: API enhancements (Diego Reis)
* github: Migrate workflows to Blacksmith runners (blacksmith-sh[bot])
* UNION (Jussi Saurio)
* xConnect for virtual tables to query core db connection (Preston Thorpe)
* Reconstruct WAL frame cache when WAL is opened (Jussi Saurio)
* set non-shared cache by default (Pere Diaz Bou)
* TPC-H with criterion and nyrkio (Pedro Muniz)
* UNION ALL (Jussi Saurio)
* Drop Table OpCodes Use Ephemeral Table As Scratch Table (Zaid Humayun)
* sqlite3-parser: Remove scanner trace-logging (Pekka Enberg)
* sqlite3: Switch to tracing logger (Pekka Enberg)
* CSV virtual table extension (Piotr Rżysko)
* remove detection of comments in the middle of query in cli (Pedro Muniz)
* btree: Remove assumption that all btrees have a rowid (Jussi Saurio)
* Output rust backtrace in python tests (Preston Thorpe)
* Optimization: lift common subexpressions from OR terms (Jussi Saurio)
* refactor: replace Operation::Subquery with Table::FromClauseSubquery (Jussi Saurio)
* Feature: Collate (Pedro Muniz)
* Update README.md (Yusheng Guo)
* Mark WHERE terms as consumed instead of deleting them (Jussi Saurio)
* Cli config 2 (Pedro Muniz)
* pager: bump default page cache size from 10 to 2000 pages (Jussi Saurio)
* long fuzz tests ci on btree changes (Pere Diaz Bou)
* Document how to run `cargo test` on Ubuntu (Zaid Humayun)
* test page_free_array (Pere Diaz Bou)
* Rename OwnedValue -> Value (Pekka Enberg)
* Improve SQLite3 C API tests (Pekka Enberg)
* github: Disable setup-node yarn cache (Pekka Enberg)
* Update Unique constraint for Primary Keys and Indexes (Pedro Muniz)

### Fixed

* Fix LIMIT handling (Jussi Saurio)
* Fix off-by-one error in max_frame after WAL load (Jussi Saurio)
* btree: fix infinite looping in backwards iteration of btree table (Jussi Saurio)
* Fix labeler labeling everything as Extensions-Other (Jussi Saurio)
* Fix bug in op_decr_jump_zero() (Jussi Saurio)
* Page cache fixes (Pere Diaz Bou)
* cli/fix: Apply default config for app (Diego Reis)
* Fix labeler (Jussi Saurio)
* Improve debug build validation speed (Pere Diaz Bou)
* optimizer: fix order by removal logic (Jussi Saurio)
* Fix updating single value (Pedro Muniz)
* Autoindex fix (Pedro Muniz)
* use temporary db in sqlite3 wal tests to fix later tests failing (Preston Thorpe)
* fix labeler correct file name extension use .yml instead of .yaml (Mohamed A. Salah)
* Fix autoindex of primary key marked as unique (Pere Diaz Bou)
* Fix: unique contraint in auto index creation (Pedro Muniz)

## 0.0.20 - 2025-05-14

### Added

* Support isnull and notnull expr (meteorgan)
* Add drop index (Anton Harniakou)
* bindings/wasm: add types property for typescript setting (오병진)
* Implement transaction support in Go adapter (Jonathan Ness)
* Initial implementation of `ALTER TABLE RENAME` (Levy A.)
* Add time.Time and bool data types support in Go adapter (Jonathan Ness)
* Add tests for INSERT with specified column-name list (Anton Harniakou)
* GROUP BY: refactor logic to support cases where no sorting is needed (Jussi Saurio)
* Add embedded library support to Go adapter (Jonathan Ness)
* Add time.Time support to Go driver parameter binding (Jonathan Ness)
* Show explanation for the NewRowid opcode (Anton Harniakou)
* Add notion of join ordering to plan (Jussi Saurio)
* Add static feature to Cargo.toml to support extensions written inside core (Pedro Muniz)
* implement Clone for Arc<Mutex> types (Pete Hayman)
* Add PRAGMA schema_version (Anton Harniakou)
* Support literal-value current_time, current_date and current_timestamp (meteorgan)
* Add state machine for op_idx_delete + DeleteState simplification (Pere Diaz Bou)
* Add the .indexes command (Anton Harniakou)
* Optimization: only initialize `Rustyline` if we are in a tty (Pedro Muniz)
* Add Antithesis Tests (eric-dinh-antithesis)
* core/types: remove duplicate serialtype implementation (Jussi Saurio)
* bindings/rust: Add Statement.columns() support (Timo Kösters)
* docs: add Rust to "Getting Started" section (Timo Kösters)
* Support xBestIndex in vtab API (Preston Thorpe)
* Feat: add support for descending indexes (Jussi Saurio)

### Updated

* github: Ensure rustmft is installed (Pekka Enberg)
* btree: Coalesce free blocks in `page_free_array()` (Mohamed Hossam)
* Count optimization (Pedro Muniz)
* bindings/java: Remove disabled annotation for UPDATE and DELETE (Kim Seon Woo)
* Refactor numeric literal (meteorgan)
* EXPLAIN should show a comment for the Insert opcode (Anton Harniakou)
* bindings/javascript: Improve compatibility with better-sqlite (Diego Reis)
* bindings/go: Upgrade ebitengine/purego to allow for use with go 1.23.9 (Preston Thorpe)
* Adjust vtab schema creation to display the underlying columns (Preston Thorpe)
* Read only mode (Pedro Muniz)
* Test that DROP TABLE also deletes the related indices (Anton Harniakou)
* reset statement before executing in rust binding (Pedro Muniz)
* Bump assorted dependencies (Preston Thorpe)
* Eliminate a superfluous read transaction when doing PRAGMA user_version (Anton Harniakou)
* update index on updated indexed columns (Pere Diaz Bou)
* Save history on exit (Piotr Rżysko)
* btree/tablebtree_move_to: micro-optimizations (Jussi Saurio)
* refactor database open_file and open (meteorgan)
* Give name to hard-coded page_size values (Anton Harniakou)
* Performance: hoist entire expressions out of hot loops if they are constant (Jussi Saurio)
* Feature: Composite Primary key constraint (Pedro Muniz)
* types: refactor serialtype again to make it faster (Jussi Saurio)
* replace vec with array in btree balancing (Lâm Hoàng Phúc)
* Pragma page size reading (Anton Harniakou)
* perf/btree: use binary search for Index seek operations (Jussi Saurio)
* expr.is_nonnull(): return true if col.primary_key || col.notnull (Jussi Saurio)
* Numeric Types Overhaul (Levy A.)
* Python script to compare vfs performance (Preston Thorpe)
* Create an automatic ephemeral index when a nested table scan would otherwise be selected (Jussi Saurio)
* Bump julian_day_converter to 0.4.5 (meteorgan)
* btree: avoid reading entire cell when only rowid needed (Jussi Saurio)
* btree: use binary search in seek/move_to for table btrees (Jussi Saurio)
* Feat: Covering indexes (Jussi Saurio)
* allow index entry delete (Pere Diaz Bou)

### Fixed

* testing/py: rename debug_print() to run_debug() (Jussi Saurio)
* Fix handling of empty strings in prepared statements (Diego Reis)
* CREATE VIRTUAL TABLE fixes (Piotr Rżysko)
* Bindings/Go: Fix symbols for FFI calls (Preston Thorpe)
* Fix bound parameters on insert statements with out of order column indexes (Preston Thorpe)
* Fix memory leak caused by unclosed virtual table cursors (Piotr Rżysko)
* Fix panic on async io due to reading locked page (Preston Thorpe)
* Fix bug: we cant remove order by terms from the head of the list (Jussi Saurio)
* Fix setting default value for primary key on UPDATE (Pere Diaz Bou)
* Fix: allow page_size=65536 (meteorgan)
* Fix `page_count`  pragma (meteorgan)
* Fix broken fuzz target due to old name (Levy A.)
* Emit `IdxDelete` instruction and some fixes on seek after deletion (Pere Diaz Bou)
* Bugfix: Explain command should display syntax errors in CLI (Anton Harniakou)
* Fix incorrect between expression documentation (Pedro Muniz)
* Fix bug: left join null flag not being cleared (Jussi Saurio)
* Fix out of bounds access on `parse_numeric_str` (Levy A.)
* Fix post balance validation (Pere Diaz Bou)

## 0.0.19 - 2025-04-16

### Added

* Add `BeginSubrtn`, `NotFound` and `Affinity` bytecodes (Diego Reis)
* Add Ansi Colors to tcl test runner (Pedro Muniz)
* support modifiers for julianday() (meteorgan)
* Implement Once and OpenAutoindex opcodes (Jussi Saurio)
* Add support for OpenEphemeral bytecode (Diego Reis)
* simulator: Add Bug Database(BugBase) (Alperen Keleş)
* feat: Add timediff data and time function (Sachin Kumar Singh)
* core/btree: Add PageContent::new() helper (Pekka Enberg)
* Add support to load log file with stress test (Pere Diaz Bou)
* Support UPDATE for virtual tables (Preston Thorpe)
* Add `.timer` command to print SQL execution statistics (Pere Diaz Bou)
* Strict table support (Ihor Andrianov)
* Support backwards index scan and seeks + utilize indexes in removing ORDER BY (Jussi Saurio)
* Add deterministic Clock (Avinash Sajjanshetty)
* Support offset clause in Update queries (Preston Thorpe)
* Support Create Index (Preston Thorpe)
* Support insert default values syntax (Preston Thorpe)
* Add support for default values in INSERT statements (Diego Reis)

### Updated

* Test: write tests for file backed db (Pedro Muniz)
* btree: move some blocks of code to more reasonable places (Jussi Saurio)
* Parse hex integers 2 (Anton Harniakou)
* More index utils (Jussi Saurio)
* Index utils (Jussi Saurio)
* Feature: VDestroy for Dropping Virtual Tables (Pedro Muniz)
* Feat balance shallower (Lâm Hoàng Phúc)
* Parse hexidecimal integers (Anton Harniakou)
* Code clean-ups (Diego Reis)
* Return null when parameter is unbound (Levy A.)
* Enhance robusteness of optimization for Binary expressions (Diego Reis)
* Check that index seek key members are not null (Jussi Saurio)
* Better diagnostics (Pedro Muniz)
* simulator: provide high level commands on top of a single runner (Alperen Keleş)
* build(deps-dev): bump vite from 6.0.7 to 6.2.6 in /bindings/wasm/test-limbo-pkg (dependabot[bot])
* btree: remove IterationState (Jussi Saurio)
* build(deps): bump pyo3 from 0.24.0 to 0.24.1 (dependabot[bot])
* Multi column indexes + index seek refactor (Jussi Saurio)
* Emit ANSI codes only when tracing is outputting to terminal (Preston Thorpe)
* B-Tree code cleanups (Pekka Enberg)
* btree index selection on rightmost pointer in `balance_non_root` (Pere Diaz Bou)
* io/linux: make syscallio the default (io_uring is really slow) (Jussi Saurio)
* Stress improvements (Pekka Enberg)
* VDBE code cleanups (Pekka Enberg)
* Memory tests to track large blob insertions (Pedro Muniz)
* Setup tracing to allow output during test runs (Preston Thorpe)
* allow insertion of multiple overflow cells (Pere Diaz Bou)
* Properly handle insertion of indexed columns (Preston Thorpe)
* VTabs: Proper handling of re-opened db files without the relevant extensions loaded (Preston Thorpe)
* Account divider cell in size while distributing cells (Pere Diaz Bou)
* Format infinite float as "Inf"/"-Inf" (jachewz)
* update sqlite download version to 2025 + remove www. (Pere Diaz Bou)
* Improve validation of btree balancing (Pere Diaz Bou)
* Aggregation without group by produces incorrect results for scalars (Ihor Andrianov)
* Dot command completion (Pedro Muniz)
* Allow reading altered tables by defaulting to null in Column insn (Preston Thorpe)
* docs(readme): update discord link (Jamie Barton)
* More VDBE cleanups (Pekka Enberg)
* Request load page on `insert_into_page` (Pere Diaz Bou)
* core/vdbe: Rename execute_insn_* to op_* (Pekka Enberg)
* Remove RWLock from Shared wal state (Pere Diaz Bou)
* VDBE with indirect function dispatch (Pere Diaz Bou)

### Fixed

* Fix truncation of error output in tests (Pedro Muniz)
* Fix Unary Negate Operation on Blobs (Pedro Muniz)
* Fix incompatibility `AND` Operation (Pedro Muniz)
* Fix: comment out incorrect assert in fuzz (Pedro Muniz)
* Fix two issues with indexes (Jussi Saurio)
* Fuzz fix some operations (Pedro Muniz)
* simulator: updates to bug base, refactors (Alperen Keleş)
* Fix overwrite cell with size less than cell size (Pere Diaz Bou)
* Fix `EXPLAIN` to be case insensitive (Pedro Muniz)
* core: Fix syscall VFS on Linux (Pekka Enberg)
* Index insert fixes (Pere Diaz Bou)
* Decrease page count on balancing fixes (Pere Diaz Bou)
* Remainder fixes (jachewz)
* Fix virtual table translation issues (Preston Thorpe)
* Fix overflow position in write_page() (Lâm Hoàng Phúc)

## 0.0.18 - 2025-04-02

### Added

* Jsonb support update (Ihor Andrianov)
* Add BTree balancing after `delete` (Krishna Vishal)
* Introduce Register struct (Pere Diaz Bou)
* Introduce immutable record (Pere Diaz Bou)
* Introduce libFuzzer (Levy A.)
* WAL frame checksum support (Daniel Boll)
* Initial JavaScript bindings with napi-rs (Pekka Enberg)
* Initial pass at `UPDATE` support (Preston Thorpe)
* Add `commit()` and placeholding insert to Python binding (Diego Reis)

### Updated

* Create plan for Update queries (Preston Thorpe)
* Validate cells inside a page after each operation (Pere Diaz Bou)
* Refactor Cli Repl Commands to use clap (Pedro Muniz)
* Allow balance_root to balance with interior pages (Pere Diaz Bou)
* Let remainder (%) accept textual arguments (Anton Harniakou)
* JSON code cleanups (Pekka Enberg)
* Allocation improvements with ImmutableRecord, OwnedRecord and read_record (Pere Diaz Bou)
* JavaScript binding improvements (Pekka Enberg)
* Kill test environment (Pekka Enberg)
* Remove public unlock method from `SpinLock` to prevent unsafe aliasing (Krishna Vishal)
* Handle limit zero case in query plan emitter (Preston Thorpe)
* Reduce MVCC cursor memory consumption (Ihor Andrianov)
* Unary `+` is a noop (Levy A.)
* JSON cache (Ihor Andrianov)
* Bump `rusqlite` to 0.34 (Pere Diaz Bou)
* core: Rename FileStorage to DatabaseFile (Pekka Enberg)
* Improve Python bindings (Diego Reis)
* Schema translation cleanups (Pekka Enberg)
* Various JSON improvements (Ihor Andrianov)
* Enable pretty mode in shell by default (Pekka Enberg)
* Improve CLI color scheme (Pekka Enberg)
* Impl Copy on some types in the pager to prevent explicit clones (Preston Thorpe)
* Syntax highlighting and hinting (Pedro Muniz)
* chore: gitignore files with an extension *.db (Anton Harniakou)
* Organize extension library and feature gate VFS (Preston Thorpe)
* fragment bench functions (Pere Diaz Bou)

### Fixed

* Remove unnecessary balance code that crashes (Pere Diaz Bou)
* Fix propagation of divider cell balancing interior page (Pere Diaz Bou)
* Fuzz test btree fix seeking. (Pere Diaz Bou)
* Fix IdxCmp insn comparisons (Jussi Saurio)
* Fixes probably all floating point math issues and floating point display issues. (Ihor Andrianov)
* Make BTreeCell/read_payload  not allocate any data + overflow fixes (Pere Diaz Bou)
* Fix `compute_shl` negate with overflow (Krishna Vishal)
* Fix a typo in README.md (Tshepang Mbambo)
* Fix platform specific FFI C pointer type casts (Preston Thorpe)
* core: Fix Destroy opcode root page handling (Pekka Enberg)
* Fix `SELECT 0.0 = 0` returning false (lgualtieri75)
* bindings/python: Fix flaky tests (Diego Reis)
* Fix io_uring WAL write corruption by ensuring buffer lifetime (Daniel Boll)

## 0.0.17 - 2025-03-19

### Added

* `BEGIN DEFERRED` support (Diego Reis)
* Experimental MVCC integration (Pekka Enberg)
* `DROP TABLE` support (Zaid Humayun)
* Initial pass on Virtual FileSystem extension module (Preston Thorpe)
* JSONB support (Ihor Andrianov)
* Shell command completion (Pedro Muniz)

### Updated

### Fixed

* Fixes and improvements to Rust bindings (yirt grek and 南宫茜)
* Transaction management fixes (Pere Diaz Bou and Diego Reis)
* JSON function fixes (Ihor Andrianov)


## 0.0.16 - 2025-03-05

### Added

* Virtual table support (Preston Thorpe)
* Improvements to Java bindings (Kim Seon Woo)
* Improvements to Rust bindings (Pekka Enberg)
* Add sqlean ipaddr extension (EmNudge)
* Add "dump" and "load" to the help menu (EmNudge)
* Initial Antithesis testing tool (Pekka Enberg)

### Fixed

* SQLite B-Tree balancing algorithm (Pere Diaz Bou)
* B-Tree improves and fixes (Pere Diaz Bou and Perston Thorpe)
* Display blobs as blob literals in `.dump` (from Mohamed Hossam)
* Fix wrong count() result if the column specified contains a NULL (lgualtieri75)
* Fix casting text to integer to match SQLite' (Preston Thorpe)
* Improve `SELECT 1` performance to be on par with SQLite (Pekka Enberg)
* Fix offset_sec normalization in extensions/time (meteorgan)
* Handle parsing URI according to SQLite specification (Preston Thorpe)
* Escape character is ignored in LIKE function (lgualtieri75)
* Fix cast_text_to_number compatibility (Pedro Muniz)
* Modify the LIKE function to work with all types (Mohamed Hossam)

## 0.0.15 - 2025-02-18

### Added

**Core:**

* Initial pass on virtual tables (Preston Thorpe)
* Import MVCC code to the source tree -- not enabled (Pekka Enberg, Piotr Sarna, Avinash Sajjanshetty)
* Implement `json_set` (Marcus Nilsson)
* Initial support for WITH clauses (common table expressions) (Jussi Saurio)
* `BEGIN IMMEDIATE` + `COMMIT` support (Pekka Enberg)
* `BEGIN EXCLUSIVE` support (Pekka Enberg)
* Add Printf Support (Zaid Humayun)
* Add support for `delete` row (Krishna Vishal)
* Implement json_quote (Pedro Muniz)
* Add read implementation of user_version pragma with ReadCookie opcode (Jonathan Webb)
* Json path refine (Ihor Andrianov)
* cli: Basic dump support (Glauber Costa)
* Support numeric column references in GROUP BY (Jussi Saurio)
* Implement the legacy_file_format pragma (Glauber Costa)
* Added IdxLE and IdxLT opcodes (Omolola Olamide)

**Java bindings:*

* Improve JDBC support with, for example, prepared statements (Kim Seon Woo)
* Rename package name `tech.turso` (Kim Seon Woo)

**Extensions:**

* Sqlean Crypto extension (Diego Reis)
* Sqlean Time extension (Pedro Muniz)
* Add support for `regexp_replace()` (lgualtieri75)

**Simulator:**

* Add NoREC testing property (Alperen Keleş)
* Add `--differential` mode against SQLite (Alperen Keleş)

### Fixed

**Core:**

* Fix 24/48 bit width serial types parsing (Nikita Sivukhin)
* Fix substr (Nikita Sivukhin)
* Fix math binary (Nikita Sivukhin)
* Fix and predicate (Nikita Sivukhin)
* Fix IdxGt, IdxGe, IdxLt, and IdxLe instructions (Jussi Saurio)
* Fix not evaling constant conditions when no tables in query (Jussi Saurio)
* Fix remainder panic on zero right-hand-side (Jussi Saurio)
* Fix invalid text columns generated by dump (Kingsley Yung)
* Fix incorrect CAST text->numeric if valid prefix is 1 char long (Jussi Saurio)
* Improve SQL statement prepare performance (Jussi Saurio)
* Fix VCC write conflict handling (Jussi Saurio)
* Fix various bugs in B-Tree handling (Nikita Sivukhin)
* Fix case and emit (Nikita Sivukhin)
* Fix coalesce (Nikita Sivukhin)
* Fix cast (Nikita Sivukhin)
* Fix string funcs (Nikita Sivukhin)
* Fix floating point truncation in JSON #877 (lgualtieri75)
* Fix bug with `SELECT` referring to a mixed-case alias (Jussi Saurio)

## 0.0.14 - 2025-02-04

### Added

**Core:**

* Improve changes() and total_changes() functions and add tests (Ben Li)
* Add support for `json_object` function (Jorge Hermo)
* Implemented json_valid function (Harin)
* Implement Not (Vrishabh)
* Initial support for wal_checkpoint pragma (Sonny)
* Implement Or and And bytecodes (Diego Reis)
* Implement strftime function (Pedro Muniz)
* implement sqlite_source_id function (Glauber Costa)
* json_patch() function implementation (Ihor Andrianov)
* json_remove() function implementation (Ihor Andrianov)
* Implement isnull / not null for filter expressions (Glauber Costa)
* Add support for offset in select queries (Ben Li)
* Support returning column names from prepared statement (Preston Thorpe)
* Implement Concat opcode (Harin)
* Table info (Glauber Costa)
* Pragma list (Glauber Costa)
* Implement Noop bytecode (Pedro Muniz)
* implement is and is not where constraints (Glauber Costa)
* Pagecount (Glauber Costa)
* Support column aliases in GROUP BY, ORDER BY and HAVING (Jussi Saurio)
* Implement json_pretty (Pedro Muniz)

**Extensions:**

* Initial pass on vector extension (Pekka Enberg)
* Enable static linking for 'built-in' extensions (Preston Thorpe)

**Go Bindings:**

* Initial support for Go database/sql driver (Preston Thorpe)
* Avoid potentially expensive operations on prepare' (Glauber Costa)

**Java Bindings:**

* Implement JDBC `ResultSet` (Kim Seon Woo)
* Implement LimboConnection `close()` (Kim Seon Woo)
* Implement close() for `LimboStatement` and `LimboResultSet` (Kim Seon Woo)
* Implement methods in `JDBC4ResultSet` (Kim Seon Woo)
* Load native library from Jar (Kim Seon Woo)
* Change logger dependency (Kim Seon Woo)
* Log driver loading error (Pekka Enberg)

**Simulator:**

* Implement `--load` and `--watch` flags (Alperen Keleş)

**Build system and CI:**

* Add Nyrkiö change point detection to 'cargo bench' workflow (Henrik Ingo)

### Fixed

* Fix `select X'1';` causes limbo to go in infinite loop (Krishna Vishal)
* Fix rowid search codegen (Nikita Sivukhin)
* Fix logical codegen (Nikita Sivukhin)
* Fix parser panic when duplicate column names are given to `CREATE TABLE` (Krishna Vishal)
* Fix panic when double quoted strings are used for column names. (Krishna Vishal)
* Fix `SELECT -9223372036854775808` result differs from SQLite (Krishna Vishal)
* Fix `SELECT ABS(-9223372036854775808)` causes limbo to panic.  (Krishna Vishal)
* Fix memory leaks, make extension types more efficient (Preston Thorpe)
* Fix table with single column PRIMARY KEY to not create extra btree (Krishna Vishal)
* Fix null cmp codegen (Nikita Sivukhin)
* Fix null expr codegen (Nikita Sivukhin)
* Fix rowid generation (Nikita Sivukhin)
* Fix shr instruction (Nikita Sivukhin)
* Fix strftime function compatibility problems (Pedro Muniz)
* Dont fsync the WAL on read queries (Jussi Saurio)

## 0.0.13 - 2025-01-19

### Added

* Initial support for native Limbo extensions (Preston Thorpe)
      
* npm packaging for node and web (Elijah Morgan)

* Add support for `rowid` keyword' (Kould)

* Add support for shift left, shift right, is and is not operators (Vrishabh)

* Add regexp extension (Vrishabh)
      
* Add counterexample minimization to simulator (Alperen Keleş)

* Initial support for binding values to prepared statements (Levy A.)

### Updated

* Java binding improvements (Kim Seon Woo)

* Reduce `liblimbo_sqlite3.a` size' (Pekka Enberg)

### Fixed

* Fix panics on invalid aggregate function arguments (Krishna Vishal)
 
* Fix null compare operations not giving null (Vrishabh)

* Run all statements from SQL argument in CLI (Vrishabh)

* Fix MustBeInt opcode semantics (Vrishabh)

* Fix recursive binary operation logic (Jussi Saurio)

* Fix SQL comment parsing in Limbo shell (Diego Reis and Clyde)

## 0.0.12 - 2025-01-14

### Added

**Core:**

* Improve JSON function support (Kacper Madej, Peter Sooley)

* Support nested parenthesized conditional expressions (Preston Thorpe)

* Add support for changes() and total_changes() functions (Lemon-Peppermint)

* Auto-create index in CREATE TABLE when necessary (Jussi Saurio)

* Add partial support for datetime() function (Preston Thorpe)

* SQL parser performance improvements (Jussi Saurio)

**Shell:**

* Show pretty parse errors in the shell (Samyak Sarnayak)

* Add CSV import support to shell (Vrishabh)

* Selectable IO backend with --io={syscall,io-uring} argument (Jorge López Tello)

**Bindings:**

* Initial version of Java bindings (Kim Seon Woo)

* Initial version of Rust bindings (Pekka Enberg)

* Add OPFS support to Wasm bindings (Elijah Morgan)

* Support uncorrelated FROM clause subqueries (Jussi Saurio)

* In-memory support to `sqlite3_open()` (Pekka Enberg)

### Fixed

* Make iterate() lazy in JavaScript bindings (Diego Reis)

* Fix integer overflow output to be same as sqlite3 (Vrishabh)

* Fix 8-bit serial type to encoding (Preston Thorpe)

* Query plan optimizer bug fixes (Jussi Saurio)

* B-Tree balancing fixes (Pere Diaz Bou)

* Fix index seek wrong on `SeekOp::LT`\`SeekOp::GT` (Kould)

* Fix arithmetic operations for text values' from Vrishabh

* Fix quote escape in SQL literals (Vrishabh)

## 0.0.11 - 2024-12-31

### Added

* Add in-memory mode to Python bindings (Jean Arhancet)

* Add json_array_length function (Peter Sooley)

* Add support for the UUID extension (Preston Thorpe)

### Changed

* Enable sqpoll by default in io_uring (Preston Thorpe)

* Simulator improvements (Alperen Keleş)

### Fixed

* Fix escaping issues with like and glob functions (Vrishabh)

* Fix `sqlite_version()` out of bound panics' (Diego Reis)

* Fix on-disk file format bugs (Jussi Saurio)

## 0.0.10 - 2024-12-18

### Added

* In-memory mode (Preston Thorpe)

* More CLI improvements (Preston Thorpe)

* Add support for replace() function (Alperen Keleş)

* Unary operator improvements (Jean Arhancet)

* Add support for unex(x, y) function (Kacper Kołodziej)

### Fixed

* Fix primary key handling when there's rowid and PK is not alias (Jussi Saurio)

## 0.0.9 - 2024-12-12

### Added

* Improve CLI (Preston Thorpe)

* Add support for iif() function (Alex Miller)

* Add support for last_insert_rowid() function (Krishna Vishal)

* Add support JOIN USING and NATURAL JOIN (Jussi Saurio)

* Add support for more scalar functions (Kacper Kołodziej)

* Add support for `HAVING` clause (Jussi Saurio)

* Add `get()` and `iterate()` to JavaScript/Wasm API (Jean Arhancet)

## 0.0.8 - 2024-11-20

### Added

* Python package build and example usage (Pekka Enberg)

## 0.0.7 - 2024-11-20

### Added

* Minor improvements to JavaScript API (Pekka Enberg)
* `CAST` support (Jussi Saurio)

### Fixed

* Fix issues found in-btree code with the DST (Pere Diaz Bou)

## 0.0.6 - 2024-11-18

### Fixed

- Fix database truncation caused by `limbo-wasm` opening file in wrong mode (Pere Diaz Bou)

## 0.0.5 - 2024-11-18

### Added

- `CREATE TABLE` support (Pere Diaz Bou)

- Add Add Database.prepare() and Statement.all() to Wasm bindings (Pekka Enberg)

- WAL improvements (Pere Diaz Bou)

- Primary key index scans and single-column secondary index scans (Jussi Saurio)

- `GROUP BY` support (Jussi Saurio)

- Overflow page support (Pere Diaz Bou)

- Improvements to Python bindings (Jean Arhancet and Lauri Virtanen)

- Improve scalar function support (Lauri Virtanen)

### Fixed

- Panic in codegen with `COUNT(*)` (Jussi Saurio)

- Fix `LIKE` to be case insensitive (RJ Barman)

## 0.0.4 - 2024-08-22

- Query planner rewrite (Jussi Saurio)

- Initial pass on Python bindings (Jean Arhancet)

- Improve scalar function support (Kim Seon Woo and Jean Arhancet)

### Added

- Partial support for `json()` function (Jean Arhancet)

## 0.0.3 - 2024-08-01

### Added

- Initial pass on the write path. Note that the write path is not transactional yet. (Pere Diaz Bou)

- More scalar functions: `unicode()` (Ethan Niser)

- Optimize point queries with integer keys (Jussi Saurio)

### Fixed

- `ORDER BY` support for nullable sorting columns and qualified identifiers (Jussi Saurio)

- Fix `.schema` command crash in the CLI ([#212](https://github.com/tursodatabase/limbo/issues/212) (Jussi Saurio)

## 0.0.2 - 2024-07-24

### Added

- Partial `LEFT JOIN` support.

- Partial `ORDER BY` support.

- Partial scalar function support.

### Fixed

- Lock database file with POSIX filesystem advisory lock when database
  is opened to prevent concurrent processes from corrupting a file.
  Please note that the locking scheme differs from SQLite, which uses
  POSIX advisory locks for every transaction. We're defaulting to
  locking on open because it's faster. (Issue #94)

### Changed

- Install to `~/.limbo/` instead of `CARGO_HOME`.

## 0.0.1 - 2024-07-17

### Added

- Partial `SELECT` statement support, including `WHERE`, `LIKE`,
  `LIMIT`, `CROSS JOIN`, and `INNER JOIN`.

- Aggregate function support.

- `EXPLAIN` statement support.

- Partial `PRAGMA` statement support, including `cache_size`.

- Asynchronous I/O support with Linux io_uring using direct I/O and
  Darwin kqueue.

- Initial pass on command line shell with following commands:
