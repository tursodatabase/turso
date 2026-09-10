# Vendored Tantivy

`tantivy/` contains the complete source tree from
[Tantivy 0.26.2](https://github.com/quickwit-oss/tantivy/tree/0.26.2), commit
[`72d1ef9a6468aa68bbc69dcc80cdf60aaf64364d`](https://github.com/quickwit-oss/tantivy/commit/72d1ef9a6468aa68bbc69dcc80cdf60aaf64364d).
Its MIT license is in `tantivy/LICENSE`.

The source was imported from the GitHub archive for that commit, including all
workspace crates, tests, fixtures, benchmarks, and `.github` files. Third-party
Cargo dependencies are still downloaded from crates.io; this is not an offline
dependency bundle. No Turso crate depends on Tantivy yet.

Tantivy remains a separate Cargo workspace, excluded from Turso's workspace.
Run its commands from `vendor/tantivy` with an explicit toolchain so they do not
use Turso's pinned Rust version:

```sh
cargo +stable test --workspace --no-default-features --features mmap,stopwords,lz4-compression,zstd-compression,failpoints,stemmer
cargo +nightly fmt --all -- --check
```

Testing with `--all-features` requires nightly because it enables `unstable`.

The original GitHub workflows are preserved under `tantivy/.github/workflows`.
GitHub only executes workflows at the repository root, so adapted copies live at
`.github/workflows/tantivy-{test,coverage,long-running}.yml`. They retain the
upstream feature matrix, doctests, benchmark compilation, coverage command, and
long-running tests. The copies use Tantivy's working directory and cache paths,
explicit toolchains, and path-filtered triggers. Clippy runs directly instead of
using the upstream check-reporting action. Coverage uploads use the `tantivy`
flag, the vendored report path, and Codecov action v5 instead of the obsolete v3.
Each workflow can also be started manually.

The only source changes are formatting with nightly 2026-09-09 in
`src/aggregation/bucket/term_agg.rs`, `src/query/seek_danger_tests.rs`, and
`src/store/reader.rs`, required for the upstream formatting check to pass.
All other upstream files, including the original workflows, are unchanged.

When updating, replace `tantivy/` from the chosen upstream commit archive,
update this provenance and the version in `NOTICE.md`, and compare all three
upstream workflows with their executable copies. Check that repository ignore
rules do not omit upstream files; `.gitignore` currently makes exceptions for
Tantivy's two `.txt` files.
