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
