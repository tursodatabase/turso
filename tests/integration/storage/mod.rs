mod autovacuum;
#[cfg(feature = "checksum")]
mod checksum;
mod header_version;
// The phantom-page reproducer reads a WAL slot's raw bytes as a b-tree page; the
// checksum feature intercepts that with a checksum lament instead of the true
// "Invalid page type" corruption, so — like `short_read` — it runs only without
// checksums (see the dedicated no-checksum CI job in .github/workflows/rust.yml).
#[cfg(not(feature = "checksum"))]
mod phantom_page;
#[cfg(not(feature = "checksum"))]
mod short_read;
