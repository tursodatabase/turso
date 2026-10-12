mod autovacuum;
#[cfg(feature = "checksum")]
mod checksum;
mod failed_read;
mod header_version;
#[cfg(not(feature = "checksum"))]
mod short_read;
