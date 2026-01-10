#[cfg(feature = "checksum")]
mod checksum;
mod header_version;
#[cfg(not(feature = "checksum"))]
mod short_read;
