//! Persists the RFC 4533 sync cookie to a plain file next to the Turso database.
//!
//! The cookie is opaque bytes as far as the client is concerned (server-defined,
//! typically a `rid=...,csn=...` string for OpenLDAP's syncprov). We store it raw on
//! disk so a freshly started connector process picks up exactly where the previous
//! invocation left off -- this is what makes the second run of the connector an
//! *incremental* resync rather than a second full scan.

use std::fs;
use std::io::ErrorKind;
use std::path::Path;

pub fn read_cookie(path: &Path) -> std::io::Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

pub fn write_cookie(path: &Path, cookie: &[u8]) -> std::io::Result<()> {
    fs::write(path, cookie)
}
