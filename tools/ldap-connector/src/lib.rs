//! Library surface for the LDAP-to-Turso CDC demo connector, shared between the CLI
//! binary and integration tests.

pub mod cookie_store;
pub mod db;
pub mod ldap_client;

use std::path::Path;

pub use db::{CdcCounts, TursoDb};
pub use ldap_client::{run_content_sync, EntryKind, LdapObject, SyncOp, SyncOutcome, SyncStats};

/// Default location, relative to the database file, where the sync cookie is stored.
pub fn default_cookie_path(db_path: &str) -> std::path::PathBuf {
    Path::new(db_path).with_extension("cookie")
}

/// Run a single sync pass end-to-end: read the persisted cookie (if any), talk to
/// LDAP, apply the resulting ops to the Turso db under CDC, and persist the new
/// cookie. Returns the sync stats so callers (CLI or tests) can report/assert on them.
pub struct SyncRunConfig<'a> {
    pub ldap_url: &'a str,
    pub bind_dn: &'a str,
    pub bind_pw: &'a str,
    pub base_dn: &'a str,
    pub db_path: &'a str,
    pub cookie_path: std::path::PathBuf,
}

pub fn run_sync(cfg: &SyncRunConfig) -> anyhow::Result<SyncStats> {
    let cookie = cookie_store::read_cookie(&cfg.cookie_path)?;
    let is_incremental = cookie.is_some();

    let outcome = run_content_sync(cfg.ldap_url, cfg.bind_dn, cfg.bind_pw, cfg.base_dn, cookie)
        .map_err(|e| anyhow::anyhow!("LDAP sync failed: {e}"))?;

    let db = TursoDb::open(cfg.db_path)?;
    db.init_schema()?;
    db.enable_cdc()?;
    db.apply_ops(&outcome.ops)?;

    if let Some(cookie) = &outcome.cookie {
        cookie_store::write_cookie(&cfg.cookie_path, cookie)?;
    }

    eprintln!(
        "[ldap-connector] {} sync: entries_seen={} upserts={} deletes={} present_unchanged={}",
        if is_incremental {
            "incremental"
        } else {
            "full"
        },
        outcome.stats.entries_seen,
        outcome.stats.upserts,
        outcome.stats.deletes,
        outcome.stats.present_unchanged,
    );

    Ok(outcome.stats)
}
