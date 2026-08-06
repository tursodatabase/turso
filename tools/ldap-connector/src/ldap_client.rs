//! LDAP-side sync logic: RFC 4533 (LDAP Content Synchronization / "syncrepl") search.
//!
//! A full scan and an incremental resync are the *same* operation from the client's
//! perspective: both are a Sync Request-controlled search, differing only in whether a
//! persisted cookie is attached. With no cookie, the server treats every matching entry
//! as freshly "Add"ed (a full baseline). With a cookie, the server (via OpenLDAP's
//! `syncprov` overlay) returns only the entries that changed since that cookie was
//! issued, tagged with the LDAP sync state that says whether it's an upsert or a
//! delete, and hands back a new cookie to persist for the next round. This is what
//! makes the incremental resync a targeted query instead of a full-directory re-scan.
//!
//! Deletions do *not* come back as an ordinary search entry with attributes (the entry
//! is gone). Empirically, this OpenLDAP 2.4 + `syncprov` build reports deletions
//! exclusively via the `syncIdSet` intermediate ("SyncInfo") message -- a batch of
//! `entryUUID`s to remove -- rather than per-entry `EntryState::Delete` results (both
//! are valid per RFC 4533; which one a server uses is implementation-defined). Because
//! a deleted entry's UUID is all we get, and no attributes, resolving it to a row
//! requires the receiver to already know the UUID -> DN mapping from a prior sync. We
//! therefore persist `entryUUID` (taken from each entry's `SyncState` control, not a
//! regular attribute) alongside every row, and resolve deletes against that at apply
//! time (see `db::apply_ops`).

use std::collections::HashMap;

use ldap3::controls::{
    Control, ControlType, EntryState, RefreshMode, SyncDone, SyncInfo, SyncRequest, SyncState,
};
use ldap3::result::Result as LdapLibResult;
use ldap3::{LdapConn, Scope, SearchEntry};

/// objectClass used to identify user entries.
pub const USER_OBJECT_CLASS: &str = "inetOrgPerson";
/// objectClass used to identify group entries.
pub const GROUP_OBJECT_CLASS: &str = "groupOfNames";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    User,
    Group,
}

#[derive(Debug, Clone)]
pub struct LdapObject {
    pub dn: String,
    pub kind: EntryKind,
    /// Raw `entryUUID` bytes from this entry's SyncState control.
    pub uuid: Vec<u8>,
    pub attrs: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone)]
pub enum SyncOp {
    Upsert(LdapObject),
    /// A per-entry Delete state that came with a DN attached directly.
    Delete {
        dn: String,
        kind: EntryKind,
    },
    /// A `syncIdSet` batch delete: only the entryUUID is known, the DN must be
    /// resolved against whatever we previously stored for that UUID.
    DeleteByUuid(Vec<u8>),
}

/// Counters describing what a single sync pass observed, used both for CLI reporting
/// and as the evidence a test asserts on (e.g. "the incremental pass only saw ~15
/// entries, not 1500").
#[derive(Debug, Default, Clone, Copy)]
pub struct SyncStats {
    /// Number of ordinary SearchResultEntry messages received (tag 4) -- the direct
    /// measure of how "wide" this sync's LDAP-side response was.
    pub entries_seen: usize,
    pub upserts: usize,
    pub deletes: usize,
    pub present_unchanged: usize,
}

pub struct SyncOutcome {
    pub ops: Vec<SyncOp>,
    pub cookie: Option<Vec<u8>>,
    pub stats: SyncStats,
}

/// Classify an entry by its DN's parent OU rather than by its attributes, because a
/// Delete-state sync entry carries no attributes at all (the entry is gone) -- the DN
/// is the only thing we get back.
fn classify_dn(dn: &str) -> Option<EntryKind> {
    if dn.contains(",ou=people,") {
        Some(EntryKind::User)
    } else if dn.contains(",ou=groups,") {
        Some(EntryKind::Group)
    } else {
        None
    }
}

/// Run one RFC 4533 sync pass against `base_dn`, scoped to user and group entries.
///
/// `cookie = None` performs a full baseline sync (every matching entry comes back as an
/// upsert). `cookie = Some(..)` performs an incremental sync: only entries that changed
/// since that cookie was issued are returned.
pub fn run_content_sync(
    ldap_url: &str,
    bind_dn: &str,
    bind_pw: &str,
    base_dn: &str,
    cookie: Option<Vec<u8>>,
) -> LdapLibResult<SyncOutcome> {
    let mut ldap = LdapConn::new(ldap_url)?;
    ldap.simple_bind(bind_dn, bind_pw)?.success()?;

    let sync_request = SyncRequest {
        mode: RefreshMode::RefreshOnly,
        cookie: cookie.clone(),
        reload_hint: false,
    };
    ldap.with_controls(vec![sync_request.into()]);

    let filter = format!("(|(objectClass={USER_OBJECT_CLASS})(objectClass={GROUP_OBJECT_CLASS}))");
    let mut stream = ldap.streaming_search(base_dn, Scope::Subtree, &filter, vec!["*"])?;

    let mut ops = Vec::new();
    let mut stats = SyncStats::default();
    let mut new_cookie = cookie;

    while let Some(entry) = stream.next()? {
        match entry.0.id {
            // Application-tag 25: intermediateResponse. syncprov uses this to push a
            // fresh cookie mid-stream, and -- per this server's behavior -- to report
            // batches of deleted entryUUIDs via the syncIdSet variant.
            25 => match ldap3::controls::parse_syncinfo(entry) {
                SyncInfo::NewCookie(c) => new_cookie = Some(c),
                SyncInfo::SyncIdSet {
                    cookie,
                    refresh_deletes: true,
                    sync_uuids,
                } => {
                    stats.deletes += sync_uuids.len();
                    ops.extend(sync_uuids.into_iter().map(SyncOp::DeleteByUuid));
                    if let Some(c) = cookie {
                        new_cookie = Some(c);
                    }
                }
                SyncInfo::SyncIdSet {
                    cookie,
                    refresh_deletes: false,
                    ..
                } => {
                    // Present-list ("this is the complete set of surviving UUIDs")
                    // reconciliation is not implemented -- see README limitations.
                    // We still take the cookie so the next sync isn't stuck.
                    if let Some(c) = cookie {
                        new_cookie = Some(c);
                    }
                }
                SyncInfo::RefreshDelete { cookie, .. }
                | SyncInfo::RefreshPresent { cookie, .. } => {
                    if let Some(c) = cookie {
                        new_cookie = Some(c);
                    }
                }
            },
            // Application-tag 4: an ordinary SearchResultEntry.
            4 => {
                stats.entries_seen += 1;
                let sync_state = entry.1.iter().find_map(|c| match c {
                    Control(Some(ControlType::SyncState), raw) => Some(raw.parse::<SyncState>()),
                    _ => None,
                });
                let new_cookie_from_state = sync_state.as_ref().and_then(|s| s.cookie.clone());
                let uuid = sync_state.as_ref().map(|s| s.entry_uuid.clone());
                let se = SearchEntry::construct(entry);
                let Some(kind) = classify_dn(&se.dn) else {
                    continue; // OU containers etc. -- not part of our schema
                };
                if let Some(c) = new_cookie_from_state {
                    new_cookie = Some(c);
                }
                match sync_state.map(|s| s.state) {
                    Some(EntryState::Delete) => {
                        stats.deletes += 1;
                        ops.push(SyncOp::Delete { dn: se.dn, kind });
                    }
                    Some(EntryState::Present) => {
                        stats.present_unchanged += 1;
                    }
                    // Add, Modify, or no sync state control at all (plain search) => upsert.
                    _ => {
                        stats.upserts += 1;
                        ops.push(SyncOp::Upsert(LdapObject {
                            dn: se.dn,
                            kind,
                            uuid: uuid.unwrap_or_default(),
                            attrs: se.attrs,
                        }));
                    }
                }
            }
            _ => {} // referrals etc, ignore
        }
    }

    let res = stream.result().success()?;
    for ctrl in &res.ctrls {
        if let Control(Some(ControlType::SyncDone), raw) = ctrl {
            let done: SyncDone = raw.parse();
            if let Some(c) = done.cookie {
                new_cookie = Some(c);
            }
        }
    }

    ldap.unbind()?;

    Ok(SyncOutcome {
        ops,
        cookie: new_cookie,
        stats,
    })
}

/// Delete a set of DNs directly against the LDAP server -- used to simulate directory
/// drift (step 5 of the demo) and by tests that need to create-then-remove entries.
pub fn delete_entries(
    ldap_url: &str,
    bind_dn: &str,
    bind_pw: &str,
    dns: &[String],
) -> LdapLibResult<()> {
    let mut ldap = LdapConn::new(ldap_url)?;
    ldap.simple_bind(bind_dn, bind_pw)?.success()?;
    for dn in dns {
        ldap.delete(dn)?.success()?;
    }
    ldap.unbind()?;
    Ok(())
}
