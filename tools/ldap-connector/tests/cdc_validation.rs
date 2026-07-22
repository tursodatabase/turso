//! End-to-end CDC validation against a live OpenLDAP server.
//!
//! These tests require a running OpenLDAP container with the `syncprov` overlay
//! enabled and the demo dataset loaded (see `tools/ldap-connector/README.md` for the
//! exact setup). They are marked `#[ignore]` because they depend on that external
//! service, matching the convention used by `tools/dbhash`'s compatibility tests.
//!
//! Run with:
//! ```text
//! cargo test -p turso-ldap-connector -- --ignored
//! ```
//!
//! `test_full_sync_cdc_completeness` validates against whatever is currently in the
//! directory (queried live, not hardcoded), so it doesn't assume a specific dataset
//! size. `test_incremental_sync_captures_deltas_only` creates and deletes its own
//! throwaway entries rather than mutating the main demo dataset, so it's safe to
//! re-run without destroying the 1000-user/500-group seed data.

use std::collections::HashSet;

use ldap3::{LdapConn, Scope};
use tempfile::tempdir;

use turso_ldap_connector::ldap_client::{delete_entries, run_content_sync};
use turso_ldap_connector::TursoDb;

const LDAP_URL: &str = "ldap://localhost:3389";
const BIND_DN: &str = "cn=admin,dc=example,dc=org";
const BIND_PW: &str = "admin_password";
const BASE_DN: &str = "dc=example,dc=org";

/// Count current inetOrgPerson/groupOfNames entries directly via a plain LDAP search,
/// independent of our sync code, as the ground truth for "how many rows should have
/// been imported".
fn live_directory_counts() -> (usize, usize) {
    let mut ldap = LdapConn::new(LDAP_URL).expect("connect to LDAP");
    ldap.simple_bind(BIND_DN, BIND_PW)
        .expect("bind")
        .success()
        .expect("bind success");

    let (results, _) = ldap
        .search(
            BASE_DN,
            Scope::Subtree,
            "(objectClass=inetOrgPerson)",
            vec!["dn"],
        )
        .expect("search users")
        .success()
        .expect("search users success");
    let users = results.len();

    let (results, _) = ldap
        .search(
            BASE_DN,
            Scope::Subtree,
            "(objectClass=groupOfNames)",
            vec!["dn"],
        )
        .expect("search groups")
        .success()
        .expect("search groups success");
    let groups = results.len();

    ldap.unbind().ok();
    (users, groups)
}

fn add_throwaway_user(ldap: &mut LdapConn, uid: &str) -> String {
    let dn = format!("uid={uid},ou=people,{BASE_DN}");
    let mut oc = HashSet::new();
    oc.insert("inetOrgPerson");
    oc.insert("organizationalPerson");
    oc.insert("person");
    oc.insert("top");
    let mut cn = HashSet::new();
    cn.insert("Throwaway Test User");
    let mut sn = HashSet::new();
    sn.insert("TestUser");
    let mut l = HashSet::new();
    l.insert("Testville");
    ldap.add(
        &dn,
        vec![("objectClass", oc), ("cn", cn), ("sn", sn), ("l", l)],
    )
    .expect("add throwaway user")
    .success()
    .expect("add throwaway user success");
    dn
}

fn add_throwaway_group(ldap: &mut LdapConn, cn_val: &str, member_dn: &str) -> String {
    let dn = format!("cn={cn_val},ou=groups,{BASE_DN}");
    let mut oc = HashSet::new();
    oc.insert("groupOfNames");
    oc.insert("extensibleObject");
    oc.insert("top");
    let mut cn = HashSet::new();
    cn.insert(cn_val);
    let mut member = HashSet::new();
    member.insert(member_dn);
    ldap.add(
        &dn,
        vec![("objectClass", oc), ("cn", cn), ("member", member)],
    )
    .expect("add throwaway group")
    .success()
    .expect("add throwaway group success");
    dn
}

#[test]
#[ignore]
fn test_full_sync_cdc_completeness() {
    let (expected_users, expected_groups) = live_directory_counts();
    assert!(
        expected_users > 0 && expected_groups > 0,
        "expected seeded directory data; run scripts/gen_seed_ldif.py + ldapadd first"
    );

    let dir = tempdir().unwrap();
    let db_path = dir.path().join("full_sync.db");
    let db_path_str = db_path.to_str().unwrap();

    let outcome = run_content_sync(LDAP_URL, BIND_DN, BIND_PW, BASE_DN, None).expect("full sync");
    assert_eq!(
        outcome.stats.deletes, 0,
        "a full baseline sync must not produce deletes"
    );
    assert_eq!(
        outcome.stats.upserts,
        expected_users + expected_groups,
        "full sync must upsert exactly one row per directory entry"
    );
    assert!(
        outcome.cookie.is_some(),
        "server must hand back a sync cookie"
    );

    let db = TursoDb::open(db_path_str).unwrap();
    db.init_schema().unwrap();
    db.enable_cdc().unwrap();
    db.apply_ops(&outcome.ops).unwrap();

    let (users, groups) = db.table_counts().unwrap();
    assert_eq!(users as usize, expected_users);
    assert_eq!(groups as usize, expected_groups);

    // Exactly one INSERT per imported row, nothing else, in turso_cdc.
    let counts = db.cdc_counts_since(0).unwrap();
    assert_eq!(counts.insert, (expected_users + expected_groups) as i64);
    assert_eq!(counts.update, 0);
    assert_eq!(counts.delete, 0);
}

#[test]
#[ignore]
fn test_incremental_sync_captures_deltas_only() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("incremental_sync.db");
    let db_path_str = db_path.to_str().unwrap();
    let cookie_path = dir.path().join("incremental_sync.cookie");

    // 1. Full baseline sync (captures whatever is currently in the directory,
    //    including the throwaway entries we're about to add -- so add them first).
    let mut ldap = LdapConn::new(LDAP_URL).unwrap();
    ldap.simple_bind(BIND_DN, BIND_PW)
        .unwrap()
        .success()
        .unwrap();

    let mut throwaway_user_dns = Vec::new();
    for i in 0..15 {
        throwaway_user_dns.push(add_throwaway_user(&mut ldap, &format!("zztest{i:04}")));
    }
    let throwaway_group_dns = vec![
        add_throwaway_group(&mut ldap, "zztest-group-0001", &throwaway_user_dns[0]),
        add_throwaway_group(&mut ldap, "zztest-group-0002", &throwaway_user_dns[1]),
    ];
    ldap.unbind().ok();

    let full_outcome =
        run_content_sync(LDAP_URL, BIND_DN, BIND_PW, BASE_DN, None).expect("full sync");
    let db = TursoDb::open(db_path_str).unwrap();
    db.init_schema().unwrap();
    db.enable_cdc().unwrap();
    db.apply_ops(&full_outcome.ops).unwrap();
    let baseline_change_id = db.max_cdc_change_id().unwrap();
    let cookie = full_outcome.cookie.expect("cookie after full sync");
    std::fs::write(&cookie_path, &cookie).unwrap();

    let (users_before, groups_before) = db.table_counts().unwrap();

    // 2. Simulate drift: delete the throwaway entries directly against LDAP.
    let mut deleted_dns = throwaway_group_dns.clone();
    deleted_dns.extend(throwaway_user_dns.iter().cloned());
    delete_entries(LDAP_URL, BIND_DN, BIND_PW, &deleted_dns).expect("delete throwaway entries");

    // 3. Incremental resync using the persisted cookie.
    let persisted_cookie = std::fs::read(&cookie_path).unwrap();
    let incr_outcome =
        run_content_sync(LDAP_URL, BIND_DN, BIND_PW, BASE_DN, Some(persisted_cookie))
            .expect("incremental sync");

    // Proof this was a targeted delta query, not a full-directory re-scan: this server
    // reports deletions via a single syncIdSet intermediate message (a batch of
    // entryUUIDs), not per-entry SearchResultEntry results, so a pure-deletion
    // incremental pass sees zero ordinary entries at all -- compare against a full
    // sync, which would see 1500+.
    assert_eq!(
        incr_outcome.stats.entries_seen, 0,
        "a deletion-only incremental sync must not receive any SearchResultEntry rows"
    );
    assert_eq!(incr_outcome.stats.upserts, 0);
    assert_eq!(incr_outcome.stats.deletes, deleted_dns.len());

    db.apply_ops(&incr_outcome.ops).unwrap();

    // turso_cdc's second batch must show only deletes, no inserts/updates, and must
    // not have touched the untouched rows (row count drops by exactly the delete count).
    let counts = db.cdc_counts_since(baseline_change_id).unwrap();
    assert_eq!(counts.delete, deleted_dns.len() as i64);
    assert_eq!(counts.insert, 0);
    assert_eq!(counts.update, 0);

    let (users_after, groups_after) = db.table_counts().unwrap();
    assert_eq!(users_after, users_before - throwaway_user_dns.len() as i64);
    assert_eq!(
        groups_after,
        groups_before - throwaway_group_dns.len() as i64
    );
}
