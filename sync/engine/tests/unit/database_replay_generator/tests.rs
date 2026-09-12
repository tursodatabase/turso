use super::*;
use crate::database_tape::DatabaseReplaySessionOpts;
use std::sync::Arc;
use tempfile::NamedTempFile;
use turso_core::SqliteDialect;

enum QueryKind {
    Delete { use_rowid: bool },
    Update(Vec<bool>),
    Upsert(usize),
}

fn replay_info_for(
    ddl: &[&str],
    table: &str,
    kind: QueryKind,
    use_implicit_rowid: bool,
) -> Result<ReplayInfo> {
    let temp_file = NamedTempFile::new().unwrap();
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let db = turso_core::Database::open_file(
        io.clone(),
        temp_file.path().to_str().unwrap(),
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    for stmt in ddl {
        conn.execute(stmt).unwrap();
    }
    let generator =
        DatabaseReplayGenerator::new(conn, DatabaseReplaySessionOpts { use_implicit_rowid });
    let table = table.to_string();
    let mut gen = genawaiter::sync::Gen::new(|coro| async move {
        let coro: Coro<()> = coro.into();
        match kind {
            QueryKind::Delete { use_rowid } => {
                generator.delete_query(&coro, &table, use_rowid).await
            }
            QueryKind::Update(columns) => generator.update_query(&coro, &table, &columns).await,
            QueryKind::Upsert(columns) => generator.upsert_query(&coro, &table, columns).await,
        }
    });
    loop {
        match gen.resume_with(Ok(())) {
            genawaiter::GeneratorState::Yielded(..) => io.step().unwrap(),
            genawaiter::GeneratorState::Complete(result) => break result,
        }
    }
}

#[test]
fn test_identity_predicates_are_null_safe() {
    // Rowid tables allow NULL in PRIMARY KEY columns, and `NULL = NULL` is
    // not true: `=` predicates make replay of such a row a silent no-op.
    let ddl = &["CREATE TABLE t (a, b, c, PRIMARY KEY (a, c))"];
    let delete = replay_info_for(ddl, "t", QueryKind::Delete { use_rowid: false }, false).unwrap();
    assert_eq!(
        delete.query,
        r#"DELETE FROM "t" WHERE "a" IS ? AND "c" IS ?"#
    );

    let update =
        replay_info_for(ddl, "t", QueryKind::Update(vec![false, true, false]), false).unwrap();
    assert_eq!(
        update.query,
        r#"UPDATE "t" SET "b" = ? WHERE "a" IS ? AND "c" IS ?"#
    );
}

#[test]
fn test_identity_predicates_follow_primary_key_ordinal_order() {
    // `PRIMARY KEY (c, a)`: predicates and binds are both ordered by PK
    // ordinal, not by column declaration order.
    let ddl = &["CREATE TABLE t (a, b, c, PRIMARY KEY (c, a))"];
    let update =
        replay_info_for(ddl, "t", QueryKind::Update(vec![false, true, false]), false).unwrap();
    assert_eq!(
        update.query,
        r#"UPDATE "t" SET "b" = ? WHERE "c" IS ? AND "a" IS ?"#
    );
    assert_eq!(update.pk_column_indices, Some(vec![2, 0]));
}

#[test]
fn test_rowid_replay_avoids_user_columns_shadowing_rowid_aliases() {
    // A user column named `rowid` hijacks `WHERE rowid = ?`: the predicate
    // resolves to the user column, so the integer row-id bind matches
    // nothing and the replayed delete is a silent no-op.
    let shadow_rowid = &["CREATE TABLE t (rowid, v)"];
    let delete = replay_info_for(
        shadow_rowid,
        "t",
        QueryKind::Delete { use_rowid: true },
        true,
    )
    .unwrap();
    assert_eq!(delete.query, r#"DELETE FROM "t" WHERE "_rowid_" = ?"#);
    let update = replay_info_for(
        shadow_rowid,
        "t",
        QueryKind::Update(vec![false, true]),
        true,
    )
    .unwrap();
    assert_eq!(
        update.query,
        r#"UPDATE "t" SET "v" = ? WHERE "_rowid_" = ?"#
    );
    let upsert = replay_info_for(shadow_rowid, "t", QueryKind::Upsert(2), true).unwrap();
    assert_eq!(
        upsert.query,
        r#"INSERT OR REPLACE INTO "t"("rowid", "v", "_rowid_") VALUES (?,?,?)"#
    );

    let shadow_two = &["CREATE TABLE t (rowid, _rowid_, v)"];
    let delete =
        replay_info_for(shadow_two, "t", QueryKind::Delete { use_rowid: true }, true).unwrap();
    assert_eq!(delete.query, r#"DELETE FROM "t" WHERE "oid" = ?"#);

    // Shadowing all three aliases is legal SQLite, but then the implicit
    // rowid is unreachable from SQL: refuse instead of writing the wrong row.
    let shadow_all = &["CREATE TABLE t (rowid, _rowid_, oid, v)"];
    let err =
        replay_info_for(shadow_all, "t", QueryKind::Delete { use_rowid: true }, true).unwrap_err();
    assert!(
        err.to_string()
            .contains("shadowing every implicit rowid alias"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_update_identity_values_come_from_before_image() {
    // A key-changing UPDATE must be identified by the key it had *before*
    // the change: binding the after image matches zero rows, silently.
    let ddl = &["CREATE TABLE t (a, b, c, PRIMARY KEY (c, a))"];
    let info =
        replay_info_for(ddl, "t", QueryKind::Update(vec![false, false, true]), false).unwrap();
    assert_eq!(
        info.query,
        r#"UPDATE "t" SET "c" = ? WHERE "c" IS ? AND "a" IS ?"#
    );

    let temp_file = NamedTempFile::new().unwrap();
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let db = turso_core::Database::open_file(
        io,
        temp_file.path().to_str().unwrap(),
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let generator = DatabaseReplayGenerator::new(
        db.connect().unwrap(),
        DatabaseReplaySessionOpts {
            use_implicit_rowid: false,
        },
    );

    let text = |value: &str| turso_core::Value::from_text(value.to_string());
    // changes mask: only column `c` was updated; second half holds new values.
    let updates = turso_core::alloc::vec![
        turso_core::Value::from_i64(0),
        turso_core::Value::from_i64(0),
        turso_core::Value::from_i64(1),
        text("a1"),
        text("b1"),
        text("c9"),
    ];
    let before = turso_core::alloc::vec![text("a1"), text("b1"), text("c1")];
    let after = turso_core::alloc::vec![text("a1"), text("b1"), text("c9")];

    let values = generator.replay_values(
        &info,
        DatabaseChangeType::Update,
        0,
        after,
        Some(updates),
        Some(before),
    );

    // SET c = 'c9', then identity in PK ordinal order: c (before), a.
    assert_eq!(values.to_vec(), vec![text("c9"), text("c1"), text("a1")]);
}

#[test]
fn test_upsert_predelete_only_for_null_key_components() {
    let ddl = &["CREATE TABLE t (a, b, c, PRIMARY KEY (a, c))"];
    let info = replay_info_for(ddl, "t", QueryKind::Upsert(3), false).unwrap();
    let temp_file = NamedTempFile::new().unwrap();
    let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let db = turso_core::Database::open_file(
        io,
        temp_file.path().to_str().unwrap(),
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let generator = DatabaseReplayGenerator::new(
        db.connect().unwrap(),
        DatabaseReplaySessionOpts {
            use_implicit_rowid: false,
        },
    );
    let text = |value: &str| turso_core::Value::from_text(value.to_string());

    assert!(
        !generator.upsert_needs_null_safe_predelete(&info, &[text("a1"), text("b1"), text("c1")])
    );
    // NULL in a key column: ON CONFLICT cannot fire, so the row must be
    // pre-deleted or the upsert duplicates it.
    assert!(generator.upsert_needs_null_safe_predelete(
        &info,
        &[text("a1"), text("b1"), turso_core::Value::Null]
    ));
    // NULL in a non-key column is irrelevant.
    assert!(!generator.upsert_needs_null_safe_predelete(
        &info,
        &[text("a1"), turso_core::Value::Null, text("c1")]
    ));

    // A rowid-alias primary key is the implicit rowid: never NULL.
    let rowid_alias = replay_info_for(
        &["CREATE TABLE r (id INTEGER PRIMARY KEY, v)"],
        "r",
        QueryKind::Upsert(2),
        false,
    )
    .unwrap();
    assert!(!generator
        .upsert_needs_null_safe_predelete(&rowid_alias, &[turso_core::Value::Null, text("v")]));
}
