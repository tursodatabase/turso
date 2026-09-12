//! FTS durability contract across a crash-shaped reopen.
//!
//! locks in the post-mortem finding of the multiprocess FTS hunt (seed
//! 7399741717491843615): the on-disk segment registry survives kill/respawn
//! churn, so every generation — checkpointed, WAL-only, and rewritten — must
//! stay visible through `fts_match` after recovery, in exact agreement with
//! the base table. The hazards covered here are the ones that make FTS and
//! the base table diverge:
//!
//! * a row REPLACEd to a body without the token must stop matching (retired
//!   posting: no ghost hits),
//! * a row DELETEd must stop matching (tombstones honored after reload),
//! * an INSERTed row must start matching (published segment reloaded),
//! * the v2 control row must survive so later writes can still append (a
//!   registry scan refuses stores without it).

use crate::common::{limbo_exec_rows, TempDatabase};

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn fts_base_table_agreement_after_crash_shaped_reopen() {
    let builder =
        TempDatabase::builder().with_opts(turso_core::DatabaseOpts::new().with_index_method(true));
    let tmp_db = builder.build();
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();

    // Generation 1: checkpointed into the main file.
    for (id, body) in [
        (1, "charlie alpha"),
        (2, "bravo echo"),
        (3, "charlie foxtrot delta"),
    ] {
        conn.execute(format!("INSERT INTO docs VALUES ({id}, '{body}')"))
            .unwrap();
    }
    conn.execute("PRAGMA wal_checkpoint(FULL)").unwrap();

    // Generation 2: WAL-only (crash-shaped: the handle is dropped without a
    // final checkpoint, so recovery must reload this generation from the WAL).
    for (id, body) in [(4, "delta echo charlie"), (5, "hotel golf")] {
        conn.execute(format!("INSERT INTO docs VALUES ({id}, '{body}')"))
            .unwrap();
    }
    // REPLACE a token row to a body without the token: the retired posting
    // must be tombstoned, not left behind as a ghost hit.
    conn.execute("INSERT OR REPLACE INTO docs VALUES (1, 'alpha golf golf')")
        .unwrap();
    // DELETE a token row: the tombstone must survive the reopen.
    conn.execute("DELETE FROM docs WHERE id = 3").unwrap();

    let path = tmp_db.path.clone();
    let opts = tmp_db.db_opts;
    let flags = tmp_db.db_flags;
    drop(conn);
    drop(tmp_db);

    let reopened = TempDatabase::builder()
        .with_db_path(&path)
        .with_opts(opts)
        .with_flags(flags)
        .build();
    let conn = reopened.connect_limbo();

    let ids_of = |query: String| limbo_exec_rows(&conn, &query).remove(0).remove(0);
    let scan = |token: &str| {
        ids_of(format!(
            "SELECT group_concat(id) FROM (SELECT id FROM docs WHERE \
             (' '||body||' ') LIKE '% {token} %' ORDER BY id)"
        ))
    };
    let fts = |token: &str| {
        ids_of(format!(
            "SELECT group_concat(id) FROM (SELECT id FROM docs WHERE \
             fts_match(body, '{token}') ORDER BY id)"
        ))
    };
    let text = |v: rusqlite::types::Value| match v {
        rusqlite::types::Value::Text(s) => s,
        other => panic!("expected text, got {other:?}"),
    };

    for token in ["charlie", "alpha", "golf", "delta", "hotel"] {
        assert_eq!(
            text(fts(token)),
            text(scan(token)),
            "fts_match and the base-table scan disagree for {token:?} after a crash-shaped reopen"
        );
    }
    // Exact contents for the hunted token: only id 4 still carries it
    // (1 was replaced away, 3 was deleted; 2 never had the token).
    assert_eq!(text(fts("charlie")), "4");

    // The recovered registry must accept further writes (control row
    // present), and the agreement contract must keep holding afterwards.
    conn.execute("INSERT INTO docs VALUES (6, 'charlie bravo india')")
        .unwrap();
    assert_eq!(
        text(fts("charlie")),
        text(scan("charlie")),
        "post-reopen insert broke FTS/base agreement"
    );
    assert_eq!(text(fts("charlie")), "4,6");
}
