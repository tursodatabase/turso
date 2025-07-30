use std::io::{self, Write};

use turso::Builder;
use turso_sync_protocol::{
    database_tape::{
        DatabaseChangesIteratorMode, DatabaseChangesIteratorOpts, DatabaseReplaySessionOpts,
        DatabaseTape,
    },
    types::DatabaseTapeOperation,
};

#[tokio::main]
async fn main() {
    // let db = Builder::new_local("local.db").build().await.unwrap();
    // let db = DatabaseTape::new(db);

    // let conn = db.connect().await.unwrap();

    // loop {
    //     print!("> ");
    //     io::stdout().flush().unwrap();

    //     let mut input = String::new();
    //     let bytes_read = io::stdin().read_line(&mut input).unwrap();

    //     if bytes_read == 0 {
    //         break;
    //     }

    //     let trimmed = input.trim();
    //     if trimmed == ".exit" || trimmed == ".quit" {
    //         break;
    //     }
    //     if trimmed.starts_with(".undo ") || trimmed.starts_with(".redo ") {
    //         let first_change_id = Some(trimmed[".undo ".len()..].parse().unwrap());
    //         let mode = match &trimmed[0..(".undo".len())] {
    //             ".undo" => DatabaseChangesIteratorMode::Revert,
    //             ".redo" => DatabaseChangesIteratorMode::Apply,
    //             _ => unreachable!(),
    //         };
    //         let mut iterator = db
    //             .iterate_changes(DatabaseChangesIteratorOpts {
    //                 first_change_id,
    //                 mode,
    //                 ..Default::default()
    //             })
    //             .await
    //             .unwrap();
    //         let mut session = db
    //             .start_replay_session(DatabaseReplaySessionOpts {
    //                 preserve_rowid: true,
    //             })
    //             .await
    //             .unwrap();
    //         if let Some(change) = iterator.next().await.unwrap() {
    //             session.replay(change).await.unwrap();
    //             session.replay(DatabaseTapeOperation::Commit).await.unwrap();
    //         }
    //         continue;
    //     }
    //     let mut stmt = conn.prepare(&input).await.unwrap();
    //     let mut rows = stmt.query(()).await.unwrap();
    //     while let Some(row) = rows.next().await.unwrap() {
    //         let mut values = vec![];
    //         for i in 0..row.column_count() {
    //             let value = row.get_value(i).unwrap();
    //             match value {
    //                 turso::Value::Null => values.push("NULL".to_string()),
    //                 turso::Value::Integer(x) => values.push(format!("{x}",)),
    //                 turso::Value::Real(x) => values.push(format!("{x}")),
    //                 turso::Value::Text(x) => values.push(format!("'{x}'")),
    //                 turso::Value::Blob(x) => values.push(format!(
    //                     "x'{}'",
    //                     x.iter()
    //                         .map(|x| format!("{x:02x}"))
    //                         .collect::<Vec<_>>()
    //                         .join(""),
    //                 )),
    //             }
    //         }
    //         println!("{}", &values.join(" "));
    //         io::stdout().flush().unwrap();
    //     }
    // }
}
