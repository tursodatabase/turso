use std::{
    io::{self, Write},
    path::PathBuf,
};

use tracing_subscriber::EnvFilter;
use turso_sync::{database_sync::DatabaseSync, sync_server::TursoSyncServerOpts};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let path = PathBuf::try_from("sync.db").unwrap();
    let mut db = DatabaseSync::new(
        &path,
        TursoSyncServerOpts {
            sync_url: "https://flowing-cat-sivukhin.aws-eu-north-1.turso.io".into(),
            auth_token: Some("".into()),
            encryption_key: None,
            pull_batch_size: None,
        },
    )
    .await
    .unwrap();

    loop {
        print!("> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        let bytes_read = io::stdin().read_line(&mut input).unwrap();

        if bytes_read == 0 {
            break;
        }

        let trimmed = input.trim();
        if trimmed == ".exit" || trimmed == ".quit" {
            break;
        }
        if trimmed.starts_with(".sync-from-remote") {
            db.sync_from_remote().await.unwrap();
            continue;
        }
        if trimmed.starts_with(".sync-to-remote") {
            db.sync_to_remote().await.unwrap();
            continue;
        }
        let mut rows = db.query(&input, ()).await.unwrap();
        while let Some(row) = rows.next().await.unwrap() {
            let mut values = vec![];
            for i in 0..row.column_count() {
                let value = row.get_value(i).unwrap();
                match value {
                    turso::Value::Null => values.push(format!("NULL")),
                    turso::Value::Integer(x) => values.push(format!("{}", x)),
                    turso::Value::Real(x) => values.push(format!("{}", x)),
                    turso::Value::Text(x) => values.push(format!("'{}'", x)),
                    turso::Value::Blob(x) => values.push(format!(
                        "x'{}'",
                        x.iter()
                            .map(|x| format!("{:02x}", x))
                            .collect::<Vec<_>>()
                            .join(""),
                    )),
                }
            }
            println!("{}", &values.join(" "));
            io::stdout().flush().unwrap();
        }
    }
}
