use std::io::{self, Write};

use tracing_subscriber::EnvFilter;
use turso_sync::database::Builder;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let sync_url = std::env::var("TURSO_SYNC_URL").unwrap();
    let auth_token = std::env::var("TURSO_AUTH_TOKEN").ok();
    let local_path = std::env::var("TURSO_LOCAL_PATH").unwrap();
    let mut db = Builder::new_synced(&local_path, &sync_url, auth_token)
        .build()
        .await
        .unwrap();
    let conn = db.connect().await.unwrap();
    loop {
        print!("> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        let bytes_read = io::stdin().read_line(&mut input).unwrap();

        if bytes_read == 0 {
            break;
        }

        let trimmed = input.trim();
        match trimmed {
            ".exit" | ".quit" => break,
            ".pull" => {
                db.pull().await.unwrap();
                continue;
            }
            ".push" => {
                db.push().await.unwrap();
                continue;
            }
            ".sync" => {
                db.sync().await.unwrap();
                continue;
            }
            _ => {}
        }
        let mut rows = conn.query(&input, ()).await.unwrap();
        while let Some(row) = rows.next().await.unwrap() {
            let mut values = vec![];
            for i in 0..row.column_count() {
                let value = row.get_value(i).unwrap();
                match value {
                    turso::Value::Null => values.push("NULL".to_string()),
                    turso::Value::Integer(x) => values.push(format!("{x}")),
                    turso::Value::Real(x) => values.push(format!("{x}")),
                    turso::Value::Text(x) => values.push(format!("'{x}'")),
                    turso::Value::Blob(x) => values.push(format!(
                        "x'{}'",
                        x.iter()
                            .map(|x| format!("{x:02x}"))
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
