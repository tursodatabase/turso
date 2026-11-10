//! Real HTTP test driver used by the server's encrypted sync end-to-end suite.
use turso::sync::{Builder, RemoteEncryptionCipher};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let path = args.get(1).expect("replica path");
    let url = args.get(2).expect("URL, or - for offline");
    let action = args
        .get(3)
        .expect("bootstrap, exec, scalar, push, pull, or checkpoint");
    let key = |name: &str, default: String| match std::env::var(name).unwrap_or(default).as_str() {
        "none" => None,
        value => Some(value.to_string()),
    };
    let cipher = std::env::var("SYNC_E2E_CIPHER")
        .unwrap_or_else(|_| "aes256gcm".into())
        .parse::<RemoteEncryptionCipher>()?;
    let mut builder = Builder::new_remote(path)
        .experimental_sync_encryption(true)
        .with_encryption(
            cipher,
            key("SYNC_E2E_LOCAL_KEY", "16".repeat(32)),
            key(
                "SYNC_E2E_CLOUD_KEY",
                "CwsLCwsLCwsLCwsLCwsLCwsLCwsLCwsLCwsLCwsLCws=".into(),
            ),
        );
    if let Ok(token) = std::env::var("SYNC_E2E_AUTH_TOKEN") {
        builder = builder.with_auth_token(token);
    }
    if url != "-" {
        builder = builder.with_remote_url(url);
    } else {
        builder = builder.bootstrap_if_empty(false);
    }
    let db = builder.build().await?;
    let conn = db.connect().await?;
    let mut value = None;
    match action.as_str() {
        "bootstrap" => {}
        "exec" => {
            conn.execute(args.get(4).expect("SQL"), ()).await?;
        }
        "scalar" => {
            let mut rows = conn.query(args.get(4).expect("SQL"), ()).await?;
            value = Some(rows.next().await?.expect("one row").get::<i64>(0)?);
        }
        "push" => {
            db.push().await?;
        }
        "pull" => {
            db.pull().await?;
        }
        "checkpoint" => {
            db.checkpoint().await?;
        }
        _ => panic!("unknown action"),
    }
    let stats = db.stats().await?;
    println!(
        "{}",
        serde_json::json!({"action": action, "value": value, "revision": stats.revision, "pending": stats.cdc_operations})
    );
    Ok(())
}
