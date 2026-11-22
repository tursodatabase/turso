//! Real HTTP test driver used by the server's encrypted sync end-to-end suite.
use turso::sync::{Builder, PartialSyncOpts, RemoteEncryptionCipher};
use turso_sync_sdk_kit::rsapi::PartialBootstrapStrategy;

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
    let cipher = std::env::var("SYNC_E2E_CIPHER").unwrap_or_else(|_| "aes256gcm".into());
    let mut builder = Builder::new_remote(path);
    if action == "partial-checkpoint" {
        builder = builder.with_partial_sync_opts_experimental(PartialSyncOpts {
            bootstrap_strategy: Some(PartialBootstrapStrategy::Prefix { length: 8192 }),
            segment_size: 4096,
            prefetch: false,
        });
    }
    if cipher != "none" {
        builder = builder.with_encryption(
            cipher.parse::<RemoteEncryptionCipher>()?,
            key("SYNC_E2E_LOCAL_KEY", "16".repeat(32)),
            key(
                "SYNC_E2E_CLOUD_KEY",
                "CwsLCwsLCwsLCwsLCwsLCwsLCwsLCwsLCwsLCwsLCws=".into(),
            ),
        );
    }
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
        "partial-checkpoint" => {
            let mode = args.get(4).expect("checkpoint mode");
            assert!(matches!(mode.as_str(), "PASSIVE" | "FULL"));
            for sql in [
                "UPDATE lazy_items SET value = 'new'||substr(value, 4)".to_string(),
                format!("PRAGMA wal_checkpoint({mode})"),
            ] {
                let response: serde_json::Value = reqwest::Client::new()
                    .post(format!("{url}/v2/pipeline"))
                    .json(&serde_json::json!({"requests": [{"type": "execute", "stmt": {"sql": sql}}]}))
                    .send().await?.error_for_status()?.json().await?;
                assert_eq!(response["results"][0]["type"], "ok", "{response}");
            }
            let mut rows = conn
                .query("SELECT sum(value LIKE 'old%') FROM lazy_items", ())
                .await?;
            value = Some(rows.next().await?.expect("one row").get::<i64>(0)?);
            let mut rows = conn
                .query(
                    "SELECT count(*)||' '||sum(value LIKE 'new%') FROM lazy_items",
                    (),
                )
                .await?;
            assert_eq!(rows.next().await?.unwrap().get::<String>(0)?, "200 0");
            let mut rows = conn.query("PRAGMA integrity_check", ()).await?;
            assert_eq!(rows.next().await?.unwrap().get::<String>(0)?, "ok");
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
