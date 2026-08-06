//! turso-ldap-connector CLI - LDAP-to-Turso sync connector demonstrating CDC-tracked
//! full and incremental (RFC 4533) resync.
//!
//! Typical usage:
//! ```text
//! turso-ldap-connector sync --db ldap_sync.db
//! # ... entries change in LDAP ...
//! turso-ldap-connector sync --db ldap_sync.db   # incremental, uses persisted cookie
//! turso-ldap-connector validate --db ldap_sync.db
//! ```

use clap::{Parser, Subcommand};
use turso_ldap_connector::{default_cookie_path, run_sync, SyncRunConfig, TursoDb};

#[derive(Parser)]
#[command(name = "turso-ldap-connector")]
#[command(version, about = "LDAP-to-Turso CDC demo connector")]
struct Args {
    /// LDAP server URL
    #[arg(long, default_value = "ldap://localhost:3389")]
    ldap_url: String,

    /// Bind DN
    #[arg(long, default_value = "cn=admin,dc=example,dc=org")]
    bind_dn: String,

    /// Bind password
    #[arg(long, default_value = "admin_password")]
    bind_pw: String,

    /// Base DN to search under
    #[arg(long, default_value = "dc=example,dc=org")]
    base_dn: String,

    /// Path to the Turso database file
    #[arg(long, default_value = "ldap_sync.db")]
    db: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run one sync pass: full baseline if no cookie is persisted yet, otherwise an
    /// incremental RFC 4533 resync using the persisted cookie.
    Sync,
    /// Print turso_cdc / table row counts for manual inspection.
    Validate {
        /// Only count turso_cdc rows with change_id greater than this watermark
        /// (e.g. the max change_id observed after a prior sync pass).
        #[arg(long, default_value_t = 0)]
        since_change_id: i64,
    },
    /// Print the current max turso_cdc change_id, for use as a `validate
    /// --since-change-id` watermark on a later invocation.
    MaxChangeId,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let cookie_path = default_cookie_path(&args.db);

    match args.command {
        Command::Sync => {
            let cfg = SyncRunConfig {
                ldap_url: &args.ldap_url,
                bind_dn: &args.bind_dn,
                bind_pw: &args.bind_pw,
                base_dn: &args.base_dn,
                db_path: &args.db,
                cookie_path,
            };
            let stats = run_sync(&cfg)?;
            println!(
                "entries_seen={} upserts={} deletes={} present_unchanged={}",
                stats.entries_seen, stats.upserts, stats.deletes, stats.present_unchanged
            );
        }
        Command::Validate { since_change_id } => {
            let db = TursoDb::open(&args.db)?;
            let (users, groups) = db.table_counts()?;
            let counts = db.cdc_counts_since(since_change_id)?;
            println!("users={users} groups={groups}");
            println!(
                "turso_cdc (change_id > {since_change_id}): insert={} update={} delete={} commit={}",
                counts.insert, counts.update, counts.delete, counts.commit
            );
        }
        Command::MaxChangeId => {
            let db = TursoDb::open(&args.db)?;
            println!("{}", db.max_cdc_change_id()?);
        }
    }

    Ok(())
}
