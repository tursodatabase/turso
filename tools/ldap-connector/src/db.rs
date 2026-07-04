//! Turso-side storage: schema, CDC-enabled upserts/deletes, and CDC validation queries.

use std::collections::HashMap;
use std::num::NonZero;
use std::sync::Arc;

use turso_core::{
    Connection, Database, DatabaseOpts, LimboError, OpenFlags, PlatformIO, StepResult, Value, IO,
};

use crate::ldap_client::{EntryKind, LdapObject, SyncOp};

pub struct TursoDb {
    pub conn: Arc<Connection>,
    io: Arc<dyn IO>,
}

impl TursoDb {
    /// Open (creating if necessary) a Turso database file at `path`.
    pub fn open(path: &str) -> Result<Self, LimboError> {
        let io: Arc<dyn IO> = Arc::new(PlatformIO::new()?);
        let db = Database::open_file_with_flags(
            io.clone(),
            path,
            OpenFlags::default(),
            DatabaseOpts::new(),
            None,
        )?;
        let conn = db.connect()?;
        Ok(Self { conn, io })
    }

    /// Create the `users`/`groups` tables if they don't exist yet.
    ///
    /// Must run *before* `enable_cdc()`: CDC also tracks DDL (as changes to
    /// `sqlite_schema`), and we want `turso_cdc` to contain exactly the user/group
    /// row changes for this demo, not schema-creation noise.
    pub fn init_schema(&self) -> Result<(), LimboError> {
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS users (
                dn TEXT PRIMARY KEY,
                uuid TEXT UNIQUE,
                uid TEXT,
                cn TEXT,
                sn TEXT,
                given_name TEXT,
                mail TEXT,
                location TEXT
            )",
        )?;
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS groups (
                dn TEXT PRIMARY KEY,
                uuid TEXT UNIQUE,
                cn TEXT,
                description TEXT,
                location TEXT,
                members TEXT
            )",
        )?;
        Ok(())
    }

    /// Enable full-mode CDC capture on this connection. Must be called before any
    /// row-level writes we want tracked.
    pub fn enable_cdc(&self) -> Result<(), LimboError> {
        self.conn
            .execute("PRAGMA capture_data_changes_conn('full')")
    }

    /// Apply a batch of upserts/deletes coming from an LDAP sync pass.
    pub fn apply_ops(&self, ops: &[SyncOp]) -> Result<(), LimboError> {
        let mut user_upsert = self.conn.prepare(
            "INSERT INTO users (dn, uuid, uid, cn, sn, given_name, mail, location) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8) \
             ON CONFLICT(dn) DO UPDATE SET uuid=excluded.uuid, uid=excluded.uid, cn=excluded.cn, \
                sn=excluded.sn, given_name=excluded.given_name, mail=excluded.mail, \
                location=excluded.location",
        )?;
        let mut group_upsert = self.conn.prepare(
            "INSERT INTO groups (dn, uuid, cn, description, location, members) \
             VALUES (?1,?2,?3,?4,?5,?6) \
             ON CONFLICT(dn) DO UPDATE SET uuid=excluded.uuid, cn=excluded.cn, \
                description=excluded.description, location=excluded.location, \
                members=excluded.members",
        )?;
        let mut user_delete = self.conn.prepare("DELETE FROM users WHERE dn = ?1")?;
        let mut group_delete = self.conn.prepare("DELETE FROM groups WHERE dn = ?1")?;
        let mut user_delete_by_uuid = self.conn.prepare("DELETE FROM users WHERE uuid = ?1")?;
        let mut group_delete_by_uuid = self.conn.prepare("DELETE FROM groups WHERE uuid = ?1")?;

        for op in ops {
            match op {
                SyncOp::Upsert(obj) => match obj.kind {
                    EntryKind::User => {
                        bind_user(&mut user_upsert, obj)?;
                        user_upsert.run_ignore_rows()?;
                        user_upsert.reset()?;
                    }
                    EntryKind::Group => {
                        bind_group(&mut group_upsert, obj)?;
                        group_upsert.run_ignore_rows()?;
                        group_upsert.reset()?;
                    }
                },
                SyncOp::Delete { dn, kind } => {
                    let stmt = match kind {
                        EntryKind::User => &mut user_delete,
                        EntryKind::Group => &mut group_delete,
                    };
                    stmt.bind_at(NonZero::new(1).unwrap(), Value::from_text(dn.clone()))?;
                    stmt.run_ignore_rows()?;
                    stmt.reset()?;
                }
                SyncOp::DeleteByUuid(uuid) => {
                    // We don't know which table the entry was in (it's already gone
                    // from LDAP), so try both; only the matching one actually removes
                    // a row and generates a CDC delete record.
                    let hex_uuid = hex::encode(uuid);
                    user_delete_by_uuid
                        .bind_at(NonZero::new(1).unwrap(), Value::from_text(hex_uuid.clone()))?;
                    user_delete_by_uuid.run_ignore_rows()?;
                    user_delete_by_uuid.reset()?;

                    group_delete_by_uuid
                        .bind_at(NonZero::new(1).unwrap(), Value::from_text(hex_uuid))?;
                    group_delete_by_uuid.run_ignore_rows()?;
                    group_delete_by_uuid.reset()?;
                }
            }
        }
        Ok(())
    }

    /// Row counts for the two data tables (for reporting / assertions).
    pub fn table_counts(&self) -> Result<(i64, i64), LimboError> {
        let users = self.scalar_i64("SELECT COUNT(*) FROM users")?;
        let groups = self.scalar_i64("SELECT COUNT(*) FROM groups")?;
        Ok((users, groups))
    }

    /// The highest `change_id` currently in `turso_cdc` (0 if empty). Used as the
    /// baseline watermark to scope "what changed in this sync pass" queries.
    pub fn max_cdc_change_id(&self) -> Result<i64, LimboError> {
        self.scalar_i64("SELECT COALESCE(MAX(change_id), 0) FROM turso_cdc")
    }

    /// Per-change_type row counts in `turso_cdc` with `change_id > since`.
    pub fn cdc_counts_since(&self, since: i64) -> Result<CdcCounts, LimboError> {
        let mut stmt = self.conn.prepare(
            "SELECT change_type, COUNT(*) FROM turso_cdc WHERE change_id > ?1 GROUP BY change_type",
        )?;
        stmt.bind_at(NonZero::new(1).unwrap(), Value::from_i64(since))?;
        let mut counts = CdcCounts::default();
        loop {
            match stmt.step()? {
                StepResult::Row => {
                    let row = stmt.row().unwrap();
                    let change_type = row.get_value(0).as_int().expect("change_type is integer");
                    let n = row.get_value(1).as_int().expect("count is integer");
                    match change_type {
                        1 => counts.insert = n,
                        0 => counts.update = n,
                        -1 => counts.delete = n,
                        2 => counts.commit = n,
                        other => panic!("unexpected change_type {other} in turso_cdc"),
                    }
                }
                StepResult::IO => self.io.step()?,
                StepResult::Yield => continue,
                StepResult::Done => break,
                StepResult::Busy | StepResult::Interrupt => return Err(LimboError::Busy),
            }
        }
        Ok(counts)
    }

    fn scalar_i64(&self, sql: &str) -> Result<i64, LimboError> {
        let mut stmt = self.conn.prepare(sql)?;
        let mut value = 0i64;
        loop {
            match stmt.step()? {
                StepResult::Row => {
                    let row = stmt.row().unwrap();
                    value = row.get_value(0).as_int().expect("scalar result is integer");
                }
                StepResult::IO => self.io.step()?,
                StepResult::Yield => continue,
                StepResult::Done => break,
                StepResult::Busy | StepResult::Interrupt => return Err(LimboError::Busy),
            }
        }
        Ok(value)
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CdcCounts {
    pub insert: i64,
    pub update: i64,
    pub delete: i64,
    pub commit: i64,
}

fn attr_first(attrs: &HashMap<String, Vec<String>>, key: &str) -> Value {
    match attrs.get(key).and_then(|v| v.first()) {
        Some(s) => Value::from_text(s.clone()),
        None => Value::Null,
    }
}

fn bind_user(stmt: &mut turso_core::Statement, obj: &LdapObject) -> Result<(), LimboError> {
    stmt.bind_at(NonZero::new(1).unwrap(), Value::from_text(obj.dn.clone()))?;
    stmt.bind_at(
        NonZero::new(2).unwrap(),
        Value::from_text(hex::encode(&obj.uuid)),
    )?;
    stmt.bind_at(NonZero::new(3).unwrap(), attr_first(&obj.attrs, "uid"))?;
    stmt.bind_at(NonZero::new(4).unwrap(), attr_first(&obj.attrs, "cn"))?;
    stmt.bind_at(NonZero::new(5).unwrap(), attr_first(&obj.attrs, "sn"))?;
    stmt.bind_at(
        NonZero::new(6).unwrap(),
        attr_first(&obj.attrs, "givenName"),
    )?;
    stmt.bind_at(NonZero::new(7).unwrap(), attr_first(&obj.attrs, "mail"))?;
    stmt.bind_at(NonZero::new(8).unwrap(), attr_first(&obj.attrs, "l"))?;
    Ok(())
}

fn bind_group(stmt: &mut turso_core::Statement, obj: &LdapObject) -> Result<(), LimboError> {
    let members = obj
        .attrs
        .get("member")
        .map(|m| m.join("\n"))
        .unwrap_or_default();
    stmt.bind_at(NonZero::new(1).unwrap(), Value::from_text(obj.dn.clone()))?;
    stmt.bind_at(
        NonZero::new(2).unwrap(),
        Value::from_text(hex::encode(&obj.uuid)),
    )?;
    stmt.bind_at(NonZero::new(3).unwrap(), attr_first(&obj.attrs, "cn"))?;
    stmt.bind_at(
        NonZero::new(4).unwrap(),
        attr_first(&obj.attrs, "description"),
    )?;
    stmt.bind_at(NonZero::new(5).unwrap(), attr_first(&obj.attrs, "l"))?;
    stmt.bind_at(NonZero::new(6).unwrap(), Value::from_text(members))?;
    Ok(())
}
