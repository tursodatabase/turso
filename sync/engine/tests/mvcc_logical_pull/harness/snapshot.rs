use std::sync::Arc;

use turso_core::{Connection, Value};
use turso_sync_engine::Result;

const USER_OBJECTS: &str = "tbl_name NOT GLOB 'sqlite_*' \
     AND tbl_name NOT GLOB '__turso_internal_*' \
     AND tbl_name NOT IN ('turso_sync_last_change_id', 'turso_cdc', 'turso_cdc_version')";

#[derive(Debug, PartialEq)]
pub struct Snapshot {
    schema: Vec<Vec<Value>>,
    tables: Vec<TableRows>,
}

#[derive(Debug, PartialEq)]
struct TableRows {
    table: String,
    rows: Vec<Vec<Value>>,
}

impl Snapshot {
    pub fn capture(conn: &Arc<Connection>) -> Result<Self> {
        let schema = query(
            conn,
            &format!(
                "SELECT type, name, tbl_name, sql FROM sqlite_schema WHERE {USER_OBJECTS} ORDER BY type, name"
            ),
        )?;
        let tables = query(
            conn,
            &format!("SELECT name FROM sqlite_schema WHERE type = 'table' AND {USER_OBJECTS} ORDER BY name"),
        )?
        .into_iter()
        .flatten()
        .map(|name| TableRows::capture(conn, name.to_string()))
        .collect::<Result<_>>()?;
        Ok(Self { schema, tables })
    }
}

impl TableRows {
    fn capture(conn: &Arc<Connection>, table: String) -> Result<Self> {
        let mut rows = query(conn, &format!("SELECT * FROM \"{table}\""))?;
        rows.sort();
        Ok(Self { table, rows })
    }
}

fn query(conn: &Arc<Connection>, sql: &str) -> Result<Vec<Vec<Value>>> {
    Ok(conn.prepare(sql)?.run_collect_rows()?)
}
