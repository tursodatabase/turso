use super::*;
use crate::SqliteDialect;
use crate::{Database, DatabaseOpts, MemoryIO, OpenFlags};

/// Minimal `InternalVirtualTable` that exposes a fixed two-row table. Used
/// to verify that callers can register an arbitrary catalog table at
/// database open time and query it like any other table.
#[derive(Debug)]
struct StaticTable {
    name: &'static str,
}

impl InternalVirtualTable for StaticTable {
    fn name(&self) -> String {
        self.name.to_string()
    }
    fn sql(&self) -> String {
        format!("CREATE TABLE {}(key TEXT, value INTEGER)", self.name)
    }
    fn open(
        &self,
        _conn: Arc<Connection>,
    ) -> crate::Result<Arc<RwLock<dyn InternalVirtualTableCursor>>> {
        Ok(Arc::new(RwLock::new(StaticCursor {
            rows: vec![("alpha".to_string(), 1), ("beta".to_string(), 2)],
            position: -1,
        })))
    }
    fn best_index(
        &self,
        constraints: &[turso_ext::ConstraintInfo],
        _order_by: &[turso_ext::OrderByInfo],
    ) -> std::result::Result<turso_ext::IndexInfo, ResultCode> {
        Ok(turso_ext::IndexInfo {
            idx_num: 0,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: 1.0,
            estimated_rows: 2,
            constraint_usages: constraints
                .iter()
                .map(|_| turso_ext::ConstraintUsage {
                    argv_index: None,
                    omit: false,
                })
                .collect(),
        })
    }
}

struct StaticCursor {
    rows: Vec<(String, i64)>,
    position: i64,
}

impl InternalVirtualTableCursor for StaticCursor {
    fn next(&mut self) -> Result<bool, LimboError> {
        self.position += 1;
        Ok((self.position as usize) < self.rows.len())
    }
    fn rowid(&self) -> i64 {
        self.position
    }
    fn column(&self, column: usize) -> Result<Value, LimboError> {
        let (key, value) = &self.rows[self.position as usize];
        match column {
            0 => Ok(Value::build_text(key.clone())),
            1 => Ok(Value::from_i64(*value)),
            _ => Err(LimboError::InternalError(format!(
                "column index {column} out of range"
            ))),
        }
    }
    fn filter(
        &mut self,
        _args: &[Value],
        _idx_str: Option<String>,
        _idx_num: i32,
    ) -> Result<bool, LimboError> {
        self.position = -1;
        self.next()
    }
}

#[test]
fn registered_internal_vtab_is_visible_to_connections() {
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        crate::util::MEMORY_PATH,
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let name = db
        .register_internal_vtab(StaticTable {
            name: "external_metadata",
        })
        .unwrap();
    assert_eq!(name, "external_metadata");

    let conn = db.connect().unwrap();
    let mut stmt = conn
        .prepare("SELECT key, value FROM external_metadata")
        .unwrap();
    let rows = stmt.run_collect_rows().unwrap();
    let mapped: Vec<(String, i64)> = rows
        .into_iter()
        .map(|cols| {
            let key = match &cols[0] {
                Value::Text(t) => t.as_str().to_string(),
                other => panic!("unexpected key type {other:?}"),
            };
            let value = match &cols[1] {
                Value::Numeric(crate::Numeric::Integer(i)) => *i,
                other => panic!("unexpected value type {other:?}"),
            };
            (key, value)
        })
        .collect();
    assert_eq!(
        mapped,
        vec![("alpha".to_string(), 1), ("beta".to_string(), 2)]
    );
}

#[test]
fn registered_internal_vtab_lookup_folds_ascii_only() {
    let io: Arc<dyn crate::IO> = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        crate::util::MEMORY_PATH,
        OpenFlags::Create,
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let name = db
        .register_internal_vtab(StaticTable {
            name: "External_ΔΥΣ",
        })
        .unwrap();
    assert_eq!(name, "External_ΔΥΣ");

    let conn = db.connect().unwrap();
    let mut stmt = conn.prepare("SELECT key, value FROM external_ΔΥΣ").unwrap();
    let rows = stmt.run_collect_rows().unwrap();
    assert_eq!(rows.len(), 2);

    assert!(conn.prepare("SELECT key, value FROM external_δυσ").is_err());
}
