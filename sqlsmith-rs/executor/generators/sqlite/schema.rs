use rusqlite::{Connection, Result};

#[derive(Debug, Clone)]
pub struct TableInfo {
    pub name: String,
    pub columns: Vec<String>,
}

pub fn get(sqlite_conn: &Connection) -> Result<Vec<TableInfo>> {
    let mut tables = Vec::new();
    let mut stmt = sqlite_conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';",
    )?;
    let table_names = stmt.query_map([], |row| row.get::<_, String>(0))?;

    for table_name in table_names {
        let table_name = table_name?;
        let mut col_stmt = sqlite_conn.prepare(&format!("PRAGMA table_info('{}')", table_name))?;
        let columns = col_stmt
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>, _>>()?;
        tables.push(TableInfo {
            name: table_name,
            columns,
        });
    }
    Ok(tables)
}

/// 公共函数，用于获取表及其列信息
pub fn get_tables_with_columns(sqlite_conn: &Connection) -> Vec<(String, Vec<(String, String)>)> {
    let tables = match get(sqlite_conn) {
        Ok(t) if !t.is_empty() => t,
        _ => return Vec::new(),
    };
    let mut tables_with_columns = Vec::new();
    for table in &tables {
        let mut stmt = match sqlite_conn.prepare(&format!("PRAGMA table_info({})", table.name)) {
            Ok(stmt) => stmt,
            Err(_) => continue,
        };
        let columns_info = match stmt.query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, String>(2)?))
        }) {
            Ok(rows) => {
                let mut info = Vec::new();
                for row in rows {
                    if let Ok((name, col_type)) = row {
                        info.push((name, col_type));
                    }
                }
                info
            }
            Err(_) => continue,
        };
        tables_with_columns.push((table.name.clone(), columns_info));
    }
    tables_with_columns
}
