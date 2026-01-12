use turso::Connection;

/// 表结构体
#[derive(Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<(String, String)>, // (列名, 类型)
}

/// 获取所有表及其列
pub async fn get_tables(conn: &Connection) -> Result<Vec<Table>, Box<dyn std::error::Error>> {
    let sql: &'static str =
        "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%';";
    let mut tables = Vec::new();

    let mut rows = conn.query(sql, ()).await.unwrap();

    while let Ok(Some(row)) = rows.next().await {
        let table_name: String = row.get_value(0).unwrap().as_text().unwrap().to_string();
        let columns = get_columns(conn, &table_name).await?;
        tables.push(Table {
            name: table_name,
            columns,
        });
    }
    Ok(tables)
}

/// 获取指定表的所有列名
pub async fn get_columns(
    conn: &Connection,
    table_name: &str,
) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
    let sql = format!("PRAGMA table_info({});", table_name);
    let mut columns = Vec::new();

    let mut rows = conn.query(&sql, ()).await?;
    while let Ok(Some(row)) = rows.next().await {
        let column_name: String = row.get_value(1).unwrap().as_text().unwrap().to_string();
        let column_type: String = row.get_value(2).unwrap().as_text().unwrap().to_string();
        columns.push((column_name, column_type));
    }
    Ok(columns)
}
