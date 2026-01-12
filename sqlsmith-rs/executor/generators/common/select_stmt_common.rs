// 通用 SELECT 语句生成逻辑，供 limbo/sqlite 共享
// TableInfo: 需实现 name: &str, columns: &[String] trait
use sqlsmith_rs_common::rand_by_seed::LcgRng;

pub trait TableLike {
    fn name(&self) -> &str;
    fn columns(&self) -> Vec<String>;
}

pub fn gen_select_stmt<T: TableLike>(tables: &[T], rng: &mut LcgRng) -> Option<String> {
    if tables.is_empty() {
        return None;
    }

    // Select a random table
    let table_idx = (rng.rand().unsigned_abs() as usize) % tables.len();
    let table = &tables[table_idx];
    let columns = table.columns();
    if columns.is_empty() {
        return None;
    }

    // Select random columns
    let col_count = ((rng.rand().unsigned_abs() as usize) % columns.len()) + 1;
    let mut selected_cols = columns.clone();
    for i in (1..selected_cols.len()).rev() {
        let j = (rng.rand().unsigned_abs() as usize) % (i + 1);
        selected_cols.swap(i, j);
    }
    let selected_cols = &selected_cols[..col_count];

    // Add optional DISTINCT
    let distinct = if rng.rand().unsigned_abs() % 2 == 0 {
        "DISTINCT "
    } else {
        ""
    };

    // Add optional WHERE clause
    let where_clause = if rng.rand().unsigned_abs() % 2 == 0 {
        let col_idx = (rng.rand().unsigned_abs() as usize) % columns.len();
        let col = &columns[col_idx];
        let value = (rng.rand().unsigned_abs() % 100).to_string(); // Random value
        format!("WHERE {} = {}", col, value)
    } else {
        String::new()
    };

    // Add optional GROUP BY clause
    let group_by_clause = if rng.rand().unsigned_abs() % 2 == 0 {
        let col_idx = (rng.rand().unsigned_abs() as usize) % columns.len();
        let col = &columns[col_idx];
        format!("GROUP BY {}", col)
    } else {
        String::new()
    };

    // Add optional ORDER BY clause
    let order_by_clause = if rng.rand().unsigned_abs() % 2 == 0 {
        let col_idx = (rng.rand().unsigned_abs() as usize) % columns.len();
        let col = &columns[col_idx];
        let order = if rng.rand().unsigned_abs() % 2 == 0 {
            "ASC"
        } else {
            "DESC"
        };
        format!("ORDER BY {} {}", col, order)
    } else {
        String::new()
    };

    // Add optional LIMIT clause
    let limit_clause = if rng.rand().unsigned_abs() % 2 == 0 {
        let limit = (rng.rand().unsigned_abs() % 100).to_string(); // Random limit
        format!("LIMIT {}", limit)
    } else {
        String::new()
    };

    // Combine all parts into a full SELECT statement
    Some(format!(
        "SELECT {}{} FROM {} {} {} {} {};",
        distinct,
        selected_cols.join(", "),
        table.name(),
        where_clause,
        group_by_clause,
        order_by_clause,
        limit_clause
    ))
}
