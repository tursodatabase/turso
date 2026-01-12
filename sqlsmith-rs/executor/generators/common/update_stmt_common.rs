// 通用 UPDATE 语句生成逻辑，供 limbo/sqlite 共享
use crate::generators::common::data_type::generate_value_by_type;
use sqlsmith_rs_common::rand_by_seed::LcgRng;

pub trait TableColumnLike {
    fn name(&self) -> &str;
    fn columns(&self) -> Vec<(String, String)>; // (name, type)
}

pub fn gen_update_stmt<T: TableColumnLike>(tables: &[T], rng: &mut LcgRng) -> Option<String> {
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

    // Randomly select columns to update
    let col_count = ((rng.rand().unsigned_abs() as usize) % columns.len()) + 1;
    let mut selected_cols = columns.clone();
    for i in (1..selected_cols.len()).rev() {
        let j = (rng.rand().unsigned_abs() as usize) % (i + 1);
        selected_cols.swap(i, j);
    }
    let selected_cols = &selected_cols[..col_count];
    let set_clause: Vec<String> = selected_cols
        .iter()
        .map(|(name, ty)| {
            let value = generate_value_by_type(ty, rng);
            format!("{} = {}", name, value)
        })
        .collect();

    // Add optional WHERE clause
    let where_clause = if rng.rand().unsigned_abs() % 2 == 0 {
        let col_idx = (rng.rand().unsigned_abs() as usize) % columns.len();
        let col = &columns[col_idx].0;
        let value = generate_value_by_type(&columns[col_idx].1, rng);
        format!("WHERE {} = {}", col, value)
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

    // Add optional FROM clause (for multi-table updates)
    let from_clause = if rng.rand().unsigned_abs() % 2 == 0 && tables.len() > 1 {
        let other_table_idx = (rng.rand().unsigned_abs() as usize) % tables.len();
        let other_table = &tables[other_table_idx];
        if other_table.name() != table.name() {
            format!("FROM {}", other_table.name())
        } else {
            String::new()
        }
    } else {
        String::new()
    };

    // Combine all parts into a full UPDATE statement
    Some(format!(
        "UPDATE {} {} SET {} {} {};",
        table.name(),
        from_clause,
        set_clause.join(", "),
        where_clause,
        limit_clause
    ))
}
