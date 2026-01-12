use crate::generators::common::insert_stmt_common::TableColumnLike;
use sqlsmith_rs_common::rand_by_seed::LcgRng;

pub fn gen_delete_stmt<T: TableColumnLike>(tables: &[T], rng: &mut LcgRng) -> Option<String> {
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

    // Generate WHERE clause
    let where_clause = if rng.rand().unsigned_abs() % 2 == 0 {
        columns
            .iter()
            .take(1) // Use first column for WHERE condition
            .map(|(name, _)| {
                format!(
                    "{} = {}",
                    name,
                    match rng.rand().abs() % 4 {
                        0 => (rng.rand().abs() % 1000).to_string(),
                        1 => format!("'val{}'", rng.rand().abs() % 1000),
                        2 => "NULL".to_string(),
                        _ => "1".to_string(),
                    }
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ")
    } else {
        String::new()
    };

    // Generate LIMIT clause
    let limit_clause = if rng.rand().unsigned_abs() % 2 == 0 {
        let limit = (rng.rand().unsigned_abs() % 100).to_string(); // Random limit
        format!("LIMIT {}", limit)
    } else {
        String::new()
    };

    // Generate RETURNING clause
    let returning_clause = if rng.rand().unsigned_abs() % 2 == 0 {
        let col_count = ((rng.rand().unsigned_abs() as usize) % columns.len()) + 1;
        let mut selected_cols = columns.clone();
        for i in (1..selected_cols.len()).rev() {
            let j = (rng.rand().unsigned_abs() as usize) % (i + 1);
            selected_cols.swap(i, j);
        }
        let selected_cols = &selected_cols[..col_count];
        let col_names: Vec<&str> = selected_cols
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        format!("RETURNING {}", col_names.join(", "))
    } else {
        String::new()
    };

    // Combine all parts into a full DELETE statement
    Some(format!(
        "DELETE FROM {} {} {} {};",
        table.name(),
        if !where_clause.is_empty() {
            format!("WHERE {}", where_clause)
        } else {
            String::new()
        },
        limit_clause,
        returning_clause
    ))
}
