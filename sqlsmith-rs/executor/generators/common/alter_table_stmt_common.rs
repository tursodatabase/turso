use sqlsmith_rs_common::rand_by_seed::LcgRng;

/// Trait defining required table metadata for ALTER TABLE generation
pub trait AlterTableLike {
    fn name(&self) -> &str;
    fn columns(&self) -> Vec<(String, String)>; // (column name, type)
}

/// Generates an ALTER TABLE statement (ADD COLUMN or RENAME COLUMN)
/// # Arguments
/// * `tables` - List of available tables to alter
/// * `rng` - Random number generator for value selection
pub fn gen_alter_table_stmt<T: AlterTableLike>(
    tables: &[T],
    rng: &mut LcgRng,
) -> Option<String> {
    if tables.is_empty() {
        return None;
    }

    let table_idx = (rng.rand().unsigned_abs() as usize) % tables.len();
    let table = &tables[table_idx];

    // Randomly choose ALTER TABLE operation: 0 = ADD COLUMN, 1 = RENAME COLUMN
    match rng.rand().unsigned_abs() % 2 {
        0 => {
            // Generate a new column name and type
            let new_col_name = format!("col_{}", rng.rand().unsigned_abs() % 10000);
            let col_types = ["INTEGER", "TEXT", "REAL", "BLOB"];
            let col_type = col_types[(rng.rand().unsigned_abs() as usize) % col_types.len()];
            Some(format!(
                "ALTER TABLE {} ADD COLUMN {} {};",
                table.name(),
                new_col_name,
                col_type
            ))
        }
        _ => {
            // RENAME COLUMN (if table has at least one column)
            let columns = table.columns();
            if columns.is_empty() {
                return None;
            }
            let col_idx = (rng.rand().unsigned_abs() as usize) % columns.len();
            let old_col_name = &columns[col_idx].0;
            // Generate a random string for the new column name
            let charset = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
            let rand_len = 8;
            let new_col_name: String = (0..rand_len)
                .map(|_| {
                    let idx = (rng.rand().unsigned_abs() as usize) % charset.len();
                    charset[idx] as char
                })
                .collect();
            Some(format!(
                "ALTER TABLE {} RENAME COLUMN {} TO {};",
                table.name(),
                old_col_name,
                new_col_name
            ))
        }
    }
}