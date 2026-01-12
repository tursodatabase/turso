use sqlsmith_rs_common::rand_by_seed::LcgRng;

/// Generates a CREATE TABLE statement with random columns and types.
/// # Arguments
/// * `table_name` - Name of the table to create
/// * `rng` - Random number generator for value selection
pub fn gen_create_table_stmt(rng: &mut LcgRng) -> Option<String> {
    let col_types = ["INTEGER", "TEXT", "REAL", "BLOB"];
    let charset = b"abcdefghijklmnopqrstuvwxyz";
    let table_name: String = (0..8)
        .map(|_| {
            let idx = (rng.rand().unsigned_abs() as usize) % charset.len();
            charset[idx] as char
        })
        .collect();
    let col_count = 1 + (rng.rand().unsigned_abs() as usize) % 6; // 1~6 columns

    if col_count == 0 {
        return None;
    }

    let mut columns = Vec::new();
    for _ in 0..col_count {
        let col_name: String = (0..8)
            .map(|_| {
                let idx = (rng.rand().unsigned_abs() as usize) % charset.len();
                charset[idx] as char
            })
            .collect();
        let col_type = col_types[(rng.rand().unsigned_abs() as usize) % col_types.len()];
        columns.push(format!("{} {}", col_name, col_type));
    }

    Some(format!(
        "CREATE TABLE {} ({});",
        table_name,
        columns.join(", ")
    ))
}
