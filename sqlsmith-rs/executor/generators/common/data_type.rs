use sqlsmith_rs_common::rand_by_seed::LcgRng;

pub fn generate_value_by_type(ty: &str, rng: &mut LcgRng) -> String {
    match ty.to_uppercase().as_str() {
        "INTEGER" => (rng.rand().abs() % 1000).to_string(),
        "REAL" => format!("{}", (rng.rand().abs() as f64) / 100.0),
        "TEXT" => format!("'val{}'", rng.rand().abs() % 1000),
        "BLOB" => {
            // Generate a random BLOB with 1 to 16 bytes
            let len = (rng.rand().unsigned_abs() % 16) + 1;
            let mut blob = Vec::with_capacity(len as usize);
            for _ in 0..len {
                blob.push((rng.rand().unsigned_abs() % 256) as u8);
            }
            let hex_str = blob
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>();
            format!("X'{}'", hex_str)
        }
        "NUMERIC" => {
            // Generate NUMERIC values as integers, floats, or ISO8601 date strings
            match rng.rand().abs() % 3 {
                0 => (rng.rand().abs() % 1000).to_string(), // Integer-like NUMERIC
                1 => format!("{}", (rng.rand().abs() as f64) / 100.0), // Float-like NUMERIC
                _ => {
                    // ISO8601 date string
                    let year = 2000 + (rng.rand().abs() % 30); // Random year between 2000 and 2030
                    let month = 1 + (rng.rand().abs() % 12); // Random month
                    let day = 1 + (rng.rand().abs() % 28); // Random day
                    format!("'{}-{:02}-{:02}'", year, month, day)
                }
            }
        }
        _ => "NULL".to_string(),
    }
}
