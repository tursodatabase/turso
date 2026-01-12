use sqlsmith_rs_common::rand_by_seed::LcgRng;

/// Generates a SQL statement using SQLite's date and time functions
/// # Arguments
/// * `rng` - Random number generator for value selection
pub fn gen_datefunc_stmt(rng: &mut LcgRng) -> Option<String> {
    const FUNCTIONS: &[&str] = &["date", "time", "datetime", "julianday", "strftime"];

    // Only valid modifiers from https://sqlite.org/lang_datefunc.html
    const MODIFIERS: &[&str] = &[
        "start of month",
        "start of year",
        "start of day",
        "+1 day",
        "+1 month",
        "+1 year",
        "-1 day",
        "-1 month",
        "-1 year",
        "localtime",
        "utc",
    ];

    // Select a random function
    let func = FUNCTIONS[(rng.rand().unsigned_abs() as usize) % FUNCTIONS.len()];

    // Generate a random base date/time (static value only)
    let base_date = {
        // Generate a random static date or datetime string
        let year = 2000 + (rng.rand().abs() % 30); // 2000-2029
        let month = 1 + (rng.rand().abs() % 12);
        let day = 1 + (rng.rand().abs() % 28);
        let hour = rng.rand().abs() % 24;
        let min = rng.rand().abs() % 60;
        let sec = rng.rand().abs() % 60;
        if rng.rand().abs() % 2 == 0 {
            // Date only
            format!("'{:04}-{:02}-{:02}'", year, month, day)
        } else {
            // Full datetime
            format!(
                "'{:04}-{:02}-{:02} {:02}:{:02}:{:02}'",
                year, month, day, hour, min, sec
            )
        }
    };

    // Optionally add modifiers (up to 2, no duplicates)
    let mut modifiers: Vec<String> = Vec::new();
    let modifier_count = rng.rand().unsigned_abs() % 3; // 0, 1, or 2
    for _ in 0..modifier_count {
        let idx = (rng.rand().unsigned_abs() as usize) % MODIFIERS.len();
        let m = MODIFIERS[idx];
        if !modifiers.contains(&m.to_string()) {
            modifiers.push(m.to_string());
        }
    }

    // Optionally add a single weekday modifier (must be last if present)
    if rng.rand().unsigned_abs() % 4 == 0 {
        let weekday_n = rng.rand().unsigned_abs() % 7;
        modifiers.push(format!("weekday {}", weekday_n));
    }

    // Generate the SQL statement
    let sql = if func == "strftime" {
        // Special case for strftime: requires a format string
        let format = match rng.rand().abs() % 3 {
            0 => "'%Y-%m-%d %H:%M:%S'", // Full timestamp
            1 => "'%H:%M:%S'",          // Time only
            _ => "'%Y-%m-%d'",          // Date only
        };
        format!(
            "SELECT {}({}, {}{});",
            func,
            format,
            base_date,
            if modifiers.is_empty() {
                String::new()
            } else {
                format!(
                    ", {}",
                    modifiers
                        .iter()
                        .map(|m| format!("'{}'", m))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        )
    } else {
        format!(
            "SELECT {}({}{});",
            func,
            base_date,
            if modifiers.is_empty() {
                String::new()
            } else {
                format!(
                    ", {}",
                    modifiers
                        .iter()
                        .map(|m| format!("'{}'", m))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        )
    };

    Some(sql)
}
