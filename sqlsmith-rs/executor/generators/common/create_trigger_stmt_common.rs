// Common CREATE TRIGGER statement generation logic, shared between limbo/sqlite drivers
use sqlsmith_rs_common::rand_by_seed::LcgRng;

/// Trait defining required table/trigger metadata for trigger generation
pub trait TriggerTableLike {
    fn name(&self) -> &str;
    fn columns(&self) -> Vec<(String, String)>; // (column name, type)
    fn has_primary_key(&self) -> bool;
}

/// Generates a CREATE TRIGGER statement
/// # Arguments
/// * `tables` - List of available tables to reference in the trigger
/// * `rng` - Random number generator for value selection
pub fn gen_create_trigger_stmt<T: TriggerTableLike>(
    tables: &[T],
    rng: &mut LcgRng,
) -> Option<String> {
    if tables.is_empty() {
        return None;
    }

    // Select random table to attach trigger to
    let table_idx = (rng.rand().unsigned_abs() as usize) % tables.len();
    let table = &tables[table_idx];

    // Randomly choose trigger timing (BEFORE/AFTER)
    let timing = if rng.rand().unsigned_abs() % 2 == 0 {
        "BEFORE"
    } else {
        "AFTER"
    };

    // Randomly choose trigger event (INSERT/UPDATE/DELETE)
    let event = match rng.rand().unsigned_abs() % 3 {
        0 => "INSERT",
        1 => "UPDATE",
        _ => "DELETE",
    };

    // Simple trigger body example: log operation
    let trigger_body = format!(
        "BEGIN\n    -- Example trigger action\n    INSERT INTO trigger_log (operation, table_name) VALUES ('{}', '{}');\nEND",
        event,
        table.name()
    );

    Some(format!(
        "CREATE TRIGGER IF NOT EXISTS trig_{}_{}_{}\n{} {} ON {}\n{}",
        table.name(),
        timing.to_lowercase(),
        event.to_lowercase(),
        timing,
        event,
        table.name(),
        trigger_body
    ))
}
