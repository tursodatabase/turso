// Common DROP TRIGGER statement generation logic
use sqlsmith_rs_common::rand_by_seed::LcgRng;

/// Generates a DROP TRIGGER statement
/// # Arguments
/// * `tables` - List of available tables to base trigger names on
/// * `rng` - Random number generator for value selection
pub fn gen_drop_trigger_stmt(tables: &[impl AsRef<str>], rng: &mut LcgRng) -> Option<String> {
    if tables.is_empty() {
        return None;
    }
    let table = tables[(rng.rand().unsigned_abs() as usize) % tables.len()].as_ref();
    // TODO: Generate a valid trigger name based on the table
    let trigger_name = format!("trigger_{}_{}", table, rng.rand().unsigned_abs() % 1000);
    Some(format!("DROP TRIGGER IF EXISTS {};", trigger_name))
}
