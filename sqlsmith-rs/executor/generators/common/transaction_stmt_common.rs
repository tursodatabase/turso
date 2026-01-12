use sqlsmith_rs_common::rand_by_seed::LcgRng;

/// Generates a random transaction-related SQL statement.
/// Syntax supported:
///   - BEGIN [DEFERRED|IMMEDIATE|EXCLUSIVE] [TRANSACTION]
///   - COMMIT [TRANSACTION]
///   - END [TRANSACTION]
///   - ROLLBACK [TRANSACTION]
pub fn gen_transaction_stmt(rng: &mut LcgRng) -> Option<String> {
    let modes = ["", "DEFERRED ", "IMMEDIATE ", "EXCLUSIVE "];
    let stmt = match rng.rand().unsigned_abs() % 4 {
        0 => {
            // BEGIN [mode] [TRANSACTION]
            let mode = modes[(rng.rand().unsigned_abs() as usize) % modes.len()];
            let with_transaction = if rng.rand().unsigned_abs() % 2 == 0 {
                "TRANSACTION"
            } else {
                ""
            };
            format!("BEGIN {}{};", mode, with_transaction)
        }
        1 => {
            // COMMIT [TRANSACTION]
            let with_transaction = if rng.rand().unsigned_abs() % 2 == 0 {
                " TRANSACTION"
            } else {
                ""
            };
            format!("COMMIT{};", with_transaction)
        }
        2 => {
            // END [TRANSACTION]
            let with_transaction = if rng.rand().unsigned_abs() % 2 == 0 {
                " TRANSACTION"
            } else {
                ""
            };
            format!("END{};", with_transaction)
        }
        3 => {
            // ROLLBACK [TRANSACTION]
            let with_transaction = if rng.rand().unsigned_abs() % 2 == 0 {
                " TRANSACTION"
            } else {
                ""
            };
            format!("ROLLBACK{};", with_transaction)
        }
        _ => return None,
    };
    Some(stmt)
}
