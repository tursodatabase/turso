//! Checks that a function's declared return type (`Func::return_type`) is the
//! type its results really have.
//!
//! The list of functions comes from `Func::builtin_function_list`, which walks
//! the function enums, so a new function is checked as soon as it is added.

use crate::common::TempDatabase;
use turso_core::native_ext::ReturnType;
use turso_core::Func;

/// One SQL literal of each kind, used as every argument of a call.
const SAMPLE_ARGUMENTS: [&str; 6] = ["1", "2.5", "'abc'", "'12'", "x'0102'", "NULL"];

#[turso_macros::test]
fn results_have_the_declared_return_type(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    let mut checked_calls = 0;
    let mut wrong = Vec::new();

    for entry in Func::builtin_function_list() {
        if entry.func_type != "s" {
            continue;
        }
        // -1 means any number of arguments; try a few.
        let argument_counts: Vec<i32> = if entry.narg < 0 {
            vec![1, 2, 3]
        } else {
            vec![entry.narg]
        };
        // Experimental functions are for engine internals and take arguments
        // that SQL samples can't make valid; sequence_watermark_experimental(NULL)
        // never returns.
        if entry.name.ends_with("_experimental") {
            continue;
        }
        for argument_count in argument_counts {
            let Ok(Some(func)) = Func::resolve_function(&entry.name, argument_count as usize)
            else {
                continue;
            };
            let Some(expected) = expected_typeof(func.return_type()) else {
                continue;
            };
            for sample in SAMPLE_ARGUMENTS {
                let arguments = vec![sample; argument_count as usize].join(", ");
                let sql = format!("SELECT typeof({}({arguments}))", entry.name);
                // A call can be rejected for its arguments (for example
                // `zeroblob('abc')`); only calls that run are checked.
                let Ok(mut stmt) = conn.prepare(&sql) else {
                    continue;
                };
                let mut actual = None;
                let ran = stmt.run_with_row_callback(|row| {
                    actual = Some(row.get::<String>(0).unwrap());
                    Ok(())
                });
                let Some(actual) = actual.filter(|_| ran.is_ok()) else {
                    continue;
                };
                checked_calls += 1;
                if actual != expected && actual != "null" {
                    wrong.push(format!("{sql}: declared {expected}, got {actual}"));
                }
            }
        }
    }

    assert!(
        wrong.is_empty(),
        "functions whose result is not their declared type:\n{}",
        wrong.join("\n")
    );
    // Guards against the loop silently checking nothing.
    assert!(
        checked_calls > 200,
        "only {checked_calls} calls were checked"
    );
}

/// What `typeof()` says for a function declared with this type. `None` for
/// functions whose type is not a plain stored type.
fn expected_typeof(declared: Option<ReturnType>) -> Option<&'static str> {
    match declared? {
        ReturnType::Integer => Some("integer"),
        ReturnType::Real => Some("real"),
        ReturnType::Text => Some("text"),
        ReturnType::Blob => Some("blob"),
        // A custom type is stored as its base type and an array as text or
        // a blob, so typeof() can't tell whether the declaration is right.
        ReturnType::Custom(_) | ReturnType::Array => None,
    }
}
