use crate::common::TempDatabase;
use std::sync::Arc;
use turso_core::vdbe::StepResult;

fn count_explain_instructions(conn: &Arc<turso_core::Connection>, n: usize) -> usize {
    let values: String = (1..=n)
        .map(|i| format!("({})", i))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!("EXPLAIN SELECT (VALUES {values});");
    let mut stmt = conn.prepare(&sql).unwrap();
    let mut count = 0;
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => count += 1,
            StepResult::IO => stmt._io().step().unwrap(),
            StepResult::Done => break,
            other => panic!("unexpected step result: {other:?}"),
        }
    }
    count
}

#[turso_macros::test]
fn explain_values_instruction_count_scales_linearly(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    let small = count_explain_instructions(&conn, 100);
    let large = count_explain_instructions(&conn, 1000);
    // 10x more values → ~10x more instructions (linear scaling).
    // A ratio above 30x would indicate O(N²) instruction generation.
    assert!(
        large < 30 * small,
        "instruction count grew super-linearly with VALUES size: \
         {small} instructions for 100 values, {large} for 1000 values"
    );
}
