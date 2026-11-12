use super::model::*;
use hegel::{generators as gs, TestCase};

/// The most values the workload takes from one sequence.
const MAX_NEXTVAL_CALLS: usize = 3;
/// How far from its first value a sequence with bounds may start, so the
/// calls stay within the bounds without CYCLE.
const START_RANGE: i64 = 50;
/// The smallest distance between the bounds of a sequence with bounds.
const MIN_BOUND_RANGE: i64 = 100;

/// A sequence and the statements that use it: some nextval calls and,
/// optionally, a setval last. The values stay within the sequence's bounds,
/// so every statement succeeds on PostgreSQL.
#[hegel::composite]
pub(super) fn sequence_statements(tc: TestCase, index: usize) -> Vec<Statement> {
    let magnitude = tc.draw(gs::integers::<i64>().min_value(1).max_value(3));
    let increment = if tc.draw(gs::booleans()) {
        magnitude
    } else {
        -magnitude
    };
    let bounds = tc.draw(gs::optional(
        gs::integers::<i64>().min_value(-1000).max_value(1000),
    ));
    let bounds = bounds.map(|min| {
        let width = tc.draw(
            gs::integers::<i64>()
                .min_value(MIN_BOUND_RANGE)
                .max_value(10 * MIN_BOUND_RANGE),
        );
        (min, min + width)
    });
    // PostgreSQL's defaults: an ascending sequence counts up from 1, a
    // descending one down from -1.
    let (min, max) = bounds.unwrap_or(if increment > 0 {
        (1, i64::MAX)
    } else {
        (i64::MIN, -1)
    });
    let first = if increment > 0 { min } else { max };
    let start = tc.draw(gs::optional(
        gs::integers::<i64>().min_value(0).max_value(START_RANGE),
    ));
    let start = start.map(|offset| {
        if increment > 0 {
            first + offset
        } else {
            first - offset
        }
    });
    let name = Identifier::numbered("sequence", index);
    let mut statements = vec![Statement::CreateSequence(Sequence {
        name: name.clone(),
        increment,
        bounds,
        start,
        cycle: tc.draw(gs::booleans()),
    })];
    let calls = tc.draw(gs::integers::<usize>().max_value(MAX_NEXTVAL_CALLS));
    statements.extend((0..calls).map(|_| Statement::NextVal(name.clone())));
    if tc.draw(gs::booleans()) {
        let offset = tc.draw(
            gs::integers::<i64>()
                .min_value(0)
                .max_value(MIN_BOUND_RANGE),
        );
        statements.push(Statement::SetVal {
            sequence: name,
            value: if increment > 0 {
                min + offset
            } else {
                max - offset
            },
            is_called: tc.draw(gs::booleans()),
        });
    }
    statements
}
