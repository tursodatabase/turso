use std::hash::{Hash, Hasher};

use rustc_hash::{FxHashMap, FxHasher};
use smallvec::SmallVec;
use turso_parser::ast::{Expr, Literal};

use crate::translate::expr::{walk_expr, WalkControl};
use crate::Result;

use super::{LogicalPlan, Scalar};

pub(super) fn has_duplicates(predicates: &[Scalar], _: &LogicalPlan) -> Result<bool> {
    if predicates.len() < 2 {
        return Ok(false);
    }
    if predicates[0] == predicates[1] {
        return Ok(true);
    }
    if predicates.len() == 2 {
        return Ok(false);
    }
    let mut candidates: FxHashMap<u64, SmallVec<[usize; 1]>> =
        FxHashMap::with_capacity_and_hasher(predicates.len(), Default::default());
    for (index, predicate) in predicates.iter().enumerate() {
        let matches = candidates.entry(scalar_hash(predicate)?).or_default();
        if matches.iter().any(|&prior| predicates[prior] == *predicate) {
            return Ok(true);
        }
        matches.push(index);
    }
    Ok(false)
}

pub(super) fn deduplicate(mut predicates: Vec<Scalar>, _: &LogicalPlan) -> Result<Vec<Scalar>> {
    predicates.dedup();
    if predicates.len() < 3 {
        return Ok(predicates);
    }
    let mut candidates: FxHashMap<u64, SmallVec<[usize; 1]>> =
        FxHashMap::with_capacity_and_hasher(predicates.len(), Default::default());
    let mut retained = 0;
    for index in 0..predicates.len() {
        let matches = candidates
            .entry(scalar_hash(&predicates[index])?)
            .or_default();
        if matches
            .iter()
            .any(|&prior| predicates[prior] == predicates[index])
        {
            continue;
        }
        predicates.swap(retained, index);
        matches.push(retained);
        retained += 1;
    }
    predicates.truncate(retained);
    Ok(predicates)
}

fn scalar_hash(scalar: &Scalar) -> Result<u64> {
    let mut state = FxHasher::default();
    walk_expr(scalar.ast(), &mut |expr| {
        std::mem::discriminant(expr).hash(&mut state);
        match expr {
            Expr::Column { table, column, .. } => (table, column).hash(&mut state),
            Expr::RowId { table, .. } => table.hash(&mut state),
            Expr::Variable(variable) => variable.index.hash(&mut state),
            Expr::Binary(_, operator, _) => std::mem::discriminant(operator).hash(&mut state),
            Expr::Unary(operator, _) => std::mem::discriminant(operator).hash(&mut state),
            Expr::Collate(_, name) => name.hash(&mut state),
            Expr::FunctionCall { name, .. } | Expr::FunctionCallStar { name, .. } => {
                name.as_str().hash(&mut state);
            }
            Expr::Literal(literal) => {
                std::mem::discriminant(literal).hash(&mut state);
                match literal {
                    Literal::Numeric(value)
                    | Literal::String(value)
                    | Literal::Blob(value)
                    | Literal::Keyword(value) => value.hash(&mut state),
                    _ => {}
                }
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(state.finish())
}
