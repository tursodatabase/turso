//! Literal operator - produces constant rows for EmptyRelation and Values

use crate::incremental::dbsp::{Delta, DeltaPair, HashableRow};
use crate::incremental::operator::{
    ComputationTracker, DbspStateCursors, EvalState, IncrementalOperator,
};
use crate::sync::Mutex;
use crate::types::{IOResult, Value};
use crate::Result;
use std::sync::Arc;

#[derive(Debug)]
pub struct LiteralOperator {
    rows: Vec<Vec<Value>>,
}

impl LiteralOperator {
    pub fn new(rows: Vec<Vec<Value>>) -> Self {
        Self { rows }
    }

    pub fn single_empty_row() -> Self {
        Self::new(vec![vec![]])
    }

    fn to_delta(&self) -> Delta {
        let mut delta = Delta::new();
        for (i, row) in self.rows.iter().enumerate() {
            delta
                .changes
                .push((HashableRow::new((i + 1) as i64, row.clone()), 1));
        }
        delta
    }
}

impl IncrementalOperator for LiteralOperator {
    fn eval(
        &mut self,
        state: &mut EvalState,
        _cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>> {
        match state {
            EvalState::Init { .. } => {
                *state = EvalState::Done;
                Ok(IOResult::Done(self.to_delta()))
            }
            _ => unreachable!("LiteralOperator should be in Init state"),
        }
    }

    fn commit(
        &mut self,
        _deltas: DeltaPair,
        _cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>> {
        Ok(IOResult::Done(self.to_delta()))
    }

    fn set_tracker(&mut self, _tracker: Arc<Mutex<ComputationTracker>>) {}

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}
