// Input operator for DBSP-style incremental computation
// This operator serves as the entry point for data into the incremental computation pipeline

use crate::incremental::dbsp::{Delta, DeltaPair};
use crate::incremental::operator::{
    ComputationTracker, DbspStateCursors, EvalState, IncrementalOperator,
};
use crate::types::IOResult;
use crate::Result;
use std::sync::{Arc, Mutex};

/// Input operator - source of data for the circuit
/// Represents base relations/tables that receive external updates
#[derive(Debug)]
pub struct InputOperator {
    #[allow(dead_code)]
    name: String,
}

impl InputOperator {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl IncrementalOperator for InputOperator {
    fn eval(
        &mut self,
        state: &mut EvalState,
        _cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>> {
        match state {
            EvalState::Init { deltas } => {
                // Input operators only use left_delta, right_delta must be empty
                assert!(
                    deltas.right.is_empty(),
                    "InputOperator expects right_delta to be empty"
                );
                let output = std::mem::take(&mut deltas.left);
                *state = EvalState::Done;
                Ok(IOResult::Done(output))
            }
            _ => unreachable!(
                "InputOperator doesn't execute the state machine. Should be in Init state"
            ),
        }
    }

    fn commit(
        &mut self,
        deltas: DeltaPair,
        _cursors: &mut DbspStateCursors,
    ) -> Result<IOResult<Delta>> {
        // Input operator only uses left delta, right must be empty
        assert!(
            deltas.right.is_empty(),
            "InputOperator expects right delta to be empty in commit"
        );
        // Input operator passes through the delta unchanged during commit
        Ok(IOResult::Done(deltas.left))
    }

    fn set_tracker(&mut self, _tracker: Arc<Mutex<ComputationTracker>>) {
        // Input operator doesn't need tracking
    }
}
