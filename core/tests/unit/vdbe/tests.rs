use super::*;
use std::panic::{catch_unwind, AssertUnwindSafe};

#[test]
fn program_step_conversion_preserves_error_allocation() {
    let err = Box::new(LimboError::InternalError("test error".into()));
    let original = std::ptr::from_ref(err.as_ref());
    let result: Result<StepResult, Box<LimboError>> = ProgramStep::Error(err).into();
    let returned = result.unwrap_err();
    assert_eq!(std::ptr::from_ref(returned.as_ref()), original);
    assert!(matches!(*returned, LimboError::InternalError(ref msg) if msg == "test error"));
}

#[test]
fn normal_step_preserves_execution_state_with_and_without_tracing() {
    for trace in [false, true] {
        let io = Arc::new(crate::MemoryIO::new());
        let db =
            crate::Database::open_file(io, ":memory:", Arc::new(crate::SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        conn.set_vdbe_trace(trace);
        let mut stmt = conn.prepare("SELECT 1 UNION ALL SELECT 2").unwrap();
        assert_eq!(stmt.execution_state(), ProgramExecutionState::Init);
        assert!(matches!(stmt.step().unwrap(), StepResult::Row));
        assert_eq!(stmt.execution_state(), ProgramExecutionState::Running);
        assert!(matches!(stmt.step().unwrap(), StepResult::Row));
        assert!(matches!(stmt.step().unwrap(), StepResult::Done));
        assert_eq!(stmt.execution_state(), ProgramExecutionState::Done);

        let mut stmt = conn.prepare("SELECT abs(-9223372036854775808)").unwrap();
        assert!(matches!(stmt.step(), Err(LimboError::IntegerOverflow)));
        assert_eq!(stmt.execution_state(), ProgramExecutionState::Failed);

        conn.set_progress_handler(1, Some(Box::new(|| true)));
        let mut stmt = conn.prepare("WITH RECURSIVE t(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM t WHERE x<1000) SELECT sum(x) FROM t").unwrap();
        assert!(matches!(stmt.step().unwrap(), StepResult::Interrupt));
        assert_eq!(stmt.execution_state(), ProgramExecutionState::Interrupted);
    }
}

#[test]
fn active_opcode_helpers_initialize_defaults() {
    let mut state = ProgramState::new(1, 0);

    assert!(matches!(state.active_op_state.state, ActiveOpState::None));
    assert!(matches!(
        state.active_op_state.column(),
        OpColumnState::Start
    ));
    state.active_op_state.clear();
    assert!(state.active_op_state.parse_schema().is_none());
}

#[test]
fn nth_into_register_rejects_invalid_utf8_text() {
    let payload = [2, 15, 0xff];
    let mut iterator = crate::types::ValueIterator::new(&payload).unwrap();
    let mut destination = Register::Value(Value::Null);

    let result = iterator
        .nth_into_register(0, &mut destination)
        .expect("record contains one value");

    assert!(
        matches!(
            result,
            Err(LimboError::Corrupt(ref message))
                if message == "TEXT value contains invalid UTF-8"
        ),
        "unexpected result: {result:?}"
    );
}

#[test]
fn active_opcode_helpers_reject_mismatched_resumes() {
    let mut state = ProgramState::new(1, 0);
    *state.active_op_state.column() = OpColumnState::GetColumn;

    let panic = catch_unwind(AssertUnwindSafe(|| {
        let _ = state.active_op_state.parse_schema();
    }));
    assert!(panic.is_err(), "mismatched opcode resume should panic");
}

#[test]
fn seek_state_is_independent_from_active_opcode_slot() {
    let mut state = ProgramState::new(1, 0);

    *state.active_op_state.insert() = OpInsertState {
        sub_state: OpInsertSubState::Seek,
        has_dependent_views: false,
        old_record: None,
        is_noop_update: false,
    };
    state.seek_state = OpSeekState::MoveLast;

    assert!(matches!(
        state.active_op_state.insert().sub_state,
        OpInsertSubState::Seek
    ));
    assert!(matches!(state.seek_state, OpSeekState::MoveLast));
}

#[test]
fn register_try_clone_copies_each_variant() {
    let record_values = [Value::from_i64(1), Value::build_text("record payload")];
    let aggregate_values = crate::alloc::vec![Value::build_text("aggregate payload")];
    let registers = [
        Register::Value(Value::build_text("value")),
        Register::Aggregate(AggContext::Builtin(aggregate_values)),
        Register::Record(
            ImmutableRecord::from_values(&record_values, record_values.len()).unwrap(),
        ),
    ];

    for source in registers {
        assert_eq!(source.try_clone().unwrap(), source);
    }
}

#[test]
fn register_try_clone_from_reuses_matching_allocations() {
    use crate::types::Text;

    let src = Register::Value(Value::Text(Text::new(String::from("short"))));
    let mut dst = Register::Value(Value::Text(Text::new(String::from(
        "a destination string with plenty of capacity",
    ))));
    let ptr = match &dst {
        Register::Value(Value::Text(t)) => t.as_str().as_ptr(),
        _ => unreachable!(),
    };
    dst.try_clone_from(&src).unwrap();
    assert_eq!(dst, src);
    match &dst {
        Register::Value(Value::Text(t)) => assert_eq!(t.as_str().as_ptr(), ptr),
        _ => unreachable!(),
    }

    let src_values = [Value::from_i64(1), Value::build_text("record payload")];
    let src =
        Register::Record(ImmutableRecord::from_values(&src_values, src_values.len()).unwrap());
    let large_values = [Value::build_text(
        "a much longer record payload that dwarfs the source record",
    )];
    let mut dst =
        Register::Record(ImmutableRecord::from_values(&large_values, large_values.len()).unwrap());
    let ptr = match &dst {
        Register::Record(record) => record.get_payload().as_ptr(),
        _ => unreachable!(),
    };
    dst.try_clone_from(&src).unwrap();
    assert_eq!(dst, src);
    match &dst {
        Register::Record(record) => assert_eq!(record.get_payload().as_ptr(), ptr),
        _ => unreachable!(),
    }

    let src = Register::Aggregate(AggContext::Builtin(crate::alloc::vec![
        Value::build_text("agg state"),
        Value::from_i64(2),
    ]));
    let mut dst = Register::Value(Value::Null);
    dst.try_clone_from(&src).unwrap();
    assert_eq!(dst, src);
}

#[test]
fn register_take_buf_recycles_record_allocations() {
    let values = [Value::build_text("some record payload")];
    let record = ImmutableRecord::from_values(&values, values.len()).unwrap();
    let capacity = record.as_blob().capacity();
    let ptr = record.get_payload().as_ptr();

    let mut register = Register::Record(record);
    let rebuilt = ImmutableRecord::build(&values, register.take_buf()).unwrap();
    assert!(register.is_null());
    assert_eq!(rebuilt.as_blob().capacity(), capacity);
    assert_eq!(rebuilt.get_payload().as_ptr(), ptr);
}

#[test]
fn register_try_clone_value_from_reuses_value_slot() {
    let value = Value::build_text(String::from("payload"));
    let mut register = Register::Value(Value::build_text(String::from(
        "existing buffer with plenty of capacity to reuse",
    )));
    let ptr = match &register {
        Register::Value(Value::Text(text)) => text.as_str().as_ptr(),
        _ => unreachable!(),
    };

    register.try_clone_value_from(&value).unwrap();
    assert_eq!(register, Register::Value(value));
    match &register {
        Register::Value(Value::Text(text)) => assert_eq!(text.as_str().as_ptr(), ptr),
        _ => unreachable!(),
    }
}
