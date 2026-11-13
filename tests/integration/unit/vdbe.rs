use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use turso_core::alloc::TryClone;
use turso_core::error::LimboError;
use turso_core::security::roles::SessionRole;
use turso_core::types::ImmutableRecord;
use turso_core::types::Value;
use turso_core::vdbe::execute::OpColumnState;
use turso_core::vdbe::execute::OpInsertState;
use turso_core::vdbe::execute::OpInsertSubState;
use turso_core::vdbe::execute::OpSeekState;
use turso_core::vdbe::*;
use turso_core::ConnectOpts;
use turso_core::Result;
use turso_sqlite::SqliteDialect;

#[test]
fn change_count_increment_matches_atomic_wraparound() {
    let expected = AtomicI64::new(i64::MAX);
    expected.fetch_add(1, Ordering::Relaxed);
    let expected = expected.load(Ordering::Relaxed);

    let actual = AtomicI64::new(i64::MAX);
    bump_change_count(&actual);

    assert_eq!(actual.load(Ordering::Relaxed), expected);
}

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
        let io = Arc::new(turso_core::MemoryIO::new());
        let db = turso_core::Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).unwrap();
        let conn = db
            .connect(ConnectOpts::new(SessionRole::Superuser))
            .unwrap();
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

/// Builds a record payload holding one TEXT value of `value`.
fn one_text_record(value: &[u8]) -> Vec<u8> {
    use turso_core::storage::sqlite3_ondisk::write_varint_to_vec;
    let mut header_body = Vec::new();
    write_varint_to_vec(13 + 2 * value.len() as u64, &mut header_body);
    let mut payload = Vec::new();
    write_varint_to_vec(header_body.len() as u64 + 1, &mut payload);
    payload.extend_from_slice(&header_body);
    payload.extend_from_slice(value);
    payload
}

fn decode_one_text(payload: &[u8], destination: &mut Register) -> Result<()> {
    let mut iterator = turso_core::types::ValueIterator::new(payload)?;
    iterator
        .nth_into_register(0, destination)
        .expect("record contains one value")
}

fn register_text(destination: &Register) -> &str {
    match destination {
        Register::Value(Value::Text(text)) => text.as_str(),
        other => panic!("expected text, got {other:?}"),
    }
}

#[test]
fn text_decode_into_a_reused_register_keeps_every_length() {
    let mut destination = Register::Value(Value::Null);
    // Grow the buffer first so later values take the reuse path.
    let long: Vec<u8> = (0..80usize).map(|i| b'a' + (i % 26) as u8).collect();
    decode_one_text(&one_text_record(&long), &mut destination).unwrap();
    assert_eq!(register_text(&destination).len(), 80);

    for len in 0..=80usize {
        let value: Vec<u8> = (0..len).map(|i| b'A' + (i % 26) as u8).collect();
        decode_one_text(&one_text_record(&value), &mut destination).unwrap();
        assert_eq!(
            register_text(&destination).as_bytes(),
            value.as_slice(),
            "length {len}"
        );
    }
}

#[test]
fn text_decode_into_a_reused_register_keeps_multibyte_utf8() {
    let mut destination = Register::Value(Value::Null);
    let long: Vec<u8> = (0..80usize).map(|i| b'a' + (i % 26) as u8).collect();
    decode_one_text(&one_text_record(&long), &mut destination).unwrap();

    for value in [
        "é",
        "héllo",
        "日本語",
        "\u{1F600}",
        "aé",
        "ααααααααα",
        "ααααααααααααααααααααααααααααααααααα",
    ] {
        decode_one_text(&one_text_record(value.as_bytes()), &mut destination).unwrap();
        assert_eq!(register_text(&destination), value);
    }
}

#[test]
fn text_decode_reads_a_serial_type_that_needs_two_varint_bytes() {
    let mut destination = Register::Value(Value::Null);
    let value: Vec<u8> = (0..200usize).map(|i| b'a' + (i % 26) as u8).collect();
    decode_one_text(&one_text_record(&value), &mut destination).unwrap();
    assert_eq!(register_text(&destination).as_bytes(), value.as_slice());

    decode_one_text(&one_text_record(b"short"), &mut destination).unwrap();
    assert_eq!(register_text(&destination), "short");
}

#[test]
fn text_decode_rejects_invalid_utf8_and_leaves_the_register_usable() {
    let mut destination = Register::Value(Value::Null);
    let long: Vec<u8> = (0..80usize).map(|i| b'a' + (i % 26) as u8).collect();
    decode_one_text(&one_text_record(&long), &mut destination).unwrap();

    let short_invalid = [0xff, 0xfe];
    let long_invalid = [0xff; 20];
    for invalid in [&short_invalid[..], &long_invalid[..]] {
        let result = decode_one_text(&one_text_record(invalid), &mut destination);
        assert!(
            matches!(
                result,
                Err(LimboError::Corrupt(ref message))
                    if message == "TEXT value contains invalid UTF-8"
            ),
            "unexpected result: {result:?}"
        );
        let _ = register_text(&destination);
    }
    decode_one_text(&one_text_record(b"ok"), &mut destination).unwrap();
    assert_eq!(register_text(&destination), "ok");
}

#[test]
fn nth_into_register_rejects_invalid_utf8_text() {
    let payload = [2, 15, 0xff];
    let mut iterator = turso_core::types::ValueIterator::new(&payload).unwrap();
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
        route: None,
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
fn register_try_clone_copies_values_and_records() {
    let record_values = [Value::from_i64(1), Value::build_text("record payload")];
    let registers = [
        Register::Value(Value::build_text("value")),
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
    use turso_core::types::Text;

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
