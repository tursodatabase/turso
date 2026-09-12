use super::*;
use crate::sync::Arc;
use crate::thread;
use crate::types::Value;

/// Creates a minimal ProgramState for testing.
fn create_test_state(num_registers: usize, num_cursors: usize) -> ProgramState {
    ProgramState::new(num_registers, num_cursors)
}

/// Test that ProgramState can be safely sent between threads.
/// This validates the `unsafe impl Send for ProgramState` claim.
#[test]
fn shuttle_program_state_send() {
    shuttle::check_random(
        || {
            let mut state = create_test_state(10, 2);

            // Write some data to registers
            state.registers[0].set_int(42);
            state.registers[1].set_text(Text::new("test".to_string()));

            // Send state to another thread
            let handle = thread::spawn(move || {
                // Verify data is intact after send
                assert!(matches!(
                    &state.registers[0],
                    Register::Value(Value::Numeric(Numeric::Integer(42)))
                ));
                if let Register::Value(Value::Text(t)) = &state.registers[1] {
                    assert_eq!(t.as_str(), "test");
                } else {
                    panic!("Expected text value");
                }

                // Modify in new thread
                state.registers[2].set_int(100);
                state
            });

            let state = handle.join().unwrap();
            assert!(matches!(
                &state.registers[2],
                Register::Value(Value::Numeric(Numeric::Integer(100)))
            ));
        },
        1000,
    );
}

/// Test that ProgramState with a set result_row can be safely sent.
/// The Row contains a raw pointer that must remain valid after the send.
#[test]
fn shuttle_program_state_send_with_row() {
    shuttle::check_random(
        || {
            let mut state = create_test_state(10, 2);

            // Set up registers with test data
            state.registers[0].set_int(1);
            state.registers[1].set_int(2);
            state.registers[2].set_int(3);

            // Create a result_row pointing to registers
            state.result_row = Some(Row {
                values: &state.registers[0] as *const Register,
                count: 3,
            });

            // Send to another thread - the pointer must remain valid
            // because it points to memory owned by state (the registers Vec)
            let handle = thread::spawn(move || {
                // The row pointer should still be valid because registers moved with state
                if let Some(row) = &state.result_row {
                    assert_eq!(row.len(), 3);
                    // Read through the pointer - this validates the pointer is still valid
                    let val = row.get::<i64>(0).unwrap();
                    assert_eq!(val, 1);
                    let val = row.get::<i64>(1).unwrap();
                    assert_eq!(val, 2);
                    let val = row.get::<i64>(2).unwrap();
                    assert_eq!(val, 3);
                } else {
                    panic!("Expected result_row to be set");
                }
                state
            });

            let _ = handle.join().unwrap();
        },
        1000,
    );
}

/// Test concurrent reads of result_row through shared reference.
/// This validates the `unsafe impl Sync for ProgramState` claim for read access.
#[test]
fn shuttle_program_state_sync_concurrent_reads() {
    shuttle::check_random(
        || {
            let mut state = create_test_state(10, 2);

            // Set up registers
            state.registers[0].set_int(42);
            state.registers[1].set_int(43);

            // Create result_row
            state.result_row = Some(Row {
                values: &state.registers[0] as *const Register,
                count: 2,
            });

            let state = Arc::new(state);
            let state2 = Arc::clone(&state);
            let state3 = Arc::clone(&state);

            // Multiple threads reading concurrently
            let h1 = thread::spawn(move || {
                if let Some(row) = &state.result_row {
                    let val = row.get::<i64>(0).unwrap();
                    assert_eq!(val, 42);
                }
            });

            let h2 = thread::spawn(move || {
                if let Some(row) = &state2.result_row {
                    let val = row.get::<i64>(1).unwrap();
                    assert_eq!(val, 43);
                }
            });

            let h3 = thread::spawn(move || {
                if let Some(row) = &state3.result_row {
                    assert_eq!(row.len(), 2);
                }
            });

            h1.join().unwrap();
            h2.join().unwrap();
            h3.join().unwrap();
        },
        1000,
    );
}

/// Test that Row values read through the pointer are consistent.
/// Multiple threads reading the same row values should see the same data.
#[test]
fn shuttle_row_pointer_consistency() {
    shuttle::check_random(
        || {
            let mut state = create_test_state(10, 2);

            // Set up registers with distinct values
            for i in 0..5 {
                state.registers[i].set_int(i as i64 * 10);
            }

            state.result_row = Some(Row {
                values: &state.registers[0] as *const Register,
                count: 5,
            });

            let state = Arc::new(state);
            let mut handles = vec![];

            for _ in 0..4 {
                let state_clone = Arc::clone(&state);
                let h = thread::spawn(move || {
                    if let Some(row) = &state_clone.result_row {
                        // All threads should see the same values
                        for i in 0..5 {
                            let val = row.get::<i64>(i).unwrap();
                            assert_eq!(val, i as i64 * 10);
                        }
                    }
                });
                handles.push(h);
            }

            for h in handles {
                h.join().unwrap();
            }
        },
        1000,
    );
}

/// Test the result_row invalidation pattern.
/// When result_row is taken (invalidated), concurrent reads should not see stale data.
/// This simulates the pattern used in `normal_step()` where `result_row.take()` is called.
#[test]
fn shuttle_result_row_invalidation() {
    shuttle::check_random(
        || {
            let mut state = create_test_state(10, 2);

            state.registers[0].set_int(100);
            state.result_row = Some(Row {
                values: &state.registers[0] as *const Register,
                count: 1,
            });

            // Simulate the invalidation pattern from normal_step
            // In real code, this requires &mut self, so there's no concurrent access
            let taken_row = state.result_row.take();

            // After take(), result_row should be None
            assert!(state.result_row.is_none());

            // The taken row still holds valid data (until dropped)
            if let Some(row) = taken_row {
                let val = row.get::<i64>(0).unwrap();
                assert_eq!(val, 100);
            }
        },
        1000,
    );
}

/// Test register modification after row invalidation.
/// This validates that modifying registers after take() is safe.
#[test]
fn shuttle_register_modification_after_invalidation() {
    shuttle::check_random(
        || {
            let mut state = create_test_state(10, 2);

            state.registers[0].set_int(1);
            state.result_row = Some(Row {
                values: &state.registers[0] as *const Register,
                count: 1,
            });

            // Invalidate row (simulating what normal_step does)
            let _ = state.result_row.take();

            // Now safe to modify registers
            state.registers[0].set_int(999);

            // Create new row pointing to modified registers
            state.result_row = Some(Row {
                values: &state.registers[0] as *const Register,
                count: 1,
            });

            // New row should see new value
            if let Some(row) = &state.result_row {
                let val = row.get::<i64>(0).unwrap();
                assert_eq!(val, 999);
            }
        },
        1000,
    );
}

/// Test sequential send-receive pattern (simulating async task scheduling).
/// ProgramState is moved between threads in a producer-consumer pattern.
#[test]
fn shuttle_sequential_thread_transfer() {
    shuttle::check_random(
        || {
            let mut state = create_test_state(10, 2);
            state.registers[0].set_int(0);

            // Thread 1: increment
            let h1 = thread::spawn(move || {
                if let Register::Value(Value::Numeric(Numeric::Integer(v))) = &state.registers[0] {
                    state.registers[0].set_int(v + 1);
                }
                state
            });

            let mut state = h1.join().unwrap();

            // Thread 2: increment
            let h2 = thread::spawn(move || {
                if let Register::Value(Value::Numeric(Numeric::Integer(v))) = &state.registers[0] {
                    state.registers[0].set_int(v + 1);
                }
                state
            });

            let mut state = h2.join().unwrap();

            // Thread 3: increment
            let h3 = thread::spawn(move || {
                if let Register::Value(Value::Numeric(Numeric::Integer(v))) = &state.registers[0] {
                    state.registers[0].set_int(v + 1);
                }
                state
            });

            let state = h3.join().unwrap();

            // Final value should be 3
            assert!(matches!(
                &state.registers[0],
                Register::Value(Value::Numeric(Numeric::Integer(3)))
            ));
        },
        1000,
    );
}

/// Test that ProgramState can be wrapped in Arc for shared ownership.
/// This is the typical pattern for concurrent database operations.
#[test]
fn shuttle_arc_wrapped_state() {
    shuttle::check_random(
        || {
            let mut state = create_test_state(10, 2);

            // Initialize with test data
            for i in 0..5 {
                state.registers[i].set_int(i as i64);
            }

            let state = Arc::new(state);
            let mut handles = vec![];

            // Multiple threads reading registers through Arc
            for thread_id in 0u8..4 {
                let state_clone = Arc::clone(&state);
                let h = thread::spawn(move || {
                    // Each thread reads all registers
                    for i in 0..5 {
                        if let Register::Value(Value::Numeric(Numeric::Integer(v))) =
                            &state_clone.registers[i]
                        {
                            assert_eq!(*v, i as i64);
                        }
                    }
                    thread_id
                });
                handles.push(h);
            }

            for h in handles {
                h.join().unwrap();
            }
        },
        1000,
    );
}

/// Test Row::get_values iterator under concurrent access.
#[test]
fn shuttle_row_get_values_concurrent() {
    shuttle::check_random(
        || {
            let mut state = create_test_state(10, 2);

            state.registers[0].set_int(10);
            state.registers[1].set_int(20);
            state.registers[2].set_int(30);

            state.result_row = Some(Row {
                values: &state.registers[0] as *const Register,
                count: 3,
            });

            let state = Arc::new(state);
            let state2 = Arc::clone(&state);

            let h1 = thread::spawn(move || {
                if let Some(row) = &state.result_row {
                    let values: Vec<_> = row.get_values().collect();
                    assert_eq!(values.len(), 3);
                }
            });

            let h2 = thread::spawn(move || {
                if let Some(row) = &state2.result_row {
                    let mut sum = 0i64;
                    for val in row.get_values() {
                        if let Value::Numeric(Numeric::Integer(i)) = val {
                            sum += i;
                        }
                    }
                    assert_eq!(sum, 60); // 10 + 20 + 30
                }
            });

            h1.join().unwrap();
            h2.join().unwrap();
        },
        1000,
    );
}

/// Stress test: Many threads reading from shared ProgramState.
#[test]
fn shuttle_stress_concurrent_reads() {
    shuttle::check_random(
        || {
            let mut state = create_test_state(20, 2);

            // Fill registers with identifiable data
            for i in 0..20 {
                state.registers[i].set_int(i as i64 * 100);
            }

            state.result_row = Some(Row {
                values: &state.registers[0] as *const Register,
                count: 20,
            });

            let state = Arc::new(state);
            let mut handles = vec![];

            for thread_id in 0..6u8 {
                let state_clone = Arc::clone(&state);
                let h = thread::spawn(move || {
                    // Each thread reads different parts
                    let start = (thread_id as usize * 3) % 20;
                    if let Some(row) = &state_clone.result_row {
                        for i in 0..3 {
                            let idx = (start + i) % row.len();
                            let val = row.get::<i64>(idx).unwrap();
                            assert_eq!(val, idx as i64 * 100);
                        }
                    }
                    thread_id
                });
                handles.push(h);
            }

            for h in handles {
                h.join().unwrap();
            }
        },
        1000,
    );
}
