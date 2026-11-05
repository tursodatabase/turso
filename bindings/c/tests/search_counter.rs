use std::ptr;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Barrier};
use turso_sqlite3::*;

#[test]
fn parallel_steps_add_the_same_search_count_as_serial_steps() {
    let serial = search_count_for(1, 2000);
    assert!(serial > 0);
    assert_eq!(search_count_for(8, 2000), serial * 8);
}

fn search_count_for(connections: usize, iterations: usize) -> i32 {
    unsafe {
        let mut handles = Vec::new();
        for _ in 0..connections {
            let mut db = ptr::null_mut();
            assert_eq!(sqlite3_open(c":memory:".as_ptr(), &mut db), SQLITE_OK);
            assert_eq!(sqlite3_exec(db, c"CREATE TABLE example(id INTEGER PRIMARY KEY, value INTEGER); INSERT INTO example VALUES(42,42); CREATE INDEX value_idx ON example(value)".as_ptr(), None, ptr::null_mut(), ptr::null_mut()), SQLITE_OK);
            let mut statement = ptr::null_mut();
            assert_eq!(
                sqlite3_prepare_v2(
                    db,
                    c"SELECT id FROM example WHERE value=42".as_ptr(),
                    -1,
                    &mut statement,
                    ptr::null_mut()
                ),
                SQLITE_OK
            );
            handles.push((db as usize, statement as usize));
        }
        let counter = &*ptr::addr_of!(sqlite3_search_count).cast::<AtomicI32>();
        counter.store(0, Ordering::Relaxed);
        let barrier = Arc::new(Barrier::new(connections));
        let threads: Vec<_> = handles
            .into_iter()
            .map(|(db, statement)| {
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let db = db as *mut sqlite3;
                    let statement = statement as *mut sqlite3_stmt;
                    barrier.wait();
                    for _ in 0..iterations {
                        assert_eq!(sqlite3_step(statement), SQLITE_ROW);
                        assert_eq!(sqlite3_column_int(statement, 0), 42);
                        assert_eq!(sqlite3_step(statement), SQLITE_DONE);
                        assert_eq!(sqlite3_reset(statement), SQLITE_OK);
                    }
                    assert_eq!(sqlite3_finalize(statement), SQLITE_OK);
                    assert_eq!(sqlite3_close(db), SQLITE_OK);
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        counter.load(Ordering::Relaxed)
    }
}
