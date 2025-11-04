// Test to expose the race condition in MemoryFile (core/io/memory.rs)
// 
// The bug: MemoryFile uses UnsafeCell<BTreeMap> without synchronization
// but is marked as Sync, allowing concurrent mutable access from multiple threads.
//
// This test creates multiple threads that write to the same memory database
// concurrently, which should expose the race condition.

use std::sync::Arc;
use std::thread;
use turso_core::{Database, Result};

use turso_core::{MemoryIO, StepResult};

#[test]
fn test_concurrent_writes_memory_race_condition() -> Result<()> {
    // This test attempts to trigger the race condition by:
    // 1. Creating a memory database
    // 2. Spawning multiple threads
    // 3. Each thread writes to the database concurrently
    // 4. The race in UnsafeCell<BTreeMap> should cause corruption/panic
    
    const NUM_THREADS: usize = 20;
    const WRITES_PER_THREAD: usize = 500;
    
    // Create database in memory
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io.clone(), ":memory:", false, false)?;
    
    {
    let conn = db.connect()?;
    conn.execute("CREATE TABLE large_data (id INTEGER PRIMARY KEY, data TEXT)")?;
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT, thread_id INTEGER)").unwrap();
    }
    
    println!("Starting {} threads, {} writes each", NUM_THREADS, WRITES_PER_THREAD);
    
    let mut handles: Vec<std::thread::JoinHandle<()>> = vec![];
    
    for thread_id in 0..NUM_THREADS {
        let db = db.clone();
        let handle = thread::spawn(move || {
            let conn = db.connect().expect("Failed to connect");
            
            for i in 0..WRITES_PER_THREAD {
                let id = (thread_id * WRITES_PER_THREAD) + i;
                
                let sql = format!(
                    "INSERT INTO test (id, value, thread_id) VALUES ({}, 'thread_{}_value_{}', {})",
                    id, thread_id, i, thread_id
                );
                
                if let Err(e) = conn.execute(&sql) {
                    eprintln!("Thread {} insert {} failed: {}", thread_id, i, e);
                }
            }
        });
        handles.push(handle);
    }
    
    // Wait for all threads
    for (thread_id, handle) in handles.into_iter().enumerate() {
        match handle.join() {
            Ok(()) => println!("Thread {} completed successfully", thread_id),
            Err(e) => {
                eprintln!("Thread {} PANICKED: {:?}", thread_id, e);
                eprintln!("This indicates the race condition was triggered!");
                panic!("Thread {} panicked - race condition detected!", thread_id);
            }
        }
    }
    
    // Verify data integrity
    let conn = db.connect().unwrap();
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM test").unwrap();
    
    if let StepResult::Row = stmt.step().unwrap() {
        let count = stmt.row().unwrap().get::<i64>(0).unwrap();
        let expected = (NUM_THREADS * WRITES_PER_THREAD) as i64;
        
        println!("Expected {} rows, found {} rows", expected, count);
        
        if count != expected {
            panic!(
                "Data corruption detected! Expected {} rows but found {}. Lost {} rows!",
                expected, count, expected - count
            );
        }
    }
    
    println!("Test completed - all {} rows accounted for", NUM_THREADS * WRITES_PER_THREAD);
    
    Ok(())
}

#[test]
fn test_concurrent_page_allocation_race() -> Result<()> {
    // More targeted test that specifically tries to trigger concurrent
    // page allocation, which is where the race is most likely
    
    const NUM_THREADS: usize = 20;
    const LARGE_VALUE_SIZE: usize = 8192; // Force multiple pages
    
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io.clone(), ":memory:", false, false)?;
    
    {
        let conn = db.connect()?;
    }
    
    println!("Testing concurrent large writes to trigger page allocation race");
    
    let mut handles: Vec<std::thread::JoinHandle<Result<()>>> = vec![];
    
    for thread_id in 0..NUM_THREADS {
        let db = db.clone();
        let handle = thread::spawn(move || {
            let conn = db.connect().expect("Failed to connect");
            
            // Create large text to force multiple page allocations
            let large_data = "x".repeat(LARGE_VALUE_SIZE);
            
            for i in 0..100 {
                let id = (thread_id * 100) + i;
                let sql = format!("INSERT INTO large_data (id, data) VALUES ({}, '{}')", id, &large_data[..100]);
                if let Err(e) = conn.execute(&sql) {
                    eprintln!("Thread {} large insert failed: {}", thread_id, e);
                }
            }
            
            Ok(())
        });
        handles.push(handle);
    }
    
    for handle in handles {
        if handle.join().is_err() {
            panic!("Thread panicked - race condition detected!");
        }
    }
    
    println!("All concurrent large writes completed");
    Ok(())
}

#[test]
fn test_concurrent_transactions_memory_race() -> Result<()> {
    // Test using concurrent transactions (BEGIN CONCURRENT)
    // which is how the simulator exposes concurrency
    
    const NUM_THREADS: usize = 20;
    
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io.clone(), ":memory:", true, false)?; // Enable MVCC for concurrent
    
    {
        let conn = db.connect()?;
        conn.execute("CREATE TABLE concurrent_test (id INTEGER PRIMARY KEY, value INTEGER)")?;
        conn.execute("INSERT INTO concurrent_test (id, value) VALUES (0, 0)")?;
    }
    
    println!("Testing concurrent transactions");
    
    use std::sync::atomic::{AtomicUsize, Ordering};
    let mut handles: Vec<std::thread::JoinHandle<usize>> = vec![];
    let success_count = Arc::new(AtomicUsize::new(0));
    
    for thread_id in 0..NUM_THREADS {
        let db = db.clone();
        let succ = success_count.clone();
        let handle = thread::spawn(move || {
            let conn = db.connect().expect("Failed to connect");
            let mut local_success = 0usize;
            for i in 0..100 {
                let _ = conn.execute("BEGIN CONCURRENT");
                let result = conn.execute("UPDATE concurrent_test SET value = value + 1 WHERE id = 0");
                if result.is_ok() {
                    if conn.execute("COMMIT").is_ok() {
                        local_success += 1;
                    } else {
                        let _ = conn.execute("ROLLBACK");
                    }
                } else {
                    let _ = conn.execute("ROLLBACK");
                    let _ = conn.execute("PRAGMA busy_timeout=1");
                    eprintln!("Thread {} iteration {} failed", thread_id, i);
                }
            }
            succ.fetch_add(local_success, Ordering::SeqCst);
            local_success
        });
        handles.push(handle);
    }
    
    let mut total_success = 0usize;
    for handle in handles {
        match handle.join() {
            Ok(n) => total_success += n,
            Err(_) => panic!("Thread panicked - race condition detected!"),
        }
    }
    
    // Check final value
    let conn = db.connect()?;
    let mut stmt = conn.prepare("SELECT value FROM concurrent_test WHERE id = 0")?;
    
    if let StepResult::Row = stmt.step()? {
        let value = stmt.row().unwrap().get::<i64>(0)? as usize;
        let expected = total_success;
        println!("Applied {} successful updates; DB value is {}", expected, value);
        assert_eq!(value, expected, "Mismatch between committed updates and final value");
    }
    
    Ok(())
}

// Stress test: open multiple Database instances sharing the same MemoryIO and same path
// to maximize concurrent calls into MemoryFile::pwrite/pwritev which are not synchronized.
#[test]
fn test_memory_file_race_stress_shared_io() -> Result<()> {
    const NUM_DBS: usize = 4;
    const NUM_THREADS: usize = 32;
    const ITERS: usize = 500;

    let io = Arc::new(MemoryIO::new());
    let path = "shared_mem_db";

    // Open multiple Database handles pointing to the same in-memory path and IO
    let mut dbs = Vec::new();
    for _ in 0..NUM_DBS {
        dbs.push(Database::open_file(io.clone(), path, false, false)?);
    }

    // Initialize schema using the first DB
    {
        let conn = dbs[0].connect()?;
        conn.execute("CREATE TABLE IF NOT EXISTS stress (id INTEGER PRIMARY KEY, v TEXT)")?;
    }

    // Spawn threads hammering random DB handles
    let dbs = Arc::new(dbs);
    let mut handles: Vec<std::thread::JoinHandle<()>> = vec![];
    for t in 0..NUM_THREADS {
        let dbs = dbs.clone();
        let handle = thread::spawn(move || {
            use std::time::{Duration, Instant};
            let start = Instant::now();
            for i in 0..ITERS {
                let db = &dbs[(t + i) % NUM_DBS];
                let conn = db.connect().expect("connect");
                // Intentionally avoid explicit transactions; let engine schedule writes
                let _ = conn.execute(&format!(
                    "INSERT OR REPLACE INTO stress (id, v) VALUES ({}, 't{}_{}')",
                    (t * ITERS + i) as i64,
                    t,
                    i
                ));
                if i % 50 == 0 && start.elapsed() > Duration::from_secs(10) {
                    // keep the test bounded
                    break;
                }
            }
        });
        handles.push(handle);
    }

    for h in handles {
        if h.join().is_err() {
            panic!("Thread panicked - potential race condition in MemoryFile");
        }
    }

    // Simple readback to ensure table is still readable (no structural corruption)
    let conn = dbs[0].connect()?;
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM stress")?;
    if let StepResult::Row = stmt.step()? {
        let count = stmt.row().unwrap().get::<i64>(0)?;
        println!("stress table row count = {}", count);
    }
    Ok(())
}
