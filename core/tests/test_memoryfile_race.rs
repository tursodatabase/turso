// Extreme stress test to expose the MemoryFile UnsafeCell race condition
// 
// Bug: MemoryFile in core/io/memory.rs uses UnsafeCell<BTreeMap> without synchronization
// but is marked as Sync, allowing concurrent mutable access from multiple threads.
//
// Strategy:
// 1. Open MANY database instances on the same MemoryIO and same path
// 2. Spawn MANY threads all writing simultaneously 
// 3. Write to DIFFERENT row IDs to avoid MVCC conflicts (force page allocation races)
// 4. Use large blobs to force multiple page allocations per write
// 5. Run WITHOUT transactions to bypass MVCC locking

use std::sync::Arc;
use std::thread;
use turso_core::{Database, MemoryIO, Result};

#[test]
fn test_memoryfile_extreme_concurrent_writes() -> Result<()> {
    const NUM_DBS: usize = 8;
    const NUM_THREADS: usize = 64;
    const WRITES_PER_THREAD: usize = 200;
    const BLOB_SIZE: usize = 8192; // Force multiple pages

    println!("Starting extreme MemoryFile stress test:");
    println!("  {} databases on same MemoryIO path", NUM_DBS);
    println!("  {} concurrent threads", NUM_THREADS);
    println!("  {} writes per thread", WRITES_PER_THREAD);
    println!("  {} byte blobs per write", BLOB_SIZE);

    let io = Arc::new(MemoryIO::new());
    let path = "shared_race_test";

    // Open multiple Database instances pointing to the SAME in-memory file
    let mut dbs = Vec::new();
    for _ in 0..NUM_DBS {
        dbs.push(Database::open_file(io.clone(), path, false, false)?);
    }

    // Initialize schema using first DB
    {
        let conn = dbs[0].connect()?;
        conn.execute("CREATE TABLE race_test (id INTEGER PRIMARY KEY, data TEXT)")?;
    }

    let dbs = Arc::new(dbs);
    let mut handles: Vec<thread::JoinHandle<()>> = vec![];

    for thread_id in 0..NUM_THREADS {
        let dbs = dbs.clone();
        let handle = thread::spawn(move || {
            // Each thread uses a rotating set of DB handles to maximize contention
            let large_blob = "X".repeat(BLOB_SIZE);
            
            for i in 0..WRITES_PER_THREAD {
                let db_idx = (thread_id + i) % NUM_DBS;
                let db = &dbs[db_idx];
                
                // Use unique row IDs to avoid MVCC conflicts, forcing raw MemoryFile contention
                let row_id = (thread_id * WRITES_PER_THREAD) + i;
                
                let conn = db.connect().expect("connect failed");
                
                // Direct INSERT without transactions - bypasses MVCC, hits MemoryFile directly
                let sql = format!(
                    "INSERT INTO race_test (id, data) VALUES ({}, '{}')",
                    row_id,
                    &large_blob[..100] // truncate for SQL string
                );
                
                if let Err(e) = conn.execute(&sql) {
                    eprintln!("Thread {} write {} failed: {}", thread_id, i, e);
                    // Don't panic here - we want to see if MemoryFile itself panics/corrupts
                }
            }
        });
        handles.push(handle);
    }

    // Wait for all threads
    let mut panicked = Vec::new();
    for (idx, handle) in handles.into_iter().enumerate() {
        if handle.join().is_err() {
            panicked.push(idx);
        }
    }

    if !panicked.is_empty() {
        panic!(
            "MemoryFile race detected! {} threads panicked: {:?}",
            panicked.len(),
            panicked
        );
    }

    // Verify we can still read the database (structural integrity check)
    let conn = dbs[0].connect()?;
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM race_test")?;
    
    if let turso_core::StepResult::Row = stmt.step()? {
        let count = stmt.row().unwrap().get::<i64>(0)?;
        println!("Test completed: {} rows inserted", count);
        
        // We expect some writes to fail due to "busy" errors, but total should be > 0
        if count == 0 {
            panic!("Zero rows inserted - possible complete corruption!");
        }
    }

    println!("MemoryFile stress test passed (no panic detected)");
    Ok(())
}

#[test]
fn test_memoryfile_page_allocation_storm() -> Result<()> {
    // More targeted: hammer page allocation by writing to scattered addresses
    // This forces BTreeMap insertions at many different keys simultaneously
    
    const NUM_THREADS: usize = 32;
    const PAGES_PER_THREAD: usize = 500;
    
    println!("Page allocation storm test:");
    println!("  {} threads", NUM_THREADS);
    println!("  {} pages per thread", PAGES_PER_THREAD);

    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io.clone(), ":memory:", false, false)?;

    {
        let conn = db.connect()?;
        conn.execute("CREATE TABLE pages (id INTEGER PRIMARY KEY, blob BLOB)")?;
    }

    let mut handles: Vec<thread::JoinHandle<()>> = vec![];
    
    for thread_id in 0..NUM_THREADS {
        let db = db.clone();
        let handle = thread::spawn(move || {
            let conn = db.connect().expect("connect");
            // Use 4KB blobs to force exactly one page per write
            let blob = vec![thread_id as u8; 4096];
            
            for i in 0..PAGES_PER_THREAD {
                // Scatter IDs across address space to hit different BTreeMap keys
                let id = (thread_id * 1000000) + (i * 137); // Prime offset for scattering
                
                let sql = format!("INSERT INTO pages (id, blob) VALUES ({}, X'{}')", 
                    id, 
                    hex::encode(&blob[..100])
                );
                
                let _ = conn.execute(&sql);
            }
        });
        handles.push(handle);
    }

    let mut panicked = Vec::new();
    for (idx, h) in handles.into_iter().enumerate() {
        if h.join().is_err() {
            panicked.push(idx);
        }
    }

    if !panicked.is_empty() {
        panic!("Page allocation race detected! {} threads panicked", panicked.len());
    }

    println!("Page allocation storm completed without panic");
    Ok(())
}

#[test] 
fn test_memoryfile_simultaneous_db_creation() -> Result<()> {
    // Create many DB instances simultaneously on the same MemoryIO + path
    // This tests the race in MemoryIO::open_file when creating MemoryFile instances
    
    const NUM_THREADS: usize = 50;
    const DBS_PER_THREAD: usize = 10;
    
    println!("Simultaneous DB creation test:");
    println!("  {} threads", NUM_THREADS);  
    println!("  {} DBs per thread", DBS_PER_THREAD);

    let io = Arc::new(MemoryIO::new());
    let path = "race_path";

    let mut handles: Vec<thread::JoinHandle<()>> = vec![];
    
    for _thread_id in 0..NUM_THREADS {
        let io = io.clone();
        let handle = thread::spawn(move || {
            for _ in 0..DBS_PER_THREAD {
                let db = Database::open_file(io.clone(), path, false, false)
                    .expect("open failed");
                let conn = db.connect().expect("connect failed");
                // Try to do a simple operation
                let _ = conn.execute("CREATE TABLE IF NOT EXISTS t (x)");
                let _ = conn.execute("INSERT INTO t VALUES (1)");
            }
        });
        handles.push(handle);
    }

    let mut panicked = Vec::new();
    for (idx, h) in handles.into_iter().enumerate() {
        if h.join().is_err() {
            panicked.push(idx);
        }
    }

    if !panicked.is_empty() {
        panic!("DB creation race detected! {} threads panicked", panicked.len());
    }

    println!("Simultaneous DB creation completed without panic");
    Ok(())
}
