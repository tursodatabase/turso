-- SQLRight Crash Report Collection Database Schema
-- Version: 1.0

-- Fuzzing sessions (different runs of the fuzzer)
CREATE TABLE IF NOT EXISTS fuzzing_sessions (
    session_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_path TEXT NOT NULL UNIQUE,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_scan TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    crash_count INTEGER DEFAULT 0
);

-- Unique crashes (deduplicated by content hash + signal + finding type)
CREATE TABLE IF NOT EXISTS crashes (
    crash_id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_hash TEXT NOT NULL,
    signal_number INTEGER,
    finding_type TEXT NOT NULL DEFAULT 'crash',
    sql_content TEXT NOT NULL,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    instance_count INTEGER DEFAULT 1,
    UNIQUE(content_hash, signal_number, finding_type)
);

CREATE INDEX IF NOT EXISTS idx_crashes_content_hash ON crashes(content_hash);
CREATE INDEX IF NOT EXISTS idx_crashes_signal ON crashes(signal_number);

-- Individual crash file instances (many-to-one with crashes)
CREATE TABLE IF NOT EXISTS crash_instances (
    instance_id INTEGER PRIMARY KEY AUTOINCREMENT,
    crash_id INTEGER NOT NULL,
    session_id INTEGER NOT NULL,
    file_path TEXT NOT NULL UNIQUE,
    file_name TEXT NOT NULL,
    afl_id INTEGER,
    signal_number INTEGER,
    source_id TEXT,
    sync_id TEXT,
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_size INTEGER,
    FOREIGN KEY (crash_id) REFERENCES crashes(crash_id),
    FOREIGN KEY (session_id) REFERENCES fuzzing_sessions(session_id)
);

CREATE INDEX IF NOT EXISTS idx_crash_instances_crash_id ON crash_instances(crash_id);
CREATE INDEX IF NOT EXISTS idx_crash_instances_session_id ON crash_instances(session_id);
CREATE INDEX IF NOT EXISTS idx_crash_instances_file_path ON crash_instances(file_path);

-- Crash test results against tursodb
CREATE TABLE IF NOT EXISTS crash_tests (
    test_id INTEGER PRIMARY KEY AUTOINCREMENT,
    crash_id INTEGER NOT NULL UNIQUE,
    tested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    exit_code INTEGER,
    signal_received INTEGER,
    timed_out BOOLEAN DEFAULT 0,
    stdout_output TEXT,
    stderr_output TEXT,
    classification TEXT CHECK(classification IN ('PANIC', 'PARSE_ERROR', 'SUCCESS', 'CRASH', 'TIMEOUT', 'ERROR')),
    FOREIGN KEY (crash_id) REFERENCES crashes(crash_id)
);

CREATE INDEX IF NOT EXISTS idx_crash_tests_classification ON crash_tests(classification);

-- SQLite comparison results (differential testing)
CREATE TABLE IF NOT EXISTS differential_tests (
    comparison_id INTEGER PRIMARY KEY AUTOINCREMENT,
    crash_id INTEGER NOT NULL UNIQUE,
    tested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    sqlite_exit_code INTEGER,
    sqlite_stdout TEXT,
    sqlite_stderr TEXT,
    sqlite_classification TEXT,
    is_bug BOOLEAN DEFAULT 0,
    bug_category TEXT,
    notes TEXT,
    FOREIGN KEY (crash_id) REFERENCES crashes(crash_id)
);

CREATE INDEX IF NOT EXISTS idx_differential_tests_is_bug ON differential_tests(is_bug);
CREATE INDEX IF NOT EXISTS idx_differential_tests_bug_category ON differential_tests(bug_category);

-- Processing checkpoints for idempotency
CREATE TABLE IF NOT EXISTS processing_checkpoints (
    checkpoint_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL UNIQUE,
    last_processed_file TEXT,
    last_file_mtime REAL,
    files_processed INTEGER DEFAULT 0,
    last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES fuzzing_sessions(session_id)
);

-- View: Crashes with bug information
CREATE VIEW IF NOT EXISTS v_crashes_with_bugs AS
SELECT
    c.crash_id,
    c.content_hash,
    c.signal_number,
    c.finding_type,
    c.instance_count,
    c.first_seen,
    ct.classification AS turso_classification,
    sc.sqlite_classification,
    sc.is_bug,
    sc.bug_category,
    LENGTH(c.sql_content) AS sql_length
FROM crashes c
LEFT JOIN crash_tests ct ON c.crash_id = ct.crash_id
LEFT JOIN differential_tests sc ON c.crash_id = sc.crash_id;

-- View: Bug summary statistics
CREATE VIEW IF NOT EXISTS v_bug_summary AS
SELECT
    bug_category,
    COUNT(*) AS count,
    GROUP_CONCAT(crash_id) AS crash_ids
FROM differential_tests
WHERE is_bug = 1
GROUP BY bug_category;

-- View: Session statistics
CREATE VIEW IF NOT EXISTS v_session_stats AS
SELECT
    fs.session_id,
    fs.session_path,
    fs.crash_count AS total_files,
    COUNT(DISTINCT ci.crash_id) AS unique_crashes,
    fs.last_scan
FROM fuzzing_sessions fs
LEFT JOIN crash_instances ci ON fs.session_id = ci.session_id
GROUP BY fs.session_id;
