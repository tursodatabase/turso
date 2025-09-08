-- Test large database features and page number handling
-- SQLite uses 32-bit page numbers, max 2^32 pages
-- With 4096-byte pages, max size is ~17TB
-- With 512-byte pages, max size is ~2TB
-- We'll create a smaller but representative test

-- Use smaller page size to test page number handling with less data
PRAGMA page_size = 1024;

CREATE TABLE large_db (
    id INTEGER PRIMARY KEY,
    data BLOB,
    text_data TEXT
);

-- Create a reasonable number of pages to test page number handling
-- Insert 100,000 rows with data that spans multiple pages
WITH RECURSIVE generate_series(value) AS (
    SELECT 1
    UNION ALL
    SELECT value + 1 FROM generate_series WHERE value < 100000
)
INSERT INTO large_db (id, data, text_data)
SELECT 
    value,
    randomblob(800),  -- Most of a 1024-byte page
    'Row_' || printf('%06d', value)
FROM generate_series;

-- Create sparse entries with very high IDs to test large page numbers
INSERT INTO large_db VALUES (1000000, randomblob(500), 'Sparse_1M');
INSERT INTO large_db VALUES (2000000, randomblob(500), 'Sparse_2M');
INSERT INTO large_db VALUES (3000000, randomblob(500), 'Sparse_3M');
INSERT INTO large_db VALUES (4000000, randomblob(500), 'Sparse_4M');
INSERT INTO large_db VALUES (5000000, randomblob(500), 'Sparse_5M');

-- Test maximum integer primary key (approaching 2^63-1)
INSERT INTO large_db VALUES (9223372036854775800, randomblob(100), 'Near_max_id_1');
INSERT INTO large_db VALUES (9223372036854775801, randomblob(100), 'Near_max_id_2');
INSERT INTO large_db VALUES (9223372036854775802, randomblob(100), 'Near_max_id_3');
INSERT INTO large_db VALUES (9223372036854775803, randomblob(100), 'Near_max_id_4');
INSERT INTO large_db VALUES (9223372036854775804, randomblob(100), 'Near_max_id_5');
INSERT INTO large_db VALUES (9223372036854775805, randomblob(100), 'Near_max_id_6');
INSERT INTO large_db VALUES (9223372036854775806, randomblob(100), 'Near_max_id_7');

-- Create an index to increase page count
CREATE INDEX idx_large_text ON large_db(text_data);

-- Create additional tables to spread data across more pages
CREATE TABLE large_aux1 (
    id INTEGER PRIMARY KEY,
    ref_id INTEGER REFERENCES large_db(id),
    aux_data BLOB
);

CREATE TABLE large_aux2 (
    id INTEGER PRIMARY KEY,
    ref_id INTEGER REFERENCES large_db(id),
    aux_data BLOB
);

-- Insert data into auxiliary tables
INSERT INTO large_aux1 (id, ref_id, aux_data)
SELECT 
    value,
    value,
    randomblob(700)
FROM (
    WITH RECURSIVE generate_series(value) AS (
        SELECT 1
        UNION ALL
        SELECT value + 1 FROM generate_series WHERE value < 10000
    )
    SELECT value FROM generate_series
);

INSERT INTO large_aux2 (id, ref_id, aux_data)
SELECT 
    value,
    value * 10,
    randomblob(700)
FROM (
    WITH RECURSIVE generate_series(value) AS (
        SELECT 1
        UNION ALL
        SELECT value + 1 FROM generate_series WHERE value < 10000
    )
    SELECT value FROM generate_series
);

-- Create a table with overflow pages
CREATE TABLE large_overflow (
    id INTEGER PRIMARY KEY,
    huge_data BLOB
);

-- Insert rows with large blobs that require overflow pages
INSERT INTO large_overflow VALUES (1, randomblob(10000));
INSERT INTO large_overflow VALUES (2, randomblob(20000));
INSERT INTO large_overflow VALUES (3, randomblob(30000));
INSERT INTO large_overflow VALUES (4, randomblob(40000));
INSERT INTO large_overflow VALUES (5, randomblob(50000));

-- Test page allocation near boundaries
-- Delete some data to create freelist pages
DELETE FROM large_db WHERE id % 1000 = 0 AND id <= 50000;

-- Reinsert to test freelist reuse with large page numbers
INSERT INTO large_db (id, data, text_data)
SELECT 
    value * 1000,
    randomblob(600),
    'Reinserted_' || value
FROM (
    WITH RECURSIVE generate_series(value) AS (
        SELECT 1
        UNION ALL
        SELECT value + 1 FROM generate_series WHERE value < 50
    )
    SELECT value FROM generate_series
);

-- Update statistics
ANALYZE;