-- Test multi-level B-tree structures
-- With default 4096-byte pages, we need many rows to force multiple B-tree levels
-- Each leaf page can hold roughly 100-200 small records
-- Interior pages can reference roughly 200-500 child pages

CREATE TABLE btree_depth (
    id INTEGER PRIMARY KEY,
    data TEXT,
    padding BLOB
);

-- Create an index to test index B-tree structure as well
CREATE INDEX idx_data ON btree_depth(data);

-- Insert enough rows to create at least a 3-level B-tree
-- Using small records first to maximize entries per page
-- This should create leaf pages -> interior pages -> root page structure

-- Insert 10,000 rows with small data
-- This should create roughly 50-100 leaf pages
WITH RECURSIVE generate_series(value) AS (
    SELECT 1
    UNION ALL
    SELECT value + 1 FROM generate_series WHERE value < 10000
)
INSERT INTO btree_depth (id, data, padding)
SELECT 
    value,
    'Data_' || printf('%05d', value),
    randomblob(50)
FROM generate_series;

-- Add some larger records to test mixed page usage
INSERT INTO btree_depth (id, data, padding)
SELECT 
    10000 + value,
    'Large_' || printf('%05d', value),
    randomblob(500)
FROM (
    WITH RECURSIVE generate_series(value) AS (
        SELECT 1
        UNION ALL
        SELECT value + 1 FROM generate_series WHERE value < 1000
    )
    SELECT value FROM generate_series
);

-- Create gaps by deleting some records (tests interior page updates)
DELETE FROM btree_depth WHERE id % 100 = 0;

-- Insert records in the gaps (tests B-tree rebalancing)
INSERT INTO btree_depth (id, data, padding)
SELECT 
    value * 100,
    'Gap_' || printf('%05d', value * 100),
    randomblob(75)
FROM (
    WITH RECURSIVE generate_series(value) AS (
        SELECT 1
        UNION ALL
        SELECT value + 1 FROM generate_series WHERE value < 100
    )
    SELECT value FROM generate_series
);

-- Add records with non-sequential IDs to test B-tree insertion algorithms
INSERT INTO btree_depth VALUES (50000, 'Out_of_order_1', randomblob(100));
INSERT INTO btree_depth VALUES (25000, 'Out_of_order_2', randomblob(100));
INSERT INTO btree_depth VALUES (75000, 'Out_of_order_3', randomblob(100));
INSERT INTO btree_depth VALUES (12500, 'Out_of_order_4', randomblob(100));
INSERT INTO btree_depth VALUES (37500, 'Out_of_order_5', randomblob(100));

-- Test very sparse IDs to stress interior pages
INSERT INTO btree_depth VALUES (100000, 'Sparse_1', randomblob(50));
INSERT INTO btree_depth VALUES (200000, 'Sparse_2', randomblob(50));
INSERT INTO btree_depth VALUES (300000, 'Sparse_3', randomblob(50));
INSERT INTO btree_depth VALUES (400000, 'Sparse_4', randomblob(50));
INSERT INTO btree_depth VALUES (500000, 'Sparse_5', randomblob(50));

-- Force index B-tree depth with duplicate data values
INSERT INTO btree_depth (id, data, padding)
SELECT 
    600000 + value,
    'Duplicate_' || printf('%03d', value % 100),  -- Only 100 unique values
    randomblob(30)
FROM (
    WITH RECURSIVE generate_series(value) AS (
        SELECT 1
        UNION ALL
        SELECT value + 1 FROM generate_series WHERE value < 5000
    )
    SELECT value FROM generate_series
);

-- Analyze to update sqlite_stat1 (tests internal statistics pages)
ANALYZE;

-- Create a second table with composite primary key for different B-tree structure
CREATE TABLE btree_composite (
    a INTEGER,
    b INTEGER,
    c TEXT,
    data BLOB,
    PRIMARY KEY (a, b)
) WITHOUT ROWID;

-- Insert data into composite key table
INSERT INTO btree_composite (a, b, c, data)
SELECT 
    value / 100,
    value % 100,
    'Composite_' || value,
    randomblob(40)
FROM (
    WITH RECURSIVE generate_series(value) AS (
        SELECT 1
        UNION ALL
        SELECT value + 1 FROM generate_series WHERE value < 5000
    )
    SELECT value FROM generate_series
);