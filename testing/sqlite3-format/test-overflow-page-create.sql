CREATE TABLE overflow_test (
    id INTEGER PRIMARY KEY,
    small_data TEXT,
    large_data BLOB
);

-- SQLite's default page size is 4096 bytes
-- After headers and other overhead, approximately 3900 bytes available per page
-- To force overflow pages, we need payloads larger than this

-- Test 1: Just under the threshold (should fit in one page)
INSERT INTO overflow_test VALUES (1, 'Small', randomblob(3000));

-- Test 2: Just over threshold (requires 1 overflow page)
INSERT INTO overflow_test VALUES (2, 'Medium', randomblob(4000));

-- Test 3: Multiple overflow pages
INSERT INTO overflow_test VALUES (3, 'Large', randomblob(10000));

-- Test 4: Many overflow pages
INSERT INTO overflow_test VALUES (4, 'Very Large', randomblob(50000));

-- Test 5: Edge case - exactly at page boundary (approximately)
INSERT INTO overflow_test VALUES (5, 'Boundary', randomblob(3900));

-- Test 6: Multiple records with overflow to test freelist interaction
INSERT INTO overflow_test VALUES (6, 'Test6', randomblob(5000));
INSERT INTO overflow_test VALUES (7, 'Test7', randomblob(5000));
INSERT INTO overflow_test VALUES (8, 'Test8', randomblob(5000));
INSERT INTO overflow_test VALUES (9, 'Test9', randomblob(5000));
INSERT INTO overflow_test VALUES (10, 'Test10', randomblob(5000));

-- Test 7: Delete some records to create gaps in overflow chain
DELETE FROM overflow_test WHERE id IN (7, 9);

-- Test 8: Insert new records that might reuse overflow pages
INSERT INTO overflow_test VALUES (11, 'Reuse1', randomblob(4500));
INSERT INTO overflow_test VALUES (12, 'Reuse2', randomblob(4500));

-- Test 9: Very large TEXT (not just BLOB)
INSERT INTO overflow_test VALUES (13, 'Text overflow', 
    'Lorem ipsum dolor sit amet, consectetur adipiscing elit. ' ||
    'Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. ' ||
    substr(quote(randomblob(10000)), 4, 20000));

-- Test 10: NULL in large_data column
INSERT INTO overflow_test VALUES (14, 'NULL test', NULL);

-- Test 11: Empty blob vs NULL
INSERT INTO overflow_test VALUES (15, 'Empty blob', x'');

-- Test 12: Maximum inline vs overflow threshold
-- The first 'usable_size - 35' bytes are stored directly, rest in overflow
-- Testing exact boundary conditions
INSERT INTO overflow_test VALUES (16, 'Threshold-1', randomblob(3864));  
INSERT INTO overflow_test VALUES (17, 'Threshold', randomblob(3865));
INSERT INTO overflow_test VALUES (18, 'Threshold+1', randomblob(3866));

-- Test 13: Largest practical blob (multiple overflow pages)
INSERT INTO overflow_test VALUES (19, 'Huge', randomblob(100000));

-- Test 14: Mix of small and large in same page
INSERT INTO overflow_test VALUES (20, 'A', randomblob(100));
INSERT INTO overflow_test VALUES (21, 'B', randomblob(10000));
INSERT INTO overflow_test VALUES (22, 'C', randomblob(100));
INSERT INTO overflow_test VALUES (23, 'D', randomblob(10000));
INSERT INTO overflow_test VALUES (24, 'E', randomblob(100));