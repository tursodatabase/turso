CREATE TABLE types_test (
    i INTEGER PRIMARY KEY,
    r REAL,
    t TEXT,
    b BLOB,
    n NUMERIC,
    -- No affinity
    x
);

-- Test INTEGER storage classes (1, 2, 3, 4, 6, 8 bytes)
INSERT INTO types_test VALUES (0, 0.0, '0', x'00', 0, 0);
INSERT INTO types_test VALUES (1, 1.0, '1', x'01', 1, 1);
INSERT INTO types_test VALUES (127, 127.0, '127', x'7F', 127, 127);  -- max 1-byte
INSERT INTO types_test VALUES (-128, -128.0, '-128', x'80', -128, -128);  -- min 1-byte
INSERT INTO types_test VALUES (32767, 32767.0, '32767', x'7FFF', 32767, 32767);  -- max 2-byte
INSERT INTO types_test VALUES (-32768, -32768.0, '-32768', x'8000', -32768, -32768);  -- min 2-byte
INSERT INTO types_test VALUES (8388607, 8388607.0, '8388607', x'7FFFFF', 8388607, 8388607);  -- max 3-byte
INSERT INTO types_test VALUES (-8388608, -8388608.0, '-8388608', x'800000', -8388608, -8388608);  -- min 3-byte
INSERT INTO types_test VALUES (2147483647, 2147483647.0, '2147483647', x'7FFFFFFF', 2147483647, 2147483647);  -- max 4-byte
INSERT INTO types_test VALUES (-2147483648, -2147483648.0, '-2147483648', x'80000000', -2147483648, -2147483648);  -- min 4-byte
INSERT INTO types_test VALUES (140737488355327, 140737488355327.0, '140737488355327', x'7FFFFFFFFFFF', 140737488355327, 140737488355327);  -- max 6-byte
INSERT INTO types_test VALUES (-140737488355328, -140737488355328.0, '-140737488355328', x'800000000000', -140737488355328, -140737488355328);  -- min 6-byte
INSERT INTO types_test VALUES (9223372036854775807, 9223372036854775807.0, '9223372036854775807', x'7FFFFFFFFFFFFFFF', 9223372036854775807, 9223372036854775807);  -- max 8-byte
INSERT INTO types_test VALUES (-9223372036854775808, -9223372036854775808.0, '-9223372036854775808', x'8000000000000000', -9223372036854775808, -9223372036854775808);  -- min 8-byte

-- Test REAL values (some will be stored as integers due to affinity)
INSERT INTO types_test VALUES (NULL, 3.14159, 'pi', x'40490FDB', 3.14159, 3.14159);
INSERT INTO types_test VALUES (NULL, 2.71828, 'e', x'402DF854', 2.71828, 2.71828);
INSERT INTO types_test VALUES (NULL, 0.1, '0.1', x'3FB999999999999A', 0.1, 0.1);
INSERT INTO types_test VALUES (NULL, -0.0, '-0.0', x'8000000000000000', -0.0, -0.0);
INSERT INTO types_test VALUES (NULL, 1.23e45, '1.23e45', NULL, 1.23e45, 1.23e45);
INSERT INTO types_test VALUES (NULL, 1.7976931348623157e308, 'max double', NULL, 1.7976931348623157e308, 1.7976931348623157e308);
INSERT INTO types_test VALUES (NULL, 2.2250738585072014e-308, 'min positive double', NULL, 2.2250738585072014e-308, 2.2250738585072014e-308);

-- Test NULL values
INSERT INTO types_test VALUES (NULL, NULL, NULL, NULL, NULL, NULL);

-- Test TEXT with different lengths (serial types 13+)
INSERT INTO types_test VALUES (NULL, NULL, '', NULL, '', '');  -- empty string
INSERT INTO types_test VALUES (NULL, NULL, 'a', NULL, 'a', 'a');  -- 1 byte
INSERT INTO types_test VALUES (NULL, NULL, 'Hello, World!', NULL, 'Hello', 'Hello');  -- 13 bytes
INSERT INTO types_test VALUES (NULL, NULL, 'The quick brown fox jumps over the lazy dog', NULL, 42, 'Mixed');  -- 43 bytes

-- Test BLOB data (serial types 12+)
INSERT INTO types_test VALUES (NULL, NULL, 'blob', x'', NULL, x'');  -- empty blob
INSERT INTO types_test VALUES (NULL, NULL, 'blob', x'DEADBEEF', NULL, x'DEADBEEF');
INSERT INTO types_test VALUES (NULL, NULL, 'blob', randomblob(16), NULL, randomblob(16));
INSERT INTO types_test VALUES (NULL, NULL, 'blob', randomblob(100), NULL, randomblob(100));
INSERT INTO types_test VALUES (NULL, NULL, 'blob', randomblob(1000), NULL, randomblob(1000));

-- Test UTF-8 text
INSERT INTO types_test VALUES (NULL, NULL, 'Hello 世界', NULL, NULL, 'Unicode');
INSERT INTO types_test VALUES (NULL, NULL, '🚀🌟🎉', NULL, NULL, 'Emoji');
INSERT INTO types_test VALUES (NULL, NULL, 'Café', NULL, NULL, 'Accented');

-- Test numeric strings stored as TEXT (NUMERIC affinity)
INSERT INTO types_test VALUES (NULL, NULL, '123', NULL, '123', '123');  -- stored as integer in NUMERIC
INSERT INTO types_test VALUES (NULL, NULL, '123.45', NULL, '123.45', '123.45');  -- stored as real in NUMERIC
INSERT INTO types_test VALUES (NULL, NULL, '123abc', NULL, '123abc', '123abc');  -- stored as text in NUMERIC
INSERT INTO types_test VALUES (NULL, NULL, '0x123', NULL, '0x123', '0x123');  -- stored as text

-- Test special float values
INSERT INTO types_test VALUES (NULL, 1.0/0.0, 'infinity', NULL, 1.0/0.0, 'inf');  -- infinity
INSERT INTO types_test VALUES (NULL, -1.0/0.0, '-infinity', NULL, -1.0/0.0, '-inf');  -- -infinity
-- Note: NaN cannot be directly inserted via SQL

-- Test REAL stored as INTEGER optimization
INSERT INTO types_test VALUES (NULL, 1.0, '1.0', NULL, 1.0, 1.0);  -- should store as integer
INSERT INTO types_test VALUES (NULL, 2.0, '2.0', NULL, 2.0, 2.0);  -- should store as integer
INSERT INTO types_test VALUES (NULL, 123.0, '123.0', NULL, 123.0, 123.0);  -- should store as integer

-- Test very long strings (will create overflow pages if long enough)
INSERT INTO types_test VALUES (NULL, NULL, substr(quote(randomblob(5000)),4,10000), randomblob(5000), NULL, NULL);

-- Test edge cases for no affinity column (x)
INSERT INTO types_test VALUES (NULL, NULL, NULL, NULL, NULL, 123);  -- integer
INSERT INTO types_test VALUES (NULL, NULL, NULL, NULL, NULL, 123.45);  -- real
INSERT INTO types_test VALUES (NULL, NULL, NULL, NULL, NULL, 'text');  -- text
INSERT INTO types_test VALUES (NULL, NULL, NULL, NULL, NULL, x'424C4F42');  -- blob
