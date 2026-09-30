--
-- INT2
--

-- int2_tbl was already created and filled in test_setup.sql.
-- Here we just try to insert bad values.

INSERT INTO INT2_TBL(f1) VALUES ('34.5');
INSERT INTO INT2_TBL(f1) VALUES ('100000');
INSERT INTO INT2_TBL(f1) VALUES ('asdf');
INSERT INTO INT2_TBL(f1) VALUES ('    ');
INSERT INTO INT2_TBL(f1) VALUES ('- 1234');
INSERT INTO INT2_TBL(f1) VALUES ('4 444');
INSERT INTO INT2_TBL(f1) VALUES ('123 dt');
INSERT INTO INT2_TBL(f1) VALUES ('');


SELECT * FROM INT2_TBL;

-- Also try it with non-error-throwing API
SELECT pg_input_is_valid('34', 'int2');
SELECT pg_input_is_valid('asdf', 'int2');
SELECT pg_input_is_valid('50000', 'int2');
SELECT * FROM pg_input_error_info('50000', 'int2');

-- While we're here, check int2vector as well
SELECT pg_input_is_valid(' 1 3  5 ', 'int2vector');
SELECT * FROM pg_input_error_info('1 asdf', 'int2vector');
SELECT * FROM pg_input_error_info('50000', 'int2vector');

SELECT * FROM INT2_TBL AS f(a, b);

SELECT * FROM (TABLE int2_tbl) AS s (a, b);

SELECT i.* FROM INT2_TBL i WHERE i.f1 <> int2 '0';

SELECT i.* FROM INT2_TBL i WHERE i.f1 <> int4 '0';

SELECT i.* FROM INT2_TBL i WHERE i.f1 = int2 '0';

SELECT i.* FROM INT2_TBL i WHERE i.f1 = int4 '0';

SELECT i.* FROM INT2_TBL i WHERE i.f1 < int2 '0';

SELECT i.* FROM INT2_TBL i WHERE i.f1 < int4 '0';

SELECT i.* FROM INT2_TBL i WHERE i.f1 <= int2 '0';

SELECT i.* FROM INT2_TBL i WHERE i.f1 <= int4 '0';

SELECT i.* FROM INT2_TBL i WHERE i.f1 > int2 '0';

SELECT i.* FROM INT2_TBL i WHERE i.f1 > int4 '0';

SELECT i.* FROM INT2_TBL i WHERE i.f1 >= int2 '0';

SELECT i.* FROM INT2_TBL i WHERE i.f1 >= int4 '0';

-- positive odds
SELECT i.* FROM INT2_TBL i WHERE (i.f1 % int2 '2') = int2 '1';

-- any evens
SELECT i.* FROM INT2_TBL i WHERE (i.f1 % int4 '2') = int2 '0';

SELECT i.f1, i.f1 * int2 '2' AS x FROM INT2_TBL i;

SELECT i.f1, i.f1 * int2 '2' AS x FROM INT2_TBL i
WHERE abs(f1) < 16384;

SELECT i.f1, i.f1 * int4 '2' AS x FROM INT2_TBL i;

SELECT i.f1, i.f1 + int2 '2' AS x FROM INT2_TBL i;

SELECT i.f1, i.f1 + int2 '2' AS x FROM INT2_TBL i
WHERE f1 < 32766;

SELECT i.f1, i.f1 + int4 '2' AS x FROM INT2_TBL i;

SELECT i.f1, i.f1 - int2 '2' AS x FROM INT2_TBL i;

SELECT i.f1, i.f1 - int2 '2' AS x FROM INT2_TBL i
WHERE f1 > -32767;

SELECT i.f1, i.f1 - int4 '2' AS x FROM INT2_TBL i;

SELECT i.f1, i.f1 / int2 '2' AS x FROM INT2_TBL i;

SELECT i.f1, i.f1 / int4 '2' AS x FROM INT2_TBL i;

-- corner cases
SELECT (-1::int2<<15)::text;
SELECT ((-1::int2<<15)+1::int2)::text;

-- check sane handling of INT16_MIN overflow cases
SELECT (-32768)::int2 * (-1)::int2;
SELECT (-32768)::int2 / (-1)::int2;
SELECT (-32768)::int2 % (-1)::int2;

-- check rounding when casting from float
SELECT x, x::int2 AS int2_value
FROM (VALUES (-2.5::float8),
             (-1.5::float8),
             (-0.5::float8),
             (0.0::float8),
             (0.5::float8),
             (1.5::float8),
             (2.5::float8)) t(x);

-- check rounding when casting from numeric
SELECT x, x::int2 AS int2_value
FROM (VALUES (-2.5::numeric),
             (-1.5::numeric),
             (-0.5::numeric),
             (0.0::numeric),
             (0.5::numeric),
             (1.5::numeric),
             (2.5::numeric)) t(x);


-- non-decimal literals

SELECT int2 '0b100101';
SELECT int2 '0o273';
SELECT int2 '0x42F';

SELECT int2 '0b';
SELECT int2 '0o';
SELECT int2 '0x';

-- cases near overflow
SELECT int2 '0b111111111111111';
SELECT int2 '0b1000000000000000';
SELECT int2 '0o77777';
SELECT int2 '0o100000';
SELECT int2 '0x7FFF';
SELECT int2 '0x8000';

SELECT int2 '-0b1000000000000000';
SELECT int2 '-0b1000000000000001';
SELECT int2 '-0o100000';
SELECT int2 '-0o100001';
SELECT int2 '-0x8000';
SELECT int2 '-0x8001';


-- underscores

SELECT int2 '1_000';
SELECT int2 '1_2_3';
SELECT int2 '0xE_FF';
SELECT int2 '0o2_73';
SELECT int2 '0b_10_0101';

-- error cases
SELECT int2 '_100';
SELECT int2 '100_';
SELECT int2 '10__000';
