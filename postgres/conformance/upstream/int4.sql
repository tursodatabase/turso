--
-- INT4
--

-- int4_tbl was already created and filled in test_setup.sql.
-- Here we just try to insert bad values.

INSERT INTO INT4_TBL(f1) VALUES ('34.5');
INSERT INTO INT4_TBL(f1) VALUES ('1000000000000');
INSERT INTO INT4_TBL(f1) VALUES ('asdf');
INSERT INTO INT4_TBL(f1) VALUES ('     ');
INSERT INTO INT4_TBL(f1) VALUES ('   asdf   ');
INSERT INTO INT4_TBL(f1) VALUES ('- 1234');
INSERT INTO INT4_TBL(f1) VALUES ('123       5');
INSERT INTO INT4_TBL(f1) VALUES ('');


SELECT * FROM INT4_TBL;

-- Also try it with non-error-throwing API
SELECT pg_input_is_valid('34', 'int4');
SELECT pg_input_is_valid('asdf', 'int4');
SELECT pg_input_is_valid('1000000000000', 'int4');
SELECT * FROM pg_input_error_info('1000000000000', 'int4');

SELECT i.* FROM INT4_TBL i WHERE i.f1 <> int2 '0';

SELECT i.* FROM INT4_TBL i WHERE i.f1 <> int4 '0';

SELECT i.* FROM INT4_TBL i WHERE i.f1 = int2 '0';

SELECT i.* FROM INT4_TBL i WHERE i.f1 = int4 '0';

SELECT i.* FROM INT4_TBL i WHERE i.f1 < int2 '0';

SELECT i.* FROM INT4_TBL i WHERE i.f1 < int4 '0';

SELECT i.* FROM INT4_TBL i WHERE i.f1 <= int2 '0';

SELECT i.* FROM INT4_TBL i WHERE i.f1 <= int4 '0';

SELECT i.* FROM INT4_TBL i WHERE i.f1 > int2 '0';

SELECT i.* FROM INT4_TBL i WHERE i.f1 > int4 '0';

SELECT i.* FROM INT4_TBL i WHERE i.f1 >= int2 '0';

SELECT i.* FROM INT4_TBL i WHERE i.f1 >= int4 '0';

-- positive odds
SELECT i.* FROM INT4_TBL i WHERE (i.f1 % int2 '2') = int2 '1';

-- any evens
SELECT i.* FROM INT4_TBL i WHERE (i.f1 % int4 '2') = int2 '0';

SELECT i.f1, i.f1 * int2 '2' AS x FROM INT4_TBL i;

SELECT i.f1, i.f1 * int2 '2' AS x FROM INT4_TBL i
WHERE abs(f1) < 1073741824;

SELECT i.f1, i.f1 * int4 '2' AS x FROM INT4_TBL i;

SELECT i.f1, i.f1 * int4 '2' AS x FROM INT4_TBL i
WHERE abs(f1) < 1073741824;

SELECT i.f1, i.f1 + int2 '2' AS x FROM INT4_TBL i;

SELECT i.f1, i.f1 + int2 '2' AS x FROM INT4_TBL i
WHERE f1 < 2147483646;

SELECT i.f1, i.f1 + int4 '2' AS x FROM INT4_TBL i;

SELECT i.f1, i.f1 + int4 '2' AS x FROM INT4_TBL i
WHERE f1 < 2147483646;

SELECT i.f1, i.f1 - int2 '2' AS x FROM INT4_TBL i;

SELECT i.f1, i.f1 - int2 '2' AS x FROM INT4_TBL i
WHERE f1 > -2147483647;

SELECT i.f1, i.f1 - int4 '2' AS x FROM INT4_TBL i;

SELECT i.f1, i.f1 - int4 '2' AS x FROM INT4_TBL i
WHERE f1 > -2147483647;

SELECT i.f1, i.f1 / int2 '2' AS x FROM INT4_TBL i;

SELECT i.f1, i.f1 / int4 '2' AS x FROM INT4_TBL i;

--
-- more complex expressions
--

-- variations on unary minus parsing
SELECT -2+3 AS one;

SELECT 4-2 AS two;

SELECT 2- -1 AS three;

SELECT 2 - -2 AS four;

SELECT int2 '2' * int2 '2' = int2 '16' / int2 '4' AS true;

SELECT int4 '2' * int2 '2' = int2 '16' / int4 '4' AS true;

SELECT int2 '2' * int4 '2' = int4 '16' / int2 '4' AS true;

SELECT int4 '1000' < int4 '999' AS false;

SELECT 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 + 1 AS ten;

SELECT 2 + 2 / 2 AS three;

SELECT (2 + 2) / 2 AS two;

-- corner case
SELECT (-1::int4<<31)::text;
SELECT ((-1::int4<<31)+1)::text;

-- check sane handling of INT_MIN overflow cases
SELECT (-2147483648)::int4 * (-1)::int4;
SELECT (-2147483648)::int4 / (-1)::int4;
SELECT (-2147483648)::int4 % (-1)::int4;
SELECT (-2147483648)::int4 * (-1)::int2;
SELECT (-2147483648)::int4 / (-1)::int2;
SELECT (-2147483648)::int4 % (-1)::int2;

-- check rounding when casting from float
SELECT x, x::int4 AS int4_value
FROM (VALUES (-2.5::float8),
             (-1.5::float8),
             (-0.5::float8),
             (0.0::float8),
             (0.5::float8),
             (1.5::float8),
             (2.5::float8)) t(x);

-- check rounding when casting from numeric
SELECT x, x::int4 AS int4_value
FROM (VALUES (-2.5::numeric),
             (-1.5::numeric),
             (-0.5::numeric),
             (0.0::numeric),
             (0.5::numeric),
             (1.5::numeric),
             (2.5::numeric)) t(x);

-- test gcd()
SELECT a, b, gcd(a, b), gcd(a, -b), gcd(b, a), gcd(-b, a)
FROM (VALUES (0::int4, 0::int4),
             (0::int4, 6410818::int4),
             (61866666::int4, 6410818::int4),
             (-61866666::int4, 6410818::int4),
             ((-2147483648)::int4, 1::int4),
             ((-2147483648)::int4, 2147483647::int4),
             ((-2147483648)::int4, 1073741824::int4)) AS v(a, b);

SELECT gcd((-2147483648)::int4, 0::int4); -- overflow
SELECT gcd((-2147483648)::int4, (-2147483648)::int4); -- overflow

-- test lcm()
SELECT a, b, lcm(a, b), lcm(a, -b), lcm(b, a), lcm(-b, a)
FROM (VALUES (0::int4, 0::int4),
             (0::int4, 42::int4),
             (42::int4, 42::int4),
             (330::int4, 462::int4),
             (-330::int4, 462::int4),
             ((-2147483648)::int4, 0::int4)) AS v(a, b);

SELECT lcm((-2147483648)::int4, 1::int4); -- overflow
SELECT lcm(2147483647::int4, 2147483646::int4); -- overflow


-- non-decimal literals

SELECT int4 '0b100101';
SELECT int4 '0o273';
SELECT int4 '0x42F';

SELECT int4 '0b';
SELECT int4 '0o';
SELECT int4 '0x';

-- cases near overflow
SELECT int4 '0b1111111111111111111111111111111';
SELECT int4 '0b10000000000000000000000000000000';
SELECT int4 '0o17777777777';
SELECT int4 '0o20000000000';
SELECT int4 '0x7FFFFFFF';
SELECT int4 '0x80000000';

SELECT int4 '-0b10000000000000000000000000000000';
SELECT int4 '-0b10000000000000000000000000000001';
SELECT int4 '-0o20000000000';
SELECT int4 '-0o20000000001';
SELECT int4 '-0x80000000';
SELECT int4 '-0x80000001';


-- underscores

SELECT int4 '1_000_000';
SELECT int4 '1_2_3';
SELECT int4 '0x1EEE_FFFF';
SELECT int4 '0o2_73';
SELECT int4 '0b_10_0101';

-- error cases
SELECT int4 '_100';
SELECT int4 '100_';
SELECT int4 '100__000';
