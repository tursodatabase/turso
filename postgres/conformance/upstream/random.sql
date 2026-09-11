--
-- RANDOM
-- Test random() and allies
--
-- Tests in this file may have a small probability of failure,
-- since we are dealing with randomness.  Try to keep the failure
-- risk for any one test case under 1e-9.
--

-- There should be no duplicates in 1000 random() values.
-- (Assuming 52 random bits in the float8 results, we could
-- take as many as 3000 values and still have less than 1e-9 chance
-- of failure, per https://en.wikipedia.org/wiki/Birthday_problem)
SELECT r, count(*)
FROM (SELECT random() r FROM generate_series(1, 1000)) ss
GROUP BY r HAVING count(*) > 1;

-- The range should be [0, 1).  We can expect that at least one out of 2000
-- random values is in the lowest or highest 1% of the range with failure
-- probability less than about 1e-9.

SELECT count(*) FILTER (WHERE r < 0 OR r >= 1) AS out_of_range,
       (count(*) FILTER (WHERE r < 0.01)) > 0 AS has_small,
       (count(*) FILTER (WHERE r > 0.99)) > 0 AS has_large
FROM (SELECT random() r FROM generate_series(1, 2000)) ss;

-- Check for uniform distribution using the Kolmogorov-Smirnov test.

CREATE FUNCTION ks_test_uniform_random()
RETURNS boolean AS
$$
DECLARE
  n int := 1000;        -- Number of samples
  c float8 := 1.94947;  -- Critical value for 99.9% confidence
  ok boolean;
BEGIN
  ok := (
    WITH samples AS (
      SELECT random() r FROM generate_series(1, n) ORDER BY 1
    ), indexed_samples AS (
      SELECT (row_number() OVER())-1.0 i, r FROM samples
    )
    SELECT max(abs(i/n-r)) < c / sqrt(n) FROM indexed_samples
  );
  RETURN ok;
END
$$
LANGUAGE plpgsql;

-- As written, ks_test_uniform_random() returns true about 99.9%
-- of the time.  To get down to a roughly 1e-9 test failure rate,
-- just run it 3 times and accept if any one of them passes.
SELECT ks_test_uniform_random() OR
       ks_test_uniform_random() OR
       ks_test_uniform_random() AS uniform;

-- now test random_normal()

-- As above, there should be no duplicates in 1000 random_normal() values.
SELECT r, count(*)
FROM (SELECT random_normal() r FROM generate_series(1, 1000)) ss
GROUP BY r HAVING count(*) > 1;

-- ... unless we force the range (standard deviation) to zero.
-- This is a good place to check that the mean input does something, too.
SELECT r, count(*)
FROM (SELECT random_normal(10, 0) r FROM generate_series(1, 100)) ss
GROUP BY r;
SELECT r, count(*)
FROM (SELECT random_normal(-10, 0) r FROM generate_series(1, 100)) ss
GROUP BY r;

-- Check standard normal distribution using the Kolmogorov-Smirnov test.

CREATE FUNCTION ks_test_normal_random()
RETURNS boolean AS
$$
DECLARE
  n int := 1000;        -- Number of samples
  c float8 := 1.94947;  -- Critical value for 99.9% confidence
  ok boolean;
BEGIN
  ok := (
    WITH samples AS (
      SELECT random_normal() r FROM generate_series(1, n) ORDER BY 1
    ), indexed_samples AS (
      SELECT (row_number() OVER())-1.0 i, r FROM samples
    )
    SELECT max(abs((1+erf(r/sqrt(2)))/2 - i/n)) < c / sqrt(n)
    FROM indexed_samples
  );
  RETURN ok;
END
$$
LANGUAGE plpgsql;

-- As above, ks_test_normal_random() returns true about 99.9%
-- of the time, so try it 3 times and accept if any test passes.
SELECT ks_test_normal_random() OR
       ks_test_normal_random() OR
       ks_test_normal_random() AS standard_normal;

-- Test random(min, max)

-- invalid range bounds
SELECT random(1, 0);
SELECT random(1000000000001, 1000000000000);
SELECT random(-2.0, -3.0);
SELECT random('NaN'::numeric, 10);
SELECT random('-Inf'::numeric, 0);
SELECT random(0, 'NaN'::numeric);
SELECT random(0, 'Inf'::numeric);

-- empty range is OK
SELECT random(101, 101);
SELECT random(1000000000001, 1000000000001);
SELECT random(3.14, 3.14);

-- There should be no triple duplicates in 1000 full-range 32-bit random()
-- values.  (Each of the C(1000, 3) choices of triplets from the 1000 values
-- has a probability of 1/(2^32)^2 of being a triple duplicate, so the
-- average number of triple duplicates is 1000 * 999 * 998 / 6 / 2^64, which
-- is roughly 9e-12.)
SELECT r, count(*)
FROM (SELECT random(-2147483648, 2147483647) r
      FROM generate_series(1, 1000)) ss
GROUP BY r HAVING count(*) > 2;

-- There should be no duplicates in 1000 full-range 64-bit random() values.
SELECT r, count(*)
FROM (SELECT random_normal(-9223372036854775808, 9223372036854775807) r
      FROM generate_series(1, 1000)) ss
GROUP BY r HAVING count(*) > 1;

-- There should be no duplicates in 1000 15-digit random() numeric values.
SELECT r, count(*)
FROM (SELECT random_normal(0, 1 - 1e-15) r
      FROM generate_series(1, 1000)) ss
GROUP BY r HAVING count(*) > 1;

-- Expect at least one out of 2000 random values to be in the lowest and
-- highest 1% of the range.
SELECT (count(*) FILTER (WHERE r < -2104533975)) > 0 AS has_small,
       (count(*) FILTER (WHERE r > 2104533974)) > 0 AS has_large
FROM (SELECT random(-2147483648, 2147483647) r FROM generate_series(1, 2000)) ss;

SELECT count(*) FILTER (WHERE r < -1500000000 OR r > 1500000000) AS out_of_range,
       (count(*) FILTER (WHERE r < -1470000000)) > 0 AS has_small,
       (count(*) FILTER (WHERE r > 1470000000)) > 0 AS has_large
FROM (SELECT random(-1500000000, 1500000000) r FROM generate_series(1, 2000)) ss;

SELECT (count(*) FILTER (WHERE r < -9038904596117680292)) > 0 AS has_small,
       (count(*) FILTER (WHERE r > 9038904596117680291)) > 0 AS has_large
FROM (SELECT random(-9223372036854775808, 9223372036854775807) r
      FROM generate_series(1, 2000)) ss;

SELECT count(*) FILTER (WHERE r < -1500000000000000 OR r > 1500000000000000) AS out_of_range,
       (count(*) FILTER (WHERE r < -1470000000000000)) > 0 AS has_small,
       (count(*) FILTER (WHERE r > 1470000000000000)) > 0 AS has_large
FROM (SELECT random(-1500000000000000, 1500000000000000) r
      FROM generate_series(1, 2000)) ss;

SELECT count(*) FILTER (WHERE r < -1.5 OR r > 1.5) AS out_of_range,
       (count(*) FILTER (WHERE r < -1.47)) > 0 AS has_small,
       (count(*) FILTER (WHERE r > 1.47)) > 0 AS has_large
FROM (SELECT random(-1.500000000000000, 1.500000000000000) r
      FROM generate_series(1, 2000)) ss;

-- Every possible value should occur at least once in 2500 random() values
-- chosen from a range with 100 distinct values.
SELECT min(r), max(r), count(r) FROM (
  SELECT DISTINCT random(-50, 49) r FROM generate_series(1, 2500));

SELECT min(r), max(r), count(r) FROM (
  SELECT DISTINCT random(123000000000, 123000000099) r
  FROM generate_series(1, 2500));

SELECT min(r), max(r), count(r) FROM (
  SELECT DISTINCT random(-0.5, 0.49) r FROM generate_series(1, 2500));

-- Check for uniform distribution using the Kolmogorov-Smirnov test.

CREATE FUNCTION ks_test_uniform_random_int_in_range()
RETURNS boolean AS
$$
DECLARE
  n int := 1000;        -- Number of samples
  c float8 := 1.94947;  -- Critical value for 99.9% confidence
  ok boolean;
BEGIN
  ok := (
    WITH samples AS (
      SELECT random(0, 999999) / 1000000.0 r FROM generate_series(1, n) ORDER BY 1
    ), indexed_samples AS (
      SELECT (row_number() OVER())-1.0 i, r FROM samples
    )
    SELECT max(abs(i/n-r)) < c / sqrt(n) FROM indexed_samples
  );
  RETURN ok;
END
$$
LANGUAGE plpgsql;

SELECT ks_test_uniform_random_int_in_range() OR
       ks_test_uniform_random_int_in_range() OR
       ks_test_uniform_random_int_in_range() AS uniform_int;

CREATE FUNCTION ks_test_uniform_random_bigint_in_range()
RETURNS boolean AS
$$
DECLARE
  n int := 1000;        -- Number of samples
  c float8 := 1.94947;  -- Critical value for 99.9% confidence
  ok boolean;
BEGIN
  ok := (
    WITH samples AS (
      SELECT random(0, 999999999999) / 1000000000000.0 r FROM generate_series(1, n) ORDER BY 1
    ), indexed_samples AS (
      SELECT (row_number() OVER())-1.0 i, r FROM samples
    )
    SELECT max(abs(i/n-r)) < c / sqrt(n) FROM indexed_samples
  );
  RETURN ok;
END
$$
LANGUAGE plpgsql;

SELECT ks_test_uniform_random_bigint_in_range() OR
       ks_test_uniform_random_bigint_in_range() OR
       ks_test_uniform_random_bigint_in_range() AS uniform_bigint;

CREATE FUNCTION ks_test_uniform_random_numeric_in_range()
RETURNS boolean AS
$$
DECLARE
  n int := 1000;        -- Number of samples
  c float8 := 1.94947;  -- Critical value for 99.9% confidence
  ok boolean;
BEGIN
  ok := (
    WITH samples AS (
      SELECT random(0, 0.999999) r FROM generate_series(1, n) ORDER BY 1
    ), indexed_samples AS (
      SELECT (row_number() OVER())-1.0 i, r FROM samples
    )
    SELECT max(abs(i/n-r)) < c / sqrt(n) FROM indexed_samples
  );
  RETURN ok;
END
$$
LANGUAGE plpgsql;

SELECT ks_test_uniform_random_numeric_in_range() OR
       ks_test_uniform_random_numeric_in_range() OR
       ks_test_uniform_random_numeric_in_range() AS uniform_numeric;

-- setseed() should produce a reproducible series of random() values.

SELECT setseed(0.5);

SELECT random() FROM generate_series(1, 10);

-- Likewise for random_normal(); however, since its implementation relies
-- on libm functions that have different roundoff behaviors on different
-- machines, we have to round off the results a bit to get consistent output.
SET extra_float_digits = -1;

SELECT random_normal() FROM generate_series(1, 10);
SELECT random_normal(mean => 1, stddev => 0.1) r FROM generate_series(1, 10);

-- Reproducible random(min, max) values.
SELECT random(1, 6) FROM generate_series(1, 10);
SELECT random(-2147483648, 2147483647) FROM generate_series(1, 10);
SELECT random(-9223372036854775808, 9223372036854775807) FROM generate_series(1, 10);
SELECT random(-1e30, 1e30) FROM generate_series(1, 10);
SELECT random(-0.4, 0.4) FROM generate_series(1, 10);
SELECT random(0, 1 - 1e-30) FROM generate_series(1, 10);
SELECT n, random(0, trim_scale(abs(1 - 10.0^(-n)))) FROM generate_series(-20, 20) n;
