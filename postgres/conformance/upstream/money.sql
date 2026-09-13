--
-- MONEY
--
-- Note that we assume lc_monetary has been set to C.
--

CREATE TABLE money_data (m money);

INSERT INTO money_data VALUES ('123');
SELECT * FROM money_data;
SELECT m + '123' FROM money_data;
SELECT m + '123.45' FROM money_data;
SELECT m - '123.45' FROM money_data;
SELECT m / '2'::money FROM money_data;
SELECT m * 2 FROM money_data;
SELECT 2 * m FROM money_data;
SELECT m / 2 FROM money_data;
SELECT m * 2::int2 FROM money_data;
SELECT 2::int2 * m FROM money_data;
SELECT m / 2::int2 FROM money_data;
SELECT m * 2::int8 FROM money_data;
SELECT 2::int8 * m FROM money_data;
SELECT m / 2::int8 FROM money_data;
SELECT m * 2::float8 FROM money_data;
SELECT 2::float8 * m FROM money_data;
SELECT m / 2::float8 FROM money_data;
SELECT m * 2::float4 FROM money_data;
SELECT 2::float4 * m FROM money_data;
SELECT m / 2::float4 FROM money_data;

-- All true
SELECT m = '$123.00' FROM money_data;
SELECT m != '$124.00' FROM money_data;
SELECT m <= '$123.00' FROM money_data;
SELECT m >= '$123.00' FROM money_data;
SELECT m < '$124.00' FROM money_data;
SELECT m > '$122.00' FROM money_data;

-- All false
SELECT m = '$123.01' FROM money_data;
SELECT m != '$123.00' FROM money_data;
SELECT m <= '$122.99' FROM money_data;
SELECT m >= '$123.01' FROM money_data;
SELECT m > '$124.00' FROM money_data;
SELECT m < '$122.00' FROM money_data;

SELECT cashlarger(m, '$124.00') FROM money_data;
SELECT cashsmaller(m, '$124.00') FROM money_data;
SELECT cash_words(m) FROM money_data;
SELECT cash_words(m + '1.23') FROM money_data;

DELETE FROM money_data;
INSERT INTO money_data VALUES ('$123.45');
SELECT * FROM money_data;

DELETE FROM money_data;
INSERT INTO money_data VALUES ('$123.451');
SELECT * FROM money_data;

DELETE FROM money_data;
INSERT INTO money_data VALUES ('$123.454');
SELECT * FROM money_data;

DELETE FROM money_data;
INSERT INTO money_data VALUES ('$123.455');
SELECT * FROM money_data;

DELETE FROM money_data;
INSERT INTO money_data VALUES ('$123.456');
SELECT * FROM money_data;

DELETE FROM money_data;
INSERT INTO money_data VALUES ('$123.459');
SELECT * FROM money_data;

-- input checks
SELECT '1234567890'::money;
SELECT '12345678901234567'::money;
SELECT '123456789012345678'::money;
SELECT '9223372036854775807'::money;
SELECT '-12345'::money;
SELECT '-1234567890'::money;
SELECT '-12345678901234567'::money;
SELECT '-123456789012345678'::money;
SELECT '-9223372036854775808'::money;

-- special characters
SELECT '(1)'::money;
SELECT '($123,456.78)'::money;

-- test non-error-throwing API
SELECT pg_input_is_valid('\x0001', 'money');
SELECT * FROM pg_input_error_info('\x0001', 'money');
SELECT pg_input_is_valid('192233720368547758.07', 'money');
SELECT * FROM pg_input_error_info('192233720368547758.07', 'money');

-- documented minimums and maximums
SELECT '-92233720368547758.08'::money;
SELECT '92233720368547758.07'::money;

SELECT '-92233720368547758.09'::money;
SELECT '92233720368547758.08'::money;

-- rounding
SELECT '-92233720368547758.085'::money;
SELECT '92233720368547758.075'::money;

-- rounding vs. truncation in division
SELECT '878.08'::money / 11::float8;
SELECT '878.08'::money / 11::float4;
SELECT '878.08'::money / 11::bigint;
SELECT '878.08'::money / 11::int;
SELECT '878.08'::money / 11::smallint;

-- check for precision loss in division
SELECT '90000000000000099.00'::money / 10::bigint;
SELECT '90000000000000099.00'::money / 10::int;
SELECT '90000000000000099.00'::money / 10::smallint;

-- Cast int4/int8/numeric to money
SELECT 1234567890::money;
SELECT 12345678901234567::money;
SELECT (-12345)::money;
SELECT (-1234567890)::money;
SELECT (-12345678901234567)::money;
SELECT 1234567890::int4::money;
SELECT 12345678901234567::int8::money;
SELECT 12345678901234567::numeric::money;
SELECT (-1234567890)::int4::money;
SELECT (-12345678901234567)::int8::money;
SELECT (-12345678901234567)::numeric::money;

-- Cast from money to numeric
SELECT '12345678901234567'::money::numeric;
SELECT '-12345678901234567'::money::numeric;
SELECT '92233720368547758.07'::money::numeric;
SELECT '-92233720368547758.08'::money::numeric;

-- overflow checks
SELECT '92233720368547758.07'::money + '0.01'::money;
SELECT '-92233720368547758.08'::money - '0.01'::money;
SELECT '92233720368547758.07'::money * 2::float8;
SELECT '-1'::money / 1.175494e-38::float4;
SELECT '92233720368547758.07'::money * 2::int4;
SELECT '1'::money / 0::int2;
SELECT '42'::money * 'inf'::float8;
SELECT '42'::money * '-inf'::float8;
SELECT '42'::money * 'nan'::float4;
