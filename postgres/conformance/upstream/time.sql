--
-- TIME
--

CREATE TABLE TIME_TBL (f1 time(2));

INSERT INTO TIME_TBL VALUES ('00:00');
INSERT INTO TIME_TBL VALUES ('01:00');
-- as of 7.4, timezone spec should be accepted and ignored
INSERT INTO TIME_TBL VALUES ('02:03 PST');
INSERT INTO TIME_TBL VALUES ('11:59 EDT');
INSERT INTO TIME_TBL VALUES ('12:00');
INSERT INTO TIME_TBL VALUES ('12:01');
INSERT INTO TIME_TBL VALUES ('23:59');
INSERT INTO TIME_TBL VALUES ('11:59:59.99 PM');

INSERT INTO TIME_TBL VALUES ('2003-03-07 15:36:39 America/New_York');
INSERT INTO TIME_TBL VALUES ('2003-07-07 15:36:39 America/New_York');
-- this should fail (the timezone offset is not known)
INSERT INTO TIME_TBL VALUES ('15:36:39 America/New_York');


SELECT f1 AS "Time" FROM TIME_TBL;

SELECT f1 AS "Three" FROM TIME_TBL WHERE f1 < '05:06:07';

SELECT f1 AS "Five" FROM TIME_TBL WHERE f1 > '05:06:07';

SELECT f1 AS "None" FROM TIME_TBL WHERE f1 < '00:00';

SELECT f1 AS "Eight" FROM TIME_TBL WHERE f1 >= '00:00';

-- Check edge cases
SELECT '23:59:59.999999'::time;
SELECT '23:59:59.9999999'::time;  -- rounds up
SELECT '23:59:60'::time;  -- rounds up
SELECT '24:00:00'::time;  -- allowed
SELECT '24:00:00.01'::time;  -- not allowed
SELECT '23:59:60.01'::time;  -- not allowed
SELECT '24:01:00'::time;  -- not allowed
SELECT '25:00:00'::time;  -- not allowed

-- Test non-error-throwing API
SELECT pg_input_is_valid('12:00:00', 'time');
SELECT pg_input_is_valid('25:00:00', 'time');
SELECT pg_input_is_valid('15:36:39 America/New_York', 'time');
SELECT * FROM pg_input_error_info('25:00:00', 'time');
SELECT * FROM pg_input_error_info('15:36:39 America/New_York', 'time');

--
-- TIME simple math
--
-- We now make a distinction between time and intervals,
-- and adding two times together makes no sense at all.
-- Leave in one query to show that it is rejected,
-- and do the rest of the testing in horology.sql
-- where we do mixed-type arithmetic. - thomas 2000-12-02

SELECT f1 + time '00:01' AS "Illegal" FROM TIME_TBL;

--
-- test EXTRACT
--
SELECT EXTRACT(MICROSECOND FROM TIME '2020-05-26 13:30:25.575401');
SELECT EXTRACT(MILLISECOND FROM TIME '2020-05-26 13:30:25.575401');
SELECT EXTRACT(SECOND      FROM TIME '2020-05-26 13:30:25.575401');
SELECT EXTRACT(MINUTE      FROM TIME '2020-05-26 13:30:25.575401');
SELECT EXTRACT(HOUR        FROM TIME '2020-05-26 13:30:25.575401');
SELECT EXTRACT(DAY         FROM TIME '2020-05-26 13:30:25.575401');  -- error
SELECT EXTRACT(FORTNIGHT   FROM TIME '2020-05-26 13:30:25.575401');  -- error
SELECT EXTRACT(TIMEZONE    FROM TIME '2020-05-26 13:30:25.575401');  -- error
SELECT EXTRACT(EPOCH       FROM TIME '2020-05-26 13:30:25.575401');

-- date_part implementation is mostly the same as extract, so only
-- test a few cases for additional coverage.
SELECT date_part('microsecond', TIME '2020-05-26 13:30:25.575401');
SELECT date_part('millisecond', TIME '2020-05-26 13:30:25.575401');
SELECT date_part('second',      TIME '2020-05-26 13:30:25.575401');
SELECT date_part('epoch',       TIME '2020-05-26 13:30:25.575401');
