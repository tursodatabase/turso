--
-- TIMETZ
--

CREATE TABLE TIMETZ_TBL (f1 time(2) with time zone);

INSERT INTO TIMETZ_TBL VALUES ('00:01 PDT');
INSERT INTO TIMETZ_TBL VALUES ('01:00 PDT');
INSERT INTO TIMETZ_TBL VALUES ('02:03 PDT');
INSERT INTO TIMETZ_TBL VALUES ('07:07 PST');
INSERT INTO TIMETZ_TBL VALUES ('08:08 EDT');
INSERT INTO TIMETZ_TBL VALUES ('11:59 PDT');
INSERT INTO TIMETZ_TBL VALUES ('12:00 PDT');
INSERT INTO TIMETZ_TBL VALUES ('12:01 PDT');
INSERT INTO TIMETZ_TBL VALUES ('23:59 PDT');
INSERT INTO TIMETZ_TBL VALUES ('11:59:59.99 PM PDT');

INSERT INTO TIMETZ_TBL VALUES ('2003-03-07 15:36:39 America/New_York');
INSERT INTO TIMETZ_TBL VALUES ('2003-07-07 15:36:39 America/New_York');
-- this should fail (the timezone offset is not known)
INSERT INTO TIMETZ_TBL VALUES ('15:36:39 America/New_York');
-- this should fail (timezone not specified without a date)
INSERT INTO TIMETZ_TBL VALUES ('15:36:39 m2');
-- this should fail (dynamic timezone abbreviation without a date)
INSERT INTO TIMETZ_TBL VALUES ('15:36:39 MSK m2');


SELECT f1 AS "Time TZ" FROM TIMETZ_TBL;

SELECT f1 AS "Three" FROM TIMETZ_TBL WHERE f1 < '05:06:07-07';

SELECT f1 AS "Seven" FROM TIMETZ_TBL WHERE f1 > '05:06:07-07';

SELECT f1 AS "None" FROM TIMETZ_TBL WHERE f1 < '00:00-07';

SELECT f1 AS "Ten" FROM TIMETZ_TBL WHERE f1 >= '00:00-07';

-- Check edge cases
SELECT '23:59:59.999999 PDT'::timetz;
SELECT '23:59:59.9999999 PDT'::timetz;  -- rounds up
SELECT '23:59:60 PDT'::timetz;  -- rounds up
SELECT '24:00:00 PDT'::timetz;  -- allowed
SELECT '24:00:00.01 PDT'::timetz;  -- not allowed
SELECT '23:59:60.01 PDT'::timetz;  -- not allowed
SELECT '24:01:00 PDT'::timetz;  -- not allowed
SELECT '25:00:00 PDT'::timetz;  -- not allowed

-- Test non-error-throwing API
SELECT pg_input_is_valid('12:00:00 PDT', 'timetz');
SELECT pg_input_is_valid('25:00:00 PDT', 'timetz');
SELECT pg_input_is_valid('15:36:39 America/New_York', 'timetz');
SELECT * FROM pg_input_error_info('25:00:00 PDT', 'timetz');
SELECT * FROM pg_input_error_info('15:36:39 America/New_York', 'timetz');

--
-- TIME simple math
--
-- We now make a distinction between time and intervals,
-- and adding two times together makes no sense at all.
-- Leave in one query to show that it is rejected,
-- and do the rest of the testing in horology.sql
-- where we do mixed-type arithmetic. - thomas 2000-12-02

SELECT f1 + time with time zone '00:01' AS "Illegal" FROM TIMETZ_TBL;

--
-- test EXTRACT
--
SELECT EXTRACT(MICROSECOND FROM TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04');
SELECT EXTRACT(MILLISECOND FROM TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04');
SELECT EXTRACT(SECOND      FROM TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04');
SELECT EXTRACT(MINUTE      FROM TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04');
SELECT EXTRACT(HOUR        FROM TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04');
SELECT EXTRACT(DAY         FROM TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04');  -- error
SELECT EXTRACT(FORTNIGHT   FROM TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04');  -- error
SELECT EXTRACT(TIMEZONE    FROM TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04:30');
SELECT EXTRACT(TIMEZONE_HOUR   FROM TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04:30');
SELECT EXTRACT(TIMEZONE_MINUTE FROM TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04:30');
SELECT EXTRACT(EPOCH       FROM TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04');

-- date_part implementation is mostly the same as extract, so only
-- test a few cases for additional coverage.
SELECT date_part('microsecond', TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04');
SELECT date_part('millisecond', TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04');
SELECT date_part('second',      TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04');
SELECT date_part('epoch',       TIME WITH TIME ZONE '2020-05-26 13:30:25.575401-04');

--
-- Test timetz_zone, timetz_izone, AT LOCAL
--
BEGIN;
SET LOCAL TimeZone TO 'UTC';
CREATE VIEW timetz_local_view AS
  SELECT f1 AS dat,
       timezone(f1) AS dat_func,
       f1 AT LOCAL AS dat_at_local,
       f1 AT TIME ZONE current_setting('TimeZone') AS dat_at_tz,
       f1 AT TIME ZONE INTERVAL '00:00' AS dat_at_int
  FROM TIMETZ_TBL
  ORDER BY f1;
SELECT pg_get_viewdef('timetz_local_view', true);
TABLE timetz_local_view;
SELECT f1 AS dat,
       f1 AT TIME ZONE 'UTC+10' AS dat_at_tz,
       f1 AT TIME ZONE INTERVAL '-10:00' AS dat_at_int
  FROM TIMETZ_TBL
  ORDER BY f1;
ROLLBACK;
