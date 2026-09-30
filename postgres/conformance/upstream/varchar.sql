--
-- VARCHAR
--

--
-- Build a table for testing
-- (This temporarily hides the table created in test_setup.sql)
--

CREATE TEMP TABLE VARCHAR_TBL(f1 varchar(1));

INSERT INTO VARCHAR_TBL (f1) VALUES ('a');

INSERT INTO VARCHAR_TBL (f1) VALUES ('A');

-- any of the following three input formats are acceptable
INSERT INTO VARCHAR_TBL (f1) VALUES ('1');

INSERT INTO VARCHAR_TBL (f1) VALUES (2);

INSERT INTO VARCHAR_TBL (f1) VALUES ('3');

-- zero-length char
INSERT INTO VARCHAR_TBL (f1) VALUES ('');

-- try varchar's of greater than 1 length
INSERT INTO VARCHAR_TBL (f1) VALUES ('cd');
INSERT INTO VARCHAR_TBL (f1) VALUES ('c     ');


SELECT * FROM VARCHAR_TBL;

SELECT c.*
   FROM VARCHAR_TBL c
   WHERE c.f1 <> 'a';

SELECT c.*
   FROM VARCHAR_TBL c
   WHERE c.f1 = 'a';

SELECT c.*
   FROM VARCHAR_TBL c
   WHERE c.f1 < 'a';

SELECT c.*
   FROM VARCHAR_TBL c
   WHERE c.f1 <= 'a';

SELECT c.*
   FROM VARCHAR_TBL c
   WHERE c.f1 > 'a';

SELECT c.*
   FROM VARCHAR_TBL c
   WHERE c.f1 >= 'a';

DROP TABLE VARCHAR_TBL;

--
-- Now test longer arrays of char
--
-- This varchar_tbl was already created and filled in test_setup.sql.
-- Here we just try to insert bad values.
--

INSERT INTO VARCHAR_TBL (f1) VALUES ('abcde');

SELECT * FROM VARCHAR_TBL;

-- Also try it with non-error-throwing API
SELECT pg_input_is_valid('abcd  ', 'varchar(4)');
SELECT pg_input_is_valid('abcde', 'varchar(4)');
SELECT * FROM pg_input_error_info('abcde', 'varchar(4)');
