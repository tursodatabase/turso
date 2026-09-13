--
-- OID
--

CREATE TABLE OID_TBL(f1 oid);

INSERT INTO OID_TBL(f1) VALUES ('1234');
INSERT INTO OID_TBL(f1) VALUES ('1235');
INSERT INTO OID_TBL(f1) VALUES ('987');
INSERT INTO OID_TBL(f1) VALUES ('-1040');
INSERT INTO OID_TBL(f1) VALUES ('99999999');
INSERT INTO OID_TBL(f1) VALUES ('5     ');
INSERT INTO OID_TBL(f1) VALUES ('   10  ');
-- leading/trailing hard tab is also allowed
INSERT INTO OID_TBL(f1) VALUES ('	  15 	  ');

-- bad inputs
INSERT INTO OID_TBL(f1) VALUES ('');
INSERT INTO OID_TBL(f1) VALUES ('    ');
INSERT INTO OID_TBL(f1) VALUES ('asdfasd');
INSERT INTO OID_TBL(f1) VALUES ('99asdfasd');
INSERT INTO OID_TBL(f1) VALUES ('5    d');
INSERT INTO OID_TBL(f1) VALUES ('    5d');
INSERT INTO OID_TBL(f1) VALUES ('5    5');
INSERT INTO OID_TBL(f1) VALUES (' - 500');
INSERT INTO OID_TBL(f1) VALUES ('32958209582039852935');
INSERT INTO OID_TBL(f1) VALUES ('-23582358720398502385');

SELECT * FROM OID_TBL;

-- Also try it with non-error-throwing API
SELECT pg_input_is_valid('1234', 'oid');
SELECT pg_input_is_valid('01XYZ', 'oid');
SELECT * FROM pg_input_error_info('01XYZ', 'oid');
SELECT pg_input_is_valid('9999999999', 'oid');
SELECT * FROM pg_input_error_info('9999999999', 'oid');

-- While we're here, check oidvector as well
SELECT pg_input_is_valid(' 1 2  4 ', 'oidvector');
SELECT pg_input_is_valid('01 01XYZ', 'oidvector');
SELECT * FROM pg_input_error_info('01 01XYZ', 'oidvector');
SELECT pg_input_is_valid('01 9999999999', 'oidvector');
SELECT * FROM pg_input_error_info('01 9999999999', 'oidvector');

SELECT o.* FROM OID_TBL o WHERE o.f1 = 1234;

SELECT o.* FROM OID_TBL o WHERE o.f1 <> '1234';

SELECT o.* FROM OID_TBL o WHERE o.f1 <= '1234';

SELECT o.* FROM OID_TBL o WHERE o.f1 < '1234';

SELECT o.* FROM OID_TBL o WHERE o.f1 >= '1234';

SELECT o.* FROM OID_TBL o WHERE o.f1 > '1234';

DROP TABLE OID_TBL;
