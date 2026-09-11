--
-- macaddr8
--

-- test various cases of valid and invalid input
-- valid
SELECT '08:00:2b:01:02:03     '::macaddr8;
SELECT '    08:00:2b:01:02:03     '::macaddr8;
SELECT '    08:00:2b:01:02:03'::macaddr8;
SELECT '08:00:2b:01:02:03:04:05     '::macaddr8;
SELECT '    08:00:2b:01:02:03:04:05     '::macaddr8;
SELECT '    08:00:2b:01:02:03:04:05'::macaddr8;

SELECT '123    08:00:2b:01:02:03'::macaddr8; -- invalid
SELECT '08:00:2b:01:02:03  123'::macaddr8; -- invalid
SELECT '123    08:00:2b:01:02:03:04:05'::macaddr8; -- invalid
SELECT '08:00:2b:01:02:03:04:05  123'::macaddr8; -- invalid
SELECT '08:00:2b:01:02:03:04:05:06:07'::macaddr8; -- invalid
SELECT '08-00-2b-01-02-03-04-05-06-07'::macaddr8; -- invalid
SELECT '08002b:01020304050607'::macaddr8; -- invalid
SELECT '08002b01020304050607'::macaddr8; -- invalid
SELECT '0z002b0102030405'::macaddr8; -- invalid
SELECT '08002b010203xyza'::macaddr8; -- invalid

SELECT '08:00-2b:01:02:03:04:05'::macaddr8; -- invalid
SELECT '08:00-2b:01:02:03:04:05'::macaddr8; -- invalid
SELECT '08:00:2b:01.02:03:04:05'::macaddr8; -- invalid
SELECT '08:00:2b:01.02:03:04:05'::macaddr8; -- invalid

-- test converting a MAC address to modified EUI-64 for inclusion
-- in an ipv6 address
SELECT macaddr8_set7bit('00:08:2b:01:02:03'::macaddr8);

CREATE TABLE macaddr8_data (a int, b macaddr8);

INSERT INTO macaddr8_data VALUES (1, '08:00:2b:01:02:03');
INSERT INTO macaddr8_data VALUES (2, '08-00-2b-01-02-03');
INSERT INTO macaddr8_data VALUES (3, '08002b:010203');
INSERT INTO macaddr8_data VALUES (4, '08002b-010203');
INSERT INTO macaddr8_data VALUES (5, '0800.2b01.0203');
INSERT INTO macaddr8_data VALUES (6, '0800-2b01-0203');
INSERT INTO macaddr8_data VALUES (7, '08002b010203');
INSERT INTO macaddr8_data VALUES (8, '0800:2b01:0203');
INSERT INTO macaddr8_data VALUES (9, 'not even close'); -- invalid

INSERT INTO macaddr8_data VALUES (10, '08:00:2b:01:02:04');
INSERT INTO macaddr8_data VALUES (11, '08:00:2b:01:02:02');
INSERT INTO macaddr8_data VALUES (12, '08:00:2a:01:02:03');
INSERT INTO macaddr8_data VALUES (13, '08:00:2c:01:02:03');
INSERT INTO macaddr8_data VALUES (14, '08:00:2a:01:02:04');

INSERT INTO macaddr8_data VALUES (15, '08:00:2b:01:02:03:04:05');
INSERT INTO macaddr8_data VALUES (16, '08-00-2b-01-02-03-04-05');
INSERT INTO macaddr8_data VALUES (17, '08002b:0102030405');
INSERT INTO macaddr8_data VALUES (18, '08002b-0102030405');
INSERT INTO macaddr8_data VALUES (19, '0800.2b01.0203.0405');
INSERT INTO macaddr8_data VALUES (20, '08002b01:02030405');
INSERT INTO macaddr8_data VALUES (21, '08002b0102030405');

SELECT * FROM macaddr8_data ORDER BY 1;

CREATE INDEX macaddr8_data_btree ON macaddr8_data USING btree (b);
CREATE INDEX macaddr8_data_hash ON macaddr8_data USING hash (b);

SELECT a, b, trunc(b) FROM macaddr8_data ORDER BY 2, 1;

SELECT b <  '08:00:2b:01:02:04' FROM macaddr8_data WHERE a = 1; -- true
SELECT b >  '08:00:2b:ff:fe:01:02:04' FROM macaddr8_data WHERE a = 1; -- false
SELECT b >  '08:00:2b:ff:fe:01:02:03' FROM macaddr8_data WHERE a = 1; -- false
SELECT b::macaddr <= '08:00:2b:01:02:04' FROM macaddr8_data WHERE a = 1; -- true
SELECT b::macaddr >= '08:00:2b:01:02:04' FROM macaddr8_data WHERE a = 1; -- false
SELECT b =  '08:00:2b:ff:fe:01:02:03' FROM macaddr8_data WHERE a = 1; -- true
SELECT b::macaddr <> '08:00:2b:01:02:04'::macaddr FROM macaddr8_data WHERE a = 1; -- true
SELECT b::macaddr <> '08:00:2b:01:02:03'::macaddr FROM macaddr8_data WHERE a = 1; -- false

SELECT b <  '08:00:2b:01:02:03:04:06' FROM macaddr8_data WHERE a = 15; -- true
SELECT b >  '08:00:2b:01:02:03:04:06' FROM macaddr8_data WHERE a = 15; -- false
SELECT b >  '08:00:2b:01:02:03:04:05' FROM macaddr8_data WHERE a = 15; -- false
SELECT b <= '08:00:2b:01:02:03:04:06' FROM macaddr8_data WHERE a = 15; -- true
SELECT b >= '08:00:2b:01:02:03:04:06' FROM macaddr8_data WHERE a = 15; -- false
SELECT b =  '08:00:2b:01:02:03:04:05' FROM macaddr8_data WHERE a = 15; -- true
SELECT b <> '08:00:2b:01:02:03:04:06' FROM macaddr8_data WHERE a = 15; -- true
SELECT b <> '08:00:2b:01:02:03:04:05' FROM macaddr8_data WHERE a = 15; -- false

SELECT ~b                       FROM macaddr8_data;
SELECT  b & '00:00:00:ff:ff:ff' FROM macaddr8_data;
SELECT  b | '01:02:03:04:05:06' FROM macaddr8_data;

DROP TABLE macaddr8_data;

-- test non-error-throwing API for some core types
SELECT pg_input_is_valid('08:00:2b:01:02:03:04:ZZ', 'macaddr8');
SELECT * FROM pg_input_error_info('08:00:2b:01:02:03:04:ZZ', 'macaddr8');
SELECT pg_input_is_valid('08:00:2b:01:02:03:04:', 'macaddr8');
SELECT * FROM pg_input_error_info('08:00:2b:01:02:03:04:', 'macaddr8');
