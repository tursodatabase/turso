--
-- INET
--

-- prepare the table...

DROP TABLE INET_TBL;
CREATE TABLE INET_TBL (c cidr, i inet);
INSERT INTO INET_TBL (c, i) VALUES ('192.168.1', '192.168.1.226/24');
INSERT INTO INET_TBL (c, i) VALUES ('192.168.1.0/26', '192.168.1.226');
INSERT INTO INET_TBL (c, i) VALUES ('192.168.1', '192.168.1.0/24');
INSERT INTO INET_TBL (c, i) VALUES ('192.168.1', '192.168.1.0/25');
INSERT INTO INET_TBL (c, i) VALUES ('192.168.1', '192.168.1.255/24');
INSERT INTO INET_TBL (c, i) VALUES ('192.168.1', '192.168.1.255/25');
INSERT INTO INET_TBL (c, i) VALUES ('10', '10.1.2.3/8');
INSERT INTO INET_TBL (c, i) VALUES ('10.0.0.0', '10.1.2.3/8');
INSERT INTO INET_TBL (c, i) VALUES ('10.1.2.3', '10.1.2.3/32');
INSERT INTO INET_TBL (c, i) VALUES ('10.1.2', '10.1.2.3/24');
INSERT INTO INET_TBL (c, i) VALUES ('10.1', '10.1.2.3/16');
INSERT INTO INET_TBL (c, i) VALUES ('10', '10.1.2.3/8');
INSERT INTO INET_TBL (c, i) VALUES ('10', '11.1.2.3/8');
INSERT INTO INET_TBL (c, i) VALUES ('10', '9.1.2.3/8');
INSERT INTO INET_TBL (c, i) VALUES ('10:23::f1', '10:23::f1/64');
INSERT INTO INET_TBL (c, i) VALUES ('10:23::8000/113', '10:23::ffff');
INSERT INTO INET_TBL (c, i) VALUES ('::ffff:1.2.3.4', '::4.3.2.1/24');
-- check that CIDR rejects invalid input:
INSERT INTO INET_TBL (c, i) VALUES ('192.168.1.2/30', '192.168.1.226');
INSERT INTO INET_TBL (c, i) VALUES ('1234::1234::1234', '::1.2.3.4');
-- check that CIDR rejects invalid input when converting from text:
INSERT INTO INET_TBL (c, i) VALUES (cidr('192.168.1.2/30'), '192.168.1.226');
INSERT INTO INET_TBL (c, i) VALUES (cidr('ffff:ffff:ffff:ffff::/24'), '::192.168.1.226');
SELECT c AS cidr, i AS inet FROM INET_TBL;

-- now test some support functions

SELECT i AS inet, host(i), text(i), family(i) FROM INET_TBL;
SELECT c AS cidr, abbrev(c) AS "abbrev(cidr)",
  i AS inet, abbrev(i) AS "abbrev(inet)" FROM INET_TBL;
SELECT c AS cidr, broadcast(c) AS "broadcast(cidr)",
  i AS inet, broadcast(i) AS "broadcast(inet)" FROM INET_TBL;
SELECT c AS cidr, network(c) AS "network(cidr)",
  i AS inet, network(i) AS "network(inet)" FROM INET_TBL;
SELECT c AS cidr, masklen(c) AS "masklen(cidr)",
  i AS inet, masklen(i) AS "masklen(inet)" FROM INET_TBL;

SELECT c AS cidr, masklen(c) AS "masklen(cidr)",
  i AS inet, masklen(i) AS "masklen(inet)" FROM INET_TBL
  WHERE masklen(c) <= 8;

SELECT i AS inet, netmask(i) AS "netmask(inet)" FROM INET_TBL;
SELECT i AS inet, hostmask(i) AS "hostmask(inet)" FROM INET_TBL;

SELECT c AS cidr, i AS inet FROM INET_TBL
  WHERE c = i;

SELECT i, c,
  i < c AS lt, i <= c AS le, i = c AS eq,
  i >= c AS ge, i > c AS gt, i <> c AS ne,
  i << c AS sb, i <<= c AS sbe,
  i >> c AS sup, i >>= c AS spe,
  i && c AS ovr
  FROM INET_TBL;

SELECT max(i) AS max, min(i) AS min FROM INET_TBL;
SELECT max(c) AS max, min(c) AS min FROM INET_TBL;

-- check the conversion to/from text and setting netmask
SELECT c AS cidr, set_masklen(cidr(text(c)), 24) AS "set_masklen(cidr)",
  i AS inet, set_masklen(inet(text(i)), 24) AS "set_masklen(inet)" FROM INET_TBL;
-- check that netmask is treated as maximum value when set to -1
SELECT c AS cidr, set_masklen(cidr(text(c)), -1) AS "set_masklen(cidr)",
  i AS inet, set_masklen(inet(text(i)), -1) AS "set_masklen(inet)" FROM INET_TBL;
-- check that invalid netmask is rejected
SELECT set_masklen(inet(text(i)), 33) FROM INET_TBL;
SELECT set_masklen(cidr(text(c)), 33) FROM INET_TBL;

-- check that btree index works correctly
CREATE INDEX inet_idx1 ON inet_tbl(i);
SET enable_seqscan TO off;
EXPLAIN (COSTS OFF)
SELECT * FROM inet_tbl WHERE i<<'192.168.1.0/24'::cidr;
SELECT * FROM inet_tbl WHERE i<<'192.168.1.0/24'::cidr;
EXPLAIN (COSTS OFF)
SELECT * FROM inet_tbl WHERE i<<='192.168.1.0/24'::cidr;
SELECT * FROM inet_tbl WHERE i<<='192.168.1.0/24'::cidr;
EXPLAIN (COSTS OFF)
SELECT * FROM inet_tbl WHERE '192.168.1.0/24'::cidr >>= i;
SELECT * FROM inet_tbl WHERE '192.168.1.0/24'::cidr >>= i;
EXPLAIN (COSTS OFF)
SELECT * FROM inet_tbl WHERE '192.168.1.0/24'::cidr >> i;
SELECT * FROM inet_tbl WHERE '192.168.1.0/24'::cidr >> i;
SET enable_seqscan TO on;
DROP INDEX inet_idx1;

-- check that gist index works correctly
CREATE INDEX inet_idx2 ON inet_tbl using gist (i inet_ops);
SET enable_seqscan TO off;
SELECT * FROM inet_tbl WHERE i << '192.168.1.0/24'::cidr ORDER BY i;
SELECT * FROM inet_tbl WHERE i <<= '192.168.1.0/24'::cidr ORDER BY i;
SELECT * FROM inet_tbl WHERE i && '192.168.1.0/24'::cidr ORDER BY i;
SELECT * FROM inet_tbl WHERE i >>= '192.168.1.0/24'::cidr ORDER BY i;
SELECT * FROM inet_tbl WHERE i >> '192.168.1.0/24'::cidr ORDER BY i;
SELECT * FROM inet_tbl WHERE i < '192.168.1.0/24'::cidr ORDER BY i;
SELECT * FROM inet_tbl WHERE i <= '192.168.1.0/24'::cidr ORDER BY i;
SELECT * FROM inet_tbl WHERE i = '192.168.1.0/24'::cidr ORDER BY i;
SELECT * FROM inet_tbl WHERE i >= '192.168.1.0/24'::cidr ORDER BY i;
SELECT * FROM inet_tbl WHERE i > '192.168.1.0/24'::cidr ORDER BY i;
SELECT * FROM inet_tbl WHERE i <> '192.168.1.0/24'::cidr ORDER BY i;

-- test index-only scans
EXPLAIN (COSTS OFF)
SELECT i FROM inet_tbl WHERE i << '192.168.1.0/24'::cidr ORDER BY i;
SELECT i FROM inet_tbl WHERE i << '192.168.1.0/24'::cidr ORDER BY i;

SET enable_seqscan TO on;
DROP INDEX inet_idx2;

-- check that spgist index works correctly
CREATE INDEX inet_idx3 ON inet_tbl using spgist (i);
SET enable_seqscan TO off;
SELECT * FROM inet_tbl WHERE i << '192.168.1.0/24'::cidr ORDER BY i;
SELECT * FROM inet_tbl WHERE i <<= '192.168.1.0/24'::cidr ORDER BY i;
SELECT * FROM inet_tbl WHERE i && '192.168.1.0/24'::cidr ORDER BY i;
SELECT * FROM inet_tbl WHERE i >>= '192.168.1.0/24'::cidr ORDER BY i;
SELECT * FROM inet_tbl WHERE i >> '192.168.1.0/24'::cidr ORDER BY i;
SELECT * FROM inet_tbl WHERE i < '192.168.1.0/24'::cidr ORDER BY i;
SELECT * FROM inet_tbl WHERE i <= '192.168.1.0/24'::cidr ORDER BY i;
SELECT * FROM inet_tbl WHERE i = '192.168.1.0/24'::cidr ORDER BY i;
SELECT * FROM inet_tbl WHERE i >= '192.168.1.0/24'::cidr ORDER BY i;
SELECT * FROM inet_tbl WHERE i > '192.168.1.0/24'::cidr ORDER BY i;
SELECT * FROM inet_tbl WHERE i <> '192.168.1.0/24'::cidr ORDER BY i;

-- test index-only scans
EXPLAIN (COSTS OFF)
SELECT i FROM inet_tbl WHERE i << '192.168.1.0/24'::cidr ORDER BY i;
SELECT i FROM inet_tbl WHERE i << '192.168.1.0/24'::cidr ORDER BY i;

SET enable_seqscan TO on;
DROP INDEX inet_idx3;

-- simple tests of inet boolean and arithmetic operators
SELECT i, ~i AS "~i" FROM inet_tbl;
SELECT i, c, i & c AS "and" FROM inet_tbl;
SELECT i, c, i | c AS "or" FROM inet_tbl;
SELECT i, i + 500 AS "i+500" FROM inet_tbl;
SELECT i, i - 500 AS "i-500" FROM inet_tbl;
SELECT i, c, i - c AS "minus" FROM inet_tbl;
SELECT '127.0.0.1'::inet + 257;
SELECT ('127.0.0.1'::inet + 257) - 257;
SELECT '127::1'::inet + 257;
SELECT ('127::1'::inet + 257) - 257;
SELECT '127.0.0.2'::inet  - ('127.0.0.2'::inet + 500);
SELECT '127.0.0.2'::inet  - ('127.0.0.2'::inet - 500);
SELECT '127::2'::inet  - ('127::2'::inet + 500);
SELECT '127::2'::inet  - ('127::2'::inet - 500);
-- these should give overflow errors:
SELECT '127.0.0.1'::inet + 10000000000;
SELECT '127.0.0.1'::inet - 10000000000;
SELECT '126::1'::inet - '127::2'::inet;
SELECT '127::1'::inet - '126::2'::inet;
-- but not these
SELECT '127::1'::inet + 10000000000;
SELECT '127::1'::inet - '127::2'::inet;

-- insert one more row with addressed from different families
INSERT INTO INET_TBL (c, i) VALUES ('10', '10::/8');
-- now, this one should fail
SELECT inet_merge(c, i) FROM INET_TBL;
-- fix it by inet_same_family() condition
SELECT inet_merge(c, i) FROM INET_TBL WHERE inet_same_family(c, i);

-- Test inet sortsupport with a variety of boundary inputs:
SELECT a FROM (VALUES
  ('0.0.0.0/0'::inet),
  ('0.0.0.0/1'::inet),
  ('0.0.0.0/32'::inet),
  ('0.0.0.1/0'::inet),
  ('0.0.0.1/1'::inet),
  ('127.126.127.127/0'::inet),
  ('127.127.127.127/0'::inet),
  ('127.128.127.127/0'::inet),
  ('192.168.1.0/24'::inet),
  ('192.168.1.0/25'::inet),
  ('192.168.1.1/23'::inet),
  ('192.168.1.1/5'::inet),
  ('192.168.1.1/6'::inet),
  ('192.168.1.1/25'::inet),
  ('192.168.1.2/25'::inet),
  ('192.168.1.1/26'::inet),
  ('192.168.1.2/26'::inet),
  ('192.168.1.2/23'::inet),
  ('192.168.1.255/5'::inet),
  ('192.168.1.255/6'::inet),
  ('192.168.1.3/1'::inet),
  ('192.168.1.3/23'::inet),
  ('192.168.1.4/0'::inet),
  ('192.168.1.5/0'::inet),
  ('255.0.0.0/0'::inet),
  ('255.1.0.0/0'::inet),
  ('255.2.0.0/0'::inet),
  ('255.255.000.000/0'::inet),
  ('255.255.000.000/0'::inet),
  ('255.255.000.000/15'::inet),
  ('255.255.000.000/16'::inet),
  ('255.255.255.254/32'::inet),
  ('255.255.255.000/32'::inet),
  ('255.255.255.001/31'::inet),
  ('255.255.255.002/31'::inet),
  ('255.255.255.003/31'::inet),
  ('255.255.255.003/32'::inet),
  ('255.255.255.001/32'::inet),
  ('255.255.255.255/0'::inet),
  ('255.255.255.255/0'::inet),
  ('255.255.255.255/0'::inet),
  ('255.255.255.255/1'::inet),
  ('255.255.255.255/16'::inet),
  ('255.255.255.255/16'::inet),
  ('255.255.255.255/31'::inet),
  ('255.255.255.255/32'::inet),
  ('255.255.255.253/32'::inet),
  ('255.255.255.252/32'::inet),
  ('255.3.0.0/0'::inet),
  ('0000:0000:0000:0000:0000:0000:0000:0000/0'::inet),
  ('0000:0000:0000:0000:0000:0000:0000:0000/128'::inet),
  ('0000:0000:0000:0000:0000:0000:0000:0001/128'::inet),
  ('10:23::f1/64'::inet),
  ('10:23::f1/65'::inet),
  ('10:23::ffff'::inet),
  ('127::1'::inet),
  ('127::2'::inet),
  ('8000:0000:0000:0000:0000:0000:0000:0000/1'::inet),
  ('::1:ffff:ffff:ffff:ffff/128'::inet),
  ('::2:ffff:ffff:ffff:ffff/128'::inet),
  ('::4:3:2:0/24'::inet),
  ('::4:3:2:1/24'::inet),
  ('::4:3:2:2/24'::inet),
  ('ffff:83e7:f118:57dc:6093:6d92:689d:58cf/70'::inet),
  ('ffff:84b0:4775:536e:c3ed:7116:a6d6:34f0/44'::inet),
  ('ffff:8566:f84:5867:47f1:7867:d2ba:8a1a/69'::inet),
  ('ffff:8883:f028:7d2:4d68:d510:7d6b:ac43/73'::inet),
  ('ffff:8ae8:7c14:65b3:196:8e4a:89ae:fb30/89'::inet),
  ('ffff:8dd0:646:694c:7c16:7e35:6a26:171/104'::inet),
  ('ffff:8eef:cbf:700:eda3:ae32:f4b4:318b/121'::inet),
  ('ffff:90e7:e744:664:a93:8efe:1f25:7663/122'::inet),
  ('ffff:9597:c69c:8b24:57a:8639:ec78:6026/111'::inet),
  ('ffff:9e86:79ea:f16e:df31:8e4d:7783:532e/88'::inet),
  ('ffff:a0c7:82d3:24de:f762:6e1f:316d:3fb2/23'::inet),
  ('ffff:fffa:ffff:ffff:ffff:ffff:ffff:ffff/0'::inet),
  ('ffff:fffb:ffff:ffff:ffff:ffff:ffff:ffff/0'::inet),
  ('ffff:fffc:ffff:ffff:ffff:ffff:ffff:ffff/0'::inet),
  ('ffff:fffd:ffff:ffff:ffff:ffff:ffff:ffff/0'::inet),
  ('ffff:fffe:ffff:ffff:ffff:ffff:ffff:ffff/0'::inet),
  ('ffff:ffff:ffff:fffa:ffff:ffff:ffff:ffff/0'::inet),
  ('ffff:ffff:ffff:fffb:ffff:ffff:ffff:ffff/0'::inet),
  ('ffff:ffff:ffff:fffc:ffff:ffff:ffff:ffff/0'::inet),
  ('ffff:ffff:ffff:fffd::/128'::inet),
  ('ffff:ffff:ffff:fffd:ffff:ffff:ffff:ffff/0'::inet),
  ('ffff:ffff:ffff:fffe::/128'::inet),
  ('ffff:ffff:ffff:fffe:ffff:ffff:ffff:ffff/0'::inet),
  ('ffff:ffff:ffff:ffff:4:3:2:0/24'::inet),
  ('ffff:ffff:ffff:ffff:4:3:2:1/24'::inet),
  ('ffff:ffff:ffff:ffff:4:3:2:2/24'::inet),
  ('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/0'::inet),
  ('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/128'::inet)
) AS i(a) ORDER BY a;

-- test non-error-throwing API for some core types
SELECT pg_input_is_valid('1234', 'cidr');
SELECT * FROM pg_input_error_info('1234', 'cidr');
SELECT pg_input_is_valid('192.168.198.200/24', 'cidr');
SELECT * FROM pg_input_error_info('192.168.198.200/24', 'cidr');

SELECT pg_input_is_valid('1234', 'inet');
SELECT * FROM pg_input_error_info('1234', 'inet');
