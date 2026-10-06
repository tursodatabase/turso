-- Written by tursopg at commit e6c79b43 (before canonical table storage):
--   tursopg -q /abs/path/pg_v1_pg_int8_pk.db "$(grep -v '^--' pg_v1_pg_int8_pk.sql)"
CREATE TYPE pg_int8 AS ENUM ('a', 'b', 'c');
CREATE TABLE k (id pg_int8 PRIMARY KEY, v text);
INSERT INTO k VALUES ('a', 'first');
CREATE TABLE r (id pg_int8 PRIMARY KEY, v text);
INSERT INTO r VALUES ('b', 'second');
ALTER TABLE r RENAME TO r2;
