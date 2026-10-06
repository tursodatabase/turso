-- Written by tursopg at commit e6c79b43 (before canonical table storage):
--   tursopg -q /abs/path/pg_v1_pg_prefix_type.db "$(grep -v '^--' pg_v1_pg_prefix_type.sql)"
CREATE TYPE pg_date AS ENUM ('a', 'b');
CREATE TABLE pd (id integer PRIMARY KEY, x pg_date);
INSERT INTO pd VALUES (1, 'a');
