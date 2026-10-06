-- Written by tursodb at commit e6c79b43 with the SQLite dialect:
--   tursodb -q --experimental-custom-types /abs/path/sqlite_v1_numeric_comparison_index.db "$(grep -v '^--' sqlite_v1_numeric_comparison_index.sql)"
CREATE TABLE v (id INTEGER PRIMARY KEY, name TEXT, val numeric(10,2), other numeric(10,2)) STRICT;
INSERT INTO v VALUES (1, 'a', -5, 1), (2, 'b', 5, 2), (3, 'c', 50, 3);
CREATE UNIQUE INDEX v_positive ON v (name) WHERE val > -1;
CREATE UNIQUE INDEX v_listed ON v (name) WHERE val IN (5, 50);
CREATE UNIQUE INDEX v_above ON v (name) WHERE val > other * 2;
CREATE INDEX v_negative ON v ((val > -1));
CREATE INDEX v_sum ON v ((val + other > 4));
