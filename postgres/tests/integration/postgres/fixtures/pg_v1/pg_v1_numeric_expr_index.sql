-- Written by tursopg at commit e6c79b43 (before canonical table storage):
--   tursopg -q /abs/path/pg_v1_numeric_expr_index.db "$(grep -v '^--' pg_v1_numeric_expr_index.sql)"
CREATE TABLE x (id integer PRIMARY KEY, val numeric(10,2));
INSERT INTO x VALUES (1, 10), (2, 20.5), (3, -4);
CREATE INDEX x_double ON x ((val * 2));
CREATE INDEX x_big ON x (id) WHERE val > 15;
