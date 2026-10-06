-- Written by tursopg at commit e6c79b43 (before canonical table storage):
--   tursopg -q /abs/path/pg_v1_numeric_comparison_index.db "$(grep -v '^--' pg_v1_numeric_comparison_index.sql)"
CREATE TABLE p (id integer PRIMARY KEY, name text, price numeric(10,2), cost numeric(10,2), list numeric(10,2));
INSERT INTO p VALUES (1, 'a', 5, 10, 20), (2, 'b', 50, 10, 60), (3, 'c', 12, 10, 30);
CREATE UNIQUE INDEX p_profit ON p (name) WHERE price > cost + 1;
CREATE UNIQUE INDEX p_listed ON p (name) WHERE price IN (5, 50);
CREATE UNIQUE INDEX p_unlisted ON p (name) WHERE price NOT IN (5);
CREATE UNIQUE INDEX p_half ON p (name) WHERE price >= 0.5 * list;
CREATE INDEX p_margin ON p ((price - cost > 0));
CREATE INDEX p_double ON p ((price * 2 > 15));
CREATE INDEX p_rounded ON p ((price = 1.005));
