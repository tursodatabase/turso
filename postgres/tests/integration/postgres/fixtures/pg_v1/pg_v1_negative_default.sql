-- Written by tursopg at commit e6c79b43 (before canonical table storage):
--   tursopg -q /abs/path/pg_v1_negative_default.db "$(grep -v '^--' pg_v1_negative_default.sql)"
CREATE TABLE nd (id int PRIMARY KEY, a int DEFAULT -1, n numeric(10,2) DEFAULT -1.5, note text);
INSERT INTO nd (id, note) VALUES (1, 'x');
