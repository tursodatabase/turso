-- Written by tursopg at commit e6c79b43 (before canonical table storage):
--   tursopg -q /abs/path/pg_v1_renamed.db "$(grep -v '^--' pg_v1_renamed.sql)"
CREATE TABLE r (id bigint PRIMARY KEY, ts timestamp, n numeric(10,2), note text DEFAULT 'x');
INSERT INTO r VALUES (1, '2024-01-01 10:00:00', 2.5, 'first');
ALTER TABLE r RENAME TO r2;
