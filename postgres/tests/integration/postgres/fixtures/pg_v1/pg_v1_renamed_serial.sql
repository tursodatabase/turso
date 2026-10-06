-- Written by tursopg at commit e6c79b43 (before canonical table storage). The
-- RENAME stores a row that this tursopg cannot open again:
--   grep -v '^--' pg_v1_renamed_serial.sql | while IFS= read -r q; do tursopg -q /abs/path/pg_v1_renamed_serial.db "$q"; done
CREATE TABLE rs (id serial PRIMARY KEY, ts timestamp DEFAULT now(), flag boolean DEFAULT false, a text);
INSERT INTO rs (ts, a) VALUES ('2024-01-01 10:00:00', 'first');
ALTER TABLE rs RENAME TO rs2;
