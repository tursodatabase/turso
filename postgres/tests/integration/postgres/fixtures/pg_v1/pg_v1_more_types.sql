-- Written by tursopg at commit e6c79b43 (before canonical table storage), one
-- tursopg run for each statement, because the base hangs on a non-empty array in
-- a table with a numeric column:
--   grep -v '^--' pg_v1_more_types.sql | while IFS= read -r q; do tursopg -q /abs/path/pg_v1_more_types.db "$q"; done
CREATE TABLE more_types (id bigserial PRIMARY KEY, tt timetz, n5 numeric(5), n30 numeric(30,5), nn numeric, c3 char(3), m8 macaddr8);
INSERT INTO more_types (tt, n5, n30, nn, c3, m8) VALUES ('10:00:00', 12345, 12345.6789, 3.25, 'ab', '08:00:2b:01:02:03:04:05');
INSERT INTO more_types (n5) VALUES (-7);
CREATE TABLE more_arrays (id integer PRIMARY KEY, bia bigint[], ba boolean[], ua uuid[]);
INSERT INTO more_arrays VALUES (1, ARRAY[9000000000, -1], ARRAY[true, false], '{01945ca0-3189-76c0-9a8f-caf310fc8b8e}'), (2, '{}', '{}', '{}');
CREATE TABLE ck (id integer PRIMARY KEY, d text CHECK (d::date <> '2024-01-01'), t text CHECK (t::timestamp > '2024-01-01'));
INSERT INTO ck VALUES (1, '2024-01-01 10:00:00', NULL), (2, 'garbage', NULL), (3, NULL, '2024-01-02');
