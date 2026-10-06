-- Written by tursopg at commit e6c79b43 (before canonical table storage):
--   tursopg -q /abs/path/pg_v1_storage.db "$(cat pg_v1_storage.sql)"
-- Then turso-postgres-schema-s.db was opened alone with the same tursopg to checkpoint its WAL.
CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
CREATE DOMAIN posint AS integer CHECK (VALUE > 0);
CREATE DOMAIN money2 AS numeric(10,2);
CREATE TABLE all_types (id serial PRIMARY KEY, b boolean, s smallint, i integer, bi bigint, r real, dp double precision, n numeric(10,2), v varchar(10), t text, by bytea, u uuid, d date, tm time, ts timestamp, tz timestamptz, j json, jb jsonb, ip inet, cd cidr, mac macaddr, ia integer[], ta text[], m mood, p posint, mo money2, created timestamp DEFAULT now());
INSERT INTO all_types (b, s, i, bi, r, dp, n, v, t, by, u, d, tm, ts, tz, j, jb, ip, cd, mac, ia, ta, m, p, mo, created) VALUES (true, -7, 42, 9000000000, 1.5, 2.25, 12.34, 'short', 'some text', '\x0102ff'::bytea, '01945ca0-3189-76c0-9a8f-caf310fc8b8e', '2024-02-29', '10:11:12.5', '2024-01-02 03:04:05.678', '2024-01-02 03:04:05+02:00', '{"a": 1}', '{"b": [1, 2]}', '192.168.1.1', '10.0.0.0/8', '08:00:2b:01:02:03', ARRAY[1, 2, 3], ARRAY['x', 'y'], 'happy', 5, 7.5, '2024-05-06 07:08:09');
INSERT INTO all_types (b, s, i, bi, r, dp, n, v, t, d, tm, ts, tz, m, p, created) VALUES (false, 0, -1, -9000000000, -0.5, 0, -0.01, '', '', '1999-12-31', '00:00:00', '2024-01-01 00:00:00', '1970-01-01 00:00:00+00:00', 'sad', 1, '2024-05-06 07:08:10');
INSERT INTO all_types (created) VALUES ('2024-05-06 07:08:11');
CREATE INDEX all_types_ts ON all_types (ts);
CREATE INDEX all_types_ts_partial ON all_types (id) WHERE ts > '2024-01-01';
CREATE INDEX all_types_b_partial ON all_types (id) WHERE b;
CREATE INDEX all_types_lower_t ON all_types (lower(t));
CREATE TABLE big (id bigint PRIMARY KEY, label text UNIQUE, qty integer CHECK (qty >= 0));
INSERT INTO big VALUES (9000000001, 'first', 1), (-5, 'second', 0);
CREATE TABLE comp (a integer, b text, c numeric(5,1), PRIMARY KEY (a, b), UNIQUE (c));
INSERT INTO comp VALUES (1, 'x', 1.5), (1, 'y', 2.5);
CREATE TABLE child (id integer PRIMARY KEY, big_id bigint REFERENCES big (id), note text DEFAULT 'none');
INSERT INTO child (id, big_id) VALUES (1, 9000000001);
CREATE TABLE added (id integer PRIMARY KEY, a text);
INSERT INTO added VALUES (1, 'a');
ALTER TABLE added ADD COLUMN extra integer DEFAULT 7;
ALTER TABLE added ADD COLUMN when_ts timestamp;
INSERT INTO added VALUES (2, 'b', 8, '2024-03-04 05:06:07');
CREATE TABLE dropped (id integer PRIMARY KEY, keep text, gone numeric(10,2), n numeric(10,2));
INSERT INTO dropped VALUES (1, 'k', 1.25, 3.75);
ALTER TABLE dropped DROP COLUMN gone;
CREATE SCHEMA s;
CREATE TABLE s.st (id integer PRIMARY KEY, m mood, p posint, ts timestamp);
INSERT INTO s.st VALUES (1, 'ok', 3, '2024-01-01 12:00:00');
