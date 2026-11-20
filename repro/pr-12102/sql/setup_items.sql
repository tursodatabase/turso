PRAGMA journal_mode = 'mvcc';
PRAGMA mvcc_checkpoint_threshold = -1;
CREATE TABLE items(id INTEGER PRIMARY KEY, payload TEXT, version INTEGER, c0 INTEGER, c1 INTEGER, c2 INTEGER, c3 INTEGER, c4 INTEGER, c5 INTEGER, c6 INTEGER);
CREATE INDEX items_0 ON items(c0);
CREATE INDEX items_1 ON items(c1);
CREATE INDEX items_2 ON items(c2);
CREATE INDEX items_3 ON items(c3);
CREATE INDEX items_4 ON items(c4);
WITH RECURSIVE seq(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM seq WHERE x < 50000)
INSERT INTO items (id,payload,version,c0,c1,c2,c3,c4,c5,c6)
SELECT x,printf('%01400d',x),0,x,x+1,x+2,x+3,x+4,x+5,x+6 FROM seq;
PRAGMA wal_checkpoint(TRUNCATE);
SELECT count(*) FROM items;
