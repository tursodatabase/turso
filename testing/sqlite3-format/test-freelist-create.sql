CREATE TABLE t (
    id INTEGER PRIMARY KEY,
    data BLOB
);

INSERT INTO t VALUES (1, randomblob(500));
INSERT INTO t VALUES (2, randomblob(500));
INSERT INTO t VALUES (3, randomblob(500));
INSERT INTO t VALUES (4, randomblob(500));
INSERT INTO t VALUES (5, randomblob(500));
INSERT INTO t VALUES (6, randomblob(500));
INSERT INTO t VALUES (7, randomblob(500));
INSERT INTO t VALUES (8, randomblob(500));
INSERT INTO t VALUES (9, randomblob(500));
INSERT INTO t VALUES (10, randomblob(500));
INSERT INTO t VALUES (11, randomblob(500));
INSERT INTO t VALUES (12, randomblob(500));
INSERT INTO t VALUES (13, randomblob(500));
INSERT INTO t VALUES (14, randomblob(500));
INSERT INTO t VALUES (15, randomblob(500));
INSERT INTO t VALUES (16, randomblob(500));
INSERT INTO t VALUES (17, randomblob(500));
INSERT INTO t VALUES (18, randomblob(500));
INSERT INTO t VALUES (19, randomblob(500));
INSERT INTO t VALUES (20, randomblob(500));

INSERT INTO t SELECT id + 20, randomblob(500) FROM t WHERE id <= 20;
INSERT INTO t SELECT id + 40, randomblob(500) FROM t WHERE id <= 40;
INSERT INTO t SELECT id + 80, randomblob(500) FROM t WHERE id <= 80;

-- Delete even-numbered rows to create freelist pages
DELETE FROM t WHERE (id % 2) = 0;
