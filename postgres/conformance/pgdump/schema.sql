CREATE TABLE "Table 1" ("Mixed Case" INTEGER DEFAULT -1, big BIGINT DEFAULT -2147483648, pos BIGINT DEFAULT 2147483647);
CREATE INDEX idx ON "Table 1" ("Mixed Case" DESC, big ASC);
CREATE TABLE "Table 2" ("Mixed Key" TEXT PRIMARY KEY);
CREATE UNIQUE INDEX composite_idx ON "Table 2" ("Mixed Key" DESC);
INSERT INTO "Table 2" VALUES ('one'), ('two');
