CREATE TABLE items (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

INSERT INTO items VALUES (7, 'alice'), (19, 'bob');

CREATE TABLE empty_items (id INTEGER, name TEXT);

CREATE TABLE "Mixed Table" (
    "Mixed Key" TEXT,
    "Other Column" INTEGER,
    CONSTRAINT "Mixed PK" PRIMARY KEY ("Mixed Key")
);
CREATE INDEX mixed_column_index ON "Mixed Table" ("Other Column");
INSERT INTO "Mixed Table" VALUES ('first', 7), ('second', 19);

CREATE TABLE "Inline Table" (
    "Mixed Id" INTEGER CONSTRAINT "Inline PK" PRIMARY KEY,
    value TEXT
);
INSERT INTO "Inline Table" VALUES (11, 'inline');

CREATE SCHEMA "other schema";
CREATE TABLE "other schema"."Mixed Table" (
    "K1" TEXT,
    "K2" INTEGER,
    PRIMARY KEY ("K2", "K1")
);
INSERT INTO "other schema"."Mixed Table" VALUES ('parent', 23);

CREATE TABLE "Child Table" (
    "Ref" TEXT CONSTRAINT "Inline FK" REFERENCES "Mixed Table",
    "Code" INTEGER CONSTRAINT "Inline Unique" UNIQUE,
    "First" TEXT,
    "Second" INTEGER,
    CONSTRAINT "Cross Schema FK" FOREIGN KEY ("First", "Second")
        REFERENCES "other schema"."Mixed Table" ("K1", "K2") ON UPDATE CASCADE ON DELETE RESTRICT,
    CONSTRAINT "Pair Unique" UNIQUE ("Second", "First")
);
INSERT INTO "Child Table" VALUES ('second', 31, 'parent', 23);
