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
