CREATE TABLE items (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

INSERT INTO items VALUES (7, 'alice'), (19, 'bob');

CREATE TABLE empty_items (id INTEGER, name TEXT);
