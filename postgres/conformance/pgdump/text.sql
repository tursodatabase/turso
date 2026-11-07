CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT);

INSERT INTO notes VALUES
    (7, NULL),
    (19, ''),
    (41, E'\\N'),
    (83, E'line one\nline two\tback\\slash'),
    (97, 'olá ''Turso''');
