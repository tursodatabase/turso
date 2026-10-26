# pg_dump conformance

`make -C postgres/conformance test-pgdump` compares complete PostgreSQL and
Turso dump files byte for byte. It reports differences in headers, ownership,
catalog metadata, and data without normalizing them.

`make -C postgres/conformance test-pgdump-sequences` checks standalone sequence
restoration. It dumps the sequence fixture with `--no-owner --no-acl`, restores
the unmodified Turso dump into a fresh PostgreSQL database, and compares the
queries in `checks/sequences.sql` against the original PostgreSQL database.
The checks cover configuration, saved counter values, `is_called`, and the next
values after restoration, including descending, unused, reset, cycling, and
large-value sequences. This target does not compare dump files or cover SERIAL
ownership dependencies or table data transfer.

Both targets require `pg_dump`, `psql`, and either Docker or a disposable server
specified by `PG_DSN` with permission to create databases.
