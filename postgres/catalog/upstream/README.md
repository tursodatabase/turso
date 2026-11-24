# PostgreSQL's built-in catalog rows

`postgres.bki` holds every built-in row of every PostgreSQL system catalog:
the types, functions, operators, casts and operator classes that exist in a
new database. `src/generated.rs` of this crate is generated from it.

## Provenance

- Source: https://github.com/postgres/postgres
- The tag and the commit are in `VERSION`. They are the release that
  `postgres/conformance/upstream` takes its regression tests from.
- License: `licenses/postgres-license.md`

`postgres.bki` is not a file of the PostgreSQL source tree. PostgreSQL's
build writes it with `src/backend/catalog/genbki.pl` from the catalog data
files in `src/include/catalog/` (`pg_type.dat`, `pg_proc.dat` and the
others) and the headers beside them. Those data files name the rows they
refer to, such as `castfunc => 'int2(int8)'`, and leave out defaults, array
types and some oids. `genbki.pl` resolves all of that, so `postgres.bki`
has one line per row with every reference as an oid.

`../update.sh` produces the file by downloading the catalog files of a tag
and running PostgreSQL's own `genbki.pl` on them. The result is copied here
verbatim. Do not edit it.

## Updating to another release

```bash
postgres/catalog/update.sh REL_18_4
```

This replaces `postgres.bki` and `VERSION`, and regenerates
`src/generated.rs`. The generator stops with an error when the new file has
a column type or a quoting form that it does not know.

To regenerate `src/generated.rs` alone, after a change to the generator:

```bash
cargo run -p turso_pg_catalog_gen
```
