#!/usr/bin/env bash
# Writes functions.dat, types.dat and casts.dat from the catalog of a
# PostgreSQL server. command_tags.dat is a copy of the tags in
# src/include/tcop/cmdtaglist.h of PostgreSQL 17, which no catalog shows.
#
#   PG_DSN=postgres://postgres@127.0.0.1:5432/postgres postgres/parser/catalog/generate.sh
#
# Use a new PostgreSQL 17 server: the files list the functions and casts
# that PostgreSQL has built in, and tursopg must see the same ones.
set -euo pipefail

cd "$(dirname "$0")"
: "${PG_DSN:?set PG_DSN to the URL of a PostgreSQL server}"

# One line for each function of pg_catalog:
#
#   <kind><s if it returns a set, else -><volatility><language> <name>(<parameters>) <result type>[ =<C function>]
#
# The language is i for internal, c for C and s for SQL. A parameter is
# "[variadic ][<name> ]<type>[ default]". The C function is the AS string of
# a LANGUAGE internal function when it is not the name of the function.
psql "$PG_DSN" --no-psqlrc --tuples-only --no-align --quiet > functions.dat <<'SQL'
SELECT p.prokind::text
    || CASE WHEN p.proretset THEN 's' ELSE '-' END
    || p.provolatile::text
    || CASE l.lanname WHEN 'internal' THEN 'i' WHEN 'c' THEN 'c' WHEN 'sql' THEN 's' ELSE '?' END
    || ' ' || p.proname || '(' || coalesce(parameters.list, '') || ') ' || result.typname
    || CASE WHEN l.lanname = 'internal' AND p.prosrc <> p.proname
            THEN ' =' || p.prosrc ELSE '' END
FROM pg_proc p
JOIN pg_language l ON l.oid = p.prolang
JOIN pg_type result ON result.oid = p.prorettype
LEFT JOIN LATERAL (
    SELECT string_agg(
        CASE WHEN p.provariadic <> 0 AND a.position = p.pronargs THEN 'variadic ' ELSE '' END
        || CASE WHEN coalesce(n.name, '') <> '' THEN n.name || ' ' ELSE '' END
        || t.typname
        || CASE WHEN a.position > p.pronargs - p.pronargdefaults THEN ' default' ELSE '' END,
        ', ' ORDER BY a.position) AS list
    FROM unnest(p.proargtypes::oid[]) WITH ORDINALITY AS a(type, position)
    JOIN pg_type t ON t.oid = a.type
    LEFT JOIN LATERAL (
        SELECT x.name
        FROM unnest(coalesce(p.proargnames, '{}'),
                    coalesce(p.proargmodes,
                             array_fill('i'::"char", ARRAY[coalesce(cardinality(p.proargnames), 0)])))
             WITH ORDINALITY AS x(name, mode, position)
        WHERE x.mode IN ('i', 'b', 'v')
        ORDER BY x.position
        OFFSET a.position - 1
        LIMIT 1
    ) n ON true
) parameters ON true
WHERE p.pronamespace = 'pg_catalog'::regnamespace
ORDER BY p.proname, coalesce(parameters.list, '');
SQL

# One line for each type of pg_catalog:
#
#   <name> <kind><category> <name in messages>
#
# The kind and the category are typtype and typcategory of pg_type.
psql "$PG_DSN" --no-psqlrc --tuples-only --no-align --quiet --field-separator ' ' > types.dat <<'SQL'
SELECT t.typname, t.typtype::text || t.typcategory::text, format_type(t.oid, NULL)
FROM pg_type t
WHERE t.typnamespace = 'pg_catalog'::regnamespace
ORDER BY 1;
SQL

# One line for each implicit (i) or assignment (a) cast between built-in
# types:
#
#   <source type> <target type> <context> <method>
#
# The method is b when the cast does not change the value.
psql "$PG_DSN" --no-psqlrc --tuples-only --no-align --quiet --field-separator ' ' > casts.dat <<'SQL'
SELECT s.typname, t.typname, c.castcontext, c.castmethod
FROM pg_cast c
JOIN pg_type s ON s.oid = c.castsource
JOIN pg_type t ON t.oid = c.casttarget
WHERE c.castcontext IN ('i', 'a')
  AND s.typnamespace = 'pg_catalog'::regnamespace
  AND t.typnamespace = 'pg_catalog'::regnamespace
ORDER BY 1, 2;
SQL
