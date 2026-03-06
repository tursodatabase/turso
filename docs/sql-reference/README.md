# Turso Reference

This directory contains the Turso reference documentation — both the SQL language reference and the CLI reference. The files are `.mdx` (Markdown + JSX components) and render as readable markdown on GitHub.

## Structure

```
sql-reference/
├── data-types.mdx              # Storage classes, type affinity, STRICT tables
├── expressions.mdx             # Literals, operators, CAST, CASE, pattern matching
├── experimental-features.mdx   # How to enable experimental features (CLI + all SDKs)
├── statements/                 # DDL and DML statements
│   ├── select.mdx
│   ├── insert.mdx
│   ├── update.mdx
│   ├── ...
│   ├── create-type.mdx         # Turso-specific custom types
│   └── transactions.mdx        # Includes BEGIN CONCURRENT (Turso MVCC)
├── functions/                  # Built-in functions
│   ├── scalar.mdx
│   ├── aggregate.mdx
│   ├── json.mdx
│   ├── vector.mdx              # Turso-specific vector search
│   ├── fts.mdx                 # Turso-specific full-text search
│   └── ...
├── pragmas.mdx                 # All supported PRAGMAs (includes CDC, encryption)
├── extensions.mdx              # UUID, regexp, vector, time, CSV, percentile
├── compatibility.mdx           # SQLite compatibility notes and known differences
└── cli/                        # CLI reference
    ├── getting-started.mdx     # Installation, usage patterns, output modes
    ├── command-line-options.mdx # CLI flags and arguments
    └── shell-commands.mdx      # Dot commands (.tables, .schema, etc.)
```

## Editing guidelines

- Turso-specific features are marked with `<Info>` callouts. Keep this convention.
- Experimental features should link to `experimental-features.mdx` for enablement instructions.
- Unsupported SQLite features are documented only in `compatibility.mdx`, not on individual pages.
- Every function should have a parameter table with types, a return type, and at least one example.
- Use `sql` language tags on all code blocks.
- Keep pages self-contained — use explicit cross-references instead of "as mentioned above".

## Local preview (optional)

These docs are integrated into [turso-docs](https://github.com/tursodatabase/turso-docs) for production rendering with Mintlify. To preview locally, use mdBook (OSS, no Node required):

```bash
# Install mdbook if you don't have it
cargo install mdbook

# Preview with live reload at http://localhost:3000
cd docs/sql-reference
./preview.sh
```

The script converts `.mdx` files to markdown, transforms Mintlify callouts to blockquotes, and serves via mdBook. The `<Info>`/`<Warning>` callouts render as blockquotes — not as styled boxes, but all content is readable.

If you add a new page, update `preview.sh` to include it in the SUMMARY.
