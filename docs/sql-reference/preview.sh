#!/usr/bin/env bash
# Builds an mdBook preview from the .mdx source files.
# Usage: ./preview.sh        (builds and serves at localhost:3000)
#        ./preview.sh build   (builds only, output in ./book/)
set -euo pipefail
cd "$(dirname "$0")"

if ! command -v mdbook &>/dev/null; then
  echo "mdbook not found. Install with: cargo install mdbook"
  exit 1
fi

# Clean and create src directory for mdbook
rm -rf src
mkdir -p src/statements src/functions

# Convert .mdx files to .md:
#  - Strip YAML frontmatter
#  - Transform Mintlify callouts to blockquotes
#  - Rewrite /docs/sql-reference/ links to relative .md paths
convert() {
  local in="$1" out="$2"
  # Determine depth: files in subdirs need ../ prefix for top-level pages
  local depth=""
  case "$in" in
    statements/*|functions/*) depth="../" ;;
  esac
  sed -E \
    -e 's/^<Info>$/> **Note**/g' \
    -e 's/^<Warning>$/> **Warning**/g' \
    -e 's/^<Note>$/> **Note**/g' \
    -e 's/^<\/(Info|Warning|Note)>$//g' \
    -e '/^---$/,/^---$/d' \
    -e "s|\(/docs/sql-reference/([^)#]+)(#[^)]+)?\)|(${depth}\1.md\2)|g" \
    "$in" > "$out"
}

for f in *.mdx; do
  convert "$f" "src/${f%.mdx}.md"
done
for f in statements/*.mdx; do
  convert "$f" "src/${f%.mdx}.md"
done
for f in functions/*.mdx; do
  convert "$f" "src/${f%.mdx}.md"
done

# Generate SUMMARY.md
cat > src/SUMMARY.md << 'EOF'
# Summary

- [Data Types](data-types.md)
- [Expressions](expressions.md)

# Statements

- [SELECT](statements/select.md)
- [INSERT](statements/insert.md)
- [UPDATE](statements/update.md)
- [DELETE](statements/delete.md)
- [REPLACE](statements/replace.md)
- [UPSERT](statements/upsert.md)
- [CREATE TABLE](statements/create-table.md)
- [ALTER TABLE](statements/alter-table.md)
- [DROP TABLE](statements/drop-table.md)
- [CREATE INDEX](statements/create-index.md)
- [DROP INDEX](statements/drop-index.md)
- [CREATE VIEW](statements/create-view.md)
- [CREATE MATERIALIZED VIEW](statements/create-materialized-view.md)
- [DROP VIEW](statements/drop-view.md)
- [CREATE TRIGGER](statements/create-trigger.md)
- [DROP TRIGGER](statements/drop-trigger.md)
- [CREATE VIRTUAL TABLE](statements/create-virtual-table.md)
- [CREATE TYPE](statements/create-type.md)
- [DROP TYPE](statements/drop-type.md)
- [Transactions](statements/transactions.md)
- [ATTACH DATABASE](statements/attach-database.md)
- [DETACH DATABASE](statements/detach-database.md)
- [ANALYZE](statements/analyze.md)
- [EXPLAIN](statements/explain.md)

# Functions

- [Scalar](functions/scalar.md)
- [Aggregate](functions/aggregate.md)
- [Date & Time](functions/date-time.md)
- [Math](functions/math.md)
- [JSON](functions/json.md)
- [Window](functions/window.md)
- [Vector](functions/vector.md)
- [Full-Text Search](functions/fts.md)

# Reference

- [PRAGMAs](pragmas.md)
- [Extensions](extensions.md)
- [Experimental Features](experimental-features.md)
- [Compatibility](compatibility.md)
EOF

if [ "${1:-}" = "build" ]; then
  mdbook build
  echo "Built to ./book/"
else
  echo "Starting preview at http://localhost:3000"
  echo "Press Ctrl+C to stop."
  mdbook serve --open
fi
