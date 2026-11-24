#!/usr/bin/env bash
# Replaces upstream/postgres.bki with the one of a PostgreSQL release.
#
#   ./update.sh REL_18_4
#
# The argument is a tag of https://github.com/postgres/postgres. The script
# downloads the catalog files of that tag and runs PostgreSQL's own
# src/backend/catalog/genbki.pl on them, the script that writes postgres.bki
# when PostgreSQL is built. It needs git and perl.
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <tag of postgres/postgres, for example REL_18_4>" >&2
    exit 2
fi
tag=$1
# REL_18_4 is a release of major version 18.
major=${tag#REL_}
major=${major%%_*}
if ! [[ $major =~ ^[0-9]+$ ]]; then
    echo "error: cannot read the major version from the tag $tag" >&2
    exit 2
fi

catalog_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# genbki.pl reads every catalog header and data file, and three more
# headers for the constants it puts in the output.
git clone --quiet --depth 1 --branch "$tag" --filter=blob:none --sparse \
    -c advice.detachedHead=false \
    https://github.com/postgres/postgres.git "$work/postgres"
git -C "$work/postgres" sparse-checkout set --no-cone \
    /src/include/catalog/ \
    /src/backend/catalog/genbki.pl \
    /src/backend/catalog/Catalog.pm \
    /src/include/access/transam.h \
    /src/include/mb/pg_wchar.h \
    /src/include/pg_config_manual.h
commit=$(git -C "$work/postgres" rev-parse HEAD)

mkdir "$work/out"
(
    cd "$work/postgres"
    perl -I src/backend/catalog src/backend/catalog/genbki.pl \
        --include-path=src/include \
        --set-version="$major" \
        --output="$work/out/" \
        src/include/catalog/pg_*.h
)

cp "$work/out/postgres.bki" "$catalog_dir/upstream/postgres.bki"
cat > "$catalog_dir/upstream/VERSION" <<VERSION
tag: $tag
commit: $commit
VERSION

echo "upstream/postgres.bki is now the one of $tag ($commit)"
