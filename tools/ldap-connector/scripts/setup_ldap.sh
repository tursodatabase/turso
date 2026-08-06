#!/usr/bin/env bash
# Stands up the test OpenLDAP server used by the turso-ldap-connector demo:
# starts the container, enables the syncprov overlay (required for RFC 4533
# sync cookies), and seeds 1000 users / 500 groups across 6 geo locations.
#
# Usage: tools/ldap-connector/scripts/setup_ldap.sh
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

ADMIN_DN="cn=admin,dc=example,dc=org"
ADMIN_PW="admin_password"
CONTAINER="turso-ldap-demo"

docker compose -f docker/docker-compose.yml up -d

echo "Waiting for LDAP server to accept connections..."
until docker exec "$CONTAINER" ldapsearch -x -H ldap://localhost -b "" -s base >/dev/null 2>&1; do
    sleep 1
done

# osixia/openldap's first-ever start bootstraps a temporary slapd instance,
# stops it, then relaunches the real one -- the ldap:// readiness check above
# can catch that transient instance right before it's torn down, so the
# ldapi:/// admin call below can land in the restart gap. Retry it rather than
# relying on timing.
echo "Enabling syncprov overlay..."
docker cp docker/syncprov.ldif "$CONTAINER:/tmp/syncprov.ldif"
err_file="$(mktemp)"
trap 'rm -f "$err_file"' EXIT
synced=0
for _ in $(seq 1 30); do
    if docker exec "$CONTAINER" ldapadd -Q -Y EXTERNAL -H ldapi:/// -f /tmp/syncprov.ldif 2>"$err_file"; then
        synced=1
        break
    fi
    if ! grep -q "Can't contact LDAP server" "$err_file"; then
        echo "error: syncprov setup failed" >&2
        cat "$err_file" >&2
        exit 1
    fi
    sleep 1
done
if [ "$synced" -ne 1 ]; then
    echo "error: ldapi:/// never became available for syncprov setup" >&2
    cat "$err_file" >&2
    exit 1
fi

# OpenLDAP 2.4's back-config only dlopen()s a newly-registered module on the
# next start -- a config-only add is not enough to actually activate syncprov.
docker restart "$CONTAINER"

echo "Waiting for LDAP server to come back up..."
until docker exec "$CONTAINER" ldapsearch -x -H ldap://localhost -b "" -s base >/dev/null 2>&1; do
    sleep 1
done

echo "Confirming RFC 4533 sync control is live..."
supported_controls="$(docker exec "$CONTAINER" ldapsearch -x -H ldap://localhost -b "" -s base supportedControl)"
if ! grep -q "1.3.6.1.4.1.4203.1.9.1.1" <<<"$supported_controls"; then
    echo "error: RFC 4533 sync control not advertised by server" >&2
    exit 1
fi

echo "Generating and loading seed dataset (1000 users, 500 groups)..."
python3 scripts/gen_seed_ldif.py > /tmp/turso-ldap-seed.ldif
docker cp /tmp/turso-ldap-seed.ldif "$CONTAINER:/tmp/seed.ldif"
docker exec "$CONTAINER" ldapadd -x -H ldap://localhost \
    -D "$ADMIN_DN" -w "$ADMIN_PW" -f /tmp/seed.ldif

echo "LDAP test server ready: ldap://localhost:3389, base dn=dc=example,dc=org"
