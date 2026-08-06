# turso-ldap-connector

A demo connector that imports users/groups from an LDAP directory into a Turso
database with CDC (`PRAGMA capture_data_changes_conn('full')`) enabled, and performs
incremental resync using the real RFC 4533 LDAP Content Synchronization ("syncrepl")
protocol instead of a client-side full-scan diff.

This exists to demonstrate that CDC correctly and completely tracks changes coming
from an external system, including a resync that only touches the rows that actually
changed.

## Schema design choice: `l` attribute vs region OUs

Locations are encoded via the standard `l` (locality) attribute on each entry rather
than a per-region OU hierarchy (`ou=us-east,dc=example,dc=org`, etc.). Reasons:

- A single flat `ou=people`/`ou=groups` subtree means one search base and one filter
  work for every region; a region-OU layout would need either N separate searches or
  a base DN wildcard, complicating both the seed script and the sync's LDAP filter.
- Adding/renaming a region is a data change (an attribute value), not a schema/DIT
  change requiring new OUs and ACLs.
- Deletion detection (the hard part of this exercise) is scoped to a single subtree
  either way; OUs would not have simplified that.

## Setup

`docker/docker-compose.yml` defines the test server (`osixia/openldap`, base DN
`dc=example,dc=org`), `docker/syncprov.ldif` enables the `syncprov` overlay required
for RFC 4533 sync cookies (not on by default), and `scripts/setup_ldap.sh` drives both
plus seeding in one shot:

```bash
tools/ldap-connector/scripts/setup_ldap.sh
```

This starts the container, waits for it to accept connections, loads
`syncprov.ldif` via `cn=config`, restarts the container (OpenLDAP 2.4's back-config
only `dlopen()`s a newly-registered module on the next start -- a config-only add is
not enough to activate it), confirms the sync control (OID
`1.3.6.1.4.1.4203.1.9.1.1`) is live, then generates and loads the demo dataset (1000
`inetOrgPerson` users + 500 `groupOfNames` groups spread across six geo locations via
`scripts/gen_seed_ldif.py`).

To tear down: `docker compose -f tools/ldap-connector/docker/docker-compose.yml down`.

## Running the connector

Note: `--db` (and the other connection flags) are top-level arguments and must come
*before* the subcommand (`sync`/`validate`/`max-change-id`), per clap's derive parser.

```bash
# Full baseline sync (no cookie persisted yet -> imports everything)
cargo run -q -p turso-ldap-connector -- --db /tmp/ldap_sync.db sync

# ... simulate drift directly against LDAP, e.g.:
docker exec turso-ldap-demo ldapdelete -x -H ldap://localhost \
  -D "cn=admin,dc=example,dc=org" -w admin_password \
  "uid=user0001,ou=people,dc=example,dc=org"

# Second invocation: a *new* process, reads the persisted cookie from
# /tmp/ldap_sync.cookie, and asks the server for only what changed since then.
cargo run -q -p turso-ldap-connector -- --db /tmp/ldap_sync.db sync

# Inspect row counts / cumulative CDC counts
cargo run -q -p turso-ldap-connector -- --db /tmp/ldap_sync.db validate

# Inspect only the CDC rows written after a given watermark (e.g. after recording
# `max-change-id` right after the full sync)
cargo run -q -p turso-ldap-connector -- --db /tmp/ldap_sync.db max-change-id
cargo run -q -p turso-ldap-connector -- --db /tmp/ldap_sync.db validate --since-change-id 2966
```

The cookie is stored as a plain file next to the database (`<db>.cookie`) rather than
inside the Turso db itself, specifically so writing it doesn't also generate CDC rows
that would pollute the `turso_cdc` counts we're trying to validate.

## RFC 4533 sync control

`ldap3` (0.11) implements the RFC 4533 controls directly
(`ldap3::controls::{SyncRequest, SyncState, SyncDone, SyncInfo, parse_syncinfo}`), so
no manual ASN.1 construction was needed. A full scan and an incremental resync are the
*same* search from the client's point of view (`src/ldap_client.rs::run_content_sync`)
-- the only difference is whether a cookie is attached to the `SyncRequest` control.

**Empirical finding that changed the design**: the initial assumption was that with
`olcSpSessionLog` configured, deletions would come back as explicit per-entry
`EntryState::Delete` results carrying the DN directly (both this and the batched form
below are valid per RFC 4533; the spec leaves it server-defined). Testing against this
OpenLDAP 2.4 + `syncprov` build showed the opposite: *every* deletion, even a single
one, is reported via a `syncIdSet` intermediate ("SyncInfo") message -- a batch of
`entryUUID`s to remove, with no DN and no attributes at all. That forced a design
change: since resolving a bare UUID to a row requires already knowing the mapping, the
connector persists each entry's `entryUUID` (read from its `SyncState` control at
upsert time, not as a regular attribute) in a `uuid` column on `users`/`groups`, and
resolves `syncIdSet` deletes against that column at apply time
(`src/db.rs::apply_ops`, `SyncOp::DeleteByUuid`). The per-entry `EntryState::Delete`
path is still implemented too (`SyncOp::Delete`), in case a different server/version
combination uses it.

**Known limitation**: the *other* `syncIdSet` variant (`refresh_deletes: false`, a
"present list" of all surviving UUIDs, used for reconciliation when a client's cookie
is older than the session log's history) is parsed only for its cookie; the full "diff
the present ID set against known DNs, delete anything absent" reconciliation isn't
implemented. This demo's overlay is configured with a session log of 100 entries,
comfortably larger than the ~15-20 deletes it exercises, so that path isn't exercised
here. A production connector would need to implement it (or fall back to periodic full
reconciliation) to be correct at scale.

## Tests

```bash
cargo test -p turso-ldap-connector -- --ignored
```

Requires the live container + seeded dataset above. See `tests/cdc_validation.rs`.
