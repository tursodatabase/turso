# pytest plugin loaded by run.sh. SQLAlchemy's test runner loads a dialect's
# provisioning hooks (create/drop the per-worker test databases) from
# "<dialect package>.provision". The Turso dialect lives in turso.sqlalchemy,
# which has no such module, so register SQLite's hooks explicitly.
import sqlalchemy.dialects.sqlite.provision  # noqa: F401
