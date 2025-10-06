# sync-node

This is a minimal example showing how to use [`@tursodatabase/sync`](https://www.npmjs.com/package/@tursodatabase/database) in the node.js


The `@tursodatabase/sync` package extends a regular database with **bidirectional synchronization** between a local file and a remote [Turso Cloud](https://turso.tech/) database.

It allows you to:

* **`pull()`**: fetch and apply remote changes into your local database.
  This ensures your local state reflects the latest server-side updates.

* **`push()`**: upload local changes back to the remote database.
  This is useful when you’ve made inserts/updates locally and want to replicate them to the cloud.

Together, these methods make it possible to work **offline-first** (mutating the local database) and later sync your changes to Turso Cloud, while also bringing in any updates from other clients.

---

## Usage

```bash
npm install

TURSO_AUTH_TOKEN=$(turso db tokens create <db-name>) # create auth token for the database in the Turso Cloud
TURSO_DATABASE_URL=$(turso db show <db-name> --url) # fetch URL for the database in the Turso Cloud
node index.mjs
```

Here’s a version of your README with a short high-level explanation of what the **sync package** does, and what `pull`/`push` mean in practice:
