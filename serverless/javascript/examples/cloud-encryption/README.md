# Cloud Encryption Example

This smoll example shows how to connect to an encrypted Turso Cloud database.

## Prerequisites

You need a Turso Cloud database with encryption enabled. Refer to the documentation here for [instructions](https://docs.turso.tech/cloud/encryption).

## Running

```bash
npm i
export TURSO_DATABASE_URL="libsql://your-db.turso.io"
export TURSO_AUTH_TOKEN="your-auth-token"
export TURSO_REMOTE_ENCRYPTION_KEY="base64-encoded-key"
node index.mjs
```