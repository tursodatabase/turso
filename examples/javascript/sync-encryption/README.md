# Sync Example

This example demonstrates syncing with Turso Cloud, including optional remote encryption support.

## Prerequisites

Create an encrypted Turso Cloud database. Refer to the documentation here for [instructions](https://docs.turso.tech/cloud/encryption).

## Running

```bash
npm run build
npm install
export TURSO_REMOTE_URL="http://your-db.localhost:8080"
export TURSO_AUTH_TOKEN="your-auth-token"  # optional
export TURSO_REMOTE_ENCRYPTION_KEY="base64-encoded-key"  # optional
npm start
```
