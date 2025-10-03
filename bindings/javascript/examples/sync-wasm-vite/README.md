# sync-wasm-vite

This is a minimal example showing how to use [`@tursodatabase/sync-wasm`](https://www.npmjs.com/package/@tursodatabase/sync-wasm) in the browser with [Vite](https://vite.dev/).  

The `@tursodatabase/sync-wasm` package extends a regular database with **bidirectional synchronization** between a local file and a remote [Turso Cloud](https://turso.tech/) database.

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

export VITE_TURSO_AUTH_TOKEN=$(turso db tokens create <db-name>) # create auth token for the database in the Turso Cloud
export VITE_TURSO_DATABASE_URL=$(turso db show <db-name> --url) # fetch URL for the database in the Turso Cloud

npm run dev
# or build assets and serve them with simple node.js server
npm run build
npm run serve
```

> **⚠️ Warning:** When using `VITE_TURSO_AUTH_TOKEN` in the browser, **this token will be bundled into your client-side code**.  
That means it is **publicly visible to anyone** who loads your site.
> - Do **not** treat this token as a secret.  
> - Only use it with databases or roles that are safe to expose (e.g., read-only, demo instances).

---

## Important: COOP / COEP Headers

Because `@tursodatabase/database-wasm` relies on **SharedArrayBuffer**, you need Cross-Origin headers in development and production:

### Vite dev server config (`vite.config.js`)

```js
import { defineConfig } from 'vite'

export default defineConfig({
  server: {
    headers: {
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
    }
  }
})
```

### Static production server (`server.mjs`)

When serving the `dist/` build, also make sure your server sets these headers:

```js
res.setHeader("Cross-Origin-Opener-Policy", "same-origin");
res.setHeader("Cross-Origin-Embedder-Policy", "require-corp");
```

### Vercel deployment

If you deploy to [**Vercel**](https://vercel.com/), add a `vercel.json` file to ensure COOP / COEP headers are set:

```json
{
  "headers": [
    {
      "source": "/(.*)",
      "headers": [
        { "key": "Cross-Origin-Opener-Policy", "value": "same-origin" },
        { "key": "Cross-Origin-Embedder-Policy", "value": "require-corp" }
      ]
    }
  ]
}
```