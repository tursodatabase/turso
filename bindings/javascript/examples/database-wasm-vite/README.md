# database-wasm-vite

This is a minimal example showing how to use [`@tursodatabase/database-wasm`](https://www.npmjs.com/package/@tursodatabase/database-wasm) in the browser with [Vite](https://vite.dev/).  

---

## Usage

```bash
npm install
npm run dev
# or build assets and serve them with simple node.js server
npm run build
npm run serve
```

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