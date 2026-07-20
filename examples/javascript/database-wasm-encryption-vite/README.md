# database-wasm-encryption-vite

Browser example for local encryption with [`@tursodatabase/database-wasm`](https://www.npmjs.com/package/@tursodatabase/database-wasm) and Vite.

Pass `encryption` when calling `connect()`:

```js
const db = await connect("encrypted-guestbook.db", {
  encryption: {
    cipher: "aegis256",
    hexkey: "<64-char hex key>",
  },
});
```

Build the WASM bindings first (see [`database-wasm-vite`](../database-wasm-vite/README.md)), then:

```bash
npm install
npm run dev
```

COOP/COEP headers are required for SharedArrayBuffer — see [`database-wasm-vite`](../database-wasm-vite/README.md).
