<p align="center">
  <h1 align="center">Turso Sync for JavaScript</h1>
</p>

<p align="center">
  <a title="JavaScript" target="_blank" href="https://www.npmjs.com/package/@tursodatabase/sync"><img alt="npm" src="https://img.shields.io/npm/v/@tursodatabase/sync"></a>
  <a title="MIT" target="_blank" href="https://github.com/tursodatabase/turso/blob/main/LICENSE.md"><img src="http://img.shields.io/badge/license-MIT-orange.svg?style=flat-square"></a>
</p>
<p align="center">
  <a title="Users Discord" target="_blank" href="https://tur.so/discord"><img alt="Chat with other users of Turso on Discord" src="https://img.shields.io/discord/933071162680958986?label=Discord&logo=Discord&style=social"></a>
</p>

---

## About

This package is for syncing local Turso databases to the Turso Cloud and back.

> **⚠️ Warning:** This software is ALPHA, only use for development, testing, and experimentation. We are working to make it production ready, but do not use it for critical data right now.

## Installation

```bash
npm install @tursodatabase/sync
```

## Getting Started

To sync a database hosted at [Turso Cloud](https://turso.tech):

```js
import { connect } from '@tursodatabase/sync';

const db = await connect({
    path: 'local.db',                // path used as a prefix for local files created by sync-engine
    url: 'https://<db>.turso.io',    // URL of the remote database: turso db show <db>
    authToken: '...',                // auth token issued from the Turso Cloud: turso db tokens create <db>
    clientName: 'turso-sync-example' // arbitrary client name
});

// db has same functions as Database class from @tursodatabase/database package but adds few more methods for sync:
await db.pull(); // pull changes from the remote
await db.push(); // push changes to the remote
await db.sync(); // pull & push changes
```

## Related Packages

* The [@tursodatabase/database](https://www.npmjs.com/package/@tursodatabase/database) package provides the Turso in-memory database, compatible with SQLite.
* The [@tursodatabase/serverless](https://www.npmjs.com/package/@tursodatabase/serverless) package provides a serverless driver with the same API.

## License

This project is licensed under the [MIT license](../../LICENSE.md).

## Support

- [GitHub Issues](https://github.com/tursodatabase/turso/issues)
- [Documentation](https://docs.turso.tech)
- [Discord Community](https://tur.so/discord)