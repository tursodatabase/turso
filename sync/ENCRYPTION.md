# Encrypted sync

Native encrypted sync uses one cipher and two independent keys. The cloud key is
base64 encoded and is sent to the server over HTTPS. The local key is hex encoded
and stays on the client. Core encrypts the replica database, WAL, and MVCC log;
the sync engine encrypts the `-info` metadata and `-changes` scratch file.

Start with an existing encrypted native server database and a fresh local path.
An empty server database is supported. Initial creation requires a cloud key and
full bootstrap. Partial sync, deferred encrypted creation, conversion of an
existing plaintext replica, and key rotation are not supported.

Locally encrypted bootstrap requires `pull_bytes_threshold` to be unset or zero.
Positive values return an error before opening the target file or sending a
request. Plaintext local replicas retain chunked bootstrap support, including
legacy encrypted cloud databases. Native MVCC servers reject filtered bootstrap
requests themselves; use an unset or zero threshold for those sources.

After pending replacement recovery, fresh encrypted creation rejects any remaining
recovery marker or known replacement backup, including empty files. The error
identifies the conflicting path and leaves the artifacts untouched. Reopening an
initialized replica still permits orphaned backups left by interrupted cleanup.

With bootstrap enabled, missing or empty `-info` metadata permits downloading
again over the main database, matching plaintext bootstrap behavior. No bootstrap
marker or additional persistent state is written. Nonempty metadata must
successfully authenticate and parse; failures leave the replica untouched.
Conflicting nonempty WAL, MVCC log, revert WAL, or staging files still block fresh
creation. A database whose metadata was deleted cannot be distinguished from an
interrupted bootstrap, so its main file can also be overwritten in this case.

## Local sample server

The CLI sample server supports encrypted sync in single-database and directory
mode. Put the cloud key's hex encoding in a key file, then run:

```sh
tursodb --sync-server 127.0.0.1:8080 \
  --sync-encryption-cipher aes256gcm \
  --sync-encryption-key-file ./cloud-key.hex ./source.db
```

Use `--sync-dir ./databases` instead of `./source.db` to serve databases under
`http://127.0.0.1:8080/db/{name}` with the same configured key and cipher.
Configure clients with the base64 encoding of that cloud key and an independent
local hex key. The sample server validates encryption credentials for both
pull and SQL push requests. It verifies native page/body-chunk tags and serves
decrypted pages and MVCC frames over HTTP; keep it on loopback or use an HTTPS
proxy for network access.
SQL pipeline requests may send only the cloud key, as JavaScript's
`remoteWritesExperimental` path does. Any supplied cipher must match the server's
configuration; pull requests require both the cloud key and cipher headers.
It supports one MVCC generation, without production retained-generation storage.
Corrupt MVCC headers fail the pull before any replacement fallback; the client's
existing data and sync revision remain unchanged.

Run the actual HTTP client/server regression suite with:

```sh
cargo build -p turso_cli -p turso --bin tursodb --example sync_encryption_e2e --features turso/sync
python3 testing/cli_tests/sync_encryption.py
```

## Rust

Enable the `sync` crate feature. Local encrypted replicas require an explicit
experimental opt-in on every open, including offline reopen. String-based
feature lists use `sync_encryption`; Rust uses
`experimental_sync_encryption(true)`. The opt-in also enables core encryption.
Existing cloud-only encryption does not require it.

```rust,no_run
use turso::sync::{Builder, RemoteEncryptionCipher};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let db = Builder::new_remote("app.db")
    .experimental_sync_encryption(true)
    .with_remote_url("https://your-database.turso.io")
    .with_auth_token(std::env::var("TURSO_AUTH_TOKEN")?)
    .with_encryption(
        RemoteEncryptionCipher::Aes256Gcm,
        Some(std::env::var("LOCAL_KEY_HEX")?),
        Some(std::env::var("CLOUD_KEY_BASE64")?),
    )
    .build().await?;
db.pull().await?;
# Ok(()) }
```

The existing `with_remote_encryption(cloud_key, cipher)` builder remains available;
add `experimental_sync_encryption(true)` and
`with_local_encryption_key(local_hex_key)` to encrypt its local replica.

## JavaScript

```typescript
import { connect } from "@tursodatabase/sync";

const db = await connect({
    experimental: ["sync_encryption"],
    path: "app.db",
    url: "https://your-database.turso.io",
    authToken: process.env.TURSO_AUTH_TOKEN,
    remoteEncryption: {
        cipher: "aes256gcm",
        key: process.env.CLOUD_KEY_BASE64,
    },
    localEncryptionKey: process.env.LOCAL_KEY_HEX,
});
await db.pull();
```

The same options are available in the WebAssembly and React Native sync packages. The cipher in
`remoteEncryption` applies to both keys; there is no second local cipher option.
Encrypted browser host sidecars are base64 encoded in localStorage to preserve
arbitrary bytes. Plaintext replicas keep writing JSON text. Both representations
remain readable, so plaintext metadata written as base64 by an earlier build
returns to JSON on its next write. The database itself uses OPFS; metadata
existence is determined by the host storage. Empty metadata is treated as missing,
so bootstrap can be retried after an interrupted metadata write.

## Go

Pass `LocalEncryptionKey` (hex), `RemoteEncryptionKey` (base64), and the shared
`RemoteEncryptionCipher` to `TursoSyncDbConfig`, alongside `Path`, `RemoteUrl`,
and `AuthToken`. Set `ExperimentalFeatures: "sync_encryption"`;
`NewTursoSyncDb` enables core encryption automatically. For an
initialized offline replica, retain the local key and cipher and omit the URL
and cloud key. Requests carrying a cloud key follow redirects only within the
same origin (scheme, hostname, and effective port). Cross-origin redirects,
including HTTPS-to-HTTP downgrades, return an error before forwarding the key.

## .NET

Set `TursoSyncDatabaseOptions.LocalEncryptionKey` to the local hex key and
`ExperimentalFeatures` to `"sync_encryption"`, and `RemoteEncryption` to a
`TursoRemoteEncryptionOptions` containing the shared
`Cipher` and base64 cloud `Key`. Offline reopen retains the local key and cipher;
the remote `Key` may be null. Connection strings expose the same configuration
as `Local Encryption Key`, `Remote Encryption Cipher`, and `Remote Encryption Key`;
also set `Sync Experimental Features=sync_encryption`.

## Python

```python
from turso import sync

connection = sync.connect(
    "app.db",
    remote_url="https://your-database.turso.io",
    auth_token=auth_token,
    remote_encryption_cipher=sync.RemoteEncryptionCipher.Aes256Gcm,
    remote_encryption_key=cloud_key_base64,
    local_encryption_key=local_key_hex,
    experimental_features="sync_encryption",
)
```

The asynchronous Python API (`turso.aio.sync.connect`) accepts the same encryption
arguments.

The C SDK uses the ordinary database encryption configuration for the local key
and its existing `remote_encryption_key` and `remote_encryption_cipher` fields for
cloud settings. If both configurations name a cipher, they must agree. Low-level
SDK callers must include `sync_encryption` in the database configuration
`experimental_features` string. The core `encryption` flag alone does not opt
into encrypted sync.

## Reopen and failures

Reopen an initialized replica with the same local key and cipher. Offline reopen
does not need the cloud key or a URL; the next network operation does need the
cloud key. Keep the experimental opt-in on reopen. In JavaScript, retain
`experimental: ["sync_encryption"]`, `localEncryptionKey`, and `remoteEncryption: { cipher: "aes256gcm" }` when omitting `url` and the cloud key.

Omit the local key to use a plaintext local replica of an encrypted source.
AES-128-GCM, AES-256-GCM, and all six native AEGIS variants are supported. ChaCha20
remains supported only by the existing legacy remote-encryption path.

Authentication errors are returned to the application; they do not trigger
file deletion, key regeneration, or bootstrap. Retrying creation may restart
bootstrap when metadata is missing or empty, subject to the artifact checks
above. A failed response does not publish its revision. A page-apply failure can
leave partial local changes; applications choose whether to reopen or recreate
the replica.

The server validates the requested cipher against database metadata and the cloud
key against stored data. Pull responses carry no encryption capability flag or
wire-length field; clients derive wire boundaries from native frame headers and
the configured source cipher. This assumes a server that implements cipher
validation; support is not negotiated with older servers.

Local metadata authenticates with the supplied cipher and key, without storing
a cipher name in its JSON. Downloaded changes use a single plaintext varint
length followed by encrypted message chunks, or fixed-size encrypted page
records. There is no scratch-file header or footer: a completed batch's identity,
counts, and size stay in memory. Scratch writes complete before apply, without
an fsync; abandoned downloads are reset after restart.

The native MVCC codec checks body-chunk AEAD tags and frame CRCs; it does not
authenticate whole frames or log ordering. Existing native log versions do not
bind frame offsets, preceding frames, or all header fields, including the split
between extension and recovery bytes. An attacker who can modify stored logs
can drop, replay, or reorder complete frames and recompute CRCs without a key.
The codec can also accept a changed extension boundary with the same total body
length. Fixing these limitations requires a new native log version and compatible
recovery readers; the sync codec does not change the existing format. See the
[native log review](https://github.com/tursodatabase/turso/pull/11515).

## Validation

```sh
cargo +stable test -p turso_sync_engine -p turso_sync_sdk_kit
cargo +stable check -p turso --features turso/sync -p turso_sync_js -p py-turso
cargo +stable test -p turso_core --features conn_raw_api logical_log
cargo +stable test -p turso_core --features conn_raw_api encrypted_mvcc_reload
```

The encrypted integration tests in `sync/engine/tests/mvcc_logical_pull/encryption.rs`
cover empty and zero-page bootstrap, incremental pull, plaintext replicas,
metadata/staging confidentiality, wrong or missing local keys, incompatible bootstrap pages, replacement, failed replacement recovery, checkpoint, and reopen.
They also inject bootstrap failures before/after database sync and metadata
publication, exercise pending replacement recovery at startup, and verify that
a later create retries with missing or empty metadata, including a subprocess
that exits after database fsync before metadata publication. Nonempty partial
metadata, damaged metadata, and wrong-key retries preserve the existing files.
Tests also verify that a metadata write failure preserves the published revision
and that metadata in host storage is authoritative when core IO sees a different
file.
In the server repository, run
`cargo test --locked -p turso --lib encrypted_sync_ --features poll_io`
to exercise the production retained-fragment planner and streamer against an
in-memory S3 backend, including replay through the real sync client.

For real HTTP/S3 validation, run the server repository's
`scripts/test-sync-encryption-e2e-local.sh`. It uses this repository's
`bindings/rust/examples/sync_encryption_e2e.rs` driver and a pinned LocalStack
container, including server crash recovery with the local cache removed.

Binding regressions exercise encrypted bootstrap, SQL, checkpoint, offline
reopen, and wrong local keys through native Node, synchronous/asynchronous Python,
Go, .NET, and Chromium/WASM. They combine empty-source HTTP fixtures with real
CLI-server bootstrap, push, and pull tests. The HTTP/S3 suite above additionally
checks production-server and storage integration. Run the
usual Go and .NET suites, `pytest bindings/python/tests/test_sync_encryption.py`,
and the JavaScript native suite. After building WASM, run
`yarn workspace @tursodatabase/sync-wasm test:encryption` (Node 24 and Playwright
Chromium). React Native's `npm run test:encryption` covers configuration forwarding
and offline routing; native device execution remains separate from these bridge
tests. Its changed C++ bridge was syntax-checked against React Native 0.76 JSI.
