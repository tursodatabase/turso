// Browser storage for Turso, backed by the Origin Private File System.
//
// Turso calls in here synchronously, which OPFS supports through
// FileSystemSyncAccessHandle. Those handles can only be created asynchronously,
// and only inside a Web Worker, so `preopen` does that work up front and parks
// the handle in a registry. The synchronous functions below are what Rust
// calls, and they only ever look a handle up by its integer id.

/** @type {Map<number, FileSystemSyncAccessHandle>} */
const handles = new Map();
/** @type {Map<string, number>} */
const fdsByPath = new Map();
let nextFd = 1;

/** @type {FileSystemDirectoryHandle | null} */
let root = null;

/// OPFS nests directories, but Turso passes flat paths like "app.db-wal".
/// Slashes would be read as directories, so they are folded into the name.
function fileNameFor(path) {
  return path.replace(/^\/+/, '').replaceAll('/', '_');
}

async function opfsRoot() {
  if (root === null) {
    if (typeof navigator === 'undefined' || !navigator.storage?.getDirectory) {
      throw new Error(
        'OPFS is unavailable. Turso needs a browser context with ' +
          'navigator.storage.getDirectory().',
      );
    }
    root = await navigator.storage.getDirectory();
  }
  return root;
}

/**
 * Opens `path` and every file Turso derives from it, so that the synchronous
 * open below always finds a handle. Await this before constructing a Database.
 *
 * @param {string} path database path, e.g. "app.db"
 */
export async function preopen(path) {
  // Turso opens the database and its write-ahead log; both have to be ready
  // before it starts, because it opens them synchronously.
  for (const name of [path, `${path}-wal`]) {
    await preopenOne(name);
  }
}

async function preopenOne(path) {
  const name = fileNameFor(path);
  if (fdsByPath.has(name)) return fdsByPath.get(name);

  const dir = await opfsRoot();
  const fileHandle = await dir.getFileHandle(name, { create: true });
  if (typeof fileHandle.createSyncAccessHandle !== 'function') {
    throw new Error(
      'createSyncAccessHandle is missing. OPFS sync access handles only ' +
        'exist inside a Web Worker; run Turso in a worker.',
    );
  }
  const handle = await fileHandle.createSyncAccessHandle();

  const fd = nextFd++;
  handles.set(fd, handle);
  fdsByPath.set(name, fd);
  return fd;
}

/** Closes every open handle and forgets it. */
export function closeAll() {
  for (const handle of handles.values()) {
    handle.close();
  }
  handles.clear();
  fdsByPath.clear();
}

/** Deletes a file from OPFS. Call `closeAll` first; open handles lock it. */
export async function remove(path) {
  const dir = await opfsRoot();
  await dir.removeEntry(fileNameFor(path), { recursive: false });
}

// --- Synchronous surface called from Rust. -----------------------------------
// Each returns a negative number on failure rather than throwing, so an
// exception never has to unwind through wasm.

export function vfsOpen(path, _create) {
  const fd = fdsByPath.get(fileNameFor(path));
  return fd === undefined ? -1 : fd;
}

export function vfsRead(fd, offset, buffer) {
  const handle = handles.get(fd);
  if (handle === undefined) return -1;
  try {
    return handle.read(buffer, { at: offset });
  } catch {
    return -1;
  }
}

export function vfsWrite(fd, offset, buffer) {
  const handle = handles.get(fd);
  if (handle === undefined) return -1;
  try {
    return handle.write(buffer, { at: offset });
  } catch {
    return -1;
  }
}

export function vfsSync(fd) {
  const handle = handles.get(fd);
  if (handle === undefined) return -1;
  try {
    handle.flush();
    return 0;
  } catch {
    return -1;
  }
}

export function vfsSize(fd) {
  const handle = handles.get(fd);
  if (handle === undefined) return -1;
  try {
    return handle.getSize();
  } catch {
    return -1;
  }
}

export function vfsTruncate(fd, len) {
  const handle = handles.get(fd);
  if (handle === undefined) return -1;
  try {
    handle.truncate(len);
    return 0;
  } catch {
    return -1;
  }
}

export function vfsRemove(path) {
  // Synchronous removal is not part of OPFS. Drop the handle so the async
  // `remove` above can delete the file afterwards.
  const name = fileNameFor(path);
  const fd = fdsByPath.get(name);
  if (fd === undefined) return 0;
  try {
    handles.get(fd)?.close();
  } catch {
    // Already closed; deleting is still the caller's goal.
  }
  handles.delete(fd);
  fdsByPath.delete(name);
  return 0;
}
