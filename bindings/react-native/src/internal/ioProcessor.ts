/**
 * IO Processor
 *
 * Processes IO requests from the sync engine using JavaScript APIs.
 * This is the key benefit of the thin JSI layer - all IO is handled by
 * React Native's standard APIs (fetch, file system), not C++ code.
 *
 * Benefits:
 * - Network requests visible in React Native debugger
 * - Can customize fetch (add proxies, custom headers, etc.)
 * - Easier to test (can mock fetch)
 * - Uses platform-native networking (not C++ HTTP libraries)
 */

import { NativeSyncIoItem, NativeSyncDatabase } from '../types';

// React Native file system APIs (if available)
// Users can provide their own implementation or use react-native-fs
let fsReadFile: ((path: string) => Promise<Uint8Array>) | null = null;
let fsWriteFile: ((path: string, data: Uint8Array) => Promise<void>) | null = null;

/**
 * Set custom file system implementation
 * Users should call this to provide fs functions (e.g., from react-native-fs)
 *
 * @param readFile - Function to read file as Uint8Array
 * @param writeFile - Function to write Uint8Array to file
 */
export function setFileSystemImpl(
  readFile: (path: string) => Promise<Uint8Array>,
  writeFile: (path: string, data: Uint8Array) => Promise<void>
): void {
  fsReadFile = readFile;
  fsWriteFile = writeFile;
}

/**
 * Process a single IO item
 *
 * @param item - The IO item to process
 */
export async function processIoItem(item: NativeSyncIoItem): Promise<void> {
  try {
    const kind = item.getKind();

    switch (kind) {
      case 'HTTP':
        await processHttpRequest(item);
        break;

      case 'FULL_READ':
        await processFullRead(item);
        break;

      case 'FULL_WRITE':
        await processFullWrite(item);
        break;

      case 'NONE':
      default:
        // Nothing to do
        item.done();
        break;
    }
  } catch (error) {
    // Poison the item with error message
    const errorMsg = error instanceof Error ? error.message : String(error);
    item.poison(errorMsg);
  }
}

/**
 * Process an HTTP request using fetch()
 *
 * @param item - The IO item
 */
async function processHttpRequest(item: NativeSyncIoItem): Promise<void> {
  const request = item.getHttpRequest();

  // Build full URL (url might be in the request, or we need to construct from path)
  let url = request.url || '';
  if (!url && request.path) {
    // Path without base URL - this shouldn't normally happen
    throw new Error('HTTP request missing URL');
  }

  // Build fetch options
  const options: RequestInit = {
    method: request.method,
    headers: request.headers,
  };

  // Add body if present
  if (request.body) {
    options.body = request.body;
  }

  // Make the HTTP request
  const response = await fetch(url, options);

  // Set status code
  item.setStatus(response.status);

  // Read response body and push to item
  const responseData = await response.arrayBuffer();
  if (responseData.byteLength > 0) {
    item.pushBuffer(responseData);
  }

  // Mark as done
  item.done();
}

/**
 * Process a full read request (atomic file read)
 *
 * @param item - The IO item
 */
async function processFullRead(item: NativeSyncIoItem): Promise<void> {
  if (!fsReadFile) {
    throw new Error(
      'File system not configured. Call setFileSystemImpl() with react-native-fs or similar.'
    );
  }

  const path = item.getFullReadPath();

  try {
    // Read the file
    const data = await fsReadFile(path);

    // Push data to item
    if (data.byteLength > 0) {
      item.pushBuffer(data.buffer);
    }

    // Mark as done
    item.done();
  } catch (error) {
    // File not found is okay - treat as empty file
    if (isFileNotFoundError(error)) {
      // Empty file - just mark as done without pushing data
      item.done();
    } else {
      throw error;
    }
  }
}

/**
 * Process a full write request (atomic file write)
 *
 * @param item - The IO item
 */
async function processFullWrite(item: NativeSyncIoItem): Promise<void> {
  if (!fsWriteFile) {
    throw new Error(
      'File system not configured. Call setFileSystemImpl() with react-native-fs or similar.'
    );
  }

  const request = item.getFullWriteRequest();

  // Convert ArrayBuffer to Uint8Array
  const data = request.content ? new Uint8Array(request.content) : new Uint8Array(0);

  // Write the file atomically
  await fsWriteFile(request.path, data);

  // Mark as done
  item.done();
}

/**
 * Check if error is a file-not-found error
 *
 * @param error - The error to check
 * @returns true if file not found
 */
function isFileNotFoundError(error: unknown): boolean {
  if (error instanceof Error) {
    const message = error.message.toLowerCase();
    return (
      message.includes('enoent') ||
      message.includes('not found') ||
      message.includes('no such file')
    );
  }
  return false;
}

/**
 * Drain all pending IO items from sync engine queue and process them.
 * This is called during statement execution when partial sync needs to load missing pages.
 *
 * @param database - The native sync database
 */
export async function drainSyncIo(database: NativeSyncDatabase): Promise<void> {
  const promises: Promise<void>[] = [];

  // Take all available IO items from the queue
  while (true) {
    const ioItem = database.ioTakeItem();
    if (!ioItem) {
      break; // No more items
    }

    // Process each item
    promises.push(processIoItem(ioItem));
  }

  // Wait for all IO operations to complete
  await Promise.all(promises);

  // Step callbacks after IO processing
  // This allows the sync engine to run any post-IO callbacks
  database.ioStepCallbacks();
}
