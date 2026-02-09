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

import type { NativeSyncIoItem, NativeSyncDatabase } from '../types';

/**
 * IO context contains auth and URL information for HTTP requests
 */
export interface IoContext {
  /** Auth token for HTTP requests */
  authToken?: string | (() => string | Promise<string> | null);
  /** Base URL for normalization (e.g., 'libsql://mydb.turso.io') */
  baseUrl?: string | (() => string | null);
}

// Allow users to optionally override file system implementation
let fsReadFileOverride: ((path: string) => Promise<Uint8Array>) | null = null;
let fsWriteFileOverride: ((path: string, data: Uint8Array) => Promise<void>) | null = null;

/**
 * Set custom file system implementation (optional)
 * By default, uses built-in JSI file system functions.
 * Only call this if you need custom behavior (e.g., encryption, compression).
 *
 * @param readFile - Function to read file as Uint8Array
 * @param writeFile - Function to write Uint8Array to file
 */
export function setFileSystemImpl(
  readFile: (path: string) => Promise<Uint8Array>,
  writeFile: (path: string, data: Uint8Array) => Promise<void>
): void {
  fsReadFileOverride = readFile;
  fsWriteFileOverride = writeFile;
}

/**
 * Read file using custom implementation or built-in JSI function
 */
async function fsReadFile(path: string): Promise<Uint8Array> {
  if (fsReadFileOverride) {
    return fsReadFileOverride(path);
  }

  // Use built-in JSI function
  const buffer = __TursoProxy.fsReadFile(path);
  if (buffer === null) {
    // File not found - return empty
    return new Uint8Array(0);
  }
  return new Uint8Array(buffer);
}

/**
 * Write file using custom implementation or built-in JSI function
 */
async function fsWriteFile(path: string, data: Uint8Array): Promise<void> {
  if (fsWriteFileOverride) {
    return fsWriteFileOverride(path, data);
  }

  // Use built-in JSI function
  __TursoProxy.fsWriteFile(path, data.buffer as ArrayBuffer);
}

/**
 * Process a single IO item
 *
 * @param item - The IO item to process
 * @param context - IO context with auth and URL information
 */
export async function processIoItem(item: NativeSyncIoItem, context: IoContext): Promise<void> {
  try {
    const kind = item.getKind();

    switch (kind) {
      case 'HTTP':
        await processHttpRequest(item, context);
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
 * Normalize URL from libsql:// to https://
 */
function normalizeUrl(url: string): string {
  if (url.startsWith('libsql://')) {
    return url.replace('libsql://', 'https://');
  }
  return url;
}

/**
 * Get auth token from context (handles both string and async function)
 */
async function getAuthToken(context: IoContext): Promise<string | null> {
  if (!context.authToken) {
    return null;
  }

  if (typeof context.authToken === 'function') {
    const result = await context.authToken();
    return result ?? null;
  }

  return context.authToken;
}

/**
 * Get base URL from context (handles both string and function)
 */
function getBaseUrl(context: IoContext): string | null {
  if (!context.baseUrl) {
    return null;
  }

  if (typeof context.baseUrl === 'function') {
    return context.baseUrl() ?? null;
  }

  return context.baseUrl;
}

/**
 * Process an HTTP request using fetch()
 *
 * @param item - The IO item
 * @param context - IO context with auth and URL information
 */
async function processHttpRequest(item: NativeSyncIoItem, context: IoContext): Promise<void> {
  const request = item.getHttpRequest();

  // Resolve base URL: prefer context.baseUrl (from opts), fall back to request.url
  const rawBaseUrl = getBaseUrl(context) ?? request.url;
  if (!rawBaseUrl) {
    throw new Error('HTTP request missing URL: no URL in request and no baseUrl in context');
  }

  // Normalize URL (libsql:// -> https://)
  const baseUrl = normalizeUrl(rawBaseUrl);

  // Build full URL by combining base URL with path
  let fullUrl = baseUrl;
  if (request.path) {
    // Ensure proper URL formatting (avoid double slashes, ensure single slash)
    if (baseUrl.endsWith('/') && request.path.startsWith('/')) {
      fullUrl = baseUrl + request.path.substring(1);
    } else if (!baseUrl.endsWith('/') && !request.path.startsWith('/')) {
      fullUrl = baseUrl + '/' + request.path;
    } else {
      fullUrl = baseUrl + request.path;
    }
  }

  // Build fetch options
  const options: RequestInit = {
    method: request.method,
    headers: { ...request.headers },
  };

  // Inject Authorization header if auth token is available
  const authToken = await getAuthToken(context);
  if (authToken) {
    (options.headers as Record<string, string>)['Authorization'] = `Bearer ${authToken}`;
  }

  // Add body if present
  if (request.body) {
    options.body = request.body;
  }

  // Debug logging for HTTP requests
  console.log('[Turso HTTP] Request:', {
    method: request.method,
    url: fullUrl,
    hasBody: !!request.body,
    bodySize: request.body ? request.body.byteLength : 0,
    headers: options.headers,
  });

  // Make the HTTP request
  let response;
  try {
    response = await fetch(fullUrl, options);
  } catch (e) {
    // Detailed error logging
    const errorDetails = {
      url: fullUrl,
      method: request.method,
      hasBody: !!request.body,
      bodySize: request.body ? request.body.byteLength : 0,
      bodyType: request.body ? Object.prototype.toString.call(options.body) : 'none',
      error: e instanceof Error ? {
        message: e.message,
        name: e.name,
        stack: e.stack,
      } : String(e),
    };
    console.error('[Turso HTTP] Request failed:', JSON.stringify(errorDetails, null, 2));
    throw new Error(`HTTP request failed: ${e instanceof Error ? e.message : String(e)}. URL: ${fullUrl}, Method: ${request.method}, Body size: ${request.body ? request.body.byteLength : 0} bytes`);
  }


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
  const path = item.getFullReadPath();

  try {
    // Read the file (uses built-in JSI function or custom override)
    const data = await fsReadFile(path);

    // Push data to item
    if (data.byteLength > 0) {
      item.pushBuffer(data.buffer as ArrayBuffer);
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
  const request = item.getFullWriteRequest();

  // Convert ArrayBuffer to Uint8Array
  const data = request.content ? new Uint8Array(request.content) : new Uint8Array(0);

  // Write the file atomically (uses built-in JSI function or custom override)
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
 * @param context - IO context with auth and URL information
 */
export async function drainSyncIo(database: NativeSyncDatabase, context: IoContext): Promise<void> {
  const promises: Promise<void>[] = [];

  // Take all available IO items from the queue
  while (true) {
    const ioItem = database.ioTakeItem();
    if (!ioItem) {
      break; // No more items
    }

    // Process each item
    promises.push(processIoItem(ioItem, context));
  }

  // Wait for all IO operations to complete
  await Promise.all(promises);

  // Step callbacks after IO processing
  // This allows the sync engine to run any post-IO callbacks
  database.ioStepCallbacks();
}
