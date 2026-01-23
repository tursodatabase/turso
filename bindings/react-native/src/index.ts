/**
 * Turso React Native SDK
 *
 * Main entry point for the SDK. Supports both local-only and sync databases.
 */

import { NativeModules } from 'react-native';
import { Database } from './Database';
import type {
  DatabaseOpts,
  TursoNativeModule,
  TursoProxy as TursoProxyType,
} from './types';
import { setFileSystemImpl } from './internal/ioProcessor';

// Re-export all public types
export type {
  // Core types
  SQLiteValue,
  BindParams,
  Row,
  RunResult,

  // Database config
  DatabaseOpts,
  EncryptionOpts,

  // Sync types
  SyncStats,

  // Enums
  TursoStatus,
  TursoType,
} from './types';

// Re-export classes
export { Database } from './Database';
export { Statement } from './Statement';

// Export file system configuration function
export { setFileSystemImpl } from './internal/ioProcessor';

// Get the native module
const TursoNative: TursoNativeModule | undefined = NativeModules.Turso;

// Check if native module is available
if (!TursoNative) {
  throw new Error(
    `turso-react-native: Native module not found. Make sure you have properly linked the library.\n` +
    `- iOS: Run 'pod install' in your ios directory\n` +
    `- Android: Make sure the package is properly included in your MainApplication.java`
  );
}

// Install the JSI bindings
const installed = TursoNative.install();
if (!installed) {
  throw new Error(
    'turso-react-native: Failed to install JSI bindings. Make sure the New Architecture is enabled.'
  );
}

// Get the proxy that was installed on the global object
// __TursoProxy is declared globally in types.ts
const TursoProxy: TursoProxyType = __TursoProxy;

if (!TursoProxy) {
  throw new Error(
    'turso-react-native: JSI bindings not found on global object. This is a bug.'
  );
}

/**
 * Helper function to construct a database path in a writable directory.
 * On mobile platforms, you must use writable directories (not relative paths).
 *
 * @param filename - Database filename (e.g., 'mydb.db')
 * @param directory - Directory to use ('documents', 'database', or 'library')
 * @returns Absolute path to the database file
 *
 * @example
 * ```ts
 * import { getDbPath, connect } from '@tursodatabase/react-native';
 *
 * const dbPath = getDbPath('mydb.db');
 * const db = await connect({ path: dbPath });
 * ```
 */
export function getDbPath(filename: string, directory: 'documents' | 'database' | 'library' = 'documents'): string {
  const basePath = paths[directory];
  if (!basePath || basePath === '.') {
    throw new Error(
      `Unable to get ${directory} path for this platform. ` +
      'Make sure the native module is properly loaded.'
    );
  }
  return `${basePath}/${filename}`;
}

/**
 * Connect to a database asynchronously (matches JavaScript bindings API)
 *
 * This is the main entry point for the SDK, matching the API from
 * @tursodatabase/sync-native and @tursodatabase/database-native.
 *
 * **Path handling**: Relative paths are automatically placed in writable directories:
 * - Android: app's database directory (`/data/data/com.app/databases/`)
 * - iOS: app's documents directory
 *
 * Absolute paths and `:memory:` are used as-is.
 *
 * @param opts - Database options
 * @returns Promise resolving to Database instance
 *
 * @example Local database (relative path)
 * ```ts
 * import { connect } from '@tursodatabase/react-native';
 *
 * // Relative path automatically placed in writable directory
 * const db = await connect({ path: 'local.db' });
 * await db.exec('CREATE TABLE users (id INTEGER, name TEXT)');
 * ```
 *
 * @example Using :memory: for in-memory database
 * ```ts
 * const db = await connect({ path: ':memory:' });
 * ```
 *
 * @example Sync database
 * ```ts
 * const db = await connect({
 *   path: 'replica.db',
 *   url: 'libsql://mydb.turso.io',
 *   authToken: 'token-here',
 * });
 * const users = await db.all('SELECT * FROM users');
 * await db.push();
 * await db.pull();
 * ```
 *
 * @example Using absolute path (advanced)
 * ```ts
 * import { connect, paths } from '@tursodatabase/react-native';
 *
 * const db = await connect({ path: `${paths.documents}/mydb.db` });
 * ```
 */
export async function connect(opts: DatabaseOpts): Promise<Database> {
  const db = new Database(opts);
  await db.connect();
  return db;
}

/**
 * Returns the Turso library version.
 */
export function version(): string {
  return TursoProxy.version();
}

/**
 * Configure Turso settings such as logging.
 * Should be called before any database operations.
 *
 * @param options - Configuration options
 * @example
 * ```ts
 * import { setup } from '@tursodatabase/react-native';
 *
 * setup({ logLevel: 'debug' });
 * ```
 */
export function setup(options: {logLevel?: string}): void {
  TursoProxy.setup(options);
}

/**
 * Platform-specific writable directory paths.
 * Use these to construct absolute paths for database files.
 *
 * NOTE: With automatic path normalization, you typically don't need this.
 * Just pass relative paths like 'mydb.db' and they'll be placed in the correct directory.
 *
 * @example
 * ```ts
 * import { paths, connect } from '@tursodatabase/react-native';
 *
 * // Create database in app's documents/files directory
 * const dbPath = `${paths.documents}/mydb.db`;
 * const db = await connect({ path: dbPath });
 * ```
 */
export const paths = {
  /**
   * Primary documents/database directory (writable)
   * - iOS: App's Documents directory (absolute path)
   * - Android: App's database directory (absolute path) - preferred for databases
   */
  get documents(): string {
    return TursoNative?.IOS_DOCUMENT_PATH || TursoNative?.ANDROID_DATABASE_PATH || '.';
  },

  /**
   * Database-specific directory (writable)
   * - iOS: Same as documents
   * - Android: Database directory (absolute path)
   */
  get database(): string {
    return TursoNative?.IOS_DOCUMENT_PATH || TursoNative?.ANDROID_DATABASE_PATH || '.';
  },

  /**
   * Files directory (writable)
   * - iOS: Same as documents
   * - Android: App's files directory (absolute path)
   */
  get files(): string {
    return TursoNative?.IOS_DOCUMENT_PATH || TursoNative?.ANDROID_FILES_PATH || '.';
  },

  /**
   * Library directory (iOS only, writable)
   * - iOS: App's Library directory (absolute path)
   * - Android: Same as files
   */
  get library(): string {
    return TursoNative?.IOS_LIBRARY_PATH || TursoNative?.ANDROID_FILES_PATH || '.';
  },
};

// Default export
export default {
  connect,
  version,
  setup,
  setFileSystemImpl,
  getDbPath,
  paths,
  Database,
};
