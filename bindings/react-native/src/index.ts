/**
 * Turso React Native SDK
 *
 * Main entry point for the SDK. Supports both local-only and sync databases.
 */

import { NativeModules, Platform } from 'react-native';
import { Database, DatabaseConfig } from './Database';
import type { TursoNativeModule, TursoProxy as TursoProxyType } from './types';
import { setFileSystemImpl } from './internal/ioProcessor';

// Re-export all public types
export type {
  // Core types
  SQLiteValue,
  BindParams,
  Row,
  RunResult,

  // Database config
  LocalDatabaseConfig,
  SyncDatabaseConfig,

  // Sync types
  SyncStats,

  // Enums
  TursoStatus,
  TursoType,
} from './types';

// Re-export classes
export { Database } from './Database';
export { Statement } from './Statement';

// Export DatabaseConfig type
export type { DatabaseConfig } from './Database';

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
declare const global: {
  __TursoProxy?: TursoProxyType;
};

const TursoProxy: TursoProxyType = global.__TursoProxy!;

if (!TursoProxy) {
  throw new Error(
    'turso-react-native: JSI bindings not found on global object. This is a bug.'
  );
}

/**
 * Open a database (local or sync)
 *
 * For local databases, pass a string path.
 * For sync databases, pass a config object with remoteUrl.
 *
 * @param config - Path string (local) or config object (local/sync)
 * @returns Database instance
 *
 * @example Local database
 * ```ts
 * const db = await openDatabase('./local.db');
 * db.exec('CREATE TABLE users (id INTEGER, name TEXT)');
 * ```
 *
 * @example Sync database
 * ```ts
 * const db = await openDatabase({
 *   path: './replica.db',
 *   remoteUrl: 'libsql://mydb.turso.io',
 *   authToken: 'token-here',
 *   bootstrapIfEmpty: true,
 * });
 * await db.create(); // Open or create
 * const users = db.all('SELECT * FROM users');
 * await db.push(); // Sync local changes
 * await db.pull(); // Fetch remote changes
 * ```
 */
export async function openDatabase(config: DatabaseConfig): Promise<Database> {
  const db = new Database(config);

  // For sync databases, automatically call create()
  if (db.isSync) {
    await db.create();
  }

  return db;
}

/**
 * Create a local-only database (synchronous)
 *
 * @param path - Database file path
 * @returns Database instance (already open)
 *
 * @example
 * ```ts
 * const db = createLocalDatabase('./local.db');
 * db.exec('CREATE TABLE users (id INTEGER, name TEXT)');
 * ```
 */
export function createLocalDatabase(path: string): Database {
  return new Database(path);
}

/**
 * Create a sync database (async - requires network for bootstrap)
 *
 * @param config - Sync database configuration
 * @returns Database instance (call create() or open() to initialize)
 *
 * @example
 * ```ts
 * const db = await createSyncDatabase({
 *   path: './replica.db',
 *   remoteUrl: 'libsql://mydb.turso.io',
 *   authToken: 'token-here',
 * });
 * await db.create(); // Bootstrap if needed
 * ```
 */
export async function createSyncDatabase(
  config: import('./types').SyncDatabaseConfig
): Promise<Database> {
  const db = new Database(config);
  await db.create();
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
 * Platform-specific paths
 */
export const paths = {
  /**
   * The default database directory for the current platform.
   * - iOS: App's Documents directory
   * - Android: App's files directory
   */
  get documents(): string {
    return Platform.select({
      ios: 'Documents',
      android: 'files',
      default: '.',
    }) as string;
  },
};

// Default export
export default {
  openDatabase,
  createLocalDatabase,
  createSyncDatabase,
  version,
  setup,
  setFileSystemImpl,
  paths,
  Database,
};
