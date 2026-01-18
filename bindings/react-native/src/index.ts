/**
 * Turso React Native SDK
 *
 * Main entry point for the SDK. Supports both local-only and sync databases.
 */

import { NativeModules, Platform } from 'react-native';
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
 * Connect to a database asynchronously (matches JavaScript bindings API)
 *
 * This is the main entry point for the SDK, matching the API from
 * @tursodatabase/sync-native and @tursodatabase/database-native.
 *
 * @param opts - Database options
 * @returns Promise resolving to Database instance
 *
 * @example Local database
 * ```ts
 * const db = await connect({ path: './local.db' });
 * await db.exec('CREATE TABLE users (id INTEGER, name TEXT)');
 * ```
 *
 * @example Sync database
 * ```ts
 * const db = await connect({
 *   path: './replica.db',
 *   url: 'libsql://mydb.turso.io',
 *   authToken: 'token-here',
 * });
 * const users = await db.all('SELECT * FROM users');
 * await db.push();
 * await db.pull();
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
  connect,
  version,
  setup,
  setFileSystemImpl,
  paths,
  Database,
};
