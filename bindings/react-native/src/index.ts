import { NativeModules, Platform } from 'react-native';
import { Database } from './Database';
import type {
  OpenDatabaseOptions,
  TursoNativeModule,
  TursoProxy as TursoProxyType,
} from './types';

// Re-export types
export type {
  BindParams,
  OpenDatabaseOptions,
  Row,
  RunResult,
  SQLiteValue,
  SetupOptions,
} from './types';

// Re-export classes
export { Database } from './Database';
export { Statement } from './Statement';

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

export async function connect(options: OpenDatabaseOptions | string): Promise<Database> {
  const nativeDb = TursoProxy.open(options);
  return new Database(nativeDb);
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
    // This is handled by the native code - databases opened with relative paths
    // will be stored in the appropriate directory for each platform
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
  paths,
  Database,
};
