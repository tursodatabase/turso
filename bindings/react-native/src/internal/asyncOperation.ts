/**
 * Async Operation Driver
 *
 * Drives async operations returned by sync SDK-KIT methods.
 * This is where ALL the async logic lives - the C++ layer is just a thin bridge.
 *
 * Key responsibilities:
 * - Call resume() in a loop until DONE
 * - When IO is needed, process all pending IO items
 * - Extract and return the final result
 */

import {
  NativeSyncOperation,
  NativeSyncDatabase,
  TursoStatus,
  SyncOperationResultType,
  NativeConnection,
  NativeSyncChanges,
  SyncStats,
} from '../types';
import { processIoItem, IoContext } from './ioProcessor';

/**
 * Drive an async operation to completion
 *
 * @param operation - The native operation to drive
 * @param database - The native sync database (for IO queue access)
 * @param context - IO context with auth and URL information
 * @returns Promise that resolves when operation completes
 */
export async function driveOperation<T = void>(
  operation: NativeSyncOperation,
  database: NativeSyncDatabase,
  context: IoContext
): Promise<T> {
  while (true) {
    // Resume the operation
    const status = operation.resume();

    // Operation completed successfully
    if (status === TursoStatus.DONE) {
      // Extract and return the result based on result type
      const resultKind = operation.resultKind();

      switch (resultKind) {
        case SyncOperationResultType.NONE:
          return undefined as T;

        case SyncOperationResultType.CONNECTION:
          return operation.extractConnection() as T;

        case SyncOperationResultType.CHANGES:
          return operation.extractChanges() as T;

        case SyncOperationResultType.STATS:
          return operation.extractStats() as T;

        default:
          throw new Error(`Unknown result type: ${resultKind}`);
      }
    }

    // Operation needs IO
    if (status === TursoStatus.IO) {
      // Process all pending IO items
      await processIoQueue(database, context);

      // Step callbacks after IO processing
      database.ioStepCallbacks();

      // Continue resume loop
      continue;
    }

    // Any other status is an error
    throw new Error(`Unexpected status from operation.resume(): ${status}`);
  }
}

/**
 * Process all pending IO items in the queue
 *
 * @param database - The native sync database
 * @param context - IO context with auth and URL information
 */
async function processIoQueue(database: NativeSyncDatabase, context: IoContext): Promise<void> {
  const promises: Promise<void>[] = [];

  // Take all available IO items from the queue
  while (true) {
    const ioItem = database.ioTakeItem();
    if (!ioItem) {
      break; // No more items
    }

    // Process each item (potentially in parallel)
    promises.push(processIoItem(ioItem, context));
  }

  // Wait for all IO operations to complete
  await Promise.all(promises);
}

/**
 * Helper type for operations that return connections
 */
export async function driveConnectionOperation(
  operation: NativeSyncOperation,
  database: NativeSyncDatabase,
  context: IoContext
): Promise<NativeConnection> {
  return driveOperation<NativeConnection>(operation, database, context);
}

/**
 * Helper type for operations that return changes
 */
export async function driveChangesOperation(
  operation: NativeSyncOperation,
  database: NativeSyncDatabase,
  context: IoContext
): Promise<NativeSyncChanges | null> {
  return driveOperation<NativeSyncChanges | null>(operation, database, context);
}

/**
 * Helper type for operations that return stats
 */
export async function driveStatsOperation(
  operation: NativeSyncOperation,
  database: NativeSyncDatabase,
  context: IoContext
): Promise<SyncStats> {
  return driveOperation<SyncStats>(operation, database, context);
}

/**
 * Helper type for operations that return void
 */
export async function driveVoidOperation(
  operation: NativeSyncOperation,
  database: NativeSyncDatabase,
  context: IoContext
): Promise<void> {
  return driveOperation<void>(operation, database, context);
}
