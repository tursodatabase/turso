import { afterEach, expect, test, vi } from 'vitest';
import { Database } from '../Database';
import { SyncOperationResultType, TursoStatus } from '../types';

afterEach(() => vi.unstubAllGlobals());

function installProxy() {
  const operation = (kind: SyncOperationResultType) => ({
    resume: () => TursoStatus.DONE,
    resultKind: () => kind,
    extractConnection: () => ({}),
  });
  const newSyncDatabase = vi.fn(() => ({
    create: () => operation(SyncOperationResultType.NONE),
    connect: () => operation(SyncOperationResultType.CONNECTION),
  }));
  const newDatabase = vi.fn(() => { throw new Error('Unexpected local-only open'); });
  vi.stubGlobal('__TursoProxy', { newSyncDatabase, newDatabase });
  return newSyncDatabase;
}

test('encrypted bootstrap passes separate local and cloud keys with one cipher', async () => {
  const create = installProxy();
  const localKey = '16'.repeat(32);
  const cloudKey = 'CwsLCwsLCwsLCwsLCwsLCwsLCwsLCwsLCwsLCwsLCws=';
  const db = new Database({ path: 'replica.db', url: 'https://example.invalid',
    localEncryptionKey: localKey, remoteEncryption: { cipher: 'aes256gcm', key: cloudKey } });
  await db.connect();
  expect(create).toHaveBeenCalledWith(expect.objectContaining({
    encryption_hexkey: localKey, encryption_cipher: 'aes256gcm', experimental_features: 'encryption',
  }), expect.objectContaining({
    remoteEncryptionKey: cloudKey, remoteEncryptionCipher: 'aes256gcm', reservedBytes: 28,
    bootstrapIfEmpty: true,
  }));
});

test('offline encrypted reopen still uses the sync bridge without a cloud key', async () => {
  const create = installProxy();
  await new Database({ path: 'replica.db', localEncryptionKey: '16'.repeat(32),
    remoteEncryption: { cipher: 'aes256gcm' } }).connect();
  expect(create).toHaveBeenCalledWith(expect.objectContaining({ encryption_cipher: 'aes256gcm' }),
    expect.objectContaining({ remoteUrl: null, remoteEncryptionKey: undefined, bootstrapIfEmpty: false }));
});

test('a local key without the shared cipher is rejected before native creation', async () => {
  const create = installProxy();
  await expect(new Database({ path: 'replica.db', localEncryptionKey: '16'.repeat(32) }).connect())
    .rejects.toThrow('localEncryptionKey requires remoteEncryption.cipher');
  expect(create).not.toHaveBeenCalled();
});
