import { encryptedCiphers, encryptedServer } from '../native/encrypted-server.js';
import type { TestProject } from 'vitest/node';

type EncryptedServer = Pick<Awaited<ReturnType<typeof encryptedServer>>, 'url' | 'cipher' | 'localKey' | 'cloudKey'>;
declare module 'vitest' { export interface ProvidedContext { encryptedServers: EncryptedServer[] } }

export default async function setup(project: TestProject) {
    const servers: Awaited<ReturnType<typeof encryptedServer>>[] = [];
    try {
        for (const cipher of encryptedCiphers) servers.push(await encryptedServer(cipher));
        project.provide('encryptedServers', servers.map(({ url, cipher, localKey, cloudKey }) => ({ url, cipher, localKey, cloudKey })));
        return async () => { for (const server of servers) await server.close(); };
    } catch (error) { for (const server of servers) await server.close(); throw error; }
}
