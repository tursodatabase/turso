import type { ProtocolIo } from "@tursodatabase/sync-common";

const binaryPrefix = "turso-sync-binary-v1:";

export const BrowserIO: ProtocolIo = {
    async read(path: string): Promise<Uint8Array | null> {
        const value = localStorage.getItem(path);
        if (value == null) return null;
        if (!value.startsWith(binaryPrefix)) {
            // Existing replicas stored JSON metadata directly as text.
            return new TextEncoder().encode(value);
        }
        const binary = atob(value.slice(binaryPrefix.length));
        return Uint8Array.from(binary, c => c.charCodeAt(0));
    },
    async write(path: string, data: Buffer | Uint8Array): Promise<void> {
        // Avoid a large spread argument: replacement backups may exceed the
        // JavaScript argument limit, and ciphertext need not be valid UTF-8.
        const parts: string[] = [];
        for (let offset = 0; offset < data.length; offset += 8192) {
            parts.push(String.fromCharCode(...data.subarray(offset, offset + 8192)));
        }
        localStorage.setItem(path, binaryPrefix + btoa(parts.join("")));
    }
};
