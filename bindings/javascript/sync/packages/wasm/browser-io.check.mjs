import assert from "node:assert/strict";
import { test, beforeEach } from "node:test";
import { BrowserIO, EncryptedBrowserIO } from "./browser-io.ts";

const values = new Map();
globalThis.localStorage = {
    getItem: path => values.get(path) ?? null,
    setItem: (path, value) => values.set(path, value),
};
beforeEach(() => values.clear());

test("binary sidecars survive a host write and read", async () => {
    const bytes = Uint8Array.from({ length: 100000 }, (_, i) => i % 256);
    await EncryptedBrowserIO.write("replica-info", bytes);
    assert.ok(values.get("replica-info").startsWith("turso-sync-binary-v1:"));
    assert.ok(Buffer.from(await EncryptedBrowserIO.read("replica-info")).equals(Buffer.from(bytes)));
});
test("existing plaintext JSON metadata remains readable", async () => {
    const json = '{"table":"café","revision":"g1:o56"}';
    values.set("replica-info", json);
    assert.deepEqual(await BrowserIO.read("replica-info"), new TextEncoder().encode(json));
});
test("plaintext metadata rewrites preserve the existing JSON storage format", async () => {
    const json = '{"table":"café","revision":"g1:o56"}';
    for (const stored of [json, "turso-sync-binary-v1:" + Buffer.from(json).toString("base64")]) {
        values.set("replica-info", stored);
        await BrowserIO.write("replica-info", await BrowserIO.read("replica-info"));
        assert.equal(values.get("replica-info"), json);
        assert.deepEqual(JSON.parse(values.get("replica-info")), JSON.parse(json));
    }
});
test("missing files and existing empty files remain distinct", async () => {
    for (const [name, io] of [["plain-info", BrowserIO], ["encrypted-info", EncryptedBrowserIO]]) {
        assert.equal(await io.read(name), null);
        await io.write(name, new Uint8Array());
        assert.deepEqual(await io.read(name), new Uint8Array());
    }
    assert.equal(values.get("plain-info"), "");
});
