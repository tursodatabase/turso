import assert from "node:assert/strict";
import { test, beforeEach } from "node:test";
import { BrowserIO } from "./browser-io.ts";

const values = new Map();
globalThis.localStorage = {
    getItem: path => values.get(path) ?? null,
    setItem: (path, value) => values.set(path, value),
};
beforeEach(() => values.clear());

test("binary sidecars survive a host write and read", async () => {
    const bytes = Uint8Array.from({ length: 100000 }, (_, i) => i % 256);
    await BrowserIO.write("replica-info", bytes);
    assert.ok(Buffer.from(await BrowserIO.read("replica-info")).equals(Buffer.from(bytes)));
});
test("existing plaintext JSON metadata remains readable", async () => {
    const json = '{"table":"café","revision":"g1:o56"}';
    values.set("replica-info", json);
    assert.deepEqual(await BrowserIO.read("replica-info"), new TextEncoder().encode(json));
});
test("missing files and existing empty files remain distinct", async () => {
    assert.equal(await BrowserIO.read("replica-info"), null);
    await BrowserIO.write("replica-info", new Uint8Array());
    assert.deepEqual(await BrowserIO.read("replica-info"), new Uint8Array());
});
