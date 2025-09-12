import { readFileSync, writeFileSync } from "node:fs";

const data = readFileSync(process.env.WASM_FILE);
const b64 = data.toString("base64");
const dataUrl = "data:application/wasm;base64," + b64;
const inlined = readFileSync(process.env.JS_FILE).toString("utf8");
const replaced = inlined.replace("__PLACEHOLDER__", dataUrl);
writeFileSync(process.env.JS_FILE, replaced);