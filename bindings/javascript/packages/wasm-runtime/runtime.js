export {
	instantiateNapiModule,
	instantiateNapiModuleSync,
	MessageHandler,
} from "@emnapi/core";
export { getDefaultContext } from "@emnapi/runtime";
export * from "@tybys/wasm-util";
export { createFsProxy, createOnMessage } from "./fs-proxy.js";
