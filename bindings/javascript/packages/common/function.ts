import { NativeDatabase } from "./types.js";

export interface ScalarFunctionOptions {
  varargs?: boolean;
  deterministic?: boolean;
  safeIntegers?: boolean;
  directOnly?: boolean;
}

type ScalarFunctionImpl = (...args: any[]) => any;

const MAX_FUNCTION_ARGUMENTS = 127;

export function registerScalarFunction(
  db: NativeDatabase,
  name: string,
  optionsOrFn: ScalarFunctionOptions | ScalarFunctionImpl,
  maybeFn?: ScalarFunctionImpl,
): void {
  const options = typeof optionsOrFn === "function" ? {} : optionsOrFn ?? {};
  const fn = typeof optionsOrFn === "function" ? optionsOrFn : maybeFn;

  if (typeof name !== "string") {
    throw new TypeError("Expected first argument to be a string");
  }
  if (typeof fn !== "function") {
    throw new TypeError("Expected last argument to be a function");
  }

  const varargs = options.varargs === true;
  const deterministic = options.deterministic === true;
  const safeIntegers = options.safeIntegers === true;

  let arity = -1;
  if (!varargs) {
    arity = fn.length;
    if (!Number.isInteger(arity) || arity < 0) {
      throw new TypeError("Expected function.length to be a non-negative integer");
    }
    if (arity > MAX_FUNCTION_ARGUMENTS) {
      throw new RangeError(`User-defined functions cannot have more than ${MAX_FUNCTION_ARGUMENTS} arguments`);
    }
  }

  db.registerScalarFunction(name, arity, deterministic, safeIntegers, (args: any[]) => fn(...args));
}
