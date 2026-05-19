import { encodeValue, type NamedArg, type Value } from './protocol.js';

/**
 * Normalize execute() arguments into the form session.execute() expects:
 * an array (positional) or a plain object (named). Single non-object/non-array
 * values become a one-element positional array.
 */
export function normalizeArgs(args: any): any[] | Record<string, any> {
  if (args === undefined) return [];
  if (Array.isArray(args)) return args;
  if (args !== null && typeof args === "object" && args.constructor === Object) {
    return args;
  }
  return [args];
}

function isQueryOptions(value: any): boolean {
  return value != null
    && typeof value === "object"
    && !Array.isArray(value)
    && Object.prototype.hasOwnProperty.call(value, "queryTimeout");
}

/**
 * Split a libsql-style variadic `(...bindParameters)` argument list into the
 * `params` to bind and an optional trailing `queryOptions` object. Mirrors
 * libsql-js's `splitBindParameters` so libsql call sites work unchanged.
 */
export function splitBindParameters(bindParameters: any[]): { params: any; queryOptions: any } {
  if (bindParameters.length === 0) {
    return { params: undefined, queryOptions: undefined };
  }
  if (isQueryOptions(bindParameters[bindParameters.length - 1])) {
    if (bindParameters.length === 1) {
      return { params: undefined, queryOptions: bindParameters[0] };
    }
    return {
      params: bindParameters.length === 2 ? bindParameters[0] : bindParameters.slice(0, -1),
      queryOptions: bindParameters[bindParameters.length - 1],
    };
  }
  return {
    params: bindParameters.length === 1 ? bindParameters[0] : bindParameters,
    queryOptions: undefined,
  };
}

/**
 * Encode statement arguments into the protocol's positional/named parameter shape.
 */
export function encodeSqlArgs(args: any[] | Record<string, any> = []): { args: Value[]; namedArgs: NamedArg[] } {
  let positionalArgs: Value[] = [];
  let namedArgs: NamedArg[] = [];

  if (Array.isArray(args)) {
    positionalArgs = args.map(encodeValue);
  } else {
    const keys = Object.keys(args);
    const isNumericKeys = keys.length > 0 && keys.every(key => /^\d+$/.test(key));

    if (isNumericKeys) {
      const sortedKeys = keys.sort((a, b) => parseInt(a, 10) - parseInt(b, 10));
      const maxIndex = parseInt(sortedKeys[sortedKeys.length - 1], 10);
      positionalArgs = new Array(maxIndex);

      for (const key of sortedKeys) {
        const index = parseInt(key, 10) - 1;
        positionalArgs[index] = encodeValue(args[key]);
      }

      for (let i = 0; i < positionalArgs.length; i++) {
        if (positionalArgs[i] === undefined) {
          positionalArgs[i] = { type: 'null' };
        }
      }
    } else {
      namedArgs = Object.entries(args).map(([name, value]) => ({
        name,
        value: encodeValue(value)
      }));
    }
  }

  return { args: positionalArgs, namedArgs };
}
