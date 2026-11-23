export function splitBindParameters(bindParameters) {
  if (bindParameters.length > 1 && isQueryOptions(bindParameters[bindParameters.length - 1])) {
    return {
      params: bindParameters.slice(0, -1),
      queryOptions: bindParameters[bindParameters.length - 1],
    };
  }
  return { params: bindParameters, queryOptions: undefined };
}

function isQueryOptions(value) {
  return value != null
    && typeof value === "object"
    && !Array.isArray(value)
    && Object.prototype.hasOwnProperty.call(value, "queryTimeout");
}

// Bind the values of one call to a statement. After `bind()`, every call runs
// with the bound values, and a call that passes its own throws, as in
// better-sqlite3. A call that passes only undefined, as
// `get(undefined, { queryTimeout })` does, passes none.
export function bindCallParams(stmt, params, bound) {
  if (!bound) {
    bindParams(stmt, params);
  } else if (params.flat().some((value) => value !== undefined)) {
    throw new TypeError('This statement already has bound parameters');
  }
}

// Bind parameters to a statement.
//
// This function is used to bind parameters to a statement. It supports both
// named and positional parameters, and nested arrays.
//
// The `stmt` parameter is a statement object.
// The `params` parameter is an array of parameters.
// The `allowMissing` parameter lets `params` leave parameters without a value.
// A statement keeps its values from one call to the next, so otherwise a
// missing value throws better-sqlite3's RangeError rather than running with
// the value of the previous call.
//
// The function returns void.
export function bindParams(stmt, params, allowMissing = false) {
  if (params.length === 1 && isPlainObject(params[0])) {
    bindNamedParams(stmt, params[0], allowMissing);
    return;
  }
  const count = bindPositionalParams(stmt, params);
  if (!allowMissing && count < stmt.parameterCount()) {
    throw new RangeError('Too few parameter values were provided');
  }
}

// Check if object is plain (no prototype chain)
function isPlainObject(obj) {
  if (!obj || typeof obj !== 'object' || ArrayBuffer.isView(obj)) return false;
  const proto = Object.getPrototypeOf(obj);
  return proto === Object.prototype || proto === null;
}

// Handle named parameters
function bindNamedParams(stmt, paramObj, allowMissing) {
  const paramCount = stmt.parameterCount();
  
  for (let i = 1; i <= paramCount; i++) {
    const paramName = stmt.parameterName(i);
    if (paramName) {
      const key = paramName.substring(1); // Remove ':' or '$' prefix
      if (Object.prototype.hasOwnProperty.call(paramObj, key)) {
        bindValue(stmt, i, paramObj[key]);
      } else if (!allowMissing) {
        throw new RangeError(`Missing named parameter "${key}"`);
      }
    } else if (!allowMissing) {
      throw new RangeError('Too few parameter values were provided');
    }
  }
}

// Handle positional parameters (including nested arrays), and return how many
// values were bound
function bindPositionalParams(stmt, params) {
  let bindIndex = 1;
  for (let i = 0; i < params.length; i++) {
    const param = params[i];    
    if (Array.isArray(param)) {
      for (let j = 0; j < param.length; j++) {
        bindValue(stmt, bindIndex++, param[j]);
      }
    } else {
      bindValue(stmt, bindIndex++, param);
    }
  }
  return bindIndex - 1;
}

function bindValue(stmt, index, value) {
  stmt.bindAt(index, value);
}
