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

// Bind parameters to a statement.
//
// This function is used to bind parameters to a statement. It supports both
// named and positional parameters, and nested arrays.
//
// The `stmt` parameter is a statement object.
// The `params` parameter is an array of parameters.
//
// The function returns void.
export function bindParams(stmt, params) {
  const len = params?.length;
  if (len === 0) {
    return;
  }
  if (len === 1) {
    const param = params[0];    
    if (isPlainObject(param)) {  
      bindNamedParams(stmt, param);
      return;
    }
    if (Array.isArray(param)) {
      bindPositionalParams(stmt, [param]);
      return;
    }
    bindValue(stmt, 1, param);
    return;
  }
  bindPositionalParams(stmt, params);
}

// Check if object is plain (no prototype chain)
function isPlainObject(obj) {
  if (!obj || typeof obj !== 'object' || ArrayBuffer.isView(obj)) return false;
  const proto = Object.getPrototypeOf(obj);
  return proto === Object.prototype || proto === null;
}

// Handle named parameters
function bindNamedParams(stmt, paramObj) {
  const paramCount = stmt.parameterCount();
  
  for (let i = 1; i <= paramCount; i++) {
    const paramName = stmt.parameterName(i);
    if (paramName) {
      const key = paramName.substring(1); // Remove ':' or '$' prefix
      if (Object.prototype.hasOwnProperty.call(paramObj, key)) {
        bindValue(stmt, i, paramObj[key]);
      }
    }
  }
}

// Handle positional parameters (including nested arrays)
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
}

function bindValue(stmt, index, value) {
  stmt.bindAt(index, value);
}
