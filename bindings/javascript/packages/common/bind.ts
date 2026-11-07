// Bind parameters to a statement.
//
// This function is used to bind parameters to a statement. It supports both
// named and positional parameters, and nested arrays.
//
// The `stmt` parameter is a statement object.
// The `params` parameter is an array of parameters.
//
// The function returns void.
export function bindParams(stmt, params, permanentlyBound = false) {
  const len = params.length;
  if (permanentlyBound && len === 0) {
    return;
  }
  const named = len === 1 && isPlainObject(params[0]);
  if (named) {
    for (let i = 1; i <= stmt.parameterCount(); i++) {
      const name = stmt.parameterName(i);
      if (!name || params[0][name.substring(1)] === undefined || !Object.prototype.hasOwnProperty.call(params[0], name.substring(1))) {
        throw new RangeError('Too few parameter values were provided');
      }
    }
  } else {
    const count = params.reduce((total, value) => total + (Array.isArray(value) ? value.length : 1), 0);
    if (count < stmt.parameterCount()) {
      throw new RangeError('Too few parameter values were provided');
    }
  }
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
  if (!obj || typeof obj !== 'object') return false;
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
      const value = paramObj[key];
      
      if (value !== undefined) {
        bindValue(stmt, i, value);
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
